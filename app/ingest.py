"""Batch ingestion with content-addressed sources and replayable runs.

Key properties:

* The source bytes are captured by SHA-256 before parsing, so a later contract
  correction can replay exactly the same source even if the original path is
  gone.
* A run directory is keyed by (source digest, contract identity).  Re-submitting
  the same content - even under a different filename - returns the existing
  run.  A corrected contract has a different identity and creates a new run;
  the old run is never overwritten.
* Accepted and rejected records both retain the source digest, physical line
  span and raw field/record text.  Rejections are also partitioned by reason
  code under ``errors/``.
"""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from app.contract import FileContract
from app.numbering import normalize_number
from app.streaming import header_matches, iter_events, looks_like_header

MANIFEST_VERSION = 1
SOURCE_PREFIX = "sha256:"
_DEFAULT_RUNTIME = Path("runtime")
_FOREIGN_DELIMITERS = {";", "\t", "|"}


class IngestError(Exception):
    """Raised when an ingestion/replay request cannot be honoured."""


@dataclass(frozen=True)
class IngestResult:
    source_digest: str
    digest_hex: str
    run_id: str
    batch_dir: Path
    created: bool
    manifest: dict[str, Any]

    @property
    def duplicate(self) -> bool:
        return not self.created

    def summary(self) -> dict[str, Any]:
        manifest = self.manifest
        return {
            "source_digest": self.source_digest,
            "run_id": self.run_id,
            "batch_dir": str(self.batch_dir),
            "created": self.created,
            "duplicate": self.duplicate,
            "status": manifest.get("status"),
            "complete": manifest.get("complete"),
            "published": manifest.get("published"),
            "counts": manifest.get("counts"),
            "rejections": manifest.get("rejections"),
        }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json_line(stream: Any, payload: dict[str, Any]) -> None:
    stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    stream.write("\n")


class BatchStore:
    """Filesystem layout for immutable sources and replayable batch runs."""

    def __init__(self, root: str | Path = _DEFAULT_RUNTIME) -> None:
        self.root = Path(root)
        self.sources = self.root / "sources"
        self.batches = self.root / "batches"
        self.tmp = self.root / ".tmp"

    def source_hex(self, source_digest: str) -> str:
        value = source_digest.strip()
        if value.startswith(SOURCE_PREFIX):
            value = value[len(SOURCE_PREFIX) :]
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value.lower()):
            raise IngestError(f"invalid source digest: {source_digest!r}")
        return value.lower()

    def source_path(self, digest_hex: str) -> Path:
        return self.sources / f"{digest_hex}.source"

    def source_meta_path(self, digest_hex: str) -> Path:
        return self.sources / f"{digest_hex}.json"

    def batch_dir(self, digest_hex: str, contract: FileContract) -> Path:
        return self.batches / digest_hex / contract.run_key()

    def capture(self, path: str | Path) -> tuple[str, int]:
        """Copy source bytes into content-addressed storage while hashing.

        The caller's source file is only ever read, never modified.
        """

        source = Path(path)
        if not source.is_file():
            raise IngestError(f"source file not found: {source}")

        self.sources.mkdir(parents=True, exist_ok=True)
        self.tmp.mkdir(parents=True, exist_ok=True)
        temp_path = self.tmp / f"source-{uuid.uuid4().hex}.part"

        hasher = hashlib.sha256()
        size = 0
        try:
            with source.open("rb") as origin, temp_path.open("wb") as destination:
                while True:
                    chunk = origin.read(1024 * 1024)
                    if not chunk:
                        break
                    hasher.update(chunk)
                    destination.write(chunk)
                    size += len(chunk)

            digest_hex = hasher.hexdigest()
            target = self.source_path(digest_hex)
            if target.exists():
                temp_path.unlink()
            else:
                os.replace(temp_path, target)
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise

        self._record_alias(digest_hex, source.name)
        return digest_hex, size

    def _record_alias(self, digest_hex: str, filename: str) -> None:
        meta_path = self.source_meta_path(digest_hex)
        if meta_path.exists():
            with meta_path.open(encoding="utf-8") as stream:
                meta = json.load(stream)
        else:
            meta = {
                "source_digest": f"{SOURCE_PREFIX}{digest_hex}",
                "filenames": [],
                "first_seen": _now(),
            }
        if filename not in meta["filenames"]:
            meta["filenames"].append(filename)
        meta["last_seen"] = _now()
        with meta_path.open("w", encoding="utf-8", newline="\n") as stream:
            json.dump(meta, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")

    def source_meta(self, digest_hex: str) -> dict[str, Any]:
        meta_path = self.source_meta_path(digest_hex)
        if not meta_path.exists():
            return {
                "source_digest": f"{SOURCE_PREFIX}{digest_hex}",
                "filenames": [],
            }
        with meta_path.open(encoding="utf-8") as stream:
            return json.load(stream)

    def manifests(self) -> Iterator[dict[str, Any]]:
        if not self.batches.is_dir():
            return
        for digest_dir in sorted(self.batches.iterdir()):
            if not digest_dir.is_dir():
                continue
            for run_dir in sorted(digest_dir.iterdir()):
                manifest_path = run_dir / "manifest.json"
                if manifest_path.is_file():
                    with manifest_path.open(encoding="utf-8") as stream:
                        yield json.load(stream)


def _looks_like_format_shift(
    record: dict[str, Any], contract: FileContract
) -> bool:
    """Detect a row that most likely uses a different delimiter.

    A one-field row containing a common foreign delimiter is a strong signal
    that the file switched format, not that one data row is malformed.  Such a
    file is paused instead of rejecting rows and continuing under the old
    contract.
    """

    fields = record["fields"]
    if len(fields) != 1:
        return False
    line = record["raw_text"].rstrip("\r\n")
    return any(
        delimiter != contract.delimiter and delimiter in line
        for delimiter in _FOREIGN_DELIMITERS
    )


def _normalise_fields(
    fields: list[str], contract: FileContract
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    normalised: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []

    for column, raw in zip(contract.columns, fields):
        if column.kind == "text":
            value = raw.strip()
            if value == "":
                if column.rejects_empty:
                    errors.append(
                        {
                            "code": "missing_required",
                            "column": column.name,
                            "raw": raw,
                            "reason": f"必填列 {column.name} 为空",
                        }
                    )
                normalised[column.name] = {
                    "kind": "empty",
                    "raw": raw,
                    "value": None,
                }
            else:
                normalised[column.name] = {
                    "kind": "text",
                    "raw": raw,
                    "value": value,
                }
            continue

        number = normalize_number(
            raw,
            thousands_separator=contract.thousands_separator,
            decimal_separator=contract.decimal_separator,
        )
        normalised[column.name] = number.to_dict()

        if number.kind == "invalid":
            errors.append(
                {
                    "code": "invalid_number",
                    "column": column.name,
                    "raw": raw,
                    "reason": f"列 {column.name} 不是合法数字：{raw!r}（{number.detail}）",
                }
            )
            continue

        if number.kind == "null":
            if column.rejects_empty:
                errors.append(
                    {
                        "code": "missing_required",
                        "column": column.name,
                        "raw": raw,
                        "reason": f"必填列 {column.name} 为空",
                    }
                )
            continue

        if column.kind == "integer" and number.value is not None:
            if "." in number.value:
                errors.append(
                    {
                        "code": "not_integer",
                        "column": column.name,
                        "raw": raw,
                        "reason": f"列 {column.name} 必须是整数：{raw!r}",
                    }
                )

    return normalised, errors


def _base_record(
    source_digest: str, contract: FileContract, record: dict[str, Any]
) -> dict[str, Any]:
    return {
        "source_digest": source_digest,
        "line_start": record["line_start"],
        "line_end": record["line_end"],
        "raw_fields": record["fields"],
        "raw_text": record["raw_text"],
        "contract": {
            "name": contract.name,
            "version": contract.version,
            "fingerprint": contract.fingerprint(),
        },
    }


def _write_run(
    source_path: Path,
    source_digest: str,
    digest_hex: str,
    source_size: int,
    contract: FileContract,
    destination: Path,
    source_meta: dict[str, Any],
    chunk_size: int,
) -> dict[str, Any]:
    destination.mkdir(parents=True)
    errors_dir = destination / "errors"
    errors_dir.mkdir()

    accepted_path = destination / "accepted.jsonl"
    rejected_path = destination / "rejected.jsonl"
    skipped_path = destination / "skipped.jsonl"
    records_path = destination / "records.jsonl"
    audit_path = destination / "audit.jsonl"

    counts = {
        "physical_lines": 0,
        "comments": 0,
        "blank_lines": 0,
        "repeated_headers": 0,
        "data_records": 0,
        "accepted": 0,
        "rejected": 0,
    }
    rejections: Counter[str] = Counter()
    issues: list[dict[str, Any]] = []
    error_streams: dict[str, Any] = {}

    header_observed: list[str] | None = None
    seen_header = False
    format_issue: dict[str, Any] | None = None
    truncated = False
    max_line = 0

    def update_line(event: dict[str, Any]) -> None:
        nonlocal max_line
        sources = [event]
        if isinstance(event.get("record"), dict):
            sources.append(event["record"])
        for source in sources:
            for key in ("line", "line_start", "line_end"):
                value = source.get(key)
                if isinstance(value, int):
                    max_line = max(max_line, value)

    def add_issue(code: str, reason: str, event: dict[str, Any] | None = None, **extra: Any) -> None:
        issue = {"code": code, "reason": reason}
        if event is not None:
            update_line(event)
            issue.update(
                {
                    "line_start": event.get("line_start"),
                    "line_end": event.get("line_end"),
                    "raw_text": event.get("raw_text"),
                }
            )
        issue.update(extra)
        issues.append(issue)

    def write_rejection(
        payload: dict[str, Any],
        *,
        audit: Any,
        rejected: Any,
    ) -> None:
        code = payload["reason_code"]
        rejections[code] += 1
        counts["rejected"] += 1
        _write_json_line(rejected, payload)
        stream = error_streams.get(code)
        if stream is None:
            stream = (errors_dir / f"{code}.jsonl").open(
                "w", encoding="utf-8", newline="\n"
            )
            error_streams[code] = stream
        _write_json_line(stream, payload)
        _write_json_line(
            audit,
            {
                "event": "rejected",
                "reason_code": code,
                "reason": payload["reason"],
                "line_start": payload.get("line_start"),
                "line_end": payload.get("line_end"),
                "raw_text": payload.get("raw_text"),
            },
        )

    events = iter_events(source_path, contract, chunk_size)
    try:
        accepted = accepted_path.open("w", encoding="utf-8", newline="\n")
        rejected = rejected_path.open("w", encoding="utf-8", newline="\n")
        skipped = skipped_path.open("w", encoding="utf-8", newline="\n")
        records = records_path.open("w", encoding="utf-8", newline="\n")
        audit = audit_path.open("w", encoding="utf-8", newline="\n")
        streams = [accepted, rejected, skipped, records, audit]

        try:
            for event in events:
                kind = event["kind"]
                update_line(event)

                if kind == "comment":
                    counts["comments"] += 1
                    _write_json_line(
                        audit,
                        {
                            "event": "comment",
                            "line": event["line"],
                            "raw_text": event["raw_text"],
                        },
                    )
                    _write_json_line(
                        skipped,
                        {
                            "source_digest": source_digest,
                            "kind": "comment",
                            "line": event["line"],
                            "raw_text": event["raw_text"],
                        },
                    )
                    continue

                if kind == "blank":
                    counts["blank_lines"] += 1
                    _write_json_line(
                        skipped,
                        {
                            "source_digest": source_digest,
                            "kind": "blank",
                            "line_start": event.get("line_start"),
                            "line_end": event.get("line_end"),
                            "raw_text": event.get("raw_text"),
                        },
                    )
                    continue

                if kind == "parse_error":
                    counts["data_records"] += 1
                    truncated = True
                    payload = {
                        **_base_record(source_digest, contract, {
                            "line_start": event.get("line_start") or max_line + 1,
                            "line_end": event.get("line_end") or max_line,
                            "fields": [],
                            "raw_text": event.get("raw_text", ""),
                        }),
                        "outcome": "rejected",
                        "reason_code": "parse_error",
                        "reason": f"文件在记录结束前出现解析错误：{event['reason']}",
                    }
                    add_issue(
                        "parse_error",
                        f"文件在记录结束前出现解析错误：{event['reason']}",
                        event,
                    )
                    _write_json_line(records, payload)
                    write_rejection(payload, audit=audit, rejected=rejected)
                    break

                if kind == "decode_error":
                    format_issue = {
                        "code": "decode_error",
                        "reason": (
                            f"文件无法按契约声明的编码 {contract.encoding} 解码，"
                            "已暂停该文件"
                        ),
                    }
                    add_issue("decode_error", format_issue["reason"], event)
                    _write_json_line(
                        audit,
                        {
                            "event": "format_mismatch",
                            **format_issue,
                            "line_start": event.get("line_start"),
                            "line_end": event.get("line_end"),
                            "raw_text": event.get("raw_text"),
                        },
                    )
                    break

                record = event["record"]
                if not seen_header:
                    header_observed = list(record["fields"])
                    if header_matches(record["fields"], contract):
                        seen_header = True
                        _write_json_line(
                            audit,
                            {
                                "event": "header",
                                "status": "matched",
                                "line_start": record["line_start"],
                                "line_end": record["line_end"],
                                "raw_text": record["raw_text"],
                                "columns": header_observed,
                            },
                        )
                        continue
                    format_issue = {
                        "code": "header_mismatch",
                        "reason": "文件表头与所选契约不一致，已暂停该文件",
                        "expected": list(contract.expected_header),
                        "observed": header_observed,
                    }
                    add_issue(
                        "header_mismatch",
                        format_issue["reason"],
                        {
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                        expected=list(contract.expected_header),
                        observed=header_observed,
                    )
                    _write_json_line(
                        audit,
                        {
                            "event": "format_mismatch",
                            **format_issue,
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                    )
                    break

                if header_matches(record["fields"], contract):
                    counts["repeated_headers"] += 1
                    _write_json_line(
                        audit,
                        {
                            "event": "repeated_header",
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                    )
                    _write_json_line(
                        skipped,
                        {
                            "source_digest": source_digest,
                            "kind": "repeated_header",
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                    )
                    continue

                if looks_like_header(record["fields"], contract):
                    format_issue = {
                        "code": "header_change",
                        "reason": "文件中段出现不同表头，格式指纹已变化，已暂停该文件",
                        "expected": list(contract.expected_header),
                        "observed": list(record["fields"]),
                    }
                    add_issue(
                        "header_change",
                        format_issue["reason"],
                        record,
                        expected=list(contract.expected_header),
                        observed=list(record["fields"]),
                    )
                    _write_json_line(
                        audit,
                        {
                            "event": "format_mismatch",
                            **format_issue,
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                    )
                    break

                if _looks_like_format_shift(record, contract):
                    format_issue = {
                        "code": "delimiter_change",
                        "reason": "记录使用与契约不同的分隔符，格式指纹已变化，已暂停该文件",
                        "expected": contract.delimiter,
                        "observed": record["raw_text"].rstrip("\r\n"),
                    }
                    add_issue("delimiter_change", format_issue["reason"], record)
                    _write_json_line(
                        audit,
                        {
                            "event": "format_mismatch",
                            **format_issue,
                            "line_start": record["line_start"],
                            "line_end": record["line_end"],
                            "raw_text": record["raw_text"],
                        },
                    )
                    break

                counts["data_records"] += 1

                if record["truncated"]:
                    truncated = True
                    payload = {
                        **_base_record(source_digest, contract, record),
                        "outcome": "rejected",
                        "reason_code": "truncated_quoted_field",
                        "reason": "文件在引号字段内结束，该记录不完整；文件已暂停发布",
                    }
                    add_issue(
                        "truncated_quoted_field",
                        payload["reason"],
                        record,
                    )
                    _write_json_line(records, payload)
                    write_rejection(payload, audit=audit, rejected=rejected)
                    break

                if len(record["fields"]) != len(contract.columns):
                    payload = {
                        **_base_record(source_digest, contract, record),
                        "outcome": "rejected",
                        "reason_code": "column_count_mismatch",
                        "reason": (
                            f"列数为 {len(record['fields'])}，契约要求 "
                            f"{len(contract.columns)} 列"
                        ),
                    }
                    _write_json_line(records, payload)
                    write_rejection(payload, audit=audit, rejected=rejected)
                    continue

                normalised, errors = _normalise_fields(record["fields"], contract)
                if errors:
                    payload = {
                        **_base_record(source_digest, contract, record),
                        "outcome": "rejected",
                        "reason_code": errors[0]["code"],
                        "reason": "；".join(error["reason"] for error in errors),
                        "errors": errors,
                        "normalized": normalised,
                    }
                    _write_json_line(records, payload)
                    write_rejection(payload, audit=audit, rejected=rejected)
                    continue

                payload = {
                    **_base_record(source_digest, contract, record),
                    "outcome": "accepted",
                    "normalized": normalised,
                }
                counts["accepted"] += 1
                _write_json_line(records, payload)
                _write_json_line(accepted, payload)

            if not seen_header and format_issue is None:
                add_issue("no_header", "文件中没有找到与契约一致的表头，已暂停该文件")
                _write_json_line(
                    audit,
                    {"event": "format_mismatch", **issues[-1]},
                )
        finally:
            for stream in streams:
                stream.close()
            for stream in error_streams.values():
                stream.close()
    finally:
        events.close()

    counts["physical_lines"] = max_line

    if format_issue is not None or not seen_header:
        status = "quarantined"
        complete = False
        published = False
    elif truncated:
        status = "incomplete"
        complete = False
        published = False
    elif counts["rejected"] > 0:
        status = "partial"
        complete = False
        published = contract.row_error_policy == "publish_valid"
    else:
        status = "complete"
        complete = True
        published = True

    manifest = {
        "manifest_version": MANIFEST_VERSION,
        "run_id": contract.run_key(),
        "source_digest": source_digest,
        "source": {
            "bytes": source_size,
            "filenames": list(source_meta.get("filenames", [])),
            "sha256": digest_hex,
        },
        "contract": contract.describe(),
        "status": status,
        "complete": complete,
        "published": published,
        "counts": counts,
        "rejections": dict(sorted(rejections.items())),
        "format": {
            "expected_header": list(contract.expected_header),
            "observed_header": header_observed,
        },
        "issues": issues,
        "created_at": _now(),
    }

    with audit_path.open("a", encoding="utf-8", newline="\n") as audit_append:
        _write_json_line(
            audit_append,
            {
                "event": "batch_decided",
                "status": status,
                "complete": complete,
                "published": published,
                "counts": counts,
            },
        )
    with (destination / "manifest.json").open(
        "w", encoding="utf-8", newline="\n"
    ) as stream:
        json.dump(manifest, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")

    return manifest


def ingest_file(
    path: str | Path,
    contract: FileContract,
    store: BatchStore | None = None,
    chunk_size: int = 64 * 1024,
) -> IngestResult:
    """Capture and ingest one source file under a declared contract."""

    store = store or BatchStore()
    digest_hex, source_size = store.capture(path)
    return _process_digest(
        digest_hex,
        source_size,
        contract,
        store,
        chunk_size,
    )


def _process_digest(
    digest_hex: str,
    source_size: int,
    contract: FileContract,
    store: BatchStore,
    chunk_size: int,
) -> IngestResult:
    source_digest = f"{SOURCE_PREFIX}{digest_hex}"
    source_path = store.source_path(digest_hex)
    if not source_path.is_file():
        raise IngestError(f"captured source is missing for digest {source_digest}")

    final_dir = store.batch_dir(digest_hex, contract)
    manifest_path = final_dir / "manifest.json"
    if manifest_path.is_file():
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
        return IngestResult(
            source_digest=source_digest,
            digest_hex=digest_hex,
            run_id=contract.run_key(),
            batch_dir=final_dir,
            created=False,
            manifest=manifest,
        )
    if final_dir.exists():
        raise IngestError(f"run directory exists but has no manifest: {final_dir}")

    store.tmp.mkdir(parents=True, exist_ok=True)
    temp_dir = store.tmp / f"run-{uuid.uuid4().hex}"
    try:
        manifest = _write_run(
            source_path=source_path,
            source_digest=source_digest,
            digest_hex=digest_hex,
            source_size=source_size,
            contract=contract,
            destination=temp_dir,
            source_meta=store.source_meta(digest_hex),
            chunk_size=chunk_size,
        )
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.rename(temp_dir, final_dir)
        except OSError as error:
            if error.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                raise
            shutil.rmtree(temp_dir, ignore_errors=True)
            with manifest_path.open(encoding="utf-8") as stream:
                manifest = json.load(stream)
            return IngestResult(
                source_digest=source_digest,
                digest_hex=digest_hex,
                run_id=contract.run_key(),
                batch_dir=final_dir,
                created=False,
                manifest=manifest,
            )
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    return IngestResult(
        source_digest=source_digest,
        digest_hex=digest_hex,
        run_id=contract.run_key(),
        batch_dir=final_dir,
        created=True,
        manifest=manifest,
    )


def reprocess(
    source_digest: str,
    contract: FileContract,
    store: BatchStore | None = None,
    chunk_size: int = 64 * 1024,
) -> IngestResult:
    """Replay a captured source under a (possibly corrected) contract.

    The new run is separate from every earlier run for the same source.
    """

    store = store or BatchStore()
    digest_hex = store.source_hex(source_digest)
    source_path = store.source_path(digest_hex)
    if not source_path.is_file():
        raise IngestError(f"no captured source for digest {source_digest!r}")
    return _process_digest(
        digest_hex,
        source_path.stat().st_size,
        contract,
        store,
        chunk_size,
    )


def reprocess_run(
    run_dir: str | Path,
    contract: FileContract,
    store: BatchStore | None = None,
    chunk_size: int = 64 * 1024,
) -> IngestResult:
    manifest_path = Path(run_dir) / "manifest.json"
    if not manifest_path.is_file():
        raise IngestError(f"run manifest not found: {manifest_path}")
    with manifest_path.open(encoding="utf-8") as stream:
        manifest = json.load(stream)
    return reprocess(manifest["source_digest"], contract, store, chunk_size)
