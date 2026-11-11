"""End-to-end demonstration using only synthetic fixture data.

Run with ``python3 -m app.demo``.  The demonstration runtime is created under
``runtime/demo`` so the audit files can be inspected after the run.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from app.contract import FileContract
from app.ingest import BatchStore, ingest_file, reprocess

FIXTURES = Path("fixtures")
RUNTIME = Path("runtime/demo")
STORE_ROOT = RUNTIME / "store"

COMMA = FileContract.load("configs/weekly_comma.json")
SEMICOLON = FileContract.load("configs/weekly_semicolon.json")
SINGLE_BYTE = FileContract.load("configs/weekly_single_byte.json")
SUPPLIER = FileContract.load("configs/weekly_supplier.json")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]


def _show(result, note: str = "") -> None:
    summary = result.summary()
    print(
        f"- {note or Path(result.batch_dir).name}: "
        f"status={summary['status']} complete={summary['complete']} "
        f"published={summary['published']} created={summary['created']} "
        f"counts={summary['counts']} rejections={summary['rejections']}"
    )


def main() -> None:
    if RUNTIME.exists():
        shutil.rmtree(RUNTIME)
    RUNTIME.mkdir(parents=True)
    store = BatchStore(STORE_ROOT)

    print("== 1. comma, explicit zero and thousands separator ==")
    weekly = ingest_file(FIXTURES / "weekly.csv", COMMA, store)
    _show(weekly, "weekly.csv")
    for record in _read_jsonl(weekly.batch_dir / "accepted.jsonl"):
        quantity = record["normalized"]["quantity"]
        print(
            f"  line {record['line_start']}: raw={quantity['raw']!r} "
            f"kind={quantity['kind']} value={quantity['value']} "
            f"thousands={quantity['has_thousands_separator']}"
        )

    print("\n== 2. UTF-8 BOM ==")
    bom = ingest_file(FIXTURES / "bom.csv", COMMA, store)
    _show(bom, "bom.csv")
    print(f"  observed header={bom.manifest['format']['observed_header']}")

    print("\n== 3. semicolon delimiter and a newline inside a quoted field ==")
    quoted = ingest_file(FIXTURES / "semicolon.csv", SEMICOLON, store)
    _show(quoted, "semicolon.csv")
    for record in _read_jsonl(quoted.batch_dir / "rejected.jsonl"):
        print(
            f"  rejected {record['reason_code']} lines "
            f"{record['line_start']}-{record['line_end']}: "
            f"raw_fields={record['raw_fields']} reason={record['reason']}"
        )

    print("\n== 4. comment line and repeated header in the middle ==")
    notes = ingest_file(FIXTURES / "notes.csv", COMMA, store)
    _show(notes, "notes.csv")
    print(
        f"  comments={notes.manifest['counts']['comments']} "
        f"repeated_headers={notes.manifest['counts']['repeated_headers']} "
        f"accepted={notes.manifest['counts']['accepted']}"
    )

    print("\n== 5. truncated file (open quoted field at EOF) ==")
    truncated = ingest_file(FIXTURES / "truncated.csv", COMMA, store)
    _show(truncated, "truncated.csv")
    for record in _read_jsonl(truncated.batch_dir / "rejected.jsonl"):
        print(
            f"  rejected {record['reason_code']} lines "
            f"{record['line_start']}-{record['line_end']} raw_text={record['raw_text']!r}"
        )
    print("  earlier accepted rows are retained for review but the batch is not published")

    print("\n== 6. format fingerprint change pauses the file ==")
    changed = ingest_file(FIXTURES / "format_change.csv", COMMA, store)
    _show(changed, "format_change.csv")
    issue = changed.manifest["issues"][0]
    print(
        f"  issue={issue['code']} at line {issue['line_start']}: "
        f"expected={issue['expected']} observed={issue['observed']}"
    )

    print("\n== 7. local single-byte encoding declared by contract ==")
    single = ingest_file(FIXTURES / "single_byte_cp1252.csv", SINGLE_BYTE, store)
    _show(single, "single_byte_cp1252.csv (cp1252)")
    print(
        "  district values="
        + repr(
            [
                record["normalized"]["district"]["value"]
                for record in _read_jsonl(single.batch_dir / "accepted.jsonl")
            ]
        )
    )

    print("\n== 8. empty supplier value is allowed ==")
    supplier = ingest_file(FIXTURES / "supplier_empty.csv", SUPPLIER, store)
    _show(supplier, "supplier_empty.csv")
    for record in _read_jsonl(supplier.batch_dir / "accepted.jsonl"):
        print(
            f"  line {record['line_start']} supplier kind="
            f"{record['normalized']['supplier']['kind']}"
        )

    print("\n== 9. idempotency: same content, same or different filename ==")
    again = ingest_file(FIXTURES / "weekly.csv", COMMA, store)
    _show(again, "weekly.csv resubmitted")
    renamed = RUNTIME / "weekly_renamed.csv"
    shutil.copyfile(FIXTURES / "weekly.csv", renamed)
    renamed_result = ingest_file(renamed, COMMA, store)
    _show(renamed_result, "same bytes, different filename")
    print(
        f"  same run directory={weekly.batch_dir == renamed_result.batch_dir} "
        f"created={renamed_result.created} duplicate={renamed_result.duplicate}"
    )

    print("\n== 10. contract correction replayed from the captured original ==")
    wrong = ingest_file(FIXTURES / "semicolon_ok.csv", COMMA, store)
    _show(wrong, "semicolon_ok.csv under comma contract (wrong)")
    replay = reprocess(wrong.source_digest, SEMICOLON, store)
    _show(replay, "reprocessed under corrected semicolon contract")
    old_manifest_path = wrong.batch_dir / "manifest.json"
    with old_manifest_path.open(encoding="utf-8") as stream:
        old_manifest = json.load(stream)
    print(
        f"  old run preserved={old_manifest_path.exists()} "
        f"old status={old_manifest['status']} old published={old_manifest['published']}"
    )
    print(f"  old run={wrong.batch_dir}")
    print(f"  new run={replay.batch_dir}")
    print(f"  new run observed header={replay.manifest['format']['observed_header']}")

    print("\n== run inventory ==")
    for manifest in store.manifests():
        print(
            f"  {manifest['source_digest'][:19]}... {manifest['run_id']}: "
            f"{manifest['status']} complete={manifest['complete']} "
            f"published={manifest['published']}"
        )


if __name__ == "__main__":
    main()
