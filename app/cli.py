"""Command line entry points for ingestion and replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from app.contract import FileContract
from app.ingest import BatchStore, ingest_file, reprocess, reprocess_run


def _print(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Ingest and replay delimited supply reports under an explicit contract.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser("ingest", help="ingest a source file")
    ingest.add_argument("file")
    ingest.add_argument("--contract", required=True)
    ingest.add_argument("--store", default="runtime")
    ingest.add_argument("--chunk-size", type=int, default=64 * 1024)

    replay = subparsers.add_parser(
        "reprocess",
        help="replay a captured source under a corrected contract",
    )
    replay.add_argument("--digest", help="sha256 digest, with or without 'sha256:' prefix")
    replay.add_argument("--run", help="path to an existing run directory")
    replay.add_argument("--contract", required=True)
    replay.add_argument("--store", default="runtime")
    replay.add_argument("--chunk-size", type=int, default=64 * 1024)

    listing = subparsers.add_parser("list", help="list known batch manifests")
    listing.add_argument("--store", default="runtime")

    show = subparsers.add_parser("show", help="print a run manifest")
    show.add_argument("--run", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "ingest":
        contract = FileContract.load(args.contract)
        result = ingest_file(
            args.file,
            contract,
            BatchStore(args.store),
            chunk_size=args.chunk_size,
        )
        _print(result.summary())
        return 0

    if args.command == "reprocess":
        contract = FileContract.load(args.contract)
        if bool(args.digest) == bool(args.run):
            parser.error("provide exactly one of --digest or --run")
        if args.run:
            result = reprocess_run(
                args.run,
                contract,
                BatchStore(args.store),
                chunk_size=args.chunk_size,
            )
        else:
            result = reprocess(
                args.digest,
                contract,
                BatchStore(args.store),
                chunk_size=args.chunk_size,
            )
        _print(result.summary())
        return 0

    if args.command == "list":
        store = BatchStore(args.store)
        runs = [
            {
                "run_id": manifest.get("run_id"),
                "source_digest": manifest.get("source_digest"),
                "status": manifest.get("status"),
                "complete": manifest.get("complete"),
                "published": manifest.get("published"),
            }
            for manifest in store.manifests()
        ]
        _print({"runs": runs})
        return 0

    if args.command == "show":
        manifest_path = Path(args.run) / "manifest.json"
        with manifest_path.open(encoding="utf-8") as stream:
            _print(json.load(stream))
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
