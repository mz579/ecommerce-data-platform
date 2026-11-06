"""Command-line entry point for ODS ingestion (Phase 2).

Examples (run from the project root, venv active):

    python -m ecommerce_dw.ingestion.cli inspect data/raw/retailrocket/events.csv
    python -m ecommerce_dw.ingestion.cli load data/raw/retailrocket/events.csv
    python -m ecommerce_dw.ingestion.cli load data/raw/retailrocket/item_properties_part1.csv
    python -m ecommerce_dw.ingestion.cli load data/raw/retailrocket/item_properties_part2.csv
    python -m ecommerce_dw.ingestion.cli status
    python -m ecommerce_dw.ingestion.cli cleanup --batch 7
    python -m ecommerce_dw.ingestion.cli reconcile

Credentials come from environment variables / the project .env only; passwords
never appear in logs or stdout.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.chunk import CsvChunkReader, read_csv_header
from ecommerce_dw.ingestion.db import connect
from ecommerce_dw.ingestion.keys import sha256_file
from ecommerce_dw.ingestion.specs import (
    TABLE_SPECS,
    infer_table_for_file,
    ods_table_names,
    table_spec,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

log = logging.getLogger("ecommerce_dw.ingestion")


def _build_config():
    return db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)


def _setup_logging(verbose: bool) -> None:
    root = logging.getLogger()
    if root.handlers:
        return  # pytest / callers already configured the root logger
    root.setLevel(logging.DEBUG if verbose else logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)
    log_dir = PROJECT_ROOT / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / "ods_ingestion.log", encoding="utf-8")
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    except OSError:  # logs/ unusable -> keep console-only
        pass


def _cmd_inspect(args: argparse.Namespace) -> None:
    path = Path(args.file).resolve()
    if not path.is_file():
        raise ValueError(f"file not found: {path}")
    header = read_csv_header(path)
    reader = CsvChunkReader(path, chunk_size=args.chunk_size)
    _ = [chunk for chunk in reader.iter_chunks()]
    size = path.stat().st_size
    print(f"path          : {path}")
    print(f"bytes         : {size:,}")
    print(f"sha256        : {sha256_file(path)}")
    print(f"data_rows     : {reader.n_data_rows:,}")
    print(f"malformed_rows: {reader.n_malformed_rows}")
    print(f"header        : {list(header)}")


def _cmd_load(args: argparse.Namespace) -> None:
    path = Path(args.file).resolve()
    if not path.is_file():
        raise ValueError(f"file not found: {path}")
    table = args.table if args.table != "auto" else infer_table_for_file(path.name)
    if table is None:
        raise ValueError(
            f"cannot infer a table for {path.name!r}; pass --table with one of "
            f"{sorted(TABLE_SPECS)}"
        )
    result = loader.load_csv(
        _build_config(),
        path,
        table,
        chunk_size=args.chunk_size,
        force=args.force,
        max_resumes=args.max_resumes,
        resume_wait_seconds=args.resume_wait,
    )
    if result.skipped:
        print(
            f"skipped   : {result.source_file} (sha256={result.file_sha256[:16]}...) "
            f"is already loaded as batch #{result.batch_id}; use --force to reload"
        )
    else:
        print(
            f"loaded    : {result.table_name} <- {result.source_file} "
            f"(batch=#{result.batch_id}, rows={result.rows_committed:,}, "
            f"duration={result.duration_seconds:.2f}s, sha256={result.file_sha256[:16]}...)"
        )


def _cmd_status(args: argparse.Namespace) -> None:
    conn = connect(_build_config())
    try:
        entries = BatchLogRepo(conn).list_recent(limit=args.limit)
    finally:
        conn.close()
    if not entries:
        print("no batches recorded yet")
        return
    print(f"{'batch_id':>8}  {'status':<8}  {'rows':>10}  {'started_at':<23}  {'file'}")
    for e in entries:
        started = e.started_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        print(
            f"{e.batch_id:>8}  {e.status:<8}  {e.row_count:>10,}  {started}  "
            f"{e.source_file}  ->  {e.table_name}"
        )


def _cmd_cleanup(args: argparse.Namespace) -> None:
    cfg = _build_config()
    if args.batch is not None:
        deleted = loader.cleanup_batch(cfg, args.batch)
        print(f"cleaned batch #{args.batch}: removed {deleted:,} rows")
    else:
        deleted = loader.cleanup_file(cfg, args.file)
        print(f"cleaned file {args.file!r}: removed {deleted:,} rows")


def _cmd_reconcile(args: argparse.Namespace) -> None:
    conn = connect(_build_config())
    try:
        cursor = conn.cursor()
        tables = [args.table] if args.table else list(ods_table_names())
        for table in tables:
            table_spec(table)  # validates name
            cursor.execute(f"SELECT source_file, COUNT(*) FROM `{table}` GROUP BY source_file")
            rows = cursor.fetchall()
            total = sum(int(r[1]) for r in rows)
            print(f"== {table}  total={total:,}")
            for source_file, count in rows:
                print(f"   {source_file:<45} {int(count):>12,}")
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ecommerce-dw-ods",
        description="ODS ingestion: chunked, idempotent CSV -> MySQL loader with batch tracking.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    sub = parser.add_subparsers(dest="command", required=True)

    p_inspect = sub.add_parser(
        "inspect", help="print file metadata (bytes, sha256, row count, header)"
    )
    p_inspect.add_argument("file", help="path to a CSV file")
    p_inspect.add_argument("--chunk-size", type=int, default=100_000)

    p_load = sub.add_parser("load", help="load a CSV file into an ODS raw table")
    p_load.add_argument("file", help="path to a CSV file")
    p_load.add_argument(
        "--table",
        choices=sorted(TABLE_SPECS) + ["auto"],
        default="auto",
        help="target ODS table (default: infer from filename)",
    )
    p_load.add_argument("--chunk-size", type=int, default=loader.DEFAULT_CHUNK_SIZE)
    p_load.add_argument(
        "--max-resumes",
        type=int,
        default=loader.DEFAULT_MAX_RESUMES,
        help="retry loop count if MySQL restarts mid-load (0 disables auto-resume)",
    )
    p_load.add_argument(
        "--resume-wait",
        type=int,
        default=loader.DEFAULT_RESUME_WAIT_SECONDS,
        help="seconds to wait for MySQL to come back per resume attempt",
    )
    p_load.add_argument(
        "--force",
        action="store_true",
        help="delete the file's existing rows and reload as a fresh batch",
    )

    p_status = sub.add_parser("status", help="show recent batch log entries")
    p_status.add_argument("--limit", type=int, default=20)

    p_cleanup = sub.add_parser("cleanup", help="wipe a failed/incomplete batch and mark it CLEANED")
    group = p_cleanup.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", type=int, help="batch_id to clean")
    group.add_argument("--file", help="source file name whose batches to clean")

    p_reconcile = sub.add_parser(
        "reconcile",
        help="print per-source-file row counts of ODS tables (against the source files)",
    )
    p_reconcile.add_argument("--table", choices=sorted(TABLE_SPECS), help="only check one table")

    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    try:
        if args.command == "inspect":
            _cmd_inspect(args)
        elif args.command == "load":
            _cmd_load(args)
        elif args.command == "status":
            _cmd_status(args)
        elif args.command == "cleanup":
            _cmd_cleanup(args)
        elif args.command == "reconcile":
            _cmd_reconcile(args)
        return 0
    except (loader.LoadError, loader.BatchStateError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        log.error("%s", exc)
        return 2
    except Exception as exc:
        print(f"unexpected error: {exc}", file=sys.stderr)
        log.error("unexpected failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
