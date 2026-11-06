"""Load the four ODS source files into MySQL and reconcile row counts (Phase 2).

Order: category_tree (small) -> events -> item_properties part1 -> part2.
Files whose SHA-256 is already LOADED in ``etl_batch_log`` are skipped
(idempotent). Each load auto-resumes across transient MySQL container restarts
(see ``docs/ods_design.md`` §3.1).

For every file the script:
1. streams the source CSV to count its real data rows (excluding the header),
2. loads it via the ODS loader,
3. reconciles ``SELECT COUNT(*)`` against the source row count.

Usage (project root, venv active):

    uv run python scripts/load_all_ods.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.chunk import CsvChunkReader
from ecommerce_dw.ingestion.db import connect
from ecommerce_dw.ingestion.specs import infer_table_for_file

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "retailrocket"
# The development box can reboot the WSL VM every few minutes; give the loader
# enough headroom so a single file survives many container restarts.
MAX_RESUMES = 100
RESUME_WAIT_SECONDS = 600

SOURCES = (
    "category_tree.csv",
    "events.csv",
    "item_properties_part1.csv",
    "item_properties_part2.csv",
)


def count_source_rows(path: Path) -> int:
    """Stream one CSV and return its data-row count (header excluded)."""
    reader = CsvChunkReader(path, chunk_size=100_000)
    for _ in reader.iter_chunks():
        pass
    return reader.n_data_rows


def count_db_rows(cfg, table: str, source_file: str) -> int:
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE source_file = %s", (source_file,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _cleanup_incomplete(cfg, source_file: str) -> None:
    """Mark stale PENDING/FAILED batches clean so a fresh load can start.

    A batch left PENDING/FAILED was interrupted by an unlucky crash between
    chunks; its committed rows carry the ``ingest_batch_id`` of that dead batch
    and would corrupt a fresh run's reconciliation, so wipe them first. LOADED
    batches are left untouched (the loader skips those by SHA-256).
    """
    conn = connect(cfg)
    try:
        entries = BatchLogRepo(conn).find_for_file(source_file)
    finally:
        conn.close()
    stale = [e for e in entries if e.status in ("PENDING", "FAILED")]
    for entry in stale:
        removed = loader.cleanup_batch(cfg, entry.batch_id)
        print(
            f"   cleaned stale {entry.status} batch #{entry.batch_id} "
            f"({removed:,} rows) for {source_file}"
        )


def _wait_ready(cfg, timeout: int = 60) -> None:
    """Wait until MySQL accepts connections (tolerates container restarts)."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            conn = connect(cfg, connect_timeout=5)
            conn.close()
            return
        except pymysql.err.OperationalError:
            if time.monotonic() >= deadline:
                print(f"   WARNING: MySQL not ready after {timeout}s")
                return
            time.sleep(3)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    errors: list[str] = []
    summary = []
    for name in SOURCES:
        path = DATA_DIR / name
        table = infer_table_for_file(name) or "?"
        expected = count_source_rows(path)
        print(f"\n== loading {name} -> {table} (source data rows: {expected:,})")
        _wait_ready(cfg)
        _cleanup_incomplete(cfg, name)
        try:
            result = loader.load_csv(
                cfg,
                path,
                table,
                max_resumes=MAX_RESUMES,
                resume_wait_seconds=RESUME_WAIT_SECONDS,
            )
        except loader.BatchStateError as exc:
            print(f"   BLOCKED: {exc}")
            errors.append(str(exc))
            continue
        except loader.LoadError as exc:
            print(f"   FAILED: {exc}")
            errors.append(str(exc))
            continue
        if result.skipped:
            print(f"   skipped: already loaded as batch #{result.batch_id}")
        else:
            print(
                f"   loaded: batch #{result.batch_id}, rows={result.rows_committed:,}, "
                f"duration={result.duration_seconds:.2f}s"
            )
        _wait_ready(cfg)
        actual = count_db_rows(cfg, table, name)
        ok = actual == expected
        print(f"   reconcile: DB={actual:,} vs source={expected:,} -> {'OK' if ok else 'MISMATCH'}")
        summary.append((name, expected, actual, ok))
        if not ok:
            errors.append(f"{name}: DB={actual:,} but source has {expected:,}")

    conn = connect(cfg)
    try:
        batches = BatchLogRepo(conn).list_recent(limit=20)
    finally:
        conn.close()
    print("\n== recent batches ==")
    for e in batches:
        print(
            f"   #{e.batch_id} {e.status:<8} rows={e.row_count:>12,} "
            f"{e.source_file} -> {e.table_name}"
        )

    print("\n== reconciliation ==")
    for name, expected, actual, ok in summary:
        print(
            f"   {name:<28} source={expected:>11,}  db={actual:>11,}  {'OK' if ok else 'MISMATCH'}"
        )

    if errors:
        print("\nRECONCILE FAILURES:")
        for err in errors:
            print(f"   {err}")
        return 2
    print("\nALL GOOD")
    return 0


if __name__ == "__main__":
    sys.exit(main())
