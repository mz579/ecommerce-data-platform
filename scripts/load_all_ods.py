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

import logging
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
# MySQL can be down for minutes at a time on this dev box, so a file's whole
# pipeline (cleanup -> load -> reconcile) is re-attempted until it completes.
FILE_PIPELINE_RETRIES = 20
_WAIT_READY_TIMEOUT = 900

SOURCES = (
    "category_tree.csv",
    "events.csv",
    "item_properties_part1.csv",
    "item_properties_part2.csv",
)

# Bigger chunks cut InnoDB commit/fsync overhead per short MySQL uptime window
# (the dev box reboots the VM every few minutes). 50k rows per chunk reduces
# commits 5x vs the 10k default while keeping memory well under control.
CHUNK_SIZE_BY_FILE = {
    "events.csv": 50_000,
    "item_properties_part1.csv": 50_000,
    "item_properties_part2.csv": 50_000,
}


def _setup_logging() -> None:
    """Route the loader's progress lines to console and logs/ods_ingestion.log."""
    handlers = [logging.StreamHandler(stream=sys.stdout)]
    log_dir = PROJECT_ROOT / "logs"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(
            logging.FileHandler(log_dir / "ods_ingestion.log", encoding="utf-8", delay=True)
        )
    except OSError:
        pass
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
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


def _cleanup_incomplete(cfg, source_file: str) -> bool:
    """Mark stale PENDING/FAILED batches clean so a fresh load can start.

    A batch left PENDING/FAILED was interrupted by an unlucky crash between
    chunks; its committed rows carry the ``ingest_batch_id`` of that dead batch
    and would corrupt a fresh run's reconciliation, so wipe them first. LOADED
    batches are left untouched (the loader skips those by SHA-256).

    Returns False when MySQL is unreachable (caller retries the pipeline).
    """
    conn = None
    try:
        conn = connect(cfg, connect_timeout=5)
    except pymysql.err.OperationalError:
        return False
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
    return True


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
    _setup_logging()
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    errors: list[str] = []
    summary = []
    for name in SOURCES:
        path = DATA_DIR / name
        table = infer_table_for_file(name) or "?"
        chunk_size = CHUNK_SIZE_BY_FILE.get(name, loader.DEFAULT_CHUNK_SIZE)
        expected = count_source_rows(path)
        print(
            f"\n== loading {name} -> {table} (source data rows: {expected:,}, chunk={chunk_size:,})"
        )
        result = None
        actual = None
        ok = False
        for attempt in range(1, FILE_PIPELINE_RETRIES + 1):
            try:
                if not _cleanup_incomplete(cfg, name):
                    raise pymysql.err.OperationalError(0, "mysql not reachable")
                result = loader.load_csv(
                    cfg,
                    path,
                    table,
                    chunk_size=chunk_size,
                    max_resumes=MAX_RESUMES,
                    resume_wait_seconds=RESUME_WAIT_SECONDS,
                )
                if not result.skipped:
                    actual = count_db_rows(cfg, table, name)
                    ok = actual == expected
                    print(
                        f"   reconcile: DB={actual:,} vs source={expected:,} "
                        f"-> {'OK' if ok else 'MISMATCH'}"
                    )
                else:
                    print(f"   skipped: already loaded as batch #{result.batch_id}")
                    ok = True
                break
            except pymysql.err.OperationalError as exc:
                if attempt >= FILE_PIPELINE_RETRIES:
                    errors.append(f"{name}: MySQL unreachable after {attempt} attempts: {exc}")
                    continue
                print(f"   DB hiccup (attempt {attempt}/{FILE_PIPELINE_RETRIES}): {exc}; retrying")
                _wait_ready(cfg, _WAIT_READY_TIMEOUT)
            except loader.BatchStateError as exc:
                print(f"   BLOCKED: {exc}")
                errors.append(str(exc))
                break
            except loader.LoadError as exc:
                print(f"   FAILED: {exc}")
                errors.append(str(exc))
                break
        if result is not None and not result.skipped:
            print(
                f"   loaded: batch #{result.batch_id}, rows={result.rows_committed:,}, "
                f"duration={result.duration_seconds:.2f}s"
            )
        summary.append((name, expected, actual, ok))
        if actual is not None and not ok:
            errors.append(f"{name}: DB={actual:,} but source has {expected:,}")

    print("\n== recent batches ==")
    try:
        _wait_ready(cfg, 300)
        conn = connect(cfg, connect_timeout=5)
        try:
            batches = BatchLogRepo(conn).list_recent(limit=20)
        finally:
            conn.close()
        for e in batches:
            print(
                f"   #{e.batch_id} {e.status:<8} rows={e.row_count:>12,} "
                f"{e.source_file} -> {e.table_name}"
            )
    except pymysql.err.OperationalError:
        print("   (MySQL unreachable right now; batch list omitted)")

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
