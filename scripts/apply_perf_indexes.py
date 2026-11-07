"""Apply the Phase 8 performance indexes to an existing MySQL warehouse.

``sql/ddl/*.sql`` uses ``CREATE TABLE IF NOT EXISTS``, so an already-provisioned
database never receives indexes added later. This script closes that gap for the
five indexes introduced in Phase 8 and is fully idempotent (it checks
``information_schema.statistics`` by index name first).

Measured effect on the production dataset (2.76M ODS events, 20.28M item
properties, 2.76M DWD events; MySQL 8.0.43 / WSL2):

===========================  ==================  =============
index                        query               before -> after
===========================  ==================  =============
ods_item_properties property DWS category as-of  62.3s -> 13.2s
ods_* file_batch             gate batch GROUP BY 21.7s -> 3.9s
dwd_events_stage build       per-split scan      20.3s -> 0.19s*
===========================  ==================  =============

``*`` the DWD split win also needs the SQL rewrite in ``dwd/builder.py`` (join
``dwd_events`` by its UNIQUE ``dedup_key`` instead of materialising DISTINCT).

Rejected candidate: a functional index on
``(source_file, CAST(`timestamp` AS UNSIGNED))`` cut a DWD day-scope ODS read
from 18.8s to 0.08s, but made INSERTs of the legal raw row ``timestamp='oops'``
fail with MySQL error 3751 ("Data truncated for functional index"). The ODS contract
keeps dirty rows for DWD rejection, so the index was dropped.

Usage (project root, venv active)::

    python scripts/apply_perf_indexes.py            # ecommerce_dw
    python scripts/apply_perf_indexes.py --dry-run
    MYSQL_DATABASE=ecommerce_dw_test python scripts/apply_perf_indexes.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

# (table, index name, index columns/expressions, extra ALTER clause)
PERF_INDEXES: tuple[tuple[str, str, str, str], ...] = (
    (
        "ods_events_raw",
        "idx_ods_events_raw_file_batch",
        "source_file, ingest_batch_id",
        "ALGORITHM=INPLACE, LOCK=NONE",
    ),
    (
        "ods_item_properties_raw",
        "idx_ods_ip_raw_property",
        "property",
        "ALGORITHM=INPLACE, LOCK=NONE",
    ),
    (
        "ods_item_properties_raw",
        "idx_ods_ip_raw_file_batch",
        "source_file, ingest_batch_id",
        "ALGORITHM=INPLACE, LOCK=NONE",
    ),
    (
        "ods_category_tree_raw",
        "idx_ods_ct_raw_file_batch",
        "source_file, ingest_batch_id",
        "ALGORITHM=INPLACE, LOCK=NONE",
    ),
    (
        "dwd_events_stage",
        "idx_dwd_stage_build",
        "build_id, record_status",
        "ALGORITHM=INPLACE, LOCK=NONE",
    ),
)


def _existing_indexes(cur, database: str) -> dict[str, set[str]]:
    cur.execute(
        "SELECT TABLE_NAME, INDEX_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA = %s",
        (database,),
    )
    found: dict[str, set[str]] = {}
    for table, index in cur.fetchall():
        found.setdefault(table, set()).add(index)
    return found


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="only report missing indexes")
    args = parser.parse_args()

    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            existing = _existing_indexes(cur, cfg.database)
            missing = [spec for spec in PERF_INDEXES if spec[1] not in existing.get(spec[0], set())]
            print(
                f"database={cfg.database} perf indexes: {len(PERF_INDEXES)} total, "
                f"{len(missing)} missing"
            )
            if args.dry_run or not missing:
                for table, name, cols, _ in missing:
                    print(f"  MISSING {table}.{name} ({cols})")
                return 0
            for table, name, cols, clause in missing:
                print(f"  adding {table}.{name} ({cols}) ...", flush=True)
                cur.execute(f"ALTER TABLE `{table}` ADD INDEX `{name}` ({cols}), {clause}")
                print(f"  added  {table}.{name}")
            existing = _existing_indexes(cur, cfg.database)
            absent = [
                f"{table}.{name}"
                for table, name, _, _ in PERF_INDEXES
                if name not in existing.get(table, set())
            ]
            if absent:
                print(f"still missing after ALTER: {absent}", file=sys.stderr)
                return 2
            print("all Phase 8 performance indexes present")
            return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
