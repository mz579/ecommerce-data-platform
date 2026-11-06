"""Apply the DWD DDL files to the ecommerce_dw MySQL database (Phase 3).

The SQL files matching ``sql/ddl/dwd_*.sql`` are all `CREATE TABLE IF NOT
EXISTS`, so this script is idempotent and safe to re-run.

Usage (project root, venv active):

    python scripts/apply_dwd_ddl.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

EXPECTED_TABLES = (
    "dwd_events",
    "dwd_events_duplicate",
    "dwd_events_reject",
    "dwd_events_stage",
    "dwd_build_log",
)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    scripts = sorted((PROJECT_ROOT / "sql" / "ddl").glob("dwd_*.sql"))
    if not scripts:
        print("no sql/ddl/dwd_*.sql files found", file=sys.stderr)
        return 2

    print("applying DWD DDL...")
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        for path in scripts:
            statement = path.read_text(encoding="utf-8").strip().rstrip(";")
            with conn.cursor() as cur:
                cur.execute(statement)
            conn.commit()
            print(f"  applied {path.name}")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name IN (%s, %s, %s, %s, %s) "
                "ORDER BY table_name",
                (cfg.database, *EXPECTED_TABLES),
            )
            found = [row[0] for row in cur.fetchall()]
        print(f"tables present: {sorted(found)}")
        missing = set(EXPECTED_TABLES) - set(found)
        if missing:
            print(f"missing: {sorted(missing)}", file=sys.stderr)
            return 2
    finally:
        conn.close()
    print("DWD DDL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
