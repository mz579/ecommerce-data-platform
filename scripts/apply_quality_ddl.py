"""Apply the Phase 5 quality/incremental DDL files to ecommerce_dw (or test DB).

Creates ``etl_watermark``, ``quality_check_run``, ``quality_check_result`` and
``ads_publish_log``. All four are `CREATE TABLE IF NOT EXISTS`, so this script is
idempotent and safe to re-run.

Usage (project root, venv active):

    python scripts/apply_quality_ddl.py                 # ecommerce_dw
    MYSQL_DATABASE=ecommerce_dw_test python scripts/apply_quality_ddl.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

CONTROL_TABLES = (
    "etl_watermark",
    "quality_check_run",
    "quality_check_result",
    "ads_publish_log",
)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    ddl_dir = PROJECT_ROOT / "sql" / "ddl"
    scripts = [ddl_dir / f"{name}.sql" for name in CONTROL_TABLES]
    missing = [p for p in scripts if not p.is_file()]
    if missing:
        print(f"missing DDL files: {[p.name for p in missing]}", file=sys.stderr)
        return 2

    print(f"applying phase-5 DDL to database {cfg.database}...")
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
                "WHERE table_schema = %s AND table_name IN (%s, %s, %s, %s) "
                "ORDER BY table_name",
                (cfg.database, *CONTROL_TABLES),
            )
            found = [row[0] for row in cur.fetchall()]
        print(f"control tables present: {found}")
        if set(CONTROL_TABLES) - set(found):
            print(f"missing: {sorted(set(CONTROL_TABLES) - set(found))}", file=sys.stderr)
            return 2
    finally:
        conn.close()
    print("phase-5 DDL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
