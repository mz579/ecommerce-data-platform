"""Apply the DWS/ADS DDL files to the ecommerce_dw MySQL database (Phase 4).

The SQL files matching ``sql/ddl/dws_*.sql`` and ``sql/ddl/ads_*.sql`` are all
`CREATE TABLE IF NOT EXISTS`, so this script is idempotent and safe to re-run.

Usage (project root, venv active):

    python scripts/apply_dws_ads_ddl.py
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
    "dws_user_daily",
    "dws_item_daily",
    "dws_category_daily",
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
    "dws_ads_build_log",
)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    ddl_dir = PROJECT_ROOT / "sql" / "ddl"
    scripts = sorted(ddl_dir.glob("dws_*.sql")) + sorted(ddl_dir.glob("ads_*.sql"))
    if not scripts:
        print("no sql/ddl/dws_*.sql / ads_*.sql files found", file=sys.stderr)
        return 2

    print("applying DWS/ADS DDL...")
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
                "WHERE table_schema = %s AND table_name IN (%s, %s, %s, %s, %s, %s, %s, %s) "
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
    print("DWS/ADS DDL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
