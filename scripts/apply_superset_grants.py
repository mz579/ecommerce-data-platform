"""Apply Phase 7 Superset prerequisites to the business MySQL.

Creates:
  1. five controlled read-only views (sql/ddl/ads_views/*.sql) — the only
     "non-ADS" surfaces Superset is allowed to read (主方案: Superset 只读
     ADS 或受控视图, see docs/superset_design.md);
  2. the least-privilege read-only account ``superset_ro`` (SELECT only on the
     six ADS tables and the five views; nothing else);
  3. prints ``SHOW GRANTS`` for evidence.

The script is idempotent and safe to re-run. It connects as the root user
(read from .env ``MYSQL_ROOT_PASSWORD``) because creating users / views /
grants needs elevated privileges; the pipeline account ``ecom_app`` never
touches this path.

Usage (project root, venv active):

    python scripts/apply_superset_grants.py
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

RO_USER = "superset_ro"
RO_HOST = "%"
ADS_TABLES = (
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
    "ads_item_behavior_daily",
    "ads_funnel_interval",
)
VIEWS_DIR = PROJECT_ROOT / "sql" / "ddl" / "ads_views"
_VIEW_RE = re.compile(r"^CREATE OR REPLACE VIEW\s+(\w+)", re.IGNORECASE)


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def _collect_views() -> tuple[tuple[str, str], ...]:
    if not VIEWS_DIR.is_dir():
        print(f"views dir missing: {VIEWS_DIR}", file=sys.stderr)
        sys.exit(2)
    views = []
    for path in sorted(VIEWS_DIR.glob("*.sql")):
        # 去掉注释横幅与空行后，文件只剩一条 CREATE OR REPLACE VIEW 语句。
        cleaned = (
            "\n".join(
                line
                for line in path.read_text(encoding="utf-8").splitlines()
                if not line.strip().startswith("--") and line.strip()
            )
            .strip()
            .rstrip(";")
        )
        m = _VIEW_RE.match(cleaned)
        if not m:
            print(f"cannot extract view name from {path.name}", file=sys.stderr)
            sys.exit(2)
        views.append((m.group(1), cleaned))
    if not views:
        print(f"no view files under {VIEWS_DIR}", file=sys.stderr)
        sys.exit(2)
    return tuple(views)


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    ro_password = os.environ["MYSQL_RO_PASSWORD"]
    root_password = os.environ["MYSQL_ROOT_PASSWORD"]
    views = _collect_views()

    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user="root",
        password=root_password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            print(f"creating {len(views)} controlled views on {cfg.database}:")
            for name, statement in views:
                cur.execute(statement)
                print(f"  + {name}")
                conn.commit()

            quoted_pw = _escape_literal(ro_password)
            cur.execute(f"CREATE USER IF NOT EXISTS '{RO_USER}'@'{RO_HOST}'")
            cur.execute(f"ALTER USER '{RO_USER}'@'{RO_HOST}' IDENTIFIED BY '{quoted_pw}'")
            conn.commit()

            print(f"granting SELECT to {RO_USER}@{RO_HOST}:")
            grant_targets = list(ADS_TABLES) + [name for name, _ in views]
            for table in grant_targets:
                cur.execute(
                    f"GRANT SELECT ON `{cfg.database}`.`{table}` TO '{RO_USER}'@'{RO_HOST}'"
                )
                print(f"  SELECT {cfg.database}.{table}")
            conn.commit()

            print(f"\n== SHOW GRANTS FOR '{RO_USER}'@'{RO_HOST}' ==")
            cur.execute(f"SHOW GRANTS FOR '{RO_USER}'@'{RO_HOST}'")
            for row in cur.fetchall():
                print(f"  {row[0]}")
    finally:
        conn.close()
    print("\nsuperset-grants OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
