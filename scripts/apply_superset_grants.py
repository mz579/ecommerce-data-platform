"""Apply Phase 7 Superset prerequisites to the business MySQL.

Creates:
  1. five controlled read-only views (sql/ddl/ads_views/*.sql) — the only
     "non-ADS" surfaces Superset is allowed to read (主方案: Superset 只读
     ADS 或受控视图, see docs/superset_design.md);
  2. the least-privilege read-only account ``MYSQL_RO_USER`` (default
     ``superset_ro``; SELECT only on the six ADS tables and the five views;
     nothing else);
  3. prints ``SHOW GRANTS`` for evidence.

The script is idempotent and safe to re-run. It connects as the root user
(read from .env ``MYSQL_ROOT_PASSWORD``) because creating users / views /
grants needs elevated privileges; the pipeline account ``ecom_app`` never
touches this path.

Usage (project root, venv active):

    python scripts/apply_superset_grants.py
    python scripts/apply_superset_grants.py --dry-run   # print SQL, no DB
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

DEFAULT_RO_USER = "superset_ro"
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


def resolve_ro_user(environ: Mapping[str, str] | None = None) -> str:
    """Return the configured read-only account, defaulting to ``superset_ro``."""
    env = os.environ if environ is None else environ
    return env.get("MYSQL_RO_USER") or DEFAULT_RO_USER


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def build_grant_sql(
    database: str,
    ro_user: str,
    ro_password: str,
    grant_targets: list[str] | tuple[str, ...],
    *,
    redact_password: bool = False,
) -> list[str]:
    """Build the CREATE USER / ALTER USER / GRANT statements (no DB access)."""
    user = _escape_literal(ro_user)
    password = "<MYSQL_RO_PASSWORD>" if redact_password else _escape_literal(ro_password)
    statements = [
        f"CREATE USER IF NOT EXISTS '{user}'@'{RO_HOST}'",
        f"ALTER USER '{user}'@'{RO_HOST}' IDENTIFIED BY '{password}'",
    ]
    statements.extend(
        f"GRANT SELECT ON `{database}`.`{table}` TO '{user}'@'{RO_HOST}'" for table in grant_targets
    )
    return statements


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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the SQL that would run (password redacted) without connecting",
    )
    args = parser.parse_args(argv)

    load_dotenv(PROJECT_ROOT / ".env")
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    ro_user = resolve_ro_user()
    ro_password = os.environ["MYSQL_RO_PASSWORD"]
    views = _collect_views()
    grant_targets = list(ADS_TABLES) + [name for name, _ in views]

    if args.dry_run:
        print(
            f"[dry-run] database={cfg.database} ro_user={ro_user} host={RO_HOST} "
            f"views={len(views)} grants={len(grant_targets)}"
        )
        for name, statement in views:
            print(f"-- view: {name}")
            print(f"{statement};")
        for statement in build_grant_sql(
            cfg.database,
            ro_user,
            ro_password,
            grant_targets,
            redact_password=True,
        ):
            print(f"{statement};")
        return 0

    root_password = os.environ["MYSQL_ROOT_PASSWORD"]
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

            for statement in build_grant_sql(cfg.database, ro_user, ro_password, grant_targets)[:2]:
                cur.execute(statement)
            conn.commit()

            print(f"granting SELECT to {ro_user}@{RO_HOST}:")
            for table in grant_targets:
                cur.execute(
                    f"GRANT SELECT ON `{cfg.database}`.`{table}` "
                    f"TO '{_escape_literal(ro_user)}'@'{RO_HOST}'"
                )
                print(f"  SELECT {cfg.database}.{table}")
            conn.commit()

            print(f"\n== SHOW GRANTS FOR '{ro_user}'@'{RO_HOST}' ==")
            cur.execute(f"SHOW GRANTS FOR '{_escape_literal(ro_user)}'@'{RO_HOST}'")
            for row in cur.fetchall():
                print(f"  {row[0]}")
    finally:
        conn.close()
    print("\nsuperset-grants OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
