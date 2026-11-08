"""Create / refresh the read-only Web UI account ``webapp_ro`` (idempotent).

The FastAPI backend never connects with ``ecom_app`` (read-write pipeline
account). It uses the dedicated account configured by ``WEB_DB_USER`` /
``WEB_DB_PASSWORD`` in the project-root ``.env``. Grants are **SELECT only**
on the ADS/control/RT whitelist frozen in the Web UI spec (SPEC §2):

* ``ecommerce_dw``: 11 ADS/control tables + 4 controlled views;
* ``ecommerce_rt``: 5 realtime result/audit tables.

The script connects as MySQL ``root`` (password from ``MYSQL_ROOT_PASSWORD``)
because creating users and table grants need elevated privileges. It is safe
to re-run: the user is created with ``IF NOT EXISTS`` and the password is
re-synchronised from ``.env`` on every run.

Usage (project root, venv active):

    python scripts/create_webapp_ro.py
    python scripts/create_webapp_ro.py --dry-run   # print SQL, no DB access
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

DEFAULT_RO_USER = "webapp_ro"
RO_HOST = "%"

DW_TARGETS = (
    # ADS application tables
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_funnel_interval",
    "ads_item_ranking_daily",
    "ads_item_behavior_daily",
    "ads_retention_cohort",
    # control / audit tables exposed read-only to the ops page
    "ads_publish_log",
    "quality_check_run",
    "quality_check_result",
    "airflow_pipeline_run",
    "etl_watermark",
    # controlled views
    "v_ads_category_daily",
    "v_ads_item_orders_daily",
    "v_ads_funnel_interval",
    "v_ads_publish_status",
)

RT_TARGETS = (
    "rt_traffic_daily",
    "rt_funnel_daily",
    "rt_item_behavior_daily",
    "rt_reconciliation_daily",
    "rt_late_events",
)

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _check_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "''")


def resolve_credentials(environ: Mapping[str, str] | None = None) -> tuple[str, str]:
    """Return ``(WEB_DB_USER, WEB_DB_PASSWORD)`` with a clear failure message."""
    env = os.environ if environ is None else environ
    user = env.get("WEB_DB_USER") or DEFAULT_RO_USER
    password = env.get("WEB_DB_PASSWORD", "")
    if user in {"root", "ecom_app", "rt_app"}:
        raise SystemExit(
            f"WEB_DB_USER={user!r} is a privileged/pipeline account; this script "
            "only creates a dedicated read-only Web UI account (default webapp_ro)"
        )
    if not password:
        raise SystemExit(
            "WEB_DB_PASSWORD is not set; add WEB_DB_USER / WEB_DB_PASSWORD to the "
            "project-root .env before creating the read-only account"
        )
    return user, password


def build_grant_sql(
    dw_database: str,
    rt_database: str,
    ro_user: str,
    ro_password: str,
    *,
    redact_password: bool = False,
) -> list[str]:
    """Build CREATE USER / ALTER USER / GRANT SELECT statements (no DB access)."""
    dw_db = _check_ident(dw_database)
    rt_db = _check_ident(rt_database)
    user = _escape_literal(ro_user)
    password = "<WEB_DB_PASSWORD>" if redact_password else _escape_literal(ro_password)
    statements = [
        f"CREATE USER IF NOT EXISTS '{user}'@'{RO_HOST}'",
        f"ALTER USER '{user}'@'{RO_HOST}' IDENTIFIED BY '{password}'",
    ]
    statements.extend(
        f"GRANT SELECT ON `{dw_db}`.`{_check_ident(table)}` TO '{user}'@'{RO_HOST}'"
        for table in DW_TARGETS
    )
    statements.extend(
        f"GRANT SELECT ON `{rt_db}`.`{_check_ident(table)}` TO '{user}'@'{RO_HOST}'"
        for table in RT_TARGETS
    )
    return statements


def _missing_targets(cfg, rt_database: str) -> list[str]:
    """Return ``schema.table`` entries that do not exist in information_schema."""
    wanted = [(cfg.database, table) for table in DW_TARGETS]
    wanted += [(rt_database, table) for table in RT_TARGETS]
    placeholders = ", ".join(["(%s, %s)"] * len(wanted))
    params: list[str] = []
    for schema, table in wanted:
        params.extend([schema, table])
    sql = (
        "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES "
        f"WHERE (TABLE_SCHEMA, TABLE_NAME) IN ({placeholders})"
    )
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user="root",
        password=os.environ["MYSQL_ROOT_PASSWORD"],
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            existing = {(row[0], row[1]) for row in cur.fetchall()}
    finally:
        conn.close()
    return [f"{schema}.{table}" for schema, table in wanted if (schema, table) not in existing]


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
    rt_database = os.environ.get("RT_DATABASE", "ecommerce_rt")
    ro_user, ro_password = resolve_credentials()

    if args.dry_run:
        print(
            f"[dry-run] dw={cfg.database} rt={rt_database} ro_user={ro_user}@{RO_HOST} "
            f"grants={len(DW_TARGETS) + len(RT_TARGETS)}"
        )
        for statement in build_grant_sql(
            cfg.database, rt_database, ro_user, ro_password, redact_password=True
        ):
            print(f"{statement};")
        return 0

    missing = _missing_targets(cfg, rt_database)
    if missing:
        print("missing grant targets: " + ", ".join(missing), file=sys.stderr)
        return 2

    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user="root",
        password=os.environ["MYSQL_ROOT_PASSWORD"],
        charset="utf8mb4",
        connect_timeout=10,
    )
    try:
        with conn.cursor() as cur:
            statements = build_grant_sql(cfg.database, rt_database, ro_user, ro_password)
            for statement in statements[:2]:
                cur.execute(statement)
            conn.commit()
            print(
                f"account {ro_user}@{RO_HOST} ready; granting SELECT on "
                f"{len(DW_TARGETS)} DW + {len(RT_TARGETS)} RT targets"
            )
            for statement in statements[2:]:
                cur.execute(statement)
            conn.commit()

            print(f"\n== SHOW GRANTS FOR '{_escape_literal(ro_user)}'@'{RO_HOST}' ==")
            cur.execute(f"SHOW GRANTS FOR '{_escape_literal(ro_user)}'@'{RO_HOST}'")
            grants = [row[0] for row in cur.fetchall()]
            for grant in grants:
                print(f"  {grant}")

            write_grants = [
                grant for grant in grants if not grant.startswith(("GRANT SELECT", "GRANT USAGE"))
            ]
            if write_grants:
                print(
                    "unexpected non-read grant(s): " + "; ".join(write_grants),
                    file=sys.stderr,
                )
                return 3
    finally:
        conn.close()
    print("\nwebapp-ro-account OK (SELECT only)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
