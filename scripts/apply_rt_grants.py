"""Create/update the Phase 9.2 realtime least-privilege accounts.

Accounts (docs/streaming_design.md §7.1):

- ``rt_app``: SELECT/INSERT/UPDATE/DELETE on ``ecommerce_rt.*`` only.
  No DDL and **no** ``ecommerce_dw`` grants.
- ``rt_recon_ro``: SELECT on ``ecommerce_rt.*`` plus SELECT on the
  ``ecommerce_dw`` ADS tables.

Passwords come from ``RT_APP_PASSWORD`` / ``RT_RECON_RO_PASSWORD`` and are
never written to the repository. ``--dry-run`` prints the SQL with passwords
redacted. The script connects as root (``MYSQL_ROOT_PASSWORD``) and is
idempotent: create-if-missing, reset password, revoke previous grants for the
two dedicated accounts, then grant the exact intended privileges.

Usage (project root, venv active):

    python scripts/apply_rt_grants.py --dry-run
    python scripts/apply_rt_grants.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.streaming.rt_ddl import (
    DEFAULT_RT_DATABASE,
    ensure_allowed_database,
)
from ecommerce_dw.streaming.rt_grants import (
    ADS_TABLES,
    APP_PASSWORD_ENV,
    APP_PASSWORD_PLACEHOLDER,
    RECON_PASSWORD_ENV,
    RECON_PASSWORD_PLACEHOLDER,
    RT_APP_USER,
    RT_RECON_USER,
    USER_HOST,
    build_all_grants_sql,
    resolve_password,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rt-database",
        default=None,
        help=f"实时结果库（默认 env RT_DATABASE 或 {DEFAULT_RT_DATABASE}）",
    )
    parser.add_argument(
        "--dw-database",
        default=None,
        help="离线数仓库（只用于 ADS 只读授权，默认 env MYSQL_DATABASE 或 ecommerce_dw）",
    )
    parser.add_argument("--dry-run", action="store_true", help="打印脱敏 SQL，不连接数据库")
    return parser


def _print_grants(cur: pymysql.cursors.Cursor, user: str) -> list[str]:
    cur.execute(f"SHOW GRANTS FOR '{user}'@'{USER_HOST}'")
    grants = [row[0] for row in cur.fetchall()]
    print(f"== SHOW GRANTS FOR '{user}'@'{USER_HOST}' ==")
    for grant in grants:
        print(f"  {grant}")
    return grants


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    rt_database = args.rt_database or os.environ.get("RT_DATABASE") or DEFAULT_RT_DATABASE
    dw_database = args.dw_database or os.environ.get("MYSQL_DATABASE") or "ecommerce_dw"

    try:
        ensure_allowed_database(rt_database)
        if rt_database == dw_database:
            raise ValueError("RT database and DW database must be different")
        app_password = resolve_password(APP_PASSWORD_ENV, required=not args.dry_run)
        recon_password = resolve_password(RECON_PASSWORD_ENV, required=not args.dry_run)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    statements = build_all_grants_sql(
        RT_APP_USER,
        app_password,
        RT_RECON_USER,
        recon_password,
        rt_database=rt_database,
        dw_database=dw_database,
        ads_tables=ADS_TABLES,
        redact_password=args.dry_run,
    )
    if args.dry_run:
        print(
            f"[dry-run] rt_database={rt_database} dw_database={dw_database} "
            f"app_user={RT_APP_USER} recon_user={RT_RECON_USER}"
        )
        for statement in statements:
            print(f"{statement};")
        print(f"-- redacted: {APP_PASSWORD_PLACEHOLDER} / {RECON_PASSWORD_PLACEHOLDER}")
        return 0

    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    try:
        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user="root",
            password=os.environ["MYSQL_ROOT_PASSWORD"],
            database=rt_database,
            charset="utf8mb4",
            connect_timeout=10,
        )
    except KeyError:
        print("[error] MYSQL_ROOT_PASSWORD is required in .env", file=sys.stderr)
        return 1
    except pymysql.MySQLError as exc:
        print(
            f"[error] cannot connect to MySQL/{rt_database}: {exc}; "
            "run scripts/apply_rt_ddl.py first",
            file=sys.stderr,
        )
        return 1

    try:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            conn.commit()
            app_grants = _print_grants(cur, RT_APP_USER)
            recon_grants = _print_grants(cur, RT_RECON_USER)
    except pymysql.MySQLError as exc:
        print(f"[error] grant failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    leaked = [grant for grant in app_grants if "ecommerce_dw" in grant]
    if leaked:
        print(f"[error] rt_app has ecommerce_dw grants: {leaked}", file=sys.stderr)
        return 3
    effective_app = [g for g in app_grants if "USAGE ON *.*" not in g]
    effective_recon = [g for g in recon_grants if "USAGE ON *.*" not in g]
    print(
        f"rt_app effective grants={len(effective_app)}; "
        f"rt_recon_ro effective grants={len(effective_recon)} "
        f"(ads tables={len(ADS_TABLES)})"
    )
    print("RT grants OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
