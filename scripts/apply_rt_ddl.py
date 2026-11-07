"""Apply the Phase 9.2 realtime result-table DDL to ``ecommerce_rt``.

The five tables (rt_traffic_daily / rt_funnel_daily / rt_item_behavior_daily /
rt_reconciliation_daily / rt_late_events) follow docs/streaming_design.md §7.2
and live in ``sql/ddl/streaming/02_create_rt_tables.sql``. The script:

- refuses any target database that is not ``ecommerce_rt`` or does not end with
  ``_test`` (so ``ecommerce_dw`` can never be touched);
- creates the target schema if missing (utf8mb4 / utf8mb4_0900_ai_ci);
- applies every ``CREATE TABLE IF NOT EXISTS`` (idempotent);
- verifies the five tables exist and prints their column counts.

Usage (project root, venv active):

    python scripts/apply_rt_ddl.py
    python scripts/apply_rt_ddl.py --dry-run
    python scripts/apply_rt_ddl.py --database ecommerce_rt_test
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
    EXPECTED_RT_TABLES,
    build_create_database_sql,
    ensure_allowed_database,
    load_ddl_statements,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        default=None,
        help=f"目标库（默认 env RT_DATABASE 或 {DEFAULT_RT_DATABASE}）",
    )
    parser.add_argument("--ddl", type=Path, default=None, help="自定义 DDL 文件路径")
    parser.add_argument("--dry-run", action="store_true", help="只打印 SQL，不连接数据库")
    return parser


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    database = args.database or os.environ.get("RT_DATABASE") or DEFAULT_RT_DATABASE

    try:
        ensure_allowed_database(database)
        statements = load_ddl_statements(args.ddl)
    except (ValueError, OSError) as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2
    if not statements:
        print("[error] DDL file contains no statements", file=sys.stderr)
        return 2

    create_database = build_create_database_sql(database)
    if args.dry_run:
        print(f"[dry-run] target={database} statements={len(statements)}")
        print(f"{create_database};")
        for statement in statements:
            print(f"{statement};")
        return 0

    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    try:
        conn = pymysql.connect(
            host=cfg.host,
            port=cfg.port,
            user="root",
            password=os.environ["MYSQL_ROOT_PASSWORD"],
            charset="utf8mb4",
            connect_timeout=10,
        )
    except KeyError:
        print("[error] MYSQL_ROOT_PASSWORD is required in .env", file=sys.stderr)
        return 1
    except pymysql.MySQLError as exc:
        print(f"[error] cannot connect to MySQL: {exc}", file=sys.stderr)
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute(create_database)
            conn.select_db(database)
            print(f"applying {len(statements)} RT DDL statement(s) to {database}:")
            for statement in statements:
                cur.execute(statement)
                first_line = statement.splitlines()[0]
                print(f"  + {first_line}")
            conn.commit()

            with conn.cursor() as cur:
                cur.execute(
                    "SELECT table_name, COUNT(*) AS column_count "
                    "FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name IN "
                    "(%s, %s, %s, %s, %s) "
                    "GROUP BY table_name ORDER BY table_name",
                    (database, *EXPECTED_RT_TABLES),
                )
                rows = cur.fetchall()
        present = {row[0]: row[1] for row in rows}
        print(f"tables present in {database}:")
        for name, column_count in present.items():
            print(f"  {name}: {column_count} columns")
        missing = [name for name in EXPECTED_RT_TABLES if name not in present]
        if missing:
            print(f"[error] missing tables: {missing}", file=sys.stderr)
            return 2
    except pymysql.MySQLError as exc:
        print(f"[error] DDL failed: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    print("RT DDL OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
