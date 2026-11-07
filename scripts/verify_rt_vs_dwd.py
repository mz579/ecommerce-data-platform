"""Recompute daily metrics from dwd_events and compare them with ecommerce_rt.

Independent of the Flink job's aggregation code: expected values come from a single
batch SQL query over ``ecommerce_dw.dwd_events``; actual values are read from the three
RT tables. Exit code 0 = every metric MATCH, 1 = at least one DIFF/missing row,
2 = configuration/usage error.

Usage (project root, venv active):

    python scripts/verify_rt_vs_dwd.py 2015-07-14 2015-07-16
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from ecommerce_dw.streaming.rt_ddl import ensure_allowed_database
from ecommerce_dw.streaming.verify_rt import (
    date_range,
    format_table,
    read_expected,
    read_realtime,
    reconcile,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("date_from", type=date.fromisoformat, help="起始 UTC 日期 (YYYY-MM-DD)")
    parser.add_argument("date_to", type=date.fromisoformat, help="结束 UTC 日期 (YYYY-MM-DD，含)")
    parser.add_argument("--dwd-database", default=None, help="离线库（默认 env MYSQL_DATABASE）")
    parser.add_argument("--rt-database", default=None, help="实时库（默认 env RT_DATABASE）")
    return parser


def _connect_dwd(database: str) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
    )


def _connect_rt(database: str) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=os.environ.get("RT_MYSQL_HOST") or os.environ["MYSQL_HOST"],
        port=int(os.environ.get("RT_MYSQL_PORT") or os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("RT_APP_USER", "rt_app"),
        password=os.environ["RT_APP_PASSWORD"],
        database=database,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        connect_timeout=10,
    )


def _read_only(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
        cur.execute("SELECT 1")
        cur.fetchall()


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    try:
        days = date_range(args.date_from, args.date_to)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    dwd_database = args.dwd_database or os.environ.get("MYSQL_DATABASE", "ecommerce_dw")
    rt_database = args.rt_database or os.environ.get("RT_DATABASE", "ecommerce_rt")
    try:
        ensure_allowed_database(rt_database)
    except ValueError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 2

    try:
        dwd_conn = _connect_dwd(dwd_database)
        rt_conn = _connect_rt(rt_database)
    except KeyError as exc:
        print(f"[error] missing environment variable: {exc}", file=sys.stderr)
        return 2
    except pymysql.MySQLError as exc:
        print(f"[error] cannot connect to MySQL: {exc}", file=sys.stderr)
        return 2

    try:
        _read_only(dwd_conn)
        _read_only(rt_conn)
        with dwd_conn.cursor() as cur:
            expected = read_expected(cur, args.date_from, args.date_to)
        with rt_conn.cursor() as cur:
            actual, final_flags = read_realtime(cur, args.date_from, args.date_to)
    except pymysql.MySQLError as exc:
        print(f"[error] query failed: {exc}", file=sys.stderr)
        return 2
    finally:
        dwd_conn.close()
        rt_conn.close()

    comparisons = reconcile(expected, actual, final_flags, days)
    print(
        f"verify_rt_vs_dwd: dwd={dwd_database}.dwd_events "
        f"rt={rt_database} range={args.date_from}..{args.date_to}"
    )
    print(
        "is_final: "
        + ", ".join(f"{day.isoformat()}={final_flags.get(day, 'MISSING')}" for day in days)
    )
    print()
    print(format_table(comparisons))
    return 0 if all(row.status == "MATCH" for row in comparisons) else 1


if __name__ == "__main__":
    sys.exit(main())
