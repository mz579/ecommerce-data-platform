"""Integration test: the RT upsert must never downgrade ``is_final``.

Regression for a real defect found while demoing Phase 9.4. ``RtJdbcSink`` used
``is_final = VALUES(is_final)``, so a late / out-of-order write for the same day
could overwrite an already-final row with ``is_final=0`` while
``source_watermark`` (protected by ``GREATEST``) stayed past the finalize
deadline. That left the self-contradictory state "watermark says final, flag
says not final" — observed on 2015-07-19 and 2015-07-25.

The fix makes both columns monotonic: ``is_final = GREATEST(is_final,
VALUES(is_final))``. This test executes the *actual* statements from
``RtJdbcSink`` (parsed out of the Java source, so the test cannot drift from the
sink) against a dedicated ``*_test`` schema.

Runs only when MySQL is reachable; otherwise the module skips with an explicit
reason, keeping a plain ``pytest`` green on machines without a database.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime
from pathlib import Path

import pymysql
import pytest
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SINK_SOURCE = (
    PROJECT_ROOT
    / "streaming"
    / "flink-job"
    / "src"
    / "main"
    / "java"
    / "com"
    / "ecommerce"
    / "rt"
    / "sink"
    / "RtJdbcSink.java"
)
DDL = PROJECT_ROOT / "sql" / "ddl" / "streaming" / "02_create_rt_tables.sql"

_RT_TEST_DATABASE = "ecommerce_rt_test"


def _java_sql(constant: str) -> str:
    """Reconstruct one SQL constant from the Java source.

    The constant is a chain of ``"literal" + "literal"`` fragments; joining the
    quoted parts yields the statement the sink actually executes.
    """
    text = SINK_SOURCE.read_text(encoding="utf-8")
    match = re.search(
        rf"String {constant}\s*=(.*?);\s*(?:\n\s*public|\n\s*static|\n\s*private)",
        text,
        re.S,
    )
    if match is None:
        raise AssertionError(f"{constant} not found in {SINK_SOURCE}")
    return "".join(re.findall(r'"((?:[^"\\]|\\.)*)"', match.group(1)))


def _pymysql_sql(constant: str) -> str:
    """JDBC uses ``?`` placeholders; pymysql uses ``%s``."""
    return _java_sql(constant).replace("?", "%s")


@pytest.fixture(scope="module")
def rt_cfg():
    """Provision a throwaway ``ecommerce_rt_test`` schema (never production)."""
    load_dotenv(PROJECT_ROOT / ".env")
    host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("MYSQL_PORT", "3306"))
    root_password = os.environ.get("MYSQL_ROOT_PASSWORD", "")
    app_password = os.environ.get("RT_APP_PASSWORD", "")

    try:
        admin = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password=root_password,
            charset="utf8mb4",
            connect_timeout=3,
        )
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"MySQL root unavailable, skipping RT upsert test: {exc}")

    assert _RT_TEST_DATABASE.endswith("_test")
    try:
        with admin.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {_RT_TEST_DATABASE}")
            cur.execute(
                f"CREATE DATABASE {_RT_TEST_DATABASE} "
                "DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
            if app_password:
                for account in ("rt_app", os.environ.get("RT_APP_USER") or "rt_app"):
                    cur.execute(
                        "CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s",
                        (account, app_password),
                    )
                    cur.execute(
                        f"GRANT SELECT, INSERT, UPDATE, DELETE ON {_RT_TEST_DATABASE}.* TO %s@'%%'",
                        (account,),
                    )
        admin.commit()
    finally:
        admin.close()

    ddl_text = DDL.read_text(encoding="utf-8")
    statements = [s.strip() for s in ddl_text.split(";") if s.strip()]

    # DDL is applied as root; rt_app only ever gets DML, mirroring production.
    root = pymysql.connect(
        host=host,
        port=port,
        user="root",
        password=root_password,
        database=_RT_TEST_DATABASE,
        charset="utf8mb4",
        connect_timeout=3,
    )
    try:
        with root.cursor() as cur:
            for statement in statements:
                if statement.startswith("--"):
                    statement = "\n".join(
                        line for line in statement.splitlines() if not line.startswith("--")
                    ).strip()
                if statement:
                    cur.execute(statement)
        root.commit()
    finally:
        root.close()

    app = pymysql.connect(
        host=host,
        port=port,
        user=os.environ.get("RT_APP_USER") or "rt_app",
        password=app_password,
        database=_RT_TEST_DATABASE,
        charset="utf8mb4",
        connect_timeout=3,
    )
    try:
        yield app
    finally:
        app.close()
        cleanup = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password=root_password,
            charset="utf8mb4",
            connect_timeout=3,
        )
        try:
            with cleanup.cursor() as cur:
                cur.execute(f"DROP DATABASE IF EXISTS {_RT_TEST_DATABASE}")
            cleanup.commit()
        finally:
            cleanup.close()


def _traffic_upsert(conn, event_date, pv, watermark, is_final, updated_at):
    with conn.cursor() as cur:
        cur.execute(
            _pymysql_sql("TRAFFIC_SQL"),
            (
                event_date,
                pv,
                1,
                1,
                datetime(2015, 7, 19),
                datetime(2015, 7, 20),
                watermark,
                1 if is_final else 0,
                updated_at,
            ),
        )
    conn.commit()


def _traffic_row(conn, event_date):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pv, source_watermark, is_final FROM rt_traffic_daily WHERE event_date_utc = %s",
            (event_date,),
        )
        return cur.fetchone()


def test_traffic_is_final_cannot_be_downgraded(rt_cfg):
    day = date(2015, 7, 19)
    final_watermark = datetime(2015, 7, 21, 0, 51, 49)
    _traffic_upsert(rt_cfg, day, 100, final_watermark, True, datetime(2015, 7, 21, 1, 0))
    assert _traffic_row(rt_cfg, day) == (100, final_watermark, 1)

    # Out-of-order write for the same day: older watermark, is_final=0.
    _traffic_upsert(
        rt_cfg, day, 90, datetime(2015, 7, 20, 0, 0), False, datetime(2015, 7, 21, 2, 0)
    )

    pv, watermark, is_final = _traffic_row(rt_cfg, day)
    assert pv == 90, "cumulative metrics still take the latest write"
    assert watermark == final_watermark, "source_watermark must stay monotonic"
    assert is_final == 1, "is_final must never be downgraded to 0"


def test_funnel_is_final_cannot_be_downgraded(rt_cfg):
    day = date(2015, 7, 25)
    watermark = datetime(2015, 7, 27, 0, 20, 42)
    sql = _pymysql_sql("FUNNEL_SQL")
    now = datetime(2015, 7, 27, 1, 0)
    with rt_cfg.cursor() as cur:
        cur.execute(
            sql,
            (day, "orders", 165, datetime(2015, 7, 25), datetime(2015, 7, 26), watermark, 1, now),
        )
    rt_cfg.commit()

    with rt_cfg.cursor() as cur:
        cur.execute(
            sql,
            (
                day,
                "orders",
                160,
                datetime(2015, 7, 25),
                datetime(2015, 7, 26),
                datetime(2015, 7, 26, 0, 0),
                0,
                now,
            ),
        )
    rt_cfg.commit()

    with rt_cfg.cursor() as cur:
        cur.execute(
            "SELECT metric_value, source_watermark, is_final FROM rt_funnel_daily "
            "WHERE event_date_utc = %s AND metric_name = 'orders'",
            (day,),
        )
        metric_value, stored_watermark, is_final = cur.fetchone()
    assert metric_value == 160
    assert stored_watermark == watermark
    assert is_final == 1


def test_item_is_final_cannot_be_downgraded(rt_cfg):
    day = date(2015, 7, 25)
    sql = _pymysql_sql("ITEM_SQL")
    now = datetime(2015, 7, 27, 1, 0)
    with rt_cfg.cursor() as cur:
        cur.execute(
            sql,
            (day, 10, 5, 3, datetime(2015, 7, 25), datetime(2015, 7, 26), 1, now),
        )
    rt_cfg.commit()
    with rt_cfg.cursor() as cur:
        cur.execute(
            sql,
            (day, 9, 5, 3, datetime(2015, 7, 25), datetime(2015, 7, 26), 0, now),
        )
    rt_cfg.commit()
    with rt_cfg.cursor() as cur:
        cur.execute(
            "SELECT items_viewed, is_final FROM rt_item_behavior_daily WHERE event_date_utc = %s",
            (day,),
        )
        items_viewed, is_final = cur.fetchone()
    assert items_viewed == 9
    assert is_final == 1


def test_partial_day_may_remain_non_final(rt_cfg):
    """The fix must not force is_final=1: a partial day keeps is_final=0."""
    day = date(2015, 9, 18)
    _traffic_upsert(
        rt_cfg, day, 1479, datetime(2015, 9, 19, 0, 59, 29), False, datetime(2015, 9, 19, 1, 0)
    )
    _, _, is_final = _traffic_row(rt_cfg, day)
    assert is_final == 0


def test_java_sql_statements_are_monotonic():
    """Guard the SQL text itself, independent of a live database."""
    for constant in ("TRAFFIC_SQL", "FUNNEL_SQL", "ITEM_SQL"):
        sql = _java_sql(constant)
        assert "is_final = GREATEST(is_final, VALUES(is_final))" in sql, constant
        assert "is_final = VALUES(is_final)" not in sql, constant
