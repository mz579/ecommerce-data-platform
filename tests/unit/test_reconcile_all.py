"""Unit tests for the Phase 9.4 full-range reconciliation helpers."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from ecommerce_dw.streaming.reconcile_all import (
    ADS_FUNNEL_SQL,
    ADS_ITEM_SQL,
    ADS_TRAFFIC_SQL,
    ALL_METRICS,
    OfflineDataset,
    ReconciliationRow,
    _json_payload,
    build_full_day_rows,
    build_rows,
    build_skip_rows,
    crosscheck_ads_vs_dwd,
    day_statuses,
    load_ads,
    reconciliation_row,
    relative_diff,
    rt_app_credentials,
    summarize_rows,
    write_reconciliation,
)

DAY = date(2015, 7, 14)
CHECKED_AT = datetime(2026, 11, 7, 12, 0, 0)


def _offline_metrics(**overrides: Decimal | None) -> dict[str, Decimal | None]:
    values: dict[str, Decimal | None] = {
        "pv": Decimal(100),
        "uv": Decimal(40),
        "dau": Decimal(45),
        "view_users": Decimal(40),
        "cart_users": Decimal(10),
        "buyer_users": Decimal(5),
        "orders": Decimal(6),
        "add_to_cart_rate": Decimal("0.250000"),
        "conv_strict_rate": Decimal("0.100000"),
        "conv_global_rate": Decimal("0.125000"),
        "items_viewed": Decimal(30),
        "items_carted": Decimal(8),
        "items_bought": Decimal(5),
    }
    values.update(overrides)
    return values


class _FakeCursor:
    def __init__(self, responses: list[list[dict]] | None = None, one: dict | None = None):
        self.responses = responses or []
        self.one = one
        self.calls: list[tuple[str, tuple | None]] = []
        self.upserts: list[list[tuple]] = []

    def execute(self, sql: str, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.responses[len(self.calls) - 1]

    def fetchone(self):
        return self.one

    def executemany(self, sql: str, rows):
        self.calls.append((" ".join(sql.split()), None))
        self.upserts.append(list(rows))


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self

    def __enter__(self):
        return self._cursor

    def __exit__(self, *args):
        return False

    def commit(self):
        self.commits += 1


def test_full_day_all_match_produces_13_rows():
    offline = _offline_metrics()
    rows = build_full_day_rows(DAY, offline, dict(offline), CHECKED_AT)
    assert len(rows) == 13
    assert [row.metric for row in rows] == list(ALL_METRICS)
    assert all(row.status == "MATCH" for row in rows)
    assert all(row.abs_diff == 0 for row in rows)
    assert all(row.checked_at == CHECKED_AT for row in rows)


def test_full_day_numeric_diff_and_relative_diff():
    offline = _offline_metrics()
    realtime = dict(offline)
    realtime["pv"] = Decimal(101)
    row = next(
        item
        for item in build_full_day_rows(DAY, offline, realtime, CHECKED_AT)
        if item.metric == "pv"
    )
    assert row.status == "DIFF"
    assert row.abs_diff == Decimal(1)
    assert row.rel_diff == Decimal("0.010000")


def test_missing_realtime_metric_is_diff_with_null_actual():
    offline = _offline_metrics()
    realtime = dict(offline)
    del realtime["orders"]
    row = next(
        item
        for item in build_full_day_rows(DAY, offline, realtime, CHECKED_AT)
        if item.metric == "orders"
    )
    assert row.status == "DIFF"
    assert row.realtime_value is None
    assert row.abs_diff is None
    assert row.rel_diff is None


def test_missing_realtime_day_is_all_diff():
    rows = build_full_day_rows(DAY, _offline_metrics(), {}, CHECKED_AT)
    assert len(rows) == 13
    assert all(row.status == "DIFF" for row in rows)


def test_partial_day_produces_13_skip_rows_with_null_values():
    rows = build_skip_rows(DAY, CHECKED_AT)
    assert len(rows) == 13
    assert all(row.status == "SKIP" for row in rows)
    assert all(row.offline_value is None for row in rows)
    assert all(row.realtime_value is None for row in rows)
    assert all(row.abs_diff is None and row.rel_diff is None for row in rows)


def test_build_rows_counts_for_full_and_partial_days():
    offline = {DAY: _offline_metrics()}
    realtime = {DAY: _offline_metrics()}
    partial = date(2015, 5, 3)
    rows = build_rows([DAY], [partial], offline, realtime, CHECKED_AT)
    assert len(rows) == 26
    assert summarize_rows(rows) == {"MATCH": 13, "DIFF": 0, "SKIP": 13}
    assert day_statuses(rows)[partial]["SKIP"] == 13


def test_relative_diff_null_for_zero_or_missing_values():
    assert relative_diff(None, Decimal(1)) is None
    assert relative_diff(Decimal(0), Decimal(1)) is None
    assert relative_diff(Decimal(10), None) is None
    assert relative_diff(Decimal(10), Decimal(11)) == Decimal("0.100000")


def test_reconciliation_row_both_null_is_match():
    row = reconciliation_row(DAY, "add_to_cart_rate", None, None, CHECKED_AT)
    assert row.status == "MATCH"
    assert row.abs_diff is None
    assert row.rel_diff is None


def test_write_reconciliation_upserts_rows_idempotently():
    rows = build_full_day_rows(DAY, _offline_metrics(), _offline_metrics(), CHECKED_AT)
    cursor = _FakeCursor(one={"db": "ecommerce_rt"})
    connection = _FakeConnection(cursor)
    assert write_reconciliation(connection, rows) == 13
    assert write_reconciliation(connection, rows) == 13
    assert connection.commits == 2
    assert len(cursor.upserts) == 2
    assert cursor.upserts[0] == cursor.upserts[1]
    assert len(cursor.upserts[0][0]) == 8
    assert "ON DUPLICATE KEY UPDATE" in cursor.calls[1][0]


def test_write_reconciliation_refuses_non_rt_database():
    rows = build_full_day_rows(DAY, _offline_metrics(), _offline_metrics(), CHECKED_AT)
    cursor = _FakeCursor(one={"db": "ecommerce_dw"})
    connection = _FakeConnection(cursor)
    with pytest.raises(RuntimeError, match="refusing to write"):
        write_reconciliation(connection, rows)
    assert not cursor.upserts


class _TupleCursor(_FakeCursor):
    """pymysql's default cursor returns tuples, not dicts."""

    def __init__(self, one: tuple | None = None):
        super().__init__(one=None)
        self.tuple_row = one

    def fetchone(self):
        return self.tuple_row


def test_write_reconciliation_accepts_default_tuple_cursor():
    # Regression: the CLI used a tuple cursor and crashed on ``row["db"]``.
    rows = build_full_day_rows(DAY, _offline_metrics(), _offline_metrics(), CHECKED_AT)
    cursor = _TupleCursor(one=("ecommerce_rt",))
    connection = _FakeConnection(cursor)
    assert write_reconciliation(connection, rows) == 13
    assert connection.commits == 1
    assert len(cursor.upserts[0]) == 13


def test_write_reconciliation_tuple_cursor_still_guards_database():
    rows = build_full_day_rows(DAY, _offline_metrics(), _offline_metrics(), CHECKED_AT)
    cursor = _TupleCursor(one=("ecommerce_dw",))
    connection = _FakeConnection(cursor)
    with pytest.raises(RuntimeError, match="refusing to write"):
        write_reconciliation(connection, rows)
    assert not cursor.upserts


def test_json_payload_is_serializable_and_uses_iso_dates():
    # Regression: a raw ``date`` in the "days" list made --json-out raise TypeError.
    full_rows = build_full_day_rows(DAY, _offline_metrics(), _offline_metrics(), CHECKED_AT)
    partial = date(2015, 5, 3)
    rows = full_rows + build_skip_rows(partial, CHECKED_AT)
    payload = _json_payload(
        rows,
        [],
        date_from=partial,
        date_to=DAY,
        written=len(rows),
        final_flags={DAY: 1},
    )
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    reloaded = json.loads(text)
    assert reloaded["date_from"] == "2015-05-03"
    assert reloaded["date_to"] == "2015-07-14"
    assert reloaded["written"] == 26
    assert reloaded["summary"] == {"MATCH": 13, "DIFF": 0, "SKIP": 13}
    assert {day["event_date"] for day in reloaded["days"]} == {"2015-05-03", "2015-07-14"}
    assert reloaded["days"][0]["event_date"] == "2015-05-03"
    assert reloaded["days"][0]["skip"] == 13
    assert reloaded["days"][1]["match"] == 13


def test_rt_app_credentials_use_dedicated_account_only():
    assert rt_app_credentials({"RT_APP_PASSWORD": "secret"}) == ("rt_app", "secret")
    assert rt_app_credentials({"RT_APP_USER": "writer", "RT_APP_PASSWORD": "secret"}) == (
        "writer",
        "secret",
    )
    with pytest.raises(KeyError, match="RT_APP_PASSWORD"):
        rt_app_credentials({"RT_MYSQL_PASSWORD": "other"})


def test_load_ads_merges_traffic_funnel_and_item_tables():
    traffic = [
        {
            "event_date_utc": DAY,
            "pv": 100,
            "uv": 40,
            "dau": 45,
            "is_full_day": 1,
        }
    ]
    funnel = [
        {"event_date_utc": DAY, "metric_name": "cart_users", "metric_value": Decimal("10.000000")},
        {
            "event_date_utc": DAY,
            "metric_name": "conv_strict_users",
            "metric_value": Decimal("4.000000"),
        },
        {
            "event_date_utc": DAY,
            "metric_name": "conv_strict_rate",
            "metric_value": Decimal("0.100000"),
        },
    ]
    item = [
        {
            "event_date_utc": DAY,
            "items_viewed": 30,
            "items_carted": 8,
            "items_bought": 5,
        }
    ]
    cursor = _FakeCursor([traffic, funnel, item])
    dataset = load_ads(cursor, DAY, DAY)
    assert dataset.is_full_day == {DAY: 1}
    assert dataset.metrics[DAY]["pv"] == Decimal(100)
    assert dataset.metrics[DAY]["cart_users"] == Decimal("10.000000")
    assert dataset.metrics[DAY]["conv_strict_rate"] == Decimal("0.100000")
    assert dataset.metrics[DAY]["items_bought"] == Decimal(5)
    # Only the seven reconciled funnel metrics are merged; conv_strict_users is ignored.
    assert "conv_strict_users" not in dataset.metrics[DAY]
    assert [call[0] for call in cursor.calls] == [
        " ".join(ADS_TRAFFIC_SQL.split()),
        " ".join(ADS_FUNNEL_SQL.split()),
        " ".join(ADS_ITEM_SQL.split()),
    ]
    assert all(call[1] == (DAY, DAY) for call in cursor.calls)


def test_crosscheck_ads_vs_dwd_reports_definition_drift():
    ads = OfflineDataset(
        metrics={DAY: _offline_metrics()},
        is_full_day={DAY: 1},
    )
    dwd = {DAY: _offline_metrics()}
    assert crosscheck_ads_vs_dwd(ads, dwd, [DAY]) == []

    dwd[DAY]["pv"] = Decimal(99)
    mismatches = crosscheck_ads_vs_dwd(ads, dwd, [DAY])
    assert len(mismatches) == 1
    assert mismatches[0].event_date == DAY
    assert mismatches[0].metric == "pv"
    assert mismatches[0].ads_value == Decimal(100)
    assert mismatches[0].dwd_value == Decimal(99)


def test_row_as_tuple_order_matches_upsert_columns():
    row = ReconciliationRow(
        event_date=DAY,
        metric="pv",
        offline_value=Decimal(1),
        realtime_value=Decimal(2),
        abs_diff=Decimal(1),
        rel_diff=Decimal(1),
        status="DIFF",
        checked_at=CHECKED_AT,
    )
    assert row.as_tuple() == (
        DAY,
        "pv",
        Decimal(1),
        Decimal(2),
        Decimal(1),
        Decimal(1),
        "DIFF",
        CHECKED_AT,
    )


def test_checked_at_is_utc_naive():
    checked = datetime.now(UTC).replace(tzinfo=None)
    assert checked.tzinfo is None
