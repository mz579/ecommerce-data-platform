"""Unit tests for the Phase 9.3 independent RT-vs-DWD reconciliation helpers."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest

from ecommerce_dw.streaming.verify_rt import (
    ALL_METRICS,
    all_match,
    build_expected_sql,
    compare_day,
    compare_metric,
    date_range,
    expected_metrics,
    expected_sql_params,
    format_table,
    format_value,
    ratio,
    read_realtime,
    reconcile,
    summarize,
)

DAY = date(2015, 7, 14)


def _dwd_row(**overrides):
    row = {
        "event_date_utc": DAY,
        "pv": 100,
        "uv": 40,
        "dau": 45,
        "cart_users": 10,
        "buyer_users": 5,
        "orders": 6,
        "strict_users": 4,
        "items_viewed": 30,
        "items_carted": 8,
        "items_bought": 5,
    }
    row.update(overrides)
    return row


def _expected_actual_pair():
    expected = expected_metrics(_dwd_row())
    return expected, dict(expected)


def test_build_expected_sql_is_independent_and_parameterised():
    sql = build_expected_sql()
    assert "FROM dwd_events" in sql
    assert "rt_traffic_daily" not in sql
    assert "COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN transaction_id END)" in sql
    assert "HAVING SUM(event_type = 'view') > 0" in sql
    assert "COALESCE(s.strict_users, 0)" in sql
    assert sql.count("%s") == 4
    assert expected_sql_params(DAY, DAY) == (DAY, DAY, DAY, DAY)


def test_ratio_zero_denominator_is_null():
    assert ratio(0, 0) is None
    assert ratio(3, 0) is None


def test_ratio_rounds_half_up_to_six_places():
    assert ratio(1, 3) == Decimal("0.333333")
    assert ratio(2, 3) == Decimal("0.666667")
    assert ratio(1, 2) == Decimal("0.500000")


def test_expected_metrics_derives_all_13_metrics():
    metrics = expected_metrics(_dwd_row())
    assert set(metrics) == set(ALL_METRICS)
    assert metrics["pv"] == Decimal(100)
    assert metrics["view_users"] == Decimal(40)
    assert metrics["add_to_cart_rate"] == Decimal("0.250000")
    assert metrics["conv_strict_rate"] == Decimal("0.100000")
    assert metrics["conv_global_rate"] == Decimal("0.125000")
    assert metrics["items_bought"] == Decimal(5)


def test_expected_metrics_zero_views_yield_null_rates():
    metrics = expected_metrics(_dwd_row(uv=0, cart_users=0, buyer_users=0, strict_users=0))
    assert metrics["add_to_cart_rate"] is None
    assert metrics["conv_strict_rate"] is None
    assert metrics["conv_global_rate"] is None


def test_compare_day_all_match():
    expected, actual = _expected_actual_pair()
    comparisons = compare_day(DAY, expected, actual, is_final=1)
    assert len(comparisons) == 13
    assert all(row.status == "MATCH" for row in comparisons)
    assert all_match(comparisons)
    assert summarize(comparisons) == (13, 0)


def test_compare_day_detects_pv_diff():
    expected, actual = _expected_actual_pair()
    actual["pv"] = Decimal(101)
    comparisons = compare_day(DAY, expected, actual, is_final=0)
    pv = next(row for row in comparisons if row.metric == "pv")
    assert pv.status == "DIFF"
    assert pv.abs_diff == Decimal(1)
    assert summarize(comparisons) == (12, 1)


def test_missing_actual_key_is_diff_even_when_expected_null():
    expected = expected_metrics(_dwd_row(uv=0, cart_users=0, buyer_users=0, strict_users=0))
    actual = dict(expected)
    del actual["add_to_cart_rate"]
    row = next(
        item
        for item in compare_day(DAY, expected, actual, is_final=0)
        if item.metric == "add_to_cart_rate"
    )
    assert row.expected is None
    assert row.actual is None
    assert row.status == "DIFF"


def test_present_null_actual_matches_expected_null():
    expected = expected_metrics(_dwd_row(uv=0, cart_users=0, buyer_users=0, strict_users=0))
    actual = dict(expected)
    actual["conv_strict_rate"] = None
    row = next(
        item
        for item in compare_day(DAY, expected, actual, is_final=0)
        if item.metric == "conv_strict_rate"
    )
    assert row.status == "MATCH"


def test_compare_metric_null_value_differs_from_zero():
    row = compare_metric(DAY, "add_to_cart_rate", None, Decimal("0.000000"))
    assert row.status == "DIFF"
    assert row.abs_diff is None


def test_reconcile_reports_missing_day_as_all_diff():
    expected = {DAY: expected_metrics(_dwd_row())}
    comparisons = reconcile(expected, {}, {}, [DAY])
    assert len(comparisons) == 13
    assert all(row.status == "DIFF" for row in comparisons)


def test_date_range_and_invalid_order():
    days = date_range(date(2015, 7, 14), date(2015, 7, 16))
    assert days == [date(2015, 7, 14), date(2015, 7, 15), date(2015, 7, 16)]
    with pytest.raises(ValueError):
        date_range(date(2015, 7, 16), date(2015, 7, 14))


def test_format_table_marks_success_and_failure():
    expected, actual = _expected_actual_pair()
    ok_table = format_table(compare_day(DAY, expected, actual, is_final=1))
    assert "VERIFY_RT_VS_DWD_OK" in ok_table
    assert "SUMMARY matched=13 diff=0 total=13" in ok_table

    actual["dau"] = Decimal(999)
    bad_table = format_table(compare_day(DAY, expected, actual, is_final=1))
    assert "VERIFY_RT_VS_DWD_FAILED" in bad_table
    assert "DIFF" in bad_table


def test_format_value_renders_integral_decimals_without_decimal_point():
    assert format_value(Decimal("26007")) == "26007"
    assert format_value(Decimal("0.500000")) == "0.500000"
    assert format_value(None) == "NULL"


def test_all_match_requires_non_empty_comparisons():
    assert not all_match([])


class _FakeCursor:
    """Small fake used to exercise the three RT read queries without a database."""

    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params))

    def fetchall(self):
        return self.responses[len(self.calls) - 1]


def test_read_realtime_merges_three_tables_and_defaults_missing_funnel_metrics():
    responses = [
        [
            {
                "event_date_utc": DAY,
                "pv": 100,
                "uv": 40,
                "dau": 45,
                "is_final": 1,
            }
        ],
        [
            {
                "event_date_utc": DAY,
                "metric_name": "view_users",
                "metric_value": Decimal("40.000000"),
            },
            {
                "event_date_utc": DAY,
                "metric_name": "conv_strict_rate",
                "metric_value": None,
            },
        ],
        [
            {
                "event_date_utc": DAY,
                "items_viewed": 30,
                "items_carted": 8,
                "items_bought": 5,
            }
        ],
    ]
    cursor = _FakeCursor(responses)
    actual, final_flags = read_realtime(cursor, DAY, DAY)
    assert final_flags == {DAY: 1}
    assert actual[DAY]["pv"] == Decimal(100)
    assert actual[DAY]["view_users"] == Decimal("40.000000")
    assert actual[DAY]["conv_strict_rate"] is None
    assert "buyer_users" in actual[DAY] and actual[DAY]["buyer_users"] is None
    assert actual[DAY]["items_bought"] == Decimal(5)
    assert len(cursor.calls) == 3
    assert all("BETWEEN %s AND %s" in call[0] for call in cursor.calls)
    assert all(call[1] == (DAY, DAY) for call in cursor.calls)


def test_read_realtime_empty_tables_yield_empty_mapping():
    cursor = _FakeCursor([[], [], []])
    actual, final_flags = read_realtime(cursor, DAY, DAY)
    assert actual == {}
    assert final_flags == {}
