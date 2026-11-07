"""Independent batch-vs-realtime reconciliation for ``ecommerce_rt`` (Phase 9.3).

This module deliberately **does not import or reuse** the Flink job's aggregation code.
It recomputes the expected daily metrics from ``ecommerce_dw.dwd_events`` with SQL and
compares them to the rows the streaming job upserted into ``ecommerce_rt``.

Comparison semantics (docs/streaming_design.md §8):

- UTC day boundaries ``[D 00:00, D+1 00:00)``;
- PV counts view events including ``visitor_id IS NULL``;
- UV/DAU exclude NULL visitors;
- orders use ``COUNT(DISTINCT transaction_id)``;
- strict conversion uses visitors with both a view and a transaction on the same day;
- rates are ``DECIMAL(20,6)`` with HALF_UP rounding and are NULL when the denominator is 0.

The pure helpers (SQL text, rate calculation, status classification, formatting) are unit
tested; IO lives in ``read_expected`` / ``read_realtime`` and the CLI wrapper.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

RATE_QUANTUM = Decimal("0.000001")

TRAFFIC_METRICS = ("pv", "uv", "dau")
FUNNEL_METRICS = (
    "view_users",
    "cart_users",
    "buyer_users",
    "orders",
    "add_to_cart_rate",
    "conv_strict_rate",
    "conv_global_rate",
)
ITEM_METRICS = ("items_viewed", "items_carted", "items_bought")
ALL_METRICS = TRAFFIC_METRICS + FUNNEL_METRICS + ITEM_METRICS

EXPECTED_SQL = """
WITH per_day AS (
    SELECT event_date_utc,
           SUM(event_type = 'view') AS pv,
           COUNT(DISTINCT CASE WHEN event_type = 'view' THEN visitor_id END) AS uv,
           COUNT(DISTINCT visitor_id) AS dau,
           COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN visitor_id END)
               AS cart_users,
           COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN visitor_id END)
               AS buyer_users,
           COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN transaction_id END)
               AS orders,
           COUNT(DISTINCT CASE WHEN event_type = 'view' THEN item_id END) AS items_viewed,
           COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN item_id END)
               AS items_carted,
           COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN item_id END)
               AS items_bought
    FROM dwd_events
    WHERE event_date_utc BETWEEN %s AND %s
    GROUP BY event_date_utc
),
strict AS (
    SELECT event_date_utc, COUNT(*) AS strict_users
    FROM (
        SELECT event_date_utc, visitor_id
        FROM dwd_events
        WHERE event_date_utc BETWEEN %s AND %s
          AND visitor_id IS NOT NULL
        GROUP BY event_date_utc, visitor_id
        HAVING SUM(event_type = 'view') > 0
           AND SUM(event_type = 'transaction') > 0
    ) visitor_flags
    GROUP BY event_date_utc
)
SELECT p.event_date_utc,
       p.pv, p.uv, p.dau, p.cart_users, p.buyer_users, p.orders,
       COALESCE(s.strict_users, 0) AS strict_users,
       p.items_viewed, p.items_carted, p.items_bought
FROM per_day p
LEFT JOIN strict s ON s.event_date_utc = p.event_date_utc
ORDER BY p.event_date_utc
""".strip()


def build_expected_sql() -> str:
    """Return the independent DWD aggregate SQL (two date-range parameter pairs)."""
    return EXPECTED_SQL


def rt_recon_credentials(environ: Mapping[str, str]) -> tuple[str, str]:
    """Resolve the read-only RT reconciliation account.

    Reconciliation must never use ``rt_app`` (that account has DML rights). The
    dedicated ``rt_recon_ro`` account is read-only on ``ecommerce_rt.*`` and the
    seven ``ecommerce_dw.ads_*`` tables (Phase 9.2).
    """
    user = environ.get("RT_RECON_RO_USER") or "rt_recon_ro"
    password = environ.get("RT_RECON_RO_PASSWORD")
    if not password:
        raise KeyError("RT_RECON_RO_PASSWORD")
    return user, password


def expected_sql_params(date_from: date, date_to: date) -> tuple[date, date, date, date]:
    return (date_from, date_to, date_from, date_to)


def ratio(numerator: int | Decimal, denominator: int | Decimal) -> Decimal | None:
    """Six-decimal rate, NULL when the denominator is zero."""
    if denominator == 0:
        return None
    return (Decimal(numerator) / Decimal(denominator)).quantize(
        RATE_QUANTUM, rounding=ROUND_HALF_UP
    )


def expected_metrics(row: Mapping[str, Any]) -> dict[str, Decimal | None]:
    """Build the 13 comparable metrics from one DWD aggregate row."""
    pv = Decimal(int(row["pv"] or 0))
    uv = Decimal(int(row["uv"] or 0))
    dau = Decimal(int(row["dau"] or 0))
    cart_users = Decimal(int(row["cart_users"] or 0))
    buyer_users = Decimal(int(row["buyer_users"] or 0))
    orders = Decimal(int(row["orders"] or 0))
    strict_users = Decimal(int(row["strict_users"] or 0))
    return {
        "pv": pv,
        "uv": uv,
        "dau": dau,
        "view_users": uv,
        "cart_users": cart_users,
        "buyer_users": buyer_users,
        "orders": orders,
        "add_to_cart_rate": ratio(cart_users, uv),
        "conv_strict_rate": ratio(strict_users, uv),
        "conv_global_rate": ratio(buyer_users, uv),
        "items_viewed": Decimal(int(row["items_viewed"] or 0)),
        "items_carted": Decimal(int(row["items_carted"] or 0)),
        "items_bought": Decimal(int(row["items_bought"] or 0)),
    }


@dataclass(frozen=True)
class MetricComparison:
    event_date: date
    metric: str
    expected: Decimal | None
    actual: Decimal | None
    abs_diff: Decimal | None
    status: str
    is_final: int | None = None


def compare_metric(
    event_date: date,
    metric: str,
    expected: Decimal | None,
    actual: Decimal | None,
    *,
    is_final: int | None = None,
) -> MetricComparison:
    """Classify one metric pair; missing values never count as MATCH."""
    if expected == actual:
        status = "MATCH"
    else:
        status = "DIFF"
    diff = actual - expected if actual is not None and expected is not None else None
    return MetricComparison(
        event_date=event_date,
        metric=metric,
        expected=expected,
        actual=actual,
        abs_diff=diff,
        status=status,
        is_final=is_final,
    )


def compare_day(
    event_date: date,
    expected: Mapping[str, Decimal | None],
    actual: Mapping[str, Decimal | None],
    *,
    is_final: int | None,
) -> list[MetricComparison]:
    """Compare all 13 metrics for one day.

    ``actual`` lacking a key means the RT row/metric is missing; that is a DIFF even when
    the expected value is NULL (an absent row is not the same as a NULL rate).
    """
    comparisons: list[MetricComparison] = []
    for metric in ALL_METRICS:
        if metric not in actual:
            comparisons.append(
                MetricComparison(
                    event_date=event_date,
                    metric=metric,
                    expected=expected.get(metric),
                    actual=None,
                    abs_diff=None,
                    status="DIFF",
                    is_final=is_final,
                )
            )
        else:
            comparisons.append(
                compare_metric(
                    event_date,
                    metric,
                    expected.get(metric),
                    actual[metric],
                    is_final=is_final,
                )
            )
    return comparisons


def reconcile(
    expected_by_date: Mapping[date, Mapping[str, Decimal | None]],
    actual_by_date: Mapping[date, Mapping[str, Decimal | None]],
    final_flags: Mapping[date, int | None],
    dates: Iterable[date],
) -> list[MetricComparison]:
    comparisons: list[MetricComparison] = []
    for event_date in dates:
        if event_date not in expected_by_date:
            expected = {}
        else:
            expected = expected_by_date[event_date]
        actual = actual_by_date.get(event_date, {})
        comparisons.extend(
            compare_day(
                event_date,
                expected,
                actual,
                is_final=final_flags.get(event_date),
            )
        )
    return comparisons


def all_match(comparisons: Sequence[MetricComparison]) -> bool:
    return bool(comparisons) and all(row.status == "MATCH" for row in comparisons)


def summarize(comparisons: Sequence[MetricComparison]) -> tuple[int, int]:
    matched = sum(1 for row in comparisons if row.status == "MATCH")
    return matched, len(comparisons) - matched


def format_value(value: Decimal | None) -> str:
    if value is None:
        return "NULL"
    if value == value.to_integral_value():
        return str(int(value))
    return format(value, "f")


def format_table(comparisons: Sequence[MetricComparison]) -> str:
    header = (
        f"{'date':10s} {'metric':20s} {'expected':>14s} {'realtime':>14s} "
        f"{'diff':>10s} {'status':6s} {'is_final':>8s}"
    )
    lines = [header, "-" * len(header)]
    for row in comparisons:
        diff = "" if row.abs_diff is None else format_value(row.abs_diff)
        is_final = "NULL" if row.is_final is None else str(row.is_final)
        lines.append(
            f"{row.event_date.isoformat():10s} {row.metric:20s} "
            f"{format_value(row.expected):>14s} {format_value(row.actual):>14s} "
            f"{diff:>10s} {row.status:6s} {is_final:>8s}"
        )
    matched, diffs = summarize(comparisons)
    lines.append("")
    lines.append(f"SUMMARY matched={matched} diff={diffs} total={len(comparisons)}")
    lines.append("VERIFY_RT_VS_DWD_" + ("OK" if diffs == 0 else "FAILED"))
    return "\n".join(lines)


def read_expected(
    cursor: Any, date_from: date, date_to: date
) -> dict[date, dict[str, Decimal | None]]:
    """Execute the independent DWD aggregate and return expected metrics per day."""
    cursor.execute(build_expected_sql(), expected_sql_params(date_from, date_to))
    result: dict[date, dict[str, Decimal | None]] = {}
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        result[event_date] = expected_metrics(row)
    return result


def read_realtime(
    cursor: Any, date_from: date, date_to: date
) -> tuple[dict[date, dict[str, Decimal | None]], dict[date, int]]:
    """Read the three RT tables and merge them into one metric mapping per day.

    The cursor must already be connected to ``ecommerce_rt``.
    """
    actual: dict[date, dict[str, Decimal | None]] = {}
    final_flags: dict[date, int] = {}

    cursor.execute(
        "SELECT event_date_utc, pv, uv, dau, is_final FROM rt_traffic_daily "
        "WHERE event_date_utc BETWEEN %s AND %s",
        (date_from, date_to),
    )
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        actual[event_date] = {
            "pv": Decimal(int(row["pv"])),
            "uv": Decimal(int(row["uv"])),
            "dau": Decimal(int(row["dau"])),
        }
        final_flags[event_date] = int(row["is_final"])

    cursor.execute(
        "SELECT event_date_utc, metric_name, metric_value FROM rt_funnel_daily "
        "WHERE event_date_utc BETWEEN %s AND %s",
        (date_from, date_to),
    )
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        value = row["metric_value"]
        actual.setdefault(event_date, {})[row["metric_name"]] = (
            None if value is None else Decimal(value)
        )

    cursor.execute(
        "SELECT event_date_utc, items_viewed, items_carted, items_bought "
        "FROM rt_item_behavior_daily WHERE event_date_utc BETWEEN %s AND %s",
        (date_from, date_to),
    )
    for row in cursor.fetchall():
        event_date = row["event_date_utc"]
        actual.setdefault(event_date, {}).update(
            {
                "items_viewed": Decimal(int(row["items_viewed"])),
                "items_carted": Decimal(int(row["items_carted"])),
                "items_bought": Decimal(int(row["items_bought"])),
            }
        )

    # A missing funnel metric is represented explicitly as NULL only when the day row
    # exists; otherwise the key stays absent so compare_day reports a DIFF.
    for metrics in actual.values():
        for metric in FUNNEL_METRICS:
            metrics.setdefault(metric, None)
    return actual, final_flags


def date_range(date_from: date, date_to: date) -> list[date]:
    if date_to < date_from:
        raise ValueError(f"date_to ({date_to}) must be >= date_from ({date_from})")
    days: list[date] = []
    current = date_from
    while current <= date_to:
        days.append(current)
        current = date.fromordinal(current.toordinal() + 1)
    return days
