"""``/api/overview``：区间 KPI、日趋势、漏斗、Top 商品与 D1 留存（SPEC §3.3）。

口径严格按 SPEC：

* ``pv = SUM(ads_traffic_daily.pv)``，UV/DAU 为日均（AVG）；
* 订单 = ``SUM(ads_funnel_daily.metric_value WHERE metric_name='orders')``；
* 三个转化率一律 **加权** ``SUM(分子用户) / SUM(view_users)``（分母为 0 → null）；
* D1 = ``SUM(retained_users) / SUM(cohort_size)``（age_day=1）。
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import APIRouter

from .. import db
from ..config import settings
from ..date_range import inclusive_days, resolve_date_range
from ..serialization import as_float, as_int, as_iso, as_number, safe_ratio

router = APIRouter()

_METRICS = (
    "view_users",
    "cart_users",
    "buyer_users",
    "orders",
    "conv_strict_users",
    "conv_global_users",
)


def _sum_metric(rows: list[dict[str, object]], name: str) -> Decimal:
    total = Decimal(0)
    for row in rows:
        if row["metric_name"] == name and row["metric_value"] is not None:
            total += Decimal(row["metric_value"])  # type: ignore[arg-type]
    return total


def _empty_overview() -> dict[str, object]:
    return {
        "range": {"from": None, "to": None, "days": 0},
        "kpis": {
            "pv": 0,
            "uv_daily_avg": None,
            "dau_daily_avg": None,
            "orders": 0,
            "view_users": 0,
            "cart_users": 0,
            "buyer_users": 0,
            "add_to_cart_rate": None,
            "conv_strict_rate": None,
            "conv_global_rate": None,
            "d1_retention": None,
        },
        "traffic": [],
        "rates": [],
        "funnel": [
            {"stage": "view", "users": 0},
            {"stage": "cart", "users": 0},
            {"stage": "buy", "users": 0},
        ],
        "top_items": [],
        "d1_series": [],
    }


@router.get("/overview")
def overview(date_from: date | None = None, date_to: date | None = None) -> dict[str, object]:
    resolved_from, resolved_to = resolve_date_range(date_from, date_to)
    if resolved_from is None or resolved_to is None:
        return _empty_overview()

    params = (resolved_from, resolved_to)
    traffic_table = db.table_ref(settings.dw_database, "ads_traffic_daily")
    funnel_table = db.table_ref(settings.dw_database, "ads_funnel_daily")
    ranking_table = db.table_ref(settings.dw_database, "ads_item_ranking_daily")
    retention_table = db.table_ref(settings.dw_database, "ads_retention_cohort")

    traffic_rows = db.POOL.fetch_all(
        f"SELECT event_date_utc, pv, uv, dau FROM {traffic_table} "
        f"WHERE event_date_utc BETWEEN %s AND %s ORDER BY event_date_utc",
        params,
    )
    metric_placeholders = ", ".join(["%s"] * len(_METRICS))
    funnel_rows = db.POOL.fetch_all(
        f"SELECT event_date_utc, metric_name, metric_value FROM {funnel_table} "
        f"WHERE event_date_utc BETWEEN %s AND %s "
        f"AND metric_name IN ({metric_placeholders}) ORDER BY event_date_utc",
        (resolved_from, resolved_to, *_METRICS),
    )
    top_rows = db.POOL.fetch_all(
        f"SELECT item_id, SUM(view_cnt) AS vcnt, SUM(add_to_cart_cnt) AS acnt, "
        f"SUM(transaction_cnt) AS tcnt FROM {ranking_table} "
        f"WHERE event_date_utc BETWEEN %s AND %s "
        f"GROUP BY item_id ORDER BY SUM(transaction_cnt) DESC, item_id DESC LIMIT 10",
        params,
    )
    retention_rows = db.POOL.fetch_all(
        f"SELECT cohort_date, retained_users, cohort_size, retention_rate, window_status "
        f"FROM {retention_table} WHERE age_day = 1 "
        f"AND cohort_date BETWEEN %s AND %s ORDER BY cohort_date",
        params,
    )

    total_pv = sum(as_int(row["pv"]) for row in traffic_rows)
    uv_avg = (
        sum(as_int(row["uv"]) for row in traffic_rows) / len(traffic_rows) if traffic_rows else None
    )
    dau_avg = (
        sum(as_int(row["dau"]) for row in traffic_rows) / len(traffic_rows)
        if traffic_rows
        else None
    )

    view_users = _sum_metric(funnel_rows, "view_users")
    cart_users = _sum_metric(funnel_rows, "cart_users")
    buyer_users = _sum_metric(funnel_rows, "buyer_users")
    orders = _sum_metric(funnel_rows, "orders")
    strict_users = _sum_metric(funnel_rows, "conv_strict_users")
    global_users = _sum_metric(funnel_rows, "conv_global_users")

    # 加权 D1 只使用完整窗口 cohort（window_status='full'）；partial cohort
    # 仍保留在 d1_series 中供前端画曲线并标注，但不参与加权。
    d1_full_rows = [row for row in retention_rows if row["window_status"] == "full"]
    d1_retained = sum(
        as_int(row["retained_users"]) for row in d1_full_rows if row["retained_users"] is not None
    )
    d1_cohort = sum(
        as_int(row["cohort_size"]) for row in d1_full_rows if row["cohort_size"] is not None
    )

    daily_metrics: dict[date, dict[str, object]] = {}
    for row in funnel_rows:
        daily_metrics.setdefault(row["event_date_utc"], {})[row["metric_name"]] = row[
            "metric_value"
        ]
    rates = []
    for event_date in sorted(daily_metrics):
        metrics = daily_metrics[event_date]
        denominators = as_number(metrics.get("view_users"))
        rates.append(
            {
                "date": as_iso(event_date),
                "add_to_cart_rate": safe_ratio(metrics.get("cart_users"), denominators),
                "conv_strict_rate": safe_ratio(metrics.get("conv_strict_users"), denominators),
                "conv_global_rate": safe_ratio(metrics.get("conv_global_users"), denominators),
            }
        )

    return {
        "range": {
            "from": as_iso(resolved_from),
            "to": as_iso(resolved_to),
            "days": inclusive_days(resolved_from, resolved_to),
        },
        "kpis": {
            "pv": total_pv,
            "uv_daily_avg": as_float(uv_avg),
            "dau_daily_avg": as_float(dau_avg),
            "orders": as_int(orders),
            "view_users": as_int(view_users),
            "cart_users": as_int(cart_users),
            "buyer_users": as_int(buyer_users),
            "add_to_cart_rate": safe_ratio(cart_users, view_users),
            "conv_strict_rate": safe_ratio(strict_users, view_users),
            "conv_global_rate": safe_ratio(global_users, view_users),
            "d1_retention": safe_ratio(d1_retained, d1_cohort),
        },
        "traffic": [
            {
                "date": as_iso(row["event_date_utc"]),
                "pv": as_int(row["pv"]),
                "uv": as_int(row["uv"]),
                "dau": as_int(row["dau"]),
            }
            for row in traffic_rows
        ],
        "rates": rates,
        "funnel": [
            {"stage": "view", "users": as_int(view_users)},
            {"stage": "cart", "users": as_int(cart_users)},
            {"stage": "buy", "users": as_int(buyer_users)},
        ],
        "top_items": [
            {
                "item_id": as_int(row["item_id"]),
                "vcnt": as_int(row["vcnt"]),
                "acnt": as_int(row["acnt"]),
                "tcnt": as_int(row["tcnt"]),
            }
            for row in top_rows
        ],
        "d1_series": [
            {
                "date": as_iso(row["cohort_date"]),
                "d1_rate": safe_ratio(row["retained_users"], row["cohort_size"]),
            }
            for row in retention_rows
        ],
    }
