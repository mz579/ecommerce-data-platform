"""DuckDB 聚合与结果快照。

指标口径与现有 ``docs/metric_definitions.md`` 对齐；结果写入 ``result.json``，
结果接口只读快照，不在请求线程里做重计算。
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb

from .store import iso, utcnow

logger = logging.getLogger(__name__)

EVENT_LABELS = {
    "view": "浏览",
    "add_to_cart": "加购",
    "transaction": "购买",
    "other": "其他",
}


def _rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict[str, Any]]:
    cursor = connection.execute(sql)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _one(connection: duckdb.DuckDBPyConnection, sql: str) -> dict[str, Any]:
    rows = _rows(connection, sql)
    return rows[0] if rows else {}


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _json_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _json_value(value) for key, value in row.items()}


def _ratio(numerator: Any, denominator: Any) -> float | None:
    try:
        n = float(numerator or 0)
        d = float(denominator or 0)
    except (TypeError, ValueError):
        return None
    if d <= 0:
        return None
    return round(n / d, 6)


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def aggregate(
    db_path: Path,
    *,
    mapping: dict[str, Any],
    ingest_stats: dict[str, Any],
    build_stats: dict[str, Any],
    filename: str,
    size_bytes: int,
    file_type: str,
    encoding: str,
    elapsed_seconds: float,
) -> dict[str, Any]:
    """读取 fact_events，生成结果 JSON 结构（不落盘）。"""
    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        bounds = _one(
            connection,
            """
            SELECT MIN(event_date) AS date_min,
                   MAX(event_date) AS date_max,
                   COUNT(DISTINCT event_date) AS days
            FROM fact_events
            """,
        )
        totals = _one(
            connection,
            """
            SELECT
                COUNT(*) AS events,
                COUNT(*) FILTER (WHERE event_type = 'view') AS pv,
                COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'view') AS uv,
                COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_events,
                COUNT(*) FILTER (WHERE event_type = 'transaction') AS purchase_events,
                COUNT(
                    DISTINCT CASE WHEN event_type = 'transaction' THEN transaction_id END
                ) AS orders,
                COALESCE(SUM(amount) FILTER (WHERE event_type = 'transaction'), 0) AS revenue,
                COUNT(*) FILTER (WHERE event_type = 'other') AS other_events
            FROM fact_events
            """,
        )
        daily = [
            _json_row(row)
            for row in _rows(
                connection,
                """
                SELECT
                    event_date,
                    COUNT(*) FILTER (WHERE event_type = 'view') AS pv,
                    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'view') AS uv,
                    COUNT(DISTINCT user_id) AS dau,
                    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS cart_events,
                    COUNT(*) FILTER (WHERE event_type = 'transaction') AS purchase_events,
                    COUNT(
                    DISTINCT CASE WHEN event_type = 'transaction' THEN transaction_id END
                ) AS orders,
                    COALESCE(SUM(amount) FILTER (WHERE event_type = 'transaction'), 0) AS revenue
                FROM fact_events
                GROUP BY event_date
                ORDER BY event_date
                """,
            )
        ]
        funnel = _one(
            connection,
            """
            WITH viewers AS (
                SELECT DISTINCT user_id FROM fact_events WHERE event_type = 'view'
            ),
            carters AS (
                SELECT DISTINCT user_id FROM fact_events WHERE event_type = 'add_to_cart'
            ),
            buyers AS (
                SELECT DISTINCT user_id FROM fact_events WHERE event_type = 'transaction'
            )
            SELECT
                (SELECT COUNT(*) FROM viewers) AS view_users,
                (SELECT COUNT(*) FROM carters) AS cart_users,
                (SELECT COUNT(*) FROM buyers) AS buyer_users,
                (
                    SELECT COUNT(*) FROM buyers b
                    WHERE EXISTS (SELECT 1 FROM viewers v WHERE v.user_id = b.user_id)
                ) AS strict_conv_users
            """,
        )
        daily_users = [
            _json_row(row)
            for row in _rows(
                connection,
                """
                WITH day_users AS (
                    SELECT
                        event_date,
                        user_id,
                        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS viewed,
                        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS carted,
                        MAX(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS bought
                    FROM fact_events
                    GROUP BY event_date, user_id
                )
                SELECT
                    event_date,
                    SUM(viewed) AS view_users,
                    SUM(carted) AS cart_users,
                    SUM(bought) AS buyer_users,
                    SUM(CASE WHEN viewed = 1 AND bought = 1 THEN 1 ELSE 0 END) AS strict_conv_users
                FROM day_users
                GROUP BY event_date
                ORDER BY event_date
                """,
            )
        ]
        retention = [
            _json_row(row)
            for row in _rows(
                connection,
                """
                WITH user_days AS (
                    SELECT DISTINCT user_id, event_date
                    FROM fact_events
                    WHERE user_id IS NOT NULL
                ),
                sizes AS (
                    SELECT event_date AS cohort_date, COUNT(*) AS cohort_size
                    FROM user_days
                    GROUP BY event_date
                ),
                retained AS (
                    SELECT a.event_date AS cohort_date, COUNT(*) AS retained_users
                    FROM user_days a
                    JOIN user_days b
                      ON b.user_id = a.user_id
                     AND b.event_date = a.event_date + 1
                    GROUP BY a.event_date
                )
                SELECT
                    s.cohort_date,
                    s.cohort_size,
                    COALESCE(r.retained_users, 0) AS retained_users
                FROM sizes s
                LEFT JOIN retained r ON r.cohort_date = s.cohort_date
                ORDER BY s.cohort_date
                """,
            )
        ]
        items = [
            _json_row(row)
            for row in _rows(
                connection,
                """
                SELECT
                    item_id,
                    MAX(product_name) AS product_name,
                    COUNT(*) FILTER (WHERE event_type = 'view') AS views,
                    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS carts,
                    COUNT(*) FILTER (WHERE event_type = 'transaction') AS purchases,
                    COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'transaction') AS buyers,
                    COALESCE(SUM(amount) FILTER (WHERE event_type = 'transaction'), 0) AS revenue
                FROM fact_events
                GROUP BY item_id
                ORDER BY purchases DESC, views DESC, item_id
                LIMIT 20
                """,
            )
        ]
        event_counts = {
            row["event_type"]: _as_int(row["cnt"])
            for row in _rows(
                connection,
                "SELECT event_type, COUNT(*) AS cnt FROM fact_events GROUP BY event_type",
            )
        }
        extras = _one(
            connection,
            """
            SELECT
                COUNT(DISTINCT user_id) AS distinct_users,
                COUNT(DISTINCT item_id) AS distinct_items,
                COUNT(*) FILTER (WHERE transaction_id IS NOT NULL) AS with_transaction_id,
                COUNT(*) FILTER (WHERE category_id IS NOT NULL) AS with_category_id,
                COUNT(*) FILTER (WHERE amount IS NOT NULL) AS with_amount,
                COUNT(*) FILTER (WHERE product_name IS NOT NULL) AS with_product_name
            FROM fact_events
            """,
        )

        has_category = (
            bool(mapping.get("category_id")) and _as_int(extras.get("with_category_id")) > 0
        )
        categories: list[dict[str, Any]] = []
        if has_category:
            categories = [
                _json_row(row)
                for row in _rows(
                    connection,
                    """
                    SELECT
                        category_id,
                        COUNT(*) FILTER (WHERE event_type = 'view') AS views,
                        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS carts,
                        COUNT(*) FILTER (WHERE event_type = 'transaction') AS purchases,
                        COUNT(DISTINCT user_id) FILTER (WHERE event_type = 'transaction') AS buyers,
                        COALESCE(
                            SUM(amount) FILTER (WHERE event_type = 'transaction'), 0
                        ) AS revenue
                    FROM fact_events
                    WHERE category_id IS NOT NULL
                    GROUP BY category_id
                    ORDER BY purchases DESC, views DESC, category_id
                    LIMIT 15
                    """,
                )
            ]
    finally:
        connection.close()

    date_min_value = _json_value(bounds.get("date_min"))
    date_max_value = _json_value(bounds.get("date_max"))
    max_date: date | None = None
    if isinstance(date_max_value, str):
        try:
            max_date = date.fromisoformat(date_max_value)
        except ValueError:
            max_date = None

    d1_series: list[dict[str, Any]] = []
    full_cohort = 0
    full_retained = 0
    for row in retention:
        cohort_date = row.get("cohort_date")
        cohort_size = _as_int(row.get("cohort_size"))
        retained_users = _as_int(row.get("retained_users"))
        status = (
            "partial"
            if isinstance(cohort_date, str)
            and cohort_date == (max_date.isoformat() if max_date else None)
            else "full"
        )
        d1_series.append(
            {
                "date": cohort_date,
                "cohort_size": cohort_size,
                "retained_users": retained_users,
                "d1_rate": _ratio(retained_users, cohort_size) if status == "full" else None,
                "window_status": status,
            }
        )
        if status == "full":
            full_cohort += cohort_size
            full_retained += retained_users

    rates_by_date: dict[str, dict[str, Any]] = {}
    for row in daily:
        row["date"] = row.get("event_date")
        rates_by_date[str(row["date"])] = row
    for row in daily_users:
        date_key = str(row.get("event_date"))
        existing = rates_by_date.get(date_key)
        if existing is None:
            base: dict[str, Any] = {"date": date_key, "event_date": date_key}
            rates_by_date[date_key] = base
            daily.append(base)
        else:
            base = existing
        base["view_users"] = _as_int(row.get("view_users"))
        base["cart_users"] = _as_int(row.get("cart_users"))
        base["buyer_users"] = _as_int(row.get("buyer_users"))
        base["strict_conv_users"] = _as_int(row.get("strict_conv_users"))
    for row in daily:
        row["add_to_cart_rate"] = _ratio(row.get("cart_users"), row.get("view_users"))
        row["conv_strict_rate"] = _ratio(row.get("strict_conv_users"), row.get("view_users"))
        row["conv_global_rate"] = _ratio(row.get("buyer_users"), row.get("view_users"))

    view_users = _as_int(funnel.get("view_users"))
    cart_users = _as_int(funnel.get("cart_users"))
    buyer_users = _as_int(funnel.get("buyer_users"))
    strict_users = _as_int(funnel.get("strict_conv_users"))

    handle_transaction = (
        bool(mapping.get("transaction_id")) and _as_int(extras.get("with_transaction_id")) > 0
    )
    orders = (
        _as_int(totals.get("orders"))
        if handle_transaction
        else _as_int(totals.get("purchase_events"))
    )
    has_amount = bool(mapping.get("amount")) and _as_int(extras.get("with_amount")) > 0
    has_product_name = (
        bool(mapping.get("product_name")) and _as_int(extras.get("with_product_name")) > 0
    )
    revenue = _as_float(totals.get("revenue")) if has_amount else None

    daily_dau = [_as_int(row.get("dau")) for row in daily]
    dau_avg = round(sum(daily_dau) / len(daily_dau), 2) if daily_dau else 0

    result: dict[str, Any] = {
        "job_id": None,  # 由 router 填充
        "filename": filename,
        "file_type": file_type,
        "generated_at": iso(utcnow()),
        "elapsed_seconds": round(elapsed_seconds, 2),
        "range": {
            "date_min": date_min_value,
            "date_max": date_max_value,
            "days": _as_int(bounds.get("days")),
        },
        "totals": {
            "events": _as_int(totals.get("events")),
            "pv": _as_int(totals.get("pv")),
            "uv": _as_int(totals.get("uv")),
            "dau_avg": dau_avg,
            "view_users": view_users,
            "cart_users": cart_users,
            "buyer_users": buyer_users,
            "orders": orders,
            "revenue": revenue,
            "add_to_cart_rate": _ratio(cart_users, view_users),
            "conv_strict_rate": _ratio(strict_users, view_users),
            "conv_global_rate": _ratio(buyer_users, view_users),
            "d1_retention": _ratio(full_retained, full_cohort),
        },
        "traffic": daily,
        "funnel": [
            {
                "stage": "view",
                "label": EVENT_LABELS["view"],
                "users": view_users,
            },
            {
                "stage": "add_to_cart",
                "label": EVENT_LABELS["add_to_cart"],
                "users": cart_users,
                "rate": _ratio(cart_users, view_users),
            },
            {
                "stage": "transaction",
                "label": EVENT_LABELS["transaction"],
                "users": buyer_users,
                "rate": _ratio(buyer_users, view_users),
            },
        ],
        "d1_series": d1_series,
        "top_items": items,
        "categories": categories,
        "quality": {
            "rows_read": _as_int(ingest_stats.get("rows_read")),
            "rows_parsed": _as_int(ingest_stats.get("rows_written")),
            "rows_kept": _as_int(build_stats.get("fact_count")),
            "rows_rejected": _as_int(ingest_stats.get("rejected")),
            "rejected_reasons": ingest_stats.get("rejected_reasons") or {},
            "duplicates": _as_int(build_stats.get("duplicates")),
            "unknown_events": _as_int(ingest_stats.get("unknown_events")),
            "other_events_kept": _as_int(totals.get("other_events")),
            "event_types": event_counts,
            "date_min": date_min_value,
            "date_max": date_max_value,
            "days": _as_int(bounds.get("days")),
            "distinct_users": _as_int(extras.get("distinct_users")),
            "distinct_items": _as_int(extras.get("distinct_items")),
            "rows_with_transaction_id": _as_int(extras.get("with_transaction_id")),
            "rows_with_category_id": _as_int(extras.get("with_category_id")),
            "rows_with_amount": _as_int(extras.get("with_amount")),
            "rows_with_product_name": _as_int(extras.get("with_product_name")),
            "has_transaction_id": handle_transaction,
            "has_category_id": has_category,
            "has_amount": has_amount,
            "has_product_name": has_product_name,
            "orders_basis": "transaction_id" if handle_transaction else "purchase_events",
        },
        "provenance": {
            "filename": filename,
            "file_type": file_type,
            "size_bytes": size_bytes,
            "encoding": encoding,
            "timezone": mapping.get("timezone") or "UTC",
            "rows_estimated": ingest_stats.get("rows_total"),
            "rows_kept": _as_int(build_stats.get("fact_count")),
            "parse_seconds": ingest_stats.get("parse_seconds"),
            "elapsed_seconds": round(elapsed_seconds, 2),
            "sample": False,
        },
        "mapping": mapping,
        "has_product_name": has_product_name,
        "has_category_id": has_category,
        "has_amount": has_amount,
    }
    return result


def write_result(path: Path, result: dict[str, Any]) -> None:
    temp = path.parent / (path.name + ".tmp")
    with temp.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
    temp.replace(path)
