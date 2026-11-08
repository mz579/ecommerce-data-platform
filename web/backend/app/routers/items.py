"""``/api/items``：商品 / 类目排行与商品行为日趋势（SPEC §3.4）。"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query

from .. import db
from ..config import settings
from ..date_range import resolve_date_range
from ..serialization import as_int, as_iso

router = APIRouter()

MAX_LIMIT = 100


@router.get("/items")
def items(
    date_from: date | None = None,
    date_to: date | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = 20,
) -> dict[str, object]:
    resolved_from, resolved_to = resolve_date_range(date_from, date_to)
    if resolved_from is None or resolved_to is None:
        return {"items": [], "categories": [], "behavior": []}

    ranking_table = db.table_ref(settings.dw_database, "ads_item_ranking_daily")
    category_view = db.table_ref(settings.dw_database, "v_ads_category_daily")
    behavior_table = db.table_ref(settings.dw_database, "ads_item_behavior_daily")
    params = (resolved_from, resolved_to)

    item_rows = db.POOL.fetch_all(
        f"SELECT item_id, SUM(view_cnt) AS view_cnt, "
        f"SUM(add_to_cart_cnt) AS add_to_cart_cnt, "
        f"SUM(transaction_cnt) AS transaction_cnt, "
        f"SUM(view_uv) AS view_uv, SUM(transaction_uv) AS transaction_uv "
        f"FROM {ranking_table} WHERE event_date_utc BETWEEN %s AND %s "
        f"GROUP BY item_id ORDER BY SUM(transaction_cnt) DESC, item_id DESC LIMIT %s",
        (resolved_from, resolved_to, limit),
    )
    category_rows = db.POOL.fetch_all(
        f"SELECT category_id, SUM(view_cnt) AS view_cnt, SUM(order_cnt) AS order_cnt, "
        f"SUM(view_uv) AS view_uv, SUM(transaction_uv) AS transaction_uv "
        f"FROM {category_view} WHERE event_date_utc BETWEEN %s AND %s "
        f"GROUP BY category_id ORDER BY SUM(view_cnt) DESC, category_id ASC LIMIT 15",
        params,
    )
    behavior_rows = db.POOL.fetch_all(
        f"SELECT event_date_utc, items_viewed, items_carted, items_bought "
        f"FROM {behavior_table} WHERE event_date_utc BETWEEN %s AND %s "
        f"ORDER BY event_date_utc",
        params,
    )

    return {
        "items": [
            {
                "item_id": as_int(row["item_id"]),
                "view_cnt": as_int(row["view_cnt"]),
                "add_to_cart_cnt": as_int(row["add_to_cart_cnt"]),
                "transaction_cnt": as_int(row["transaction_cnt"]),
                "view_uv": as_int(row["view_uv"]),
                "transaction_uv": as_int(row["transaction_uv"]),
            }
            for row in item_rows
        ],
        "categories": [
            {
                "category_id": as_int(row["category_id"]),
                "view_cnt": as_int(row["view_cnt"]),
                "order_cnt": as_int(row["order_cnt"]),
                "view_uv": as_int(row["view_uv"]),
                "transaction_uv": as_int(row["transaction_uv"]),
            }
            for row in category_rows
        ],
        "behavior": [
            {
                "date": as_iso(row["event_date_utc"]),
                "items_viewed": as_int(row["items_viewed"]),
                "items_carted": as_int(row["items_carted"]),
                "items_bought": as_int(row["items_bought"]),
            }
            for row in behavior_rows
        ],
    }
