"""``/api/realtime``：实时结果表状态、批流对账与迟到事件（SPEC §3.6）。"""

from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException

from .. import db
from ..config import settings
from ..date_range import validate_mysql_date
from ..serialization import as_int, as_iso, as_number

router = APIRouter()

_RT_TABLES = (
    "rt_traffic_daily",
    "rt_funnel_daily",
    "rt_item_behavior_daily",
)


def _table_stats(table: str) -> dict[str, object]:
    ref = db.table_ref(settings.rt_database, table)
    row = db.POOL.fetch_one(
        f"SELECT COUNT(*) AS row_count, COALESCE(SUM(is_final), 0) AS final_rows, "
        f"MAX(event_date_utc) AS latest_date, MAX(updated_at) AS updated_at FROM {ref}"
    )
    return {
        "table": table,
        "rows": as_int(row["row_count"]) if row else 0,
        "final_rows": as_int(row["final_rows"]) if row else 0,
        "latest_date": as_iso(row["latest_date"]) if row else None,
        "updated_at": as_iso(row["updated_at"]) if row else None,
    }


@router.get("/realtime")
def realtime(date_from: date | None = None, date_to: date | None = None) -> dict[str, object]:
    validate_mysql_date(date_from, "date_from")
    validate_mysql_date(date_to, "date_to")
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")

    recon_table = db.table_ref(settings.rt_database, "rt_reconciliation_daily")
    late_table = db.table_ref(settings.rt_database, "rt_late_events")

    status_rows = db.POOL.fetch_all(
        f"SELECT status, COUNT(*) AS cnt FROM {recon_table} GROUP BY status"
    )
    counts = {"MATCH": 0, "DIFF": 0, "SKIP": 0}
    for row in status_rows:
        if row["status"] in counts:
            counts[row["status"]] = as_int(row["cnt"])
    checked = db.POOL.fetch_one(f"SELECT MAX(checked_at) AS checked_at FROM {recon_table}")

    if date_from is not None or date_to is not None:
        conditions = []
        params: list[object] = []
        if date_from is not None:
            conditions.append("event_date_utc >= %s")
            params.append(date_from)
        if date_to is not None:
            conditions.append("event_date_utc <= %s")
            params.append(date_to)
        date_rows = db.POOL.fetch_all(
            f"SELECT DISTINCT event_date_utc FROM {recon_table} "
            f"WHERE {' AND '.join(conditions)} ORDER BY event_date_utc",
            params,
        )
    else:
        date_rows = db.POOL.fetch_all(
            f"SELECT DISTINCT event_date_utc FROM {recon_table} "
            f"ORDER BY event_date_utc DESC LIMIT 7"
        )
        date_rows.reverse()

    compare: list[dict[str, object]] = []
    dates = [row["event_date_utc"] for row in date_rows]
    if dates:
        placeholders = ", ".join(["%s"] * len(dates))
        compare_rows = db.POOL.fetch_all(
            f"SELECT event_date_utc, metric_name, offline_value, realtime_value, status "
            f"FROM {recon_table} WHERE event_date_utc IN ({placeholders}) "
            f"ORDER BY event_date_utc, metric_name",
            dates,
        )
        compare = [
            {
                "date": as_iso(row["event_date_utc"]),
                "metric": row["metric_name"],
                "offline": as_number(row["offline_value"]),
                "realtime": as_number(row["realtime_value"]),
                "status": row["status"],
            }
            for row in compare_rows
        ]

    late = db.POOL.fetch_one(f"SELECT COUNT(*) AS cnt FROM {late_table}")
    return {
        "tables": [_table_stats(table) for table in _RT_TABLES],
        "reconciliation": {
            "MATCH": counts["MATCH"],
            "DIFF": counts["DIFF"],
            "SKIP": counts["SKIP"],
            "checked_at": as_iso(checked["checked_at"]) if checked else None,
        },
        "compare": compare,
        "late_events": as_int(late["cnt"]) if late else 0,
    }
