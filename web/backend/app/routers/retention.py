"""``/api/retention``：cohort 留存矩阵、D1 曲线与加权 D1（SPEC §3.5）。"""

from __future__ import annotations

from datetime import date
from typing import Annotated

from fastapi import APIRouter, Query

from .. import db
from ..config import settings
from ..date_range import resolve_date_range
from ..serialization import as_float, as_int, as_iso, safe_ratio

router = APIRouter()

MAX_AGE_LIMIT = 30


@router.get("/retention")
def retention(
    date_from: date | None = None,
    date_to: date | None = None,
    max_age: Annotated[int, Query(ge=0, le=MAX_AGE_LIMIT)] = 14,
) -> dict[str, object]:
    resolved_from, resolved_to = resolve_date_range(date_from, date_to)
    if resolved_from is None or resolved_to is None:
        return {"cohorts": [], "d1_series": [], "weighted_d1": None}

    retention_table = db.table_ref(settings.dw_database, "ads_retention_cohort")
    rows = db.POOL.fetch_all(
        f"SELECT cohort_date, age_day, cohort_size, retained_users, retention_rate, "
        f"window_status FROM {retention_table} "
        f"WHERE cohort_date BETWEEN %s AND %s AND age_day BETWEEN 0 AND %s "
        f"ORDER BY cohort_date, age_day",
        (resolved_from, resolved_to, max_age),
    )

    cohorts: list[dict[str, object]] = []
    cohort_index: dict[date, dict[str, object]] = {}
    for row in rows:
        cohort_date = row["cohort_date"]
        cohort = cohort_index.get(cohort_date)
        if cohort is None:
            cohort = {
                "cohort_date": as_iso(cohort_date),
                "cohort_size": as_int(row["cohort_size"]),
                "window_status": row["window_status"],
                "values": [],
            }
            cohort_index[cohort_date] = cohort
            cohorts.append(cohort)
        values = cohort["values"]
        assert isinstance(values, list)
        rate = (
            as_float(row["retention_rate"])
            if row["retention_rate"] is not None
            else safe_ratio(row["retained_users"], row["cohort_size"])
        )
        values.append(
            {
                "age_day": as_int(row["age_day"]),
                "rate": rate,
                "window_status": row["window_status"],
            }
        )

    d1_rows = [row for row in rows if as_int(row["age_day"]) == 1]
    # d1_series 保留全部 cohort（含 partial，前端标 *）；加权只算完整窗口。
    d1_full_rows = [row for row in d1_rows if row["window_status"] == "full"]
    d1_retained = sum(as_int(row["retained_users"]) for row in d1_full_rows)
    d1_cohort = sum(as_int(row["cohort_size"]) for row in d1_full_rows)

    return {
        "cohorts": cohorts,
        "d1_series": [
            {
                "date": as_iso(row["cohort_date"]),
                "d1_rate": (
                    as_float(row["retention_rate"])
                    if row["retention_rate"] is not None
                    else safe_ratio(row["retained_users"], row["cohort_size"])
                ),
                "window_status": row["window_status"],
            }
            for row in d1_rows
        ],
        "weighted_d1": safe_ratio(d1_retained, d1_cohort),
    }
