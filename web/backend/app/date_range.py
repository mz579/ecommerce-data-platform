"""日期区间解析与校验。

默认区间 = ``ads_traffic_daily`` 的 min/max（SPEC §3）；显式传入时校验
``date_from <= date_to``，非法组合返回 400（而不是让 SQL 返回空结果或 500）。
"""

from __future__ import annotations

from datetime import date

from fastapi import HTTPException

from . import db
from .config import settings

MYSQL_MIN_DATE = date(1000, 1, 1)
MYSQL_MAX_DATE = date(9999, 12, 31)


def validate_mysql_date(value: date | None, name: str) -> None:
    """MySQL DATE 只支持 1000-01-01..9999-12-31；越界按 400 处理而非 SQL 500。"""
    if value is not None and not (MYSQL_MIN_DATE <= value <= MYSQL_MAX_DATE):
        raise HTTPException(
            status_code=400,
            detail=f"{name} must be between 1000-01-01 and 9999-12-31",
        )


def resolve_date_range(
    date_from: date | None,
    date_to: date | None,
) -> tuple[date | None, date | None]:
    """返回最终使用的 (from, to)；无数据时返回 (None, None)。"""
    validate_mysql_date(date_from, "date_from")
    validate_mysql_date(date_to, "date_to")
    if date_from is not None and date_to is not None:
        if date_from > date_to:
            raise HTTPException(status_code=400, detail="date_from must be <= date_to")
        return date_from, date_to

    row = db.POOL.fetch_one(
        f"SELECT MIN(event_date_utc) AS date_min, MAX(event_date_utc) AS date_max "
        f"FROM {db.table_ref(settings.dw_database, 'ads_traffic_daily')}"
    )
    date_min = row["date_min"] if row else None
    date_max = row["date_max"] if row else None

    resolved_from = date_from if date_from is not None else date_min
    resolved_to = date_to if date_to is not None else date_max
    if resolved_from is None or resolved_to is None:
        return None, None
    if resolved_from > resolved_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    return resolved_from, resolved_to


def inclusive_days(date_from: date | None, date_to: date | None) -> int:
    if date_from is None or date_to is None:
        return 0
    return (date_to - date_from).days + 1
