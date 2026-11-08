"""JSON 安全转换：Decimal / date / datetime / NaN / Infinity。

SPEC §3 要求响应不得出现 NaN/Infinity。PyMySQL 的 DECIMAL 列返回
``decimal.Decimal``，FastAPI 默认会转 float，但非有限值会序列化失败；
本模块统一在路由层做显式转换（整数值转 int，其余转 float）。
"""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

DETAIL_LIMIT = 500


def as_number(value: Any) -> int | float | None:
    """把数据库数值转成 JSON 友好的 int/float；非有限值返回 None。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def as_int(value: Any, default: int = 0) -> int:
    """计数 / 行数 / 用户数一律输出整数。"""
    number = as_number(value)
    if number is None:
        return default
    return int(number)


def as_float(value: Any) -> float | None:
    number = as_number(value)
    if number is None:
        return None
    return float(number)


def as_iso(value: Any) -> str | None:
    """date/datetime 转 ISO-8601；DB 内时间为 UTC，naive 值补 ``Z``。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat(timespec="microseconds") + "Z"
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def safe_ratio(numerator: Any, denominator: Any) -> float | None:
    """比率口径：分母为空或 0 时返回 None（前端显示 "—"）。"""
    if numerator is None or denominator is None:
        return None
    try:
        num = float(numerator)
        den = float(denominator)
    except (TypeError, ValueError):
        return None
    if den == 0 or not math.isfinite(den) or not math.isfinite(num):
        return None
    result = num / den
    return result if math.isfinite(result) else None


def truncate_detail(value: Any, limit: int = DETAIL_LIMIT) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text[:limit]
