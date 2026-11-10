"""事件类型与时间的规范化。

口径与 ``docs/metric_definitions.md`` 对齐：view / add_to_cart / transaction /
other 四类；时间统一转成 **naive UTC**，写入 DuckDB 的 TIMESTAMP。
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil import parser as date_parser

# 归一值 -> 允许的原始写法（比较前会去掉空格、下划线、连字符并转小写）
EVENT_ALIASES: dict[str, tuple[str, ...]] = {
    "view": ("view", "pv", "pageview", "browse", "click", "浏览", "查看", "曝光"),
    "add_to_cart": ("addtocart", "cart", "addcart", "加购", "加入购物车"),
    "transaction": (
        "transaction",
        "purchase",
        "order",
        "pay",
        "buy",
        "购买",
        "下单",
        "支付",
        "成交",
    ),
}

_ALIAS_LOOKUP: dict[str, str] = {
    alias.replace("_", "").replace("-", "").replace(" ", ""): canonical
    for canonical, aliases in EVENT_ALIASES.items()
    for alias in aliases
}

_EVENT_KEY_RE = re.compile(r"[\s_\-]+")
_NUMERIC_EPOCH_RE = re.compile(r"^\d{10}$|^\d{13}$|^\d{16}$")
_DATE8_RE = re.compile(r"^\d{8}$")
_AMOUNT_STRIP_RE = re.compile(r"[,\s¥￥$€£]")


def _event_key(value: object) -> str:
    return _EVENT_KEY_RE.sub("", str(value).strip().lower())


def normalize_event(value: object) -> str:
    """把任意事件值归一为 view / add_to_cart / transaction / other。"""
    if value is None:
        return "other"
    key = _event_key(value)
    if not key:
        return "other"
    return _ALIAS_LOOKUP.get(key, "other")


def is_known_event(value: object) -> bool:
    return normalize_event(value) != "other"


def _to_naive_utc(dt: datetime, tz_name: str) -> datetime | None:
    try:
        if dt.tzinfo is None:
            zone = ZoneInfo(tz_name) if tz_name else UTC
            dt = dt.replace(tzinfo=zone)
        converted = dt.astimezone(UTC).replace(tzinfo=None)
        # 排除 dateutil 把 "888" 解释成公元 888 年这类伪时间。
        if not 1970 <= converted.year <= 2100:
            return None
        return converted
    except (ZoneInfoNotFoundError, ValueError, OverflowError, OSError):
        return None


def _from_epoch(value: float) -> datetime | None:
    if value <= 0:
        return None
    try:
        if value >= 1e15:  # 微秒
            seconds = value / 1_000_000
        elif value >= 1e12:  # 毫秒
            seconds = value / 1_000
        else:
            seconds = value
        return datetime.fromtimestamp(seconds, tz=UTC).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None


def parse_timestamp(value: object, tz_name: str = "UTC") -> datetime | None:
    """解析常见时间写法；失败返回 None。

    * 10 位数字 → Unix 秒；13 位 → 毫秒；16 位 → 微秒；
    * 8 位 ``YYYYMMDD`` → 当日 00:00；
    * 字符串优先 ISO 8601，再由 dateutil 兜底；
    * Excel 日期单元格由 openpyxl 直接给出 datetime，不走数字分支。
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return _to_naive_utc(value, tz_name)
    if isinstance(value, date):
        return _to_naive_utc(datetime(value.year, value.month, value.day), tz_name)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        # 只有明显是 epoch 的量级才按时间解释，避免把普通数字/Excel 序列号当时间。
        return _from_epoch(float(value)) if float(value) >= 1e9 else None

    text = str(value).strip()
    if not text:
        return None

    if _NUMERIC_EPOCH_RE.fullmatch(text):
        return _from_epoch(float(text))

    if _DATE8_RE.fullmatch(text):
        try:
            return _to_naive_utc(datetime.strptime(text, "%Y%m%d"), tz_name)
        except ValueError:
            return None

    # 纯数字但既不是 epoch 也不是 YYYYMMDD：不要交给 dateutil 猜年份。
    if text.isdigit():
        return None

    iso_text = text[:-1] + "+00:00" if text.endswith(("Z", "z")) else text
    try:
        return _to_naive_utc(datetime.fromisoformat(iso_text), tz_name)
    except ValueError:
        pass

    try:
        return _to_naive_utc(date_parser.parse(text, fuzzy=False), tz_name)
    except (ValueError, OverflowError, TypeError, date_parser.ParserError):
        return None


def parse_amount(value: object) -> float | None:
    """金额：容忍千分位、货币符号与括号负数；失败返回 None。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = _AMOUNT_STRIP_RE.sub("", str(value).strip())
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


def validate_timezone(name: str) -> str:
    """校验 IANA 时区；非法值抛 ValueError（接口层映射 400）。"""
    candidate = (name or "UTC").strip()
    try:
        ZoneInfo(candidate)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {candidate}") from exc
    return candidate
