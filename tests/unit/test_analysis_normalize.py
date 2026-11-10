"""上传分析：事件/时间/金额规范化。"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from app.analysis.normalize import (
    normalize_event,
    parse_amount,
    parse_timestamp,
    validate_timezone,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("view", "view"),
        ("PV", "view"),
        ("page_view", "view"),
        ("浏览", "view"),
        ("AddToCart", "add_to_cart"),
        ("add_to_cart", "add_to_cart"),
        ("加入购物车", "add_to_cart"),
        ("TRANSACTION", "transaction"),
        ("order", "transaction"),
        ("购买", "transaction"),
        ("收藏", "other"),
        ("", "other"),
        (None, "other"),
    ],
)
def test_normalize_event(raw: object, expected: str) -> None:
    assert normalize_event(raw) == expected


def test_parse_epoch_seconds_and_millis() -> None:
    assert parse_timestamp("1433116800") == datetime(2015, 6, 1, 0, 0, 0)
    assert parse_timestamp("1433116800000") == datetime(2015, 6, 1, 0, 0, 0)
    assert parse_timestamp(1433116800) == datetime(2015, 6, 1, 0, 0, 0)


def test_parse_timezone_aware_string_to_utc() -> None:
    assert parse_timestamp("2015-06-01 08:00:00", "Asia/Shanghai") == datetime(2015, 6, 1, 0, 0, 0)
    aware = datetime(2015, 6, 1, 8, 0, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    assert parse_timestamp(aware) == datetime(2015, 6, 1, 0, 0, 0)


def test_parse_datetime_and_invalid() -> None:
    assert parse_timestamp(datetime(2015, 6, 1, 12, 30)) == datetime(2015, 6, 1, 12, 30)
    assert parse_timestamp("not-a-date") is None
    assert parse_timestamp("888") is None
    assert parse_timestamp("") is None


def test_parse_amount_tolerates_currency_and_commas() -> None:
    assert parse_amount("¥1,234.50") == 1234.5
    assert parse_amount("(12.5)") == -12.5
    assert parse_amount("abc") is None
    assert parse_amount(None) is None


def test_validate_timezone() -> None:
    assert validate_timezone("UTC") == "UTC"
    with pytest.raises(ValueError):
        validate_timezone("Mars/Olympus")
