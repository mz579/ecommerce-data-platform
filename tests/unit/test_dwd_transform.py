"""Unit tests for the pure DWD transform (no database).

Covers the mapping (view / addtocart->add_to_cart / transaction / unknown),
timestamp normalization, sentinel handling, transaction semantics, quality
flags and the business-key rule.
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from ecommerce_dw.dwd.transform import (
    EVENT_TYPE_MAP,
    FLAG_VISITOR_ZERO,
    TRANSFORM_VERSION,
    RejectReason,
    date_ms_range,
    make_dedup_key,
    ms_to_utc_date,
    ms_to_utc_datetime,
    parse_bigint,
    parse_ts_ms,
    transform_event,
)

# A stable reference millisecond value (2015-05-03 03:00:04.384 UTC).
_MS = 1_430_622_004_384


# --------------------------------------------------------------------------- #
# event-type mapping (the master-plan alignment is exercised here)            #
# --------------------------------------------------------------------------- #


def test_mapping_addtocart_becomes_add_to_cart_but_raw_is_preserved():
    out = transform_event(str(_MS), "1", "addtocart", "10", "")
    assert out.is_ok
    assert out.event_type == "add_to_cart"
    assert EVENT_TYPE_MAP["addtocart"] == "add_to_cart"
    assert out.quality_flags_csv == "ok"


def test_mapping_view_and_transaction_pass_through():
    assert transform_event(str(_MS), "1", "view", "10", "").event_type == "view"
    txn = transform_event(str(_MS), "1", "transaction", "10", "99")
    assert txn.event_type == "transaction"
    assert txn.transaction_id == 99


def test_unknown_event_type_is_rejected_with_raw_name():
    out = transform_event(str(_MS), "1", "purchase", "10", "")
    assert out.status == "REJECT"
    assert out.event_type == "unknown"
    assert out.reject_reason == RejectReason.UNKNOWN_EVENT_TYPE
    assert "purchase" in (out.reject_message or "")


def test_standard_event_types_set_matches_pipeline_contract():
    assert sorted(EVENT_TYPE_MAP) == ["addtocart", "transaction", "view"]
    assert sorted(EVENT_TYPE_MAP.values()) == ["add_to_cart", "transaction", "view"]


# --------------------------------------------------------------------------- #
# timestamp normalization                                                     #
# --------------------------------------------------------------------------- #


def test_valid_millis_to_datetime_and_date_utc():
    out = transform_event(str(_MS), "1", "view", "10", "")
    assert out.event_time_utc == datetime(2015, 5, 3, 3, 0, 4, 384000)
    assert out.event_date_utc == date(2015, 5, 3)
    assert ms_to_utc_datetime(_MS) == datetime(2015, 5, 3, 3, 0, 4, 384000)
    assert ms_to_utc_date(_MS) == date(2015, 5, 3)


def test_day_boundary_is_utc_midnight():
    day_ms = _MS - (_MS % 86_400_000)
    assert ms_to_utc_date(day_ms) == date(2015, 5, 3)
    assert ms_to_utc_date(day_ms - 1) == date(2015, 5, 2)


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "abc", "12.5", "0", "-1", "-1000", "0x10", "1e3", None],
)
def test_invalid_timestamps_are_rejected(raw):
    value = "" if raw is None else raw
    out = transform_event(value, "1", "view", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_TIMESTAMP


def test_parse_ts_ms_whitespace_is_stripped():
    assert parse_ts_ms("  1430622004384  ") == _MS


def test_huge_timestamp_out_of_datetime_range_is_rejected():
    out = transform_event("10" * 30, "1", "view", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_TIMESTAMP


# --------------------------------------------------------------------------- #
# visitor handling                                                            #
# --------------------------------------------------------------------------- #


def test_visitor_zero_sentinel_becomes_null_with_flag():
    out = transform_event(str(_MS), "0", "view", "10", "")
    assert out.is_ok
    assert out.visitor_id is None
    assert FLAG_VISITOR_ZERO in out.quality_flags
    assert "visitor_zero" in out.quality_flags_csv


def test_missing_visitor_is_rejected():
    out = transform_event(str(_MS), "", "view", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.MISSING_VISITOR


def test_non_numeric_visitor_is_rejected_not_defaulted():
    out = transform_event(str(_MS), "abc", "view", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_VISITOR_ID


def test_out_of_bigint_range_visitor_is_rejected():
    out = transform_event(str(_MS), str(2**63), "view", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_VISITOR_ID


# --------------------------------------------------------------------------- #
# item handling                                                               #
# --------------------------------------------------------------------------- #


def test_missing_item_is_rejected():
    out = transform_event(str(_MS), "1", "view", "", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.MISSING_ITEM


def test_non_numeric_item_is_rejected():
    out = transform_event(str(_MS), "1", "view", "abc", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_ITEM_ID


def test_item_out_of_bigint_range_is_rejected():
    out = transform_event(str(_MS), "1", "view", str(-(2**63) - 1), "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_ITEM_ID


# --------------------------------------------------------------------------- #
# transactionid semantics                                                     #
# --------------------------------------------------------------------------- #


def test_transaction_requires_transactionid():
    out = transform_event(str(_MS), "1", "transaction", "10", "")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.MISSING_TXN_ON_TRANSACTION


def test_non_transaction_with_transactionid_is_rejected():
    out = transform_event(str(_MS), "1", "view", "10", "99")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.UNEXPECTED_TXNID


def test_non_numeric_transactionid_is_rejected():
    out = transform_event(str(_MS), "1", "transaction", "10", "abc")
    assert out.status == "REJECT"
    assert out.reject_reason == RejectReason.INVALID_TRANSACTION_ID


def test_ok_event_carries_validated_transaction_id():
    out = transform_event(str(_MS), "1", "transaction", "10", "9001")
    assert out.is_ok
    assert out.transaction_id == 9001


# --------------------------------------------------------------------------- #
# business key                                                                #
# --------------------------------------------------------------------------- #


def test_business_key_is_deterministic():
    a = make_dedup_key(_MS, 7, "view", 10, None)
    b = make_dedup_key(_MS, 7, "view", 10, None)
    assert a == b
    assert len(a) == 64
    assert all(c in "0123456789abcdef" for c in a)


def test_business_key_changes_when_any_component_changes():
    base = (_MS, 7, "view", 10, None)
    variants = [
        (_MS + 1, 7, "view", 10, None),
        (_MS, 8, "view", 10, None),
        (_MS, 7, "add_to_cart", 10, None),
        (_MS, 7, "view", 11, None),
        (_MS, 7, "view", 10, 5),
    ]
    keys = {make_dedup_key(*base)}
    for v in variants:
        keys.add(make_dedup_key(*v))
    assert len(keys) == 1 + len(variants)


def test_business_key_matches_coalesce_transaction_rule():
    # NULL and 0 transaction_id collapse onto the same key (COALESCE(txn, 0)).
    assert make_dedup_key(_MS, 7, "transaction", 10, None) == make_dedup_key(
        _MS, 7, "transaction", 10, 0
    )


def test_business_key_distinguishes_null_visitor_from_real_ids():
    # NULL visitor (sentinel "0") and any real positive visitor differ.
    assert make_dedup_key(_MS, None, "view", 10, None) != make_dedup_key(_MS, 0, "view", 10, None)
    assert make_dedup_key(_MS, None, "view", 10, None) != make_dedup_key(_MS, 1, "view", 10, None)


def test_transform_produces_same_key_as_make_dedup_key():
    out = transform_event(str(_MS), "7", "addtocart", "10", "")
    assert out.dedup_key == make_dedup_key(_MS, 7, "add_to_cart", 10, None)


# --------------------------------------------------------------------------- #
# helpers and flags                                                           #
# --------------------------------------------------------------------------- #


def test_quality_flags_ok_default_and_sorted_once():
    assert transform_event(str(_MS), "7", "view", "10", "").quality_flags_csv == "ok"
    out = transform_event(str(_MS), "0", "view", "10", "")
    assert out.quality_flags_csv == "visitor_zero"  # single flag, no trailing comma


def test_date_ms_range_covers_the_whole_utc_day():
    start, end = date_ms_range(date(2015, 5, 3))
    assert end - start == 86_400_000
    assert ms_to_utc_date(start) == date(2015, 5, 3)
    assert ms_to_utc_date(end - 1) == date(2015, 5, 3)
    assert ms_to_utc_date(end) == date(2015, 5, 4)


def test_parse_bigint_boundaries():
    assert parse_bigint(str(2**63 - 1), field="itemid") == 2**63 - 1
    assert parse_bigint(str(-(2**63)), field="itemid") == -(2**63)
    with pytest.raises(ValueError):
        parse_bigint(str(2**63), field="itemid")
    with pytest.raises(ValueError):
        parse_bigint("", field="itemid")


def test_transform_version_is_stable_for_crash_resume_guard():
    assert TRANSFORM_VERSION == "1"
