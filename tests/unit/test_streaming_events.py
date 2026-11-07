"""Pure-logic tests for the event_v1 message contract (Phase 9.2, no IO)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest

from ecommerce_dw.streaming import events

BASE_ROW = {
    "dedup_key": "a" * 64,
    "event_time_utc": datetime(2015, 7, 14, 0, 0, 0, 553000),
    "visitor_id": 12345,
    "event_type": "view",
    "item_id": 124552,
    "transaction_id": None,
    "raw_event_type": "view",
    "source_file": "events.csv",
    "source_row_number": 2320960,
    "quality_flags": "ok",
}
INGEST = datetime(2026, 11, 7, 6, 0, 0, 123000, tzinfo=UTC)


def _message(**overrides):
    row = {**BASE_ROW, **overrides}
    return events.build_event_message(row, ingest_time=INGEST)


def _valid_message() -> dict:
    return _message()


def test_event_id_is_dedup_key_and_deterministic():
    first = _valid_message()
    second = _valid_message()
    assert first["event_id"] == BASE_ROW["dedup_key"]
    assert first == second


def test_event_time_is_utc_iso_millisecond_with_z():
    message = _valid_message()
    assert message["event_time"] == "2015-07-14T00:00:00.553Z"
    assert message["ingest_time"] == "2026-11-07T06:00:00.123Z"
    assert message["schema_version"] == 1


def test_format_utc_iso_ms_converts_aware_datetime_and_truncates_microseconds():
    aware = datetime(2015, 7, 14, 8, 0, 0, 123456, tzinfo=timezone(timedelta(hours=8)))
    assert events.format_utc_iso_ms(aware) == "2015-07-14T00:00:00.123Z"


def test_null_visitor_is_preserved_and_uses_zero_key():
    message = _message(visitor_id=None, quality_flags="visitor_zero")
    assert message["visitor_id"] is None
    assert events.kafka_key_for_visitor(message["visitor_id"]) == "0"
    assert message["quality_flags"] == ["visitor_zero"]
    assert events.validate_event(message).valid


def test_kafka_key_is_decimal_visitor_id():
    assert events.kafka_key_for_visitor(12345) == "12345"


def test_quality_flags_mapping():
    assert events.parse_quality_flags("ok") == []
    assert events.parse_quality_flags(None) == []
    assert events.parse_quality_flags("") == []
    assert events.parse_quality_flags("visitor_zero") == ["visitor_zero"]
    assert events.parse_quality_flags("visitor_zero, late") == ["visitor_zero", "late"]
    assert _message(quality_flags="ok")["quality_flags"] == []


def test_transaction_event_requires_and_keeps_transaction_id():
    message = _message(
        event_type="transaction",
        transaction_id=90001,
        raw_event_type="transaction",
    )
    assert message["transaction_id"] == 90001
    assert events.validate_event(message).valid


def test_transaction_id_zero_is_accepted_for_source_parity():
    # events.csv line 271795 has transactionid=0; DWD keeps it and ADS counts it
    # as an order, so event_v1 must be able to carry it unchanged.
    message = _message(event_type="transaction", transaction_id=0, raw_event_type="transaction")
    report = events.validate_event(message)
    assert report.valid, report.reason
    assert message["transaction_id"] == 0


def test_negative_transaction_id_is_rejected_by_contract():
    message = _message(event_type="transaction", transaction_id=-1)
    report = events.validate_event(message)
    assert not report.valid
    assert "transaction_id" in (report.reason or "")


def test_non_transaction_with_transaction_id_is_rejected_by_contract():
    message = _message(transaction_id=90001)
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("schema:")


def test_transaction_without_transaction_id_is_rejected_by_contract():
    message = _message(event_type="transaction", transaction_id=None)
    report = events.validate_event(message)
    assert not report.valid
    assert "transaction_id" in (report.reason or "")


def test_valid_message_passes_schema():
    report = events.validate_event(_valid_message())
    assert report.valid
    assert report.event_type == "view"
    assert report.event_id == BASE_ROW["dedup_key"]


def test_invalid_json_goes_to_report():
    report = events.validate_event(b"{not json")
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("invalid_json")


def test_invalid_utf8_goes_to_report():
    report = events.validate_event(b"\xff\xfe")
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("invalid_utf8")


def test_non_object_payload_is_rejected():
    report = events.validate_event("[]")
    assert not report.valid
    assert report.reason == "invalid_payload: not a JSON object"


def test_missing_required_field_is_rejected():
    message = _valid_message()
    del message["item_id"]
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("schema:")


def test_unknown_event_type_is_rejected():
    message = _valid_message()
    message["event_type"] = "purchase"
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("schema:")


def test_event_time_without_z_is_rejected():
    message = _valid_message()
    message["event_time"] = "2015-07-14T00:00:00.553+00:00"
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("schema:")


def test_microsecond_precision_is_rejected():
    message = _valid_message()
    message["event_time"] = "2015-07-14T00:00:00.553123Z"
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason == "schema: event_time: millisecond precision required"


def test_schema_version_above_supported_is_rejected():
    message = _valid_message()
    message["schema_version"] = 2
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason == "unsupported_schema_version: 2"


def test_schema_version_zero_is_rejected_by_schema():
    message = _valid_message()
    message["schema_version"] = 0
    report = events.validate_event(message)
    assert not report.valid
    assert report.reason is not None
    assert report.reason.startswith("schema:")


def test_encode_event_is_utf8_json():
    message = _valid_message()
    encoded = events.encode_event(message)
    assert isinstance(encoded, bytes)
    assert json.loads(encoded.decode("utf-8")) == message


def test_schema_file_is_draft_2020_12():
    schema = events.load_schema()
    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert schema["properties"]["event_id"]["maxLength"] == 128


def test_build_event_message_rejects_non_datetime():
    with pytest.raises(TypeError):
        events.build_event_message({**BASE_ROW, "event_time_utc": "2015-07-14"})
