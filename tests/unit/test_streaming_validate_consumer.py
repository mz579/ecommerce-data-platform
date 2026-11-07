"""Pure-logic tests for the validating consumer / DLQ decision (Phase 9.2)."""

from __future__ import annotations

from ecommerce_dw.streaming import validate_consumer
from ecommerce_dw.streaming.events import ValidationReport


class FakeMessage:
    """Minimal duck-typed message for stats tests (no broker needed)."""

    def __init__(self, partition: int, offset: int) -> None:
        self._partition = partition
        self._offset = offset

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset


def test_consumer_config_disables_auto_commit_and_sets_reset():
    config = validate_consumer.build_consumer_config("localhost:9092", "g1")
    assert config["enable.auto.commit"] is False
    assert config["auto.offset.reset"] == "earliest"
    assert config["group.id"] == "g1"
    latest = validate_consumer.build_consumer_config("localhost:9092", "g1", from_beginning=False)
    assert latest["auto.offset.reset"] == "latest"


def test_default_group_id_is_unique_per_run():
    first = validate_consumer.default_group_id()
    second = validate_consumer.default_group_id()
    assert first.startswith("ecommerce-rt-acceptance-")
    assert first != second


def test_dlq_headers_include_reason_source_and_original_payload():
    payload = b'{"event_id":"bad"}'
    headers = validate_consumer.build_dlq_headers(
        error_reason="invalid_json: Expecting value (line 1)",
        source_topic="ecommerce.events.v1",
        partition=2,
        offset=987,
        payload=payload,
    )
    assert dict(headers) == {
        "error_reason": b"invalid_json: Expecting value (line 1)",
        "source_topic": b"ecommerce.events.v1",
        "partition": b"2",
        "offset": b"987",
        "payload": payload,
    }


def test_dlq_headers_default_to_unknown_reason_and_empty_payload():
    headers = dict(
        validate_consumer.build_dlq_headers(
            error_reason=None,
            source_topic="t",
            partition=0,
            offset=0,
            payload=None,
        )
    )
    assert headers["error_reason"] == b"unknown"
    assert headers["payload"] == b""


def test_stats_track_valid_invalid_types_and_offsets():
    stats = validate_consumer.ConsumeStats()
    stats.record(FakeMessage(0, 10), ValidationReport(True, None, "view", "id-1"))
    stats.record(FakeMessage(0, 11), ValidationReport(True, None, "view", "id-2"))
    stats.record(FakeMessage(1, 5), ValidationReport(False, "invalid_json: bad", "view", "id-3"))
    assert stats.total == 3
    assert stats.valid == 2
    assert stats.invalid == 1
    assert stats.event_types["view"] == 2
    assert stats.invalid_reasons["invalid_json"] == 1
    assert stats.partitions[0].consumed_min == 10
    assert stats.partitions[0].consumed_max == 11
    assert stats.partitions[1].consumed_min == 5


def test_stats_duplicate_event_id_accounting():
    stats = validate_consumer.ConsumeStats()
    for offset in (0, 1, 2):
        stats.record(FakeMessage(0, offset), ValidationReport(True, None, "view", "same-id"))
    stats.record(FakeMessage(0, 3), ValidationReport(True, None, "view", "other-id"))
    assert stats.distinct_event_ids == 2
    assert stats.duplicate_event_ids == 1
    assert stats.duplicate_records == 2
    assert stats.valid - stats.distinct_event_ids == 2


def test_invalid_reason_is_grouped_by_prefix():
    stats = validate_consumer.ConsumeStats()
    stats.record(FakeMessage(0, 0), ValidationReport(False, "schema: item_id: bad", None, None))
    stats.record(FakeMessage(0, 1), ValidationReport(False, "schema: event_type: bad", None, None))
    assert stats.invalid_reasons["schema"] == 2
    assert stats.event_types == {}
