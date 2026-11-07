"""Pure-logic tests for the Kafka replay producer (Phase 9.2, no broker)."""

from __future__ import annotations

import json
import time
from datetime import date

import pytest

from ecommerce_dw.streaming import replay


def test_producer_config_matches_design_4_3():
    config = replay.build_producer_config("localhost:9092", delivery_timeout_ms=90_000)
    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["acks"] == "all"
    assert config["enable.idempotence"] is True
    assert config["compression.type"] == "lz4"
    assert config["max.in.flight.requests.per.connection"] <= 5
    assert config["max.in.flight.requests.per.connection"] == 5
    assert config["delivery.timeout.ms"] == 90_000
    assert config["request.timeout.ms"] <= config["delivery.timeout.ms"]
    assert config["retries"] > 0


def test_producer_config_allows_inflight_override_below_limit():
    config = replay.build_producer_config("kafka:9092")
    assert config["delivery.timeout.ms"] == replay.DEFAULT_DELIVERY_TIMEOUT_MS
    assert config["max.in.flight.requests.per.connection"] <= 5


def test_producer_config_rejects_non_positive_delivery_timeout():
    with pytest.raises(ValueError):
        replay.build_producer_config("localhost:9092", delivery_timeout_ms=0)


def test_resolve_bootstrap_precedence(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "env-broker:9092")
    assert replay.resolve_bootstrap("cli-broker:9092") == "cli-broker:9092"
    assert replay.resolve_bootstrap(None) == "env-broker:9092"
    monkeypatch.delenv("KAFKA_BOOTSTRAP")
    assert replay.resolve_bootstrap(None) == "localhost:9092"


def test_query_orders_by_event_time_then_event_sk():
    sql = replay.validate_query_args(date(2015, 7, 14), date(2015, 7, 14), None)
    assert "FROM dwd_events" in sql
    assert "event_date_utc BETWEEN %s AND %s" in sql
    assert "ORDER BY event_time_utc, event_sk" in sql
    assert "LIMIT" not in sql


def test_query_limit_and_range_validation():
    sql = replay.validate_query_args(date(2015, 7, 14), date(2015, 7, 15), 10)
    assert sql.endswith("LIMIT 10")
    with pytest.raises(ValueError):
        replay.validate_query_args(date(2015, 7, 15), date(2015, 7, 14), None)
    with pytest.raises(ValueError):
        replay.validate_query_args(date(2015, 7, 14), date(2015, 7, 14), 0)


def test_failure_spool_writes_jsonl_with_payload(tmp_path):
    spool = tmp_path / "failed.jsonl"
    item = replay.PendingMessage(
        topic="ecommerce.events.v1",
        key=b"12345",
        value=b'{"event_id":"x"}',
        payload={"event_id": "x", "visitor_id": 12345},
        attempts=3,
        error="Local: Message timed out",
    )
    replay.write_failure_spool(spool, [item])
    record = json.loads(spool.read_text(encoding="utf-8").strip())
    assert record["topic"] == "ecommerce.events.v1"
    assert record["key"] == "12345"
    assert record["payload"] == {"event_id": "x", "visitor_id": 12345}
    assert record["attempts"] == 3
    assert record["error"] == "Local: Message timed out"
    assert "failed_at" in record


def test_failure_spool_writes_nothing_when_empty(tmp_path):
    spool = tmp_path / "failed.jsonl"
    replay.write_failure_spool(spool, [])
    assert not spool.exists()


def test_default_spool_path_is_under_logs():
    path = replay._default_spool_path()
    assert path.parent == replay.DEFAULT_SPOOL_DIR
    assert path.name.startswith("replay_failed_")
    assert path.suffix == ".jsonl"


def test_replay_stats_throughput_positive():
    stats = replay.ReplayStats(acked=100, started_at=time.monotonic() - 1.0)
    assert stats.throughput > 0


def test_throttle_does_not_sleep_without_rate(monkeypatch):
    monkeypatch.setattr(replay.time, "sleep", lambda _seconds: pytest.fail("must not sleep"))
    replay._throttle(1_000, rate=0, started_at=0.0)


class _BackpressuredProducer:
    """First two produce calls raise BufferError, third succeeds."""

    def __init__(self) -> None:
        self.calls = 0
        self.polls = 0

    def produce(self, *_args, **_kwargs) -> None:
        self.calls += 1
        if self.calls < 3:
            raise BufferError("queue full")

    def poll(self, _timeout: float) -> None:
        self.polls += 1


def test_delivery_tracker_retries_buffer_error():
    producer = _BackpressuredProducer()
    stats = replay.ReplayStats()
    tracker = replay.DeliveryTracker(producer, stats)
    item = replay.PendingMessage(
        topic="t",
        key=b"1",
        value=b"{}",
        payload={"event_id": "x"},
    )
    tracker.send(item)
    assert producer.calls == 3
    assert producer.polls >= 2
    assert item in tracker.inflight.values()
