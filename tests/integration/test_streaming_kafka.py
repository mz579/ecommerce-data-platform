"""Kafka integration tests for the Phase 9.2 replay/validate/DLQ path.

A unique throw-away topic pair (``ecommerce.events.selftest.<uuid>`` + its
``.dlq``) is created per test and deleted afterwards. The production topics
(``ecommerce.events.v1`` / ``.dlq``) are never referenced here.

Kafka being unreachable skips the module with an explicit reason, matching the
existing integration-test convention.
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic, OffsetSpec

from ecommerce_dw.streaming.events import (
    build_event_message,
    encode_event,
    kafka_key_for_visitor,
)
from ecommerce_dw.streaming.replay import build_producer_config
from ecommerce_dw.streaming.validate_consumer import (
    build_consumer_config,
    consume_and_validate,
)

BOOTSTRAP = "localhost:9092"
TOPIC_PREFIX = "ecommerce.events.selftest."
INGEST_TIME = datetime(2026, 11, 7, 6, 0, 0, tzinfo=UTC)


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:8]


def _unique_group(prefix: str) -> str:
    return f"{prefix}-{_unique_suffix()}"


@pytest.fixture(scope="module")
def kafka_admin() -> AdminClient:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP, "client.id": "ecommerce-selftest-admin"})
    try:
        admin.list_topics(timeout=5)
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Kafka not reachable at {BOOTSTRAP}: {type(exc).__name__}: {exc}")
    return admin


@pytest.fixture
def test_topics(kafka_admin: AdminClient):
    """Create a unique test topic pair; delete both after the test."""
    created: list[str] = []

    def _create() -> tuple[str, str]:
        suffix = _unique_suffix()
        topic = f"{TOPIC_PREFIX}{suffix}"
        dlq = f"{topic}.dlq"
        futures = kafka_admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=1,
                    replication_factor=1,
                    config={"cleanup.policy": "delete", "retention.ms": "3600000"},
                ),
                NewTopic(
                    dlq,
                    num_partitions=1,
                    replication_factor=1,
                    config={"cleanup.policy": "delete", "retention.ms": "3600000"},
                ),
            ],
            operation_timeout=10,
        )
        for name, future in futures.items():
            future.result(timeout=10)
            created.append(name)
        assert topic.startswith(TOPIC_PREFIX)
        return topic, dlq

    yield _create

    if created:
        futures = kafka_admin.delete_topics(created, operation_timeout=10)
        for future in futures.values():
            try:
                future.result(timeout=10)
            except KafkaException as exc:  # pragma: no cover - cleanup best effort
                print(f"[teardown] delete topic failed: {exc}")


def _sample_rows(count: int = 10) -> list[dict]:
    base = datetime(2015, 7, 14, 0, 0, 0)
    rows = []
    for index in range(count):
        rows.append(
            {
                "dedup_key": f"{index + 1:064x}",
                "event_time_utc": base.replace(microsecond=index * 1000),
                "visitor_id": None if index == 0 else 1000 + index,
                "event_type": "transaction" if index % 5 == 4 else "view",
                "item_id": 5000 + index,
                "transaction_id": 9000 + index if index % 5 == 4 else None,
                "raw_event_type": "transaction" if index % 5 == 4 else "view",
                "source_file": "events.csv",
                "source_row_number": index + 2,
                "quality_flags": "ok",
            }
        )
    return rows


def _latest_offset(admin: AdminClient, topic: str, partition: int = 0) -> int:
    tp = TopicPartition(topic, partition)
    futures = admin.list_offsets({tp: OffsetSpec.latest()})
    return futures[tp].result(timeout=10).offset


def _produce(producer: Producer, topic: str, items: list[tuple[bytes | None, bytes]]) -> None:
    errors: list[str] = []

    def _callback(err, _msg) -> None:
        if err is not None:
            errors.append(str(err))

    for key, value in items:
        producer.produce(topic, key=key, value=value, on_delivery=_callback)
    remaining = producer.flush(30)
    assert remaining == 0
    assert errors == []


def _consume_one(topic: str, timeout_s: float = 20.0):
    consumer = Consumer(
        build_consumer_config(
            BOOTSTRAP, _unique_group("ecommerce-selftest-dlq"), from_beginning=True
        )
    )
    consumer.subscribe([topic])
    try:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                raise AssertionError(f"Kafka error: {msg.error()}")
            return msg
        raise AssertionError(f"no message received from {topic} within {timeout_s}s")
    finally:
        consumer.close()


def test_produce_and_validate_roundtrip(kafka_admin, test_topics):
    topic, dlq = test_topics()
    rows = _sample_rows(10)
    messages = [build_event_message(row, ingest_time=INGEST_TIME) for row in rows]

    producer = Producer(build_producer_config(BOOTSTRAP, client_id="ecommerce-selftest-produce"))
    _produce(
        producer,
        topic,
        [
            (kafka_key_for_visitor(message["visitor_id"]).encode(), encode_event(message))
            for message in messages
        ],
    )

    stats = consume_and_validate(
        bootstrap=BOOTSTRAP,
        topic=topic,
        dlq_topic=dlq,
        group=_unique_group("ecommerce-selftest-consume"),
        from_beginning=True,
        max_messages=len(messages),
        idle_timeout=15,
    )

    assert stats.total == 10
    assert stats.valid == 10
    assert stats.invalid == 0
    assert stats.distinct_event_ids == 10
    assert stats.duplicate_event_ids == 0
    assert stats.event_types["view"] == 8
    assert stats.event_types["transaction"] == 2
    assert stats.partitions[0].beginning == 0
    assert stats.partitions[0].end == 10
    assert stats.partitions[0].consumed_min == 0
    assert stats.partitions[0].consumed_max == 9
    assert _latest_offset(kafka_admin, dlq) == 0


def test_invalid_json_lands_in_test_dlq(kafka_admin, test_topics):
    topic, dlq = test_topics()
    bad_payload = b'{"event_id": "broken", "event_type": '
    producer = Producer(build_producer_config(BOOTSTRAP, client_id="ecommerce-selftest-bad"))
    _produce(producer, topic, [(b"0", bad_payload)])

    stats = consume_and_validate(
        bootstrap=BOOTSTRAP,
        topic=topic,
        dlq_topic=dlq,
        group=_unique_group("ecommerce-selftest-consume-bad"),
        from_beginning=True,
        max_messages=1,
        idle_timeout=15,
    )
    assert stats.total == 1
    assert stats.valid == 0
    assert stats.invalid == 1
    assert stats.invalid_reasons["invalid_json"] == 1
    assert _latest_offset(kafka_admin, dlq) == 1

    dlq_message = _consume_one(dlq)
    headers = dict(dlq_message.headers() or [])
    assert dlq_message.value() == bad_payload
    assert headers["source_topic"] == topic.encode()
    assert headers["partition"] == b"0"
    assert headers["offset"] == b"0"
    assert headers["payload"] == bad_payload
    assert headers["error_reason"].startswith(b"invalid_json")
