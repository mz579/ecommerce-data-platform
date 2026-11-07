"""Kafka 消费 / schema 校验 / DLQ 生产者（Phase 9.2）。

流程：从指定 topic 消费 → 用 ``streaming/schemas/event_v1.json`` 逐条校验 →
合法消息计数并提交 offset；非法消息原样 produce 到 ``<topic>.dlq``（headers
携带 error_reason / source_topic / partition / offset / payload），随后同样
提交 offset。结束时输出总量、合法/非法数、event_type 分布、distinct event_id、
重复 event_id 与各分区 offset 区间。

CLI 入口：``scripts/consume_validate.py``（本模块 ``main``）。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer
from dotenv import load_dotenv

from ecommerce_dw.streaming.events import (
    ValidationReport,
    get_validator,
    validate_event,
)
from ecommerce_dw.streaming.replay import (
    build_producer_config,
    resolve_bootstrap,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TOPIC = "ecommerce.events.v1"


@dataclass
class PartitionStats:
    """单个分区的消费 offset 区间与消息量。"""

    consumed_min: int | None = None
    consumed_max: int | None = None
    messages: int = 0
    beginning: int | None = None
    end: int | None = None


@dataclass
class ConsumeStats:
    """消费校验的累计统计。"""

    total: int = 0
    valid: int = 0
    invalid: int = 0
    event_types: Counter[str] = field(default_factory=Counter)
    event_ids: Counter[str] = field(default_factory=Counter)
    invalid_reasons: Counter[str] = field(default_factory=Counter)
    partitions: dict[int, PartitionStats] = field(
        default_factory=lambda: defaultdict(PartitionStats)
    )
    started_at: float = field(default_factory=time.monotonic)
    last_progress: float = field(default_factory=time.monotonic)

    @property
    def elapsed_seconds(self) -> float:
        return max(time.monotonic() - self.started_at, 1e-9)

    @property
    def distinct_event_ids(self) -> int:
        return len(self.event_ids)

    @property
    def duplicate_event_ids(self) -> int:
        """出现次数 > 1 的 event_id 个数（distinct id 维度）。"""
        return sum(1 for count in self.event_ids.values() if count > 1)

    @property
    def duplicate_records(self) -> int:
        """重复消息条数 = 合法消息数 - distinct event_id 数。"""
        return self.valid - self.distinct_event_ids

    def record(self, msg: Message, report: ValidationReport) -> None:
        self.total += 1
        partition = self.partitions[msg.partition()]
        partition.messages += 1
        offset = msg.offset()
        if partition.consumed_min is None or offset < partition.consumed_min:
            partition.consumed_min = offset
        if partition.consumed_max is None or offset > partition.consumed_max:
            partition.consumed_max = offset
        if report.valid:
            self.valid += 1
            if report.event_type is not None:
                self.event_types[report.event_type] += 1
            if report.event_id is not None:
                self.event_ids[report.event_id] += 1
        else:
            self.invalid += 1
            reason = (report.reason or "unknown").split(":", 1)[0]
            self.invalid_reasons[reason] += 1


def build_consumer_config(
    bootstrap: str,
    group: str,
    *,
    from_beginning: bool = True,
    client_id: str = "ecommerce-validate",
) -> dict[str, Any]:
    """构造校验消费者配置；关闭自动提交，由处理逻辑显式提交。"""
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group,
        "client.id": client_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.partition.eof": False,
        "session.timeout.ms": 10_000,
        "max.poll.interval.ms": 300_000,
    }


def default_group_id() -> str:
    """默认验收消费者组：UTC 时间戳 + PID + 随机后缀，保证每次运行独立。"""
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"ecommerce-rt-acceptance-{stamp}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


def build_dlq_headers(
    *,
    error_reason: str | None,
    source_topic: str,
    partition: int,
    offset: int,
    payload: bytes | None,
) -> list[tuple[str, bytes]]:
    """DLQ headers：错误原因、来源 topic/分区/offset、原始 payload。"""
    raw = payload or b""
    return [
        ("error_reason", (error_reason or "unknown").encode("utf-8")),
        ("source_topic", source_topic.encode("utf-8")),
        ("partition", str(partition).encode("ascii")),
        ("offset", str(offset).encode("ascii")),
        ("payload", raw),
    ]


def _print_progress(stats: ConsumeStats) -> None:
    rate = stats.total / stats.elapsed_seconds
    print(
        f"[progress] total={stats.total} valid={stats.valid} invalid={stats.invalid} "
        f"rate={rate:.1f} msg/s",
        flush=True,
    )


def consume_and_validate(  # noqa: PLR0913 - 参数为显式运行配置
    *,
    bootstrap: str,
    topic: str,
    dlq_topic: str,
    group: str,
    from_beginning: bool = True,
    max_messages: int = 0,
    idle_timeout: float = 10.0,
    schema_path: str | Path | None = None,
    progress_interval: float = 5.0,
    consumer: Consumer | None = None,
    dlq_producer: Producer | None = None,
) -> ConsumeStats:
    """消费并逐条校验，返回 ``ConsumeStats``。

    测试可注入已构造的 consumer/producer；此时调用方负责关闭，本函数不关闭。
    """
    owns_consumer = consumer is None
    owns_producer = dlq_producer is None
    if consumer is None:
        consumer = Consumer(build_consumer_config(bootstrap, group, from_beginning=from_beginning))
    if dlq_producer is None:
        dlq_producer = Producer(
            build_producer_config(bootstrap, client_id="ecommerce-validate-dlq")
        )
    validator = get_validator(schema_path)
    stats = ConsumeStats()

    consumer.subscribe([topic])
    last_activity = time.monotonic()
    try:
        while True:
            if max_messages and stats.total >= max_messages:
                break
            msg = consumer.poll(0.5)
            if msg is None:
                if time.monotonic() - last_activity >= idle_timeout:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:  # pragma: no cover
                    continue
                raise KafkaException(msg.error())

            last_activity = time.monotonic()
            report = validate_event(msg.value() or b"", validator=validator)
            stats.record(msg, report)
            if not report.valid:
                dlq_errors: list[str] = []

                def _dlq_callback(err, _dlq_msg, errors: list[str] = dlq_errors) -> None:
                    if err is not None:
                        errors.append(str(err))

                dlq_producer.produce(
                    dlq_topic,
                    key=msg.key(),
                    value=msg.value(),
                    headers=build_dlq_headers(
                        error_reason=report.reason,
                        source_topic=topic,
                        partition=msg.partition(),
                        offset=msg.offset(),
                        payload=msg.value(),
                    ),
                    on_delivery=_dlq_callback,
                )
                pending = dlq_producer.flush(10.0)
                if pending or dlq_errors:
                    raise RuntimeError(
                        f"DLQ delivery failed (pending={pending}, errors={dlq_errors}); "
                        "offset not committed"
                    )
            # 逐条提交（异步队列内部有序），避免 auto-commit 提交未处理 offset。
            consumer.commit(message=msg, asynchronous=True)

            now = time.monotonic()
            if now - stats.last_progress >= progress_interval:
                _print_progress(stats)
                stats.last_progress = now

        # 末尾同步提交，确保异步提交队列落盘。
        if stats.total:
            consumer.commit()
        _collect_partition_offsets(consumer, stats)
    finally:
        if owns_consumer:
            consumer.close()
        if owns_producer:
            dlq_producer.flush(10.0)

    return stats


def _collect_partition_offsets(consumer: Consumer, stats: ConsumeStats) -> None:
    """补充分区的 beginning/end offset（assignment 为空时跳过）。"""
    try:
        assignment = consumer.assignment()
    except KafkaException:  # pragma: no cover - 已关闭的 consumer
        return
    if not assignment:
        return
    for tp in assignment:
        try:
            beginning, end = consumer.get_watermark_offsets(tp, timeout=5.0)
        except KafkaException as exc:  # pragma: no cover - broker 不可用
            print(f"[warn] cannot query partition offsets for {tp}: {exc}", file=sys.stderr)
            return
        part = stats.partitions[tp.partition]
        part.beginning = beginning
        part.end = end


def _format_counter(counter: Counter[str]) -> str:
    return json.dumps(dict(sorted(counter.items())), ensure_ascii=False)


def print_summary(stats: ConsumeStats, *, topic: str, group: str, offset_reset: str) -> None:
    """输出验收汇总（数字均可直接写进 evidence 日志）。"""
    print("== consume summary ==")
    print(f"topic={topic} group={group} auto.offset.reset={offset_reset}")
    print(
        f"total={stats.total} valid={stats.valid} invalid={stats.invalid} "
        f"elapsed={stats.elapsed_seconds:.2f}s"
    )
    print(f"event_types={_format_counter(stats.event_types)}")
    print(f"invalid_reasons={_format_counter(stats.invalid_reasons)}")
    print(
        f"distinct_event_ids={stats.distinct_event_ids} "
        f"duplicate_event_ids={stats.duplicate_event_ids} "
        f"duplicate_records={stats.duplicate_records}"
    )
    print("partitions:")
    if not stats.partitions:
        print("  (none assigned)")
    for number in sorted(stats.partitions):
        part = stats.partitions[number]
        print(
            f"  p{number} beginning={part.beginning} end={part.end} "
            f"consumed_min={part.consumed_min} consumed_max={part.consumed_max} "
            f"messages={part.messages}"
        )


def build_arg_parser() -> argparse.ArgumentParser:
    """构造 CLI 参数解析器。"""
    parser = argparse.ArgumentParser(
        description="Consume Kafka events, validate against event_v1, route bad payloads to DLQ."
    )
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"源 topic（默认 {DEFAULT_TOPIC}）")
    parser.add_argument(
        "--dlq-topic",
        default=None,
        help="死信 topic；默认 <topic>.dlq",
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap；默认 env KAFKA_BOOTSTRAP 或 localhost:9092",
    )
    parser.add_argument(
        "--group",
        default=None,
        help="消费者组；默认每次运行生成唯一验收组名",
    )
    parser.add_argument("--max-messages", type=int, default=0, help="最多消费 N 条（0=不限）")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=10.0,
        help="无消息多久后结束（秒，默认 10）",
    )
    reset = parser.add_mutually_exclusive_group()
    reset.add_argument(
        "--from-beginning",
        dest="from_beginning",
        action="store_true",
        default=True,
        help="新消费者组从 earliest 开始（默认）",
    )
    reset.add_argument(
        "--from-latest",
        dest="from_beginning",
        action="store_false",
        help="新消费者组从 latest 开始",
    )
    parser.add_argument("--schema", default=None, help="自定义 JSON Schema 路径")
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=5.0,
        help="进度打印间隔秒数（默认 5）",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI 入口（``scripts/consume_validate.py`` 直接调用）。"""
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.max_messages < 0:
        parser.error("--max-messages must be >= 0")
    if args.idle_timeout <= 0:
        parser.error("--idle-timeout must be > 0")
    topic = args.topic
    dlq_topic = args.dlq_topic or f"{topic}.dlq"
    bootstrap = resolve_bootstrap(args.bootstrap)
    group = args.group or default_group_id()
    offset_reset = "earliest" if args.from_beginning else "latest"

    try:
        stats = consume_and_validate(
            bootstrap=bootstrap,
            topic=topic,
            dlq_topic=dlq_topic,
            group=group,
            from_beginning=args.from_beginning,
            max_messages=args.max_messages,
            idle_timeout=args.idle_timeout,
            schema_path=args.schema,
            progress_interval=args.progress_interval,
        )
    except (KafkaException, OSError, RuntimeError) as exc:
        print(f"[fatal] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print_summary(stats, topic=topic, group=group, offset_reset=offset_reset)
    return 0
