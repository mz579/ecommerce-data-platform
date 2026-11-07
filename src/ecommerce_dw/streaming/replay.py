"""DWD → Kafka 回放器（Phase 9.2）。

只读查询 ``ecommerce_dw.dwd_events``（会话级只读事务），把每行构造成
event_v1 消息并按 ``(event_time_utc, event_sk)`` 顺序发送，保证同一
``visitor_id``（Kafka key）分区内有序。

Producer 约定见 ``docs/streaming_design.md`` §4.3：
``acks=all``、``enable.idempotence=true``、``max.in.flight<=5``、
``compression.type=lz4``、明确的 ``delivery.timeout.ms``；发送失败有界重试，
最终失败写入本地 JSONL spool，并让进程退出码非 0。

CLI 入口：``scripts/replay_to_kafka.py``（本模块 ``main``）。
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pymysql
from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

from ecommerce_dw.config import DbConfig, db_config_from_env
from ecommerce_dw.streaming.events import (
    ValidationReport,
    build_event_message,
    encode_event,
    get_validator,
    kafka_key_for_visitor,
    validate_event,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TOPIC = "ecommerce.events.v1"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_DELIVERY_TIMEOUT_MS = 120_000
DEFAULT_MAX_SEND_ATTEMPTS = 3
DEFAULT_SPOOL_DIR = Path("logs") / "streaming"

_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}

_DWD_COLUMNS = (
    "event_sk",
    "dedup_key",
    "visitor_id",
    "item_id",
    "raw_event_type",
    "event_type",
    "event_time_utc",
    "transaction_id",
    "quality_flags",
    "source_file",
    "source_row_number",
)


class ReplayError(RuntimeError):
    """回放流程中的可预期错误（连接、查询、发送超时等）。"""


@dataclass
class ReplayStats:
    """回放过程的计数快照，用于周期进度与最终汇总。"""

    queried: int = 0
    sent: int = 0
    acked: int = 0
    failed_attempts: int = 0
    retried: int = 0
    spooled: int = 0
    invalid: int = 0
    started_at: float = field(default_factory=time.monotonic)

    @property
    def elapsed_seconds(self) -> float:
        return max(time.monotonic() - self.started_at, 1e-9)

    @property
    def throughput(self) -> float:
        return self.acked / self.elapsed_seconds


@dataclass(frozen=True)
class PendingMessage:
    """一条尚未确认（或最终失败）的 Kafka 消息，用于重试与 spool。"""

    topic: str
    key: bytes | None
    value: bytes
    payload: Mapping[str, Any]
    attempts: int = 1
    error: str = ""


_TOKENS = itertools.count(1)


class DeliveryTracker:
    """维护 in-flight 消息与 delivery 回调，失败消息进入 ``failures``。

    ``flush`` 超时后仍留在 in-flight 的消息也计入失败（``error=flush_timeout``），
    避免任何消息在无记录的情况下消失。
    """

    def __init__(self, producer: Producer, stats: ReplayStats) -> None:
        self.producer = producer
        self.stats = stats
        self.inflight: dict[int, PendingMessage] = {}
        self.failures: list[PendingMessage] = []

    def send(self, item: PendingMessage, attempt: int = 1) -> None:
        token = next(_TOKENS)
        self.inflight[token] = item

        def _callback(err, _msg, tok: int = token, att: int = attempt) -> None:
            pending = self.inflight.pop(tok, None)
            if pending is None:
                return
            if err is not None:
                self.stats.failed_attempts += 1
                self.failures.append(
                    replace(pending, attempts=att, error=str(err)),
                )
            else:
                self.stats.acked += 1

        deadline = time.monotonic() + 30.0
        while True:
            try:
                self.producer.produce(
                    item.topic,
                    key=item.key,
                    value=item.value,
                    on_delivery=_callback,
                )
                break
            except BufferError:
                # 本地队列满：驱动回调排空队列，最多等待 30 秒。
                if time.monotonic() >= deadline:
                    self.inflight.pop(token, None)
                    raise ReplayError(
                        f"producer queue stayed full for 30s at event_id="
                        f"{item.payload.get('event_id')}"
                    ) from None
                self.producer.poll(0.5)
        self.producer.poll(0)

    def flush(self, timeout_s: float) -> int:
        remaining = self.producer.flush(timeout_s)
        for token, item in list(self.inflight.items()):
            self.inflight.pop(token)
            self.stats.failed_attempts += 1
            self.failures.append(
                replace(item, attempts=item.attempts, error="flush_timeout"),
            )
        return remaining


def build_producer_config(
    bootstrap_servers: str,
    *,
    client_id: str = "ecommerce-replay",
    delivery_timeout_ms: int = DEFAULT_DELIVERY_TIMEOUT_MS,
    retries: int = 10,
) -> dict[str, Any]:
    """按设计 §4.3 构造幂等 producer 配置（纯函数，便于单测断言）。"""
    timeout = int(delivery_timeout_ms)
    if timeout <= 0:
        raise ValueError("delivery_timeout_ms must be positive")
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "max.in.flight.requests.per.connection": 5,
        "delivery.timeout.ms": timeout,
        "request.timeout.ms": min(30_000, timeout),
        "retries": int(retries),
        "retry.backoff.ms": 500,
    }


def resolve_bootstrap(value: str | None) -> str:
    """CLI 显式值 > ``KAFKA_BOOTSTRAP`` 环境变量 > ``localhost:9092``。"""
    return value or os.environ.get("KAFKA_BOOTSTRAP") or DEFAULT_BOOTSTRAP


def _build_query(date_from: date, date_to: date, limit: int | None) -> str:
    if date_to < date_from:
        raise ValueError(f"--to ({date_to}) must be >= --from ({date_from})")
    columns = ", ".join(_DWD_COLUMNS)
    sql = (
        f"SELECT {columns} FROM dwd_events "
        "WHERE event_date_utc BETWEEN %s AND %s "
        "ORDER BY event_time_utc, event_sk"
    )
    if limit is not None:
        if limit <= 0:
            raise ValueError("--limit must be positive")
        sql += f" LIMIT {int(limit)}"
    return sql


def validate_query_args(date_from: date, date_to: date, limit: int | None) -> str:
    """校验查询参数并返回 SQL（CLI 与测试共用；非法参数抛 ``ValueError``）。"""
    return _build_query(date_from, date_to, limit)


def connect_dwd(cfg: DbConfig) -> pymysql.connections.Connection:
    """建立只读 MySQL 连接；``SET SESSION TRANSACTION READ ONLY`` 兜底防误写。"""
    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
        cursorclass=pymysql.cursors.SSDictCursor,
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute("SET SESSION TRANSACTION READ ONLY")
        cur.execute("SELECT 1")
        cur.fetchall()
    return conn


def iter_dwd_rows(
    conn: pymysql.connections.Connection,
    date_from: date,
    date_to: date,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    """流式读取指定 UTC 日期范围的 DWD 行（已按 event_time, event_sk 排序）。"""
    query = _build_query(date_from, date_to, limit)
    with conn.cursor() as cur:
        cur.execute(query, (date_from, date_to))
        for row in cur:
            yield dict(row)


def _default_spool_path() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return DEFAULT_SPOOL_DIR / f"replay_failed_{stamp}.jsonl"


def write_failure_spool(path: Path, failures: list[PendingMessage]) -> None:
    """把最终失败的消息逐行写入本地 JSONL spool（不静默丢弃）。"""
    if not failures:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for item in failures:
            record = {
                "failed_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "topic": item.topic,
                "key": None if item.key is None else item.key.decode("utf-8", "replace"),
                "payload": dict(item.payload),
                "attempts": item.attempts,
                "error": item.error,
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _throttle(sent: int, rate: float, started_at: float) -> None:
    """按目标速率节流（0 表示不限速）。"""
    if rate <= 0:
        return
    target = started_at + sent / rate
    delay = target - time.monotonic()
    if delay > 0:
        time.sleep(min(delay, 1.0))


def _print_progress(stats: ReplayStats) -> None:
    print(
        f"[progress] queried={stats.queried} sent={stats.sent} acked={stats.acked} "
        f"failed={stats.failed_attempts} invalid={stats.invalid} "
        f"rate={stats.throughput:.1f} msg/s",
        flush=True,
    )


def _retry_pending(
    tracker: DeliveryTracker,
    pending: list[PendingMessage],
    stats: ReplayStats,
    max_attempts: int,
) -> list[PendingMessage]:
    """对发送失败的消息做有界重试（应用层），返回最终失败列表。"""
    current = pending
    for attempt in range(2, max_attempts + 1):
        if not current:
            break
        stats.retried += len(current)
        print(f"[retry] attempt {attempt}/{max_attempts}: {len(current)} message(s)", flush=True)
        tracker.failures = []
        for item in current:
            tracker.send(item, attempt=attempt)
        tracker.flush(60.0)
        current = tracker.failures
    return current


def run_replay(args: argparse.Namespace, cfg: DbConfig) -> int:
    """执行回放，返回进程退出码（0 成功；2 schema 缺陷；3 发送失败）。"""
    stats = ReplayStats()
    bootstrap = resolve_bootstrap(args.bootstrap)
    topic = args.topic or DEFAULT_TOPIC
    using_validation = not args.no_schema_validation
    validator = get_validator(args.schema) if using_validation else None

    conn = connect_dwd(cfg)
    producer: Producer | None = None
    tracker: DeliveryTracker | None = None
    last_progress = time.monotonic()
    try:
        if not args.dry_run:
            producer = Producer(
                build_producer_config(
                    bootstrap,
                    delivery_timeout_ms=args.delivery_timeout_ms,
                )
            )
            tracker = DeliveryTracker(producer, stats)

        for row in iter_dwd_rows(conn, args.date_from, args.date_to, args.limit):
            stats.queried += 1
            message = build_event_message(row)
            item = PendingMessage(
                topic=topic,
                key=kafka_key_for_visitor(message["visitor_id"]).encode("utf-8"),
                value=encode_event(message),
                payload=message,
            )
            if validator is not None:
                report: ValidationReport = validate_event(item.payload, validator=validator)
                if not report.valid:
                    stats.invalid += 1
                    print(
                        f"[schema-error] event_id={item.payload.get('event_id')} "
                        f"reason={report.reason}",
                        file=sys.stderr,
                    )
                    continue
            stats.sent += 1
            if tracker is not None:
                tracker.send(item)
                _throttle(stats.sent, args.rate, stats.started_at)
            else:
                # dry-run：本地构造并通过校验即视为处理成功。
                stats.acked += 1

            now = time.monotonic()
            if now - last_progress >= args.progress_interval:
                _print_progress(stats)
                last_progress = now

        pending: list[PendingMessage] = []
        if tracker is not None:
            print("[replay] flushing producer ...", flush=True)
            remaining = tracker.flush(60.0)
            if remaining:
                print(
                    f"[warn] flush timed out with {remaining} message(s) still queued",
                    file=sys.stderr,
                )
            pending = _retry_pending(tracker, tracker.failures, stats, args.max_send_attempts)
            stats.spooled = len(pending)
            if pending:
                write_failure_spool(args.failure_spool, pending)
    finally:
        conn.close()

    elapsed = stats.elapsed_seconds
    print("== replay summary ==")
    print(f"topic={topic} bootstrap={bootstrap} dry_run={args.dry_run}")
    print(f"date_range={args.date_from}..{args.date_to} limit={args.limit} rate={args.rate}")
    print(f"queried={stats.queried} sent={stats.sent} acked={stats.acked}")
    print(
        f"failed_attempts={stats.failed_attempts} retried={stats.retried} spooled={stats.spooled}"
    )
    print(f"invalid_schema={stats.invalid}")
    print(f"elapsed={elapsed:.2f}s throughput={stats.throughput:.1f} msg/s")
    if stats.spooled:
        print(f"failure_spool={args.failure_spool}")

    if stats.invalid:
        print(f"[error] {stats.invalid} message(s) failed schema validation", file=sys.stderr)
        return 2
    if stats.spooled:
        print(f"[error] {stats.spooled} message(s) could not be delivered", file=sys.stderr)
        return 3
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    """构造 CLI 参数解析器（``--from``/``--to`` 等，见模块文档）。"""
    parser = argparse.ArgumentParser(
        description="Replay ecommerce_dw.dwd_events into Kafka as event_v1 messages."
    )
    parser.add_argument(
        "--from",
        dest="date_from",
        type=date.fromisoformat,
        required=True,
        help="event_date_utc 起始日（含），YYYY-MM-DD",
    )
    parser.add_argument(
        "--to",
        dest="date_to",
        type=date.fromisoformat,
        default=None,
        help="event_date_utc 结束日（含），默认等于 --from",
    )
    parser.add_argument("--limit", type=int, default=None, help="最多回放 N 行（测试用）")
    parser.add_argument("--rate", type=float, default=0.0, help="目标速率（条/秒），0 表示不限速")
    parser.add_argument(
        "--topic", default=DEFAULT_TOPIC, help=f"目标 topic（默认 {DEFAULT_TOPIC}）"
    )
    parser.add_argument(
        "--bootstrap",
        default=None,
        help="Kafka bootstrap；默认 env KAFKA_BOOTSTRAP 或 localhost:9092",
    )
    parser.add_argument("--dry-run", action="store_true", help="只读取并校验，不发送")
    parser.add_argument(
        "--no-schema-validation",
        action="store_true",
        help="关闭发送前的 JSON Schema 校验（默认开启）",
    )
    parser.add_argument("--schema", default=None, help="自定义 JSON Schema 路径")
    parser.add_argument(
        "--delivery-timeout-ms",
        type=int,
        default=DEFAULT_DELIVERY_TIMEOUT_MS,
        help=f"delivery.timeout.ms（默认 {DEFAULT_DELIVERY_TIMEOUT_MS}）",
    )
    parser.add_argument(
        "--max-send-attempts",
        type=int,
        default=DEFAULT_MAX_SEND_ATTEMPTS,
        help=f"单条消息最大发送尝试次数（默认 {DEFAULT_MAX_SEND_ATTEMPTS}）",
    )
    parser.add_argument(
        "--failure-spool",
        type=Path,
        default=None,
        help="最终失败消息的 JSONL spool 路径",
    )
    parser.add_argument(
        "--progress-interval",
        type=float,
        default=5.0,
        help="进度打印间隔秒数（默认 5）",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI 入口（``scripts/replay_to_kafka.py`` 直接调用）。"""
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    args.date_to = args.date_to or args.date_from
    args.failure_spool = args.failure_spool or _default_spool_path()
    if args.max_send_attempts < 1:
        parser.error("--max-send-attempts must be >= 1")
    if args.rate < 0:
        parser.error("--rate must be >= 0")
    try:
        _build_query(args.date_from, args.date_to, args.limit)
    except ValueError as exc:
        parser.error(str(exc))

    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    try:
        return run_replay(args, cfg)
    except (KafkaException, ReplayError, pymysql.MySQLError, OSError) as exc:
        print(f"[fatal] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
