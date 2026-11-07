"""Kafka 事件契约 v1 的消息构造与 JSON Schema 校验（Phase 9.2）。

实现依据：
- ``docs/streaming_design.md`` §3（字段、时间格式、schema_version 演进）；
- ``streaming/schemas/event_v1.json``（Draft 2020-12，机器可读契约）。

本模块不做任何 IO（Schema 文件读取除外），便于单元测试；Kafka 发送与
MySQL 查询分别在 ``replay`` / ``validate_consumer`` 中实现。
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "streaming" / "schemas" / "event_v1.json"

#: 本回放器/消费者支持的最高契约主版本（§3.3：更高版本不进业务逻辑）。
SUPPORTED_SCHEMA_VERSION = 1

#: DWD quality_flags 的正常值；不映射为数组元素。
NORMAL_QUALITY_FLAG = "ok"

#: visitor_id 为 NULL 时的 Kafka key 哨兵（§4 Topic 设计）。
NULL_VISITOR_KEY = "0"

_UTC = UTC
_VALIDATORS: dict[str, Draft202012Validator] = {}


@dataclass(frozen=True)
class ValidationReport:
    """一条消息的校验结果（合法与否 + 首个失败原因 + 便于统计的字段）。"""

    valid: bool
    reason: str | None = None
    event_type: str | None = None
    event_id: str | None = None


def format_utc_iso_ms(value: datetime) -> str:
    """格式化为 UTC ISO-8601 毫秒精度、以 ``Z`` 结尾（契约 §3.1）。

    naive datetime 按 UTC 解释（MySQL ``DATETIME`` 无时区，DWD 已统一存 UTC）；
    aware datetime 先转换到 UTC。微秒位截断到毫秒（DWD 列为 ``DATETIME(3)``）。
    """
    if value.tzinfo is not None:
        value = value.astimezone(_UTC)
    return (
        f"{value.year:04d}-{value.month:02d}-{value.day:02d}"
        f"T{value.hour:02d}:{value.minute:02d}:{value.second:02d}"
        f".{value.microsecond // 1000:03d}Z"
    )


def kafka_key_for_visitor(visitor_id: int | None) -> str:
    """Kafka key = visitor_id 十进制字符串；NULL → ``"0"``（§4）。"""
    return NULL_VISITOR_KEY if visitor_id is None else str(visitor_id)


def parse_quality_flags(raw: str | None) -> list[str]:
    """把 DWD 逗号分隔的 quality_flags 转成契约数组。

    ``"ok"``（以及空串/NULL）表示无异常 → 空数组；其余按逗号拆分并去空白。
    """
    if raw is None:
        return []
    text = raw.strip()
    if not text or text == NORMAL_QUALITY_FLAG:
        return []
    return [part.strip() for part in text.split(",") if part.strip()]


def build_event_message(
    row: Mapping[str, Any],
    *,
    ingest_time: datetime | None = None,
) -> dict[str, Any]:
    """把一个 ``dwd_events`` 行（mapping）构造成 event_v1 消息。

    必需键：``dedup_key`` / ``event_time_utc`` / ``visitor_id`` / ``event_type``
    / ``item_id`` / ``transaction_id``；可选键：``raw_event_type`` /
    ``source_file`` / ``source_row_number`` / ``quality_flags``。

    ``event_id`` 直接复用 DWD ``dedup_key``（确定性 SHA-256），重复回放产生
    相同 ``event_id``，供 9.3 按 event_id 去重（§5）。
    """
    event_time = row["event_time_utc"]
    if not isinstance(event_time, datetime):
        raise TypeError(f"event_time_utc must be datetime, got {type(event_time)!r}")

    message: dict[str, Any] = {
        "event_id": row["dedup_key"],
        "event_time": format_utc_iso_ms(event_time),
        "ingest_time": format_utc_iso_ms(ingest_time or datetime.now(_UTC)),
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "visitor_id": row["visitor_id"],
        "event_type": row["event_type"],
        "item_id": row["item_id"],
        "transaction_id": row["transaction_id"],
    }
    for optional in ("raw_event_type", "source_file", "source_row_number"):
        value = row.get(optional)
        if value is not None:
            message[optional] = value
    message["quality_flags"] = parse_quality_flags(row.get("quality_flags"))
    return message


def encode_event(message: Mapping[str, Any]) -> bytes:
    """把消息对象编码为 UTF-8 JSON（紧凑分隔符，减小 Kafka 消息体）。"""
    return json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def load_schema(path: str | Path | None = None) -> dict[str, Any]:
    """读取 JSON Schema 文件（默认 ``streaming/schemas/event_v1.json``）。"""
    schema_path = Path(path) if path is not None else DEFAULT_SCHEMA_PATH
    with schema_path.open(encoding="utf-8") as handle:
        schema = json.load(handle)
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:  # pragma: no cover - 契约文件损坏属配置错误
        raise ValueError(f"invalid JSON schema at {schema_path}: {exc.message}") from exc
    return schema


def get_validator(path: str | Path | None = None) -> Draft202012Validator:
    """返回带 ``format`` 检查的 Draft 2020-12 校验器（按路径缓存）。

    ``FormatChecker`` 需要 ``rfc3339-validator`` 才能校验 ``date-time``；
    该依赖已写入 pyproject，缺失时格式校验会静默跳过——因此下方
    ``validate_event`` 另外显式解析两个时间字段兜底。
    """
    schema_path = str(Path(path) if path is not None else DEFAULT_SCHEMA_PATH)
    validator = _VALIDATORS.get(schema_path)
    if validator is None:
        validator = Draft202012Validator(load_schema(schema_path), format_checker=FormatChecker())
        _VALIDATORS[schema_path] = validator
    return validator


def _parse_time_field(value: Any, field: str) -> str | None:
    """显式校验 UTC ISO-8601 毫秒时间字段，返回失败原因或 None。"""
    if not isinstance(value, str):
        return f"schema: {field}: not a string"
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return f"schema: {field}: not an ISO-8601 datetime"
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(None):
        return f"schema: {field}: must be UTC (Z suffix)"
    if parsed.microsecond % 1000:
        return f"schema: {field}: millisecond precision required"
    return None


def validate_event(
    raw: bytes | str | Mapping[str, Any],
    *,
    validator: Draft202012Validator | None = None,
) -> ValidationReport:
    """用契约校验一条消息，返回 ``ValidationReport``。

    校验顺序：UTF-8/JSON 解析 → JSON Schema（含条件规则）→ 显式 UTC 毫秒时间
    → schema_version 上限。任一失败都会给出稳定的 ``reason``，消费者把它写入
    DLQ header，便于排查。
    """
    if isinstance(raw, (bytes, bytearray)):
        try:
            text = bytes(raw).decode("utf-8")
        except UnicodeDecodeError as exc:
            return ValidationReport(False, f"invalid_utf8: {exc.reason}")
    elif isinstance(raw, str):
        text = raw
    else:
        text = None

    if text is not None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            return ValidationReport(False, f"invalid_json: {exc.msg} (line {exc.lineno})")
    else:
        payload = raw

    if not isinstance(payload, Mapping):
        return ValidationReport(False, "invalid_payload: not a JSON object")

    checker = validator or get_validator()
    errors = sorted(checker.iter_errors(dict(payload)), key=lambda err: list(err.absolute_path))
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.absolute_path) or "$"
        return ValidationReport(
            False,
            f"schema: {path}: {first.message}",
            event_type=payload.get("event_type"),
            event_id=payload.get("event_id"),
        )

    for field in ("event_time", "ingest_time"):
        reason = _parse_time_field(payload.get(field), field)
        if reason is not None:
            return ValidationReport(
                False,
                reason,
                event_type=payload.get("event_type"),
                event_id=payload.get("event_id"),
            )

    version = payload.get("schema_version")
    if isinstance(version, int) and version > SUPPORTED_SCHEMA_VERSION:
        # §3.3：不猜测、不降级解析，原样进 DLQ 并告警。
        return ValidationReport(
            False,
            f"unsupported_schema_version: {version}",
            event_type=payload.get("event_type"),
            event_id=payload.get("event_id"),
        )

    return ValidationReport(
        True,
        None,
        event_type=payload.get("event_type"),
        event_id=payload.get("event_id"),
    )
