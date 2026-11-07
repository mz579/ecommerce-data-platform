"""Kafka + Flink 实时扩展（Phase 9）。

本包只放与 Kafka 消息契约/回放/校验相关的纯逻辑与薄客户端封装；
Flink 作业（9.3）不在此包实现。
"""

from __future__ import annotations

from ecommerce_dw.streaming.events import (
    DEFAULT_SCHEMA_PATH,
    SUPPORTED_SCHEMA_VERSION,
    ValidationReport,
    build_event_message,
    encode_event,
    format_utc_iso_ms,
    kafka_key_for_visitor,
    load_schema,
    parse_quality_flags,
    validate_event,
)

__all__ = [
    "DEFAULT_SCHEMA_PATH",
    "SUPPORTED_SCHEMA_VERSION",
    "ValidationReport",
    "build_event_message",
    "encode_event",
    "format_utc_iso_ms",
    "kafka_key_for_visitor",
    "load_schema",
    "parse_quality_flags",
    "validate_event",
]
