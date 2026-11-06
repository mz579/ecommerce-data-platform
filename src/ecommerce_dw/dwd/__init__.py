"""DWD 明细标准化层（Phase 3）。

从 ODS ``ods_events_raw`` 构建去重、标准化、可审计的 DWD 行为明细：
``dwd_events`` / ``dwd_events_duplicate`` / ``dwd_events_reject``。
"""

from ecommerce_dw.dwd.builder import BuildError, BuildResult, build_dwd, cleanup_build, verify_build
from ecommerce_dw.dwd.transform import (
    EVENT_TYPE_MAP,
    TRANSFORM_VERSION,
    RejectReason,
    TransformOutcome,
    make_dedup_key,
    ms_to_utc_date,
    ms_to_utc_datetime,
    parse_bigint,
    parse_ts_ms,
    transform_event,
)

__all__ = [
    "BuildError",
    "BuildResult",
    "build_dwd",
    "cleanup_build",
    "verify_build",
    "EVENT_TYPE_MAP",
    "TRANSFORM_VERSION",
    "RejectReason",
    "TransformOutcome",
    "make_dedup_key",
    "ms_to_utc_date",
    "ms_to_utc_datetime",
    "parse_bigint",
    "parse_ts_ms",
    "transform_event",
]
