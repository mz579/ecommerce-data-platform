"""Incremental processing: watermark-driven tail / late-data runs (Phase 5)."""

from ecommerce_dw.incremental.planner import (
    WorkPlan,
    compute_frontier,
    plan_backfill,
    plan_incremental,
)
from ecommerce_dw.incremental.runner import (
    IncrementalError,
    IncrementalResult,
    init_watermark,
    run_incremental,
)
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE, WatermarkRepo

__all__ = [
    "DEFAULT_PIPELINE",
    "IncrementalError",
    "IncrementalResult",
    "WatermarkRepo",
    "WorkPlan",
    "compute_frontier",
    "init_watermark",
    "plan_backfill",
    "plan_incremental",
    "run_incremental",
]
