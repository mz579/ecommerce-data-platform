"""Phase 6 orchestration support: window resolution, run control, validations.

These helpers are shared by the unified CLI (``python -m ecommerce_dw.cli
{check-sources,validate-ods,batch-register,batch-finish}``) and the Airflow DAG
in ``airflow/dags/ecommerce_daily_pipeline.py``.  Pure / database-free logic
lives in :mod:`ecommerce_dw.ops.window` and is safe to import from a DAG module
at import time (no IO); database touches happen only inside task callables.
"""

from ecommerce_dw.ops.control import finish_run, plan_and_register, publish_after_gate
from ecommerce_dw.ops.validate import check_source_files, validate_ods
from ecommerce_dw.ops.window import MODE_FORCE, MODE_TAIL, RunWindow, resolve_window

__all__ = [
    "MODE_FORCE",
    "MODE_TAIL",
    "RunWindow",
    "check_source_files",
    "finish_run",
    "plan_and_register",
    "publish_after_gate",
    "resolve_window",
    "validate_ods",
]
