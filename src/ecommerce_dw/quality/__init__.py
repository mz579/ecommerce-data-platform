"""数据质量门禁模块（Phase 5）。

- ``rules.py``: 规则库 —— 五类（完整性 / 唯一性 / 合法性 / 范围 / 跨层对账）规则。
- ``runner.py``: 运行器 —— 执行全部规则、把结构化结果落库（quality_check_run /
  quality_check_result）、门禁 FAIL 时对外暴露为非零退出 / publish 拒绝。
"""

from ecommerce_dw.quality.models import (
    QUALITY_CATEGORIES,
    CheckRule,
    RuleOutcome,
    RunStatus,
    date_filter_sql,
)
from ecommerce_dw.quality.rules import build_rule_catalog
from ecommerce_dw.quality.runner import (
    QualityGateError,
    QualityReport,
    RecordedRun,
    list_run_results,
    list_runs,
    publish_ads_scope,
    run_quality_checks,
    run_quality_gate,
)

__all__ = [
    "CheckRule",
    "QUALITY_CATEGORIES",
    "QualityGateError",
    "QualityReport",
    "RecordedRun",
    "RuleOutcome",
    "RunStatus",
    "build_rule_catalog",
    "date_filter_sql",
    "list_run_results",
    "list_runs",
    "publish_ads_scope",
    "run_quality_checks",
    "run_quality_gate",
]
