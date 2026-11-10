"""分析沙箱异常。"""

from __future__ import annotations


class AnalysisError(RuntimeError):
    """分析流程错误基类。"""


class AnalysisLimitError(AnalysisError):
    """行数 / 体积超过配置上限。"""


class JobCancelled(AnalysisError):
    """用户在解析/聚合过程中取消。"""
