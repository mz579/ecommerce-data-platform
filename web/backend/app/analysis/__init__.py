"""上传分析沙箱。

与只读看板分离：本包只读写 ``ANALYSIS_DATA_DIR``，不导入 ``app.db``，
不接触 MySQL。每个任务一个 DuckDB 文件，分析完成后结果以 JSON 快照返回。
"""

from .router import router

__all__ = ["router"]
