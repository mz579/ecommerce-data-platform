"""ODS 入库模块（Phase 2）。

提供分块、幂等、可断点恢复的 CSV -> MySQL ODS 贴源层入库能力：
- 内存有上限的分块读取（默认 10k 行/批，可调）
- 每批次在 ``etl_batch_log`` 登记 SHA-256 / 行数 / 起止时间 / 状态
- 同 sha256 已 LOADED 的文件重复导入自动跳过；失败批次 cleanup 后可重跑
- 确定性代理键规则见 ``docs/ods_design.md``
"""

from ecommerce_dw.ingestion.keys import make_row_key, sha256_file
from ecommerce_dw.ingestion.loader import (
    BatchStateError,
    LoadError,
    LoadResult,
    cleanup_batch,
    cleanup_file,
    load_csv,
)
from ecommerce_dw.ingestion.specs import TABLE_SPECS, infer_table_for_file

__all__ = [
    "BatchStateError",
    "LoadError",
    "LoadResult",
    "TABLE_SPECS",
    "cleanup_batch",
    "cleanup_file",
    "infer_table_for_file",
    "load_csv",
    "make_row_key",
    "sha256_file",
]
