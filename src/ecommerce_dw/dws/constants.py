"""DWS/ADS 聚合构建的常量与口径钉死（Phase 4）。

所有数值口径来自 ``docs/metric_definitions.md``，本模块是它们的最小可执行落点。
任何口径/版本变更必须先改这里，再进行一次 FULL 重建。
"""

from __future__ import annotations

from datetime import date

# 聚合口径版本：逻辑/口径变更必须升版本并全量重建（同 Phase 3 TRANSFORM_VERSION）。
AGG_VERSION = "1"

# 数据窗口内的完整自然日集合（metric_definitions.md §3/§8）：
# 全区间 [2015-05-03, 2015-09-18] 共 139 个 UTC 日，其中首末日为部分日；
# 完整日 = [2015-05-04, 2015-09-17]。
FULL_WINDOW_START = date(2015, 5, 4)
FULL_WINDOW_END = date(2015, 9, 17)


# 留存 cohort 的最小有效区间（metric_definitions.md §8）：D0 ∈ [05-04, 09-16]，
# 因为 D0+1 必须落在完整日窗口内。由完整窗口推导，保证在测试里用自定义窗口
# 时同样自洽。
def retention_d0_range(
    full_start: date = FULL_WINDOW_START,
    full_end: date = FULL_WINDOW_END,
) -> tuple[date, date]:
    """返回可产出『完整窗口』次日留存的 cohort 日区间 [start, end]."""
    from datetime import timedelta

    return full_start, full_end - timedelta(days=1)


# 商品热度日排行默认取每日 Top-N（同进排行的数据落在 dws_item_daily 全量）。
DEFAULT_TOP_N = 100

# ads_funnel_daily 指标长表：全部 metric_name 取值（构建时逐日产出）。
FUNNEL_METRICS = (
    "view_users",
    "cart_users",
    "buyer_users",
    "orders",
    "add_to_cart_rate",
    "conv_strict_users",
    "conv_global_users",
    "conv_strict_rate",
    "conv_global_rate",
)


# --------------------------------------------------------------------------- #
# 部分日 / 留存窗口标记者（纯函数，便于单测）                                   #
# --------------------------------------------------------------------------- #


def is_full_day(
    day: date,
    full_start: date = FULL_WINDOW_START,
    full_end: date = FULL_WINDOW_END,
) -> bool:
    """day 是否落在完整自然日窗口内（数据窗口内首末日为部分日）。"""
    return full_start <= day <= full_end


def retention_window_status(
    cohort_date: date,
    age_day: int,
    full_start: date = FULL_WINDOW_START,
    full_end: date = FULL_WINDOW_END,
) -> str:
    """cohort 行的回访窗口完整性：'full' / 'partial'.

    'full' 要求 cohort 日与回访日（cohort_date + age_day）都在完整日窗口内；
    其余（首日部分日 cohort、窗口末尾不足、或回访日超出数据全域）一律 'partial'。
    """
    from datetime import timedelta

    return_day = cohort_date + timedelta(days=age_day)
    if is_full_day(cohort_date, full_start, full_end) and is_full_day(
        return_day, full_start, full_end
    ):
        return "full"
    return "partial"
