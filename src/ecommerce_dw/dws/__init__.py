"""DWS/ADS 汇总与应用层（Phase 4）。

构建入口 ``python -m ecommerce_dw.dws.cli``；核心逻辑在 ``builder``（编排 +
    不变式校验 + 独立复核），DML 语句在 ``agg_sql``，口径常量在 ``constants``。
"""
