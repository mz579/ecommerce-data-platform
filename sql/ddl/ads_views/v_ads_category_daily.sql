-- ============================================================================
-- Phase 7（Superset）受控只读视图：类目维度日汇总（镜像 dws_category_daily）
--
-- 用途: DWS 的类目日汇总不在 ADS 层，本视图把它以「受控只读视图」形式暴露给
--       Superset（主方案要求 Superset 只读 ADS 或受控视图）。列为业务列白名单，
--       不含血缘列。类目 0 = 未分类桶（属性快照窗口早于事件日的商品）。
-- 粒度: 一行 = 一个类目在一个 UTC 日的行为汇总。
-- 口径: 与 dws_category_daily 完全一致（distinct visitor / distinct txn 均为
--       商品维度的行内去重口径）。
-- 安全: DEFINER 视图。
-- 可重复执行: CREATE OR REPLACE VIEW。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_category_daily AS
SELECT event_date_utc,
       category_id,
       view_cnt,
       add_to_cart_cnt,
       transaction_cnt,
       view_uv,
       add_to_cart_uv,
       transaction_uv,
       order_cnt
FROM dws_category_daily