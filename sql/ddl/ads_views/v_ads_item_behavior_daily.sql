-- ============================================================================
-- Phase 7（Superset）受控只读视图：商品行为商品数（浏览/加购/成交商品数）
--
-- Phase 7 R16 重构：按日去重商品数已物化为 ads_item_behavior_daily 物理表
-- （随 DWS/ADS 构建按日幂等重建，见 sql/ddl/ads_item_behavior_daily.sql）。
-- 本视图退化为薄视图，仅做列投影 —— 冷启动从 ~10.0s 降到 ~0.00s，且
-- superset_ro 只需对本视图 SELECT。
--
-- 口径（与原视图及 docs/metric_definitions.md §9 一致）由物化表保证，
-- 每个量 = 当日发生过该行为的 distinct item_id 数（跨日求和会重复计数，
-- 图表口径标注「每日商品数」而非「区间商品数」）。
-- 粒度: 一行 = 一个 UTC 日 + 三个商品数。
-- 安全: DEFINER 视图。
-- 可重复执行: CREATE OR REPLACE VIEW。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_item_behavior_daily AS
SELECT event_date_utc,
       items_viewed,
       items_carted,
       items_bought
FROM ads_item_behavior_daily