-- ============================================================================
-- Phase 7（Superset）受控只读视图：商品行为商品数（浏览/加购/成交商品数）
--
-- 用途: 「浏览过/加购过/成交过的商品数」的每日对比。每个量 = 当日发生过该
--       行为的 distinct item_id 数（商品维度的日内去重；跨日求和会重复计数，
--       图表口径标注「每日商品数」而非「区间商品数」）。
-- 粒度: 一行 = 一个 UTC 日 + 三个商品数。
-- 来源: dwd_events（仅需 item 粒度行为，不需类别归属）。
-- 安全: DEFINER 视图。
-- 可重复执行: CREATE OR REPLACE VIEW。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_item_behavior_daily AS
SELECT event_date_utc,
       COUNT(DISTINCT CASE WHEN event_type = 'view' THEN item_id END)
           AS items_viewed,
       COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN item_id END)
           AS items_carted,
       COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN item_id END)
           AS items_bought
FROM dwd_events
WHERE item_id IS NOT NULL
GROUP BY event_date_utc