-- ============================================================================
-- Phase 7（Superset）受控只读视图：商品「日 × distinct 订单数」
--
-- 用途: 商品维度的订单数只能 = distinct(transaction_id)，**禁止**用
--       transaction 事件行数冒充订单数（22,457 行事件只对应 17,672 单）。
-- 粒度: 一行 = 一个商品在一个 UTC 日的去重订单数。窗口不硬编码，
--       时间过滤交给报表（Superset 原生筛选器按 event_date_utc 下发）。
-- 来源: dwd_events（DWD 标准明细），仅 transaction 事件。
-- 安全: DEFINER 视图，访问方只见订单数列。
-- 可重复执行: CREATE OR REPLACE VIEW。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_item_orders_daily AS
SELECT event_date_utc,
       item_id,
       COUNT(DISTINCT transaction_id) AS order_cnt
FROM dwd_events
WHERE event_type = 'transaction'
  AND item_id IS NOT NULL
GROUP BY event_date_utc, item_id