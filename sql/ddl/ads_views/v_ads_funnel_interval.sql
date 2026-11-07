-- ============================================================================
-- Phase 7（Superset）受控只读视图：区间层用户级漏斗（完整窗口口径）
--
-- 用途: 把「完整窗口 [2015-05-04, 2015-09-17]」的区间用户级漏斗钉死为一个
--       单行只读视图，供 Superset KPI 卡片直接读取。口径与
--       docs/metric_definitions.md §2/§4/§7 及 docs/dws_ads_design.md §8.3 完全一致：
--         view_users         = 区间浏览用户数（view 去重访客，即区间 UV）
--         cart_users         = 区间加购用户数（口径 A 分子）
--         buyer_users        = 区间交易用户数
--         orders             = 区间订单数 = COUNT(DISTINCT transaction_id)
--         conv_strict_users  = 交易用户 ∩ 浏览用户（严格漏斗分子）
--         conv_global_users  = 全部交易用户（全局口径分子，可 > 分母）
--         add_to_cart_rate   = cart_users / view_users（分母 0 → NULL）
--         conv_strict_rate   = conv_strict_users / view_users
--         conv_global_rate   = conv_global_users / view_users（可 > 100%，合法）
-- 时区: UTC；窗口 = 完整自然日集合（95 个完整日之外的 05/03、09/18 不参与）。
-- 只读: 仅含 SELECT。安全模型 = DEFINER（默认）：视图主体按建库用户（root）执行，
--        访问者只需对本视图有 SELECT，不接触底层 dwd_events，天然最小授权。
-- 可重复执行: CREATE OR REPLACE VIEW；列结构如需调整请走迁移。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_funnel_interval AS
SELECT u.view_users,
       u.cart_users,
       u.buyer_users,
       o.orders,
       u.conv_strict_users,
       u.conv_global_users,
       u.cart_users / NULLIF(u.view_users, 0) AS add_to_cart_rate,
       u.conv_strict_users / NULLIF(u.view_users, 0) AS conv_strict_rate,
       u.conv_global_users / NULLIF(u.view_users, 0) AS conv_global_rate
FROM (
    SELECT SUM(viewed) AS view_users,
           SUM(carted) AS cart_users,
           SUM(bought) AS buyer_users,
           SUM(bought AND viewed) AS conv_strict_users,
           SUM(bought) AS conv_global_users
    FROM (
        SELECT visitor_id,
               MAX(event_type = 'view') AS viewed,
               MAX(event_type = 'add_to_cart') AS carted,
               MAX(event_type = 'transaction') AS bought
        FROM dwd_events
        WHERE visitor_id IS NOT NULL
          AND event_date_utc BETWEEN '2015-05-04' AND '2015-09-17'
          AND event_type IN ('view', 'add_to_cart', 'transaction')
        GROUP BY visitor_id
    ) v
) u
CROSS JOIN (
    SELECT COUNT(DISTINCT transaction_id) AS orders
    FROM dwd_events
    WHERE event_type = 'transaction'
      AND event_date_utc BETWEEN '2015-05-04' AND '2015-09-17'
) o