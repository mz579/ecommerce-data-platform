-- ============================================================================
-- Phase 7（Superset）受控只读视图：区间层用户级漏斗（完整窗口口径）
--
-- Phase 7 R16 重构：昂贵窗口聚合已物化为 ads_funnel_interval 物理表（随 FULL
-- 构建幂等重建，见 sql/ddl/ads_funnel_interval.sql）。本视图退化为薄视图，
-- 仅做列投影，不再触碰 dwd_events —— 冷启动从 ~9.3s 降到 ~0.00s，且
-- superset_ro 只需对本视图 SELECT，不接触底层表。
--
-- 口径（与 docs/metric_definitions.md §2/§4/§7 一致）由物化表保证：
--   view_users         = 区间浏览用户数（view 去重访客，即区间 UV）
--   cart_users         = 区间加购用户数（口径 A 分子）
--   buyer_users        = 区间交易用户数
--   orders             = 区间订单数 = COUNT(DISTINCT transaction_id)
--   conv_strict_users  = 交易用户 ∩ 浏览用户（严格漏斗分子）
--   conv_global_users  = 全部交易用户（全局口径分子，可 > 分母）
--   add_to_cart_rate   = cart_users / view_users（分母 0 → NULL）
--   conv_strict_rate   = conv_strict_users / view_users
--   conv_global_rate   = conv_global_users / view_users（可 > 100%，合法）
-- 窗口: 物化时为构建完整窗口 [window_start, window_end]（生产 = 完整日集合
--       [2015-05-04, 2015-09-17]）。
-- 安全: DEFINER（默认）：访问者只需对本视图 SELECT。
-- 可重复执行: CREATE OR REPLACE VIEW；列结构如需调整请走迁移。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_funnel_interval AS
SELECT view_users,
       cart_users,
       buyer_users,
       orders,
       conv_strict_users,
       conv_global_users,
       add_to_cart_rate,
       conv_strict_rate,
       conv_global_rate
FROM ads_funnel_interval