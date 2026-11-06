-- ============================================================================
-- ADS 应用层：行为漏斗日报（Phase 4）
--
-- 层级: ADS（Application Data Store / 应用层）。
-- 用途: 以「长表 + 口径名」承载每日漏斗各步骤用户数与转化率，一行 = 一个指标值。
--       每行以 metric_name 区分口径与量纲，metric_value 为对应口径数值。
-- 口径名（均 UTC 日口径，visitor 口径排除未知访客桶）:
--   view_users             浏览用户数（= 当日 UV，分母）
--   cart_users             加购用户数（当日 add_to_cart 去重访客）
--   buyer_users            交易用户数（当日 transaction 去重访客）
--   orders                 订单数（当日 distinct(transaction_id)，直接取自 dwd_events
--                           —— 一个 transaction_id 可跨日/跨行，不可用用户行求和）
--   add_to_cart_rate       加购率口径A = cart_users / view_users（分母 0 输出 NULL）
--   conv_strict_users      严格漏斗转化分子 = 当日交易用户 ∩ 浏览用户
--   conv_global_users      全局转化分子 = 当日全部交易用户（可 > 分母，不告警）
--   conv_strict_rate       严格漏斗转化率 = conv_strict_users / view_users（分母 0 → NULL）
--   conv_global_rate       全局转化率 = conv_global_users / view_users（可 > 100%，合法）
-- 边界: 分母（view_users）= 0 时转化率口径输出 NULL 而非 0 或除零错误；
--       全局口径分子无条件 ⊆ 分母的假设（实测 428 名交易用户无浏览）。
-- 部分日: is_full_day 语义同 ads_traffic_daily。
-- 来源: dws_user_daily（按人指标）+ dwd_events（orders 全局去重）。
-- 幂等性: 按 event_date_utc 整日/全量重建即可；重复执行结果一致。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_funnel_daily (
    event_date_utc DATE          NOT NULL COMMENT 'UTC 日（口径窗口，日界=世界时零点）',
    metric_name    VARCHAR(48)   NOT NULL COMMENT '口径名（view_users/cart_users/buyer_users/orders/add_to_cart_rate/conv_*）',
    metric_value   DECIMAL(20,6) NULL COMMENT '口径数值；比率类为 (0, n) 小数或 NULL（分母为 0 时）',
    is_full_day     TINYINT       NOT NULL COMMENT '1=完整自然日；0=部分日（数据窗口不完整）',
    build_id        BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at       DATETIME(3)   NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, metric_name),
    KEY idx_fun_metric (metric_name, event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：行为漏斗日报（长表：日 + 口径名 + 指标值；各步骤用户数与严格/全局转化双口径），一行 = 一日一指标';