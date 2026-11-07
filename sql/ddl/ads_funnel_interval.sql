-- ============================================================================
-- ADS 应用层：区间（完整窗口）用户级漏斗——物化单行表（Phase 7 R16）
--
-- 背景: 原 v_ads_funnel_interval 视图对 dwd_events 做整窗用户级聚合，冷启动约
--       9.3s/次；6 张 KPI 卡片同读它使看板加载远超前端 60s 超时、截图卡在
--       'Waiting on'。R16 要求把昂贵聚合预计算进 ADS 物理表、随构建幂等重建。
-- 粒度: 一行 = 一个完整窗口 [window_start, window_end] 的区间漏斗（id 恒为 1）。
-- 窗口: FULL 构建以构建参数 window_start / window_end 为窗口（生产 = 完整日
--       集合 [2015-05-04, 2015-09-17]，首末日不参与）。
-- 口径: 与 docs/metric_definitions.md §2/§4/§7 及原视图完全一致：
--   view_users        = 窗口内 view 去重访客（区间 UV，分母）
--   cart_users        = 窗口内 add_to_cart 去重访客
--   buyer_users       = 窗口内 transaction 去重访客
--   orders            = 窗口内 COUNT(DISTINCT transaction_id)
--   conv_strict_users = 窗口内 交易用户 ∩ 浏览用户（严格漏斗分子）
--   conv_global_users = 窗口内 全部交易用户（全局口径分子，可 > 分母）
--   *_rate            = 分子 / view_users（分母 0 → NULL；conv_global_rate 可 >1 合法）
-- 来源: dwd_events（与原有视图同一份聚合 SQL，仅改为 INSERT ... SELECT 落表）。
-- 幂等: FULL 构建先按全量删除再重插（delete + insert）；DATES 构建不触碰本表
--       （窗口级数据不受单日重建影响，保留最后一次 FULL 的 build_id 血缘）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；列结构变更请走迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_funnel_interval (
    id                  TINYINT UNSIGNED  NOT NULL COMMENT '固定 = 1（单行区间口报表）',
    window_start        DATE              NOT NULL COMMENT '窗口起点（UTC，含）',
    window_end          DATE              NOT NULL COMMENT '窗口终点（UTC，含）',
    view_users          BIGINT UNSIGNED   NOT NULL COMMENT '窗口内浏览用户数（view 去重访客 = 区间 UV）',
    cart_users          BIGINT UNSIGNED   NOT NULL COMMENT '窗口内加购用户数',
    buyer_users         BIGINT UNSIGNED   NOT NULL COMMENT '窗口内交易用户数',
    orders              BIGINT UNSIGNED   NOT NULL COMMENT '窗口内订单数 = distinct(transaction_id)',
    conv_strict_users   BIGINT UNSIGNED   NOT NULL COMMENT '窗口内交易用户 ∩ 浏览用户',
    conv_global_users   BIGINT UNSIGNED   NOT NULL COMMENT '窗口内全部交易用户（全局口径分子）',
    add_to_cart_rate    DECIMAL(20,6)     NULL COMMENT 'cart_users / view_users（分母 0 → NULL）',
    conv_strict_rate    DECIMAL(20,6)     NULL COMMENT 'conv_strict_users / view_users',
    conv_global_rate    DECIMAL(20,6)     NULL COMMENT 'conv_global_users / view_users（可 > 1，合法）',
    build_id            BIGINT UNSIGNED   NOT NULL COMMENT 'dws_ads_build_log.build_id（最后一次产生本行的 FULL 批次）',
    loaded_at           DATETIME(3)       NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：区间（完整窗口）用户级漏斗物化单行表（KPI 卡片直接读取，0.00s 冷启动）'