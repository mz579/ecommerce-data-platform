-- ============================================================================
-- DWS 汇总层：用户行为日汇总（Phase 4）
--
-- 层级: DWS（Data Warehouse Summary / 汇总层）。
-- 用途: 从 DWD 标准明细 dwd_events 按「UTC 日 + 访客」聚合出一行的行为计数，
--       ADS 层 PV 之外的按人指标（UV / DAU / 加购率 / 转化率）都从这里取数。
-- 粒度: 一行 = 一个访客在一个 UTC 日的去重汇总（visitor_id 非空或未知桶）。
-- 时区: 事件时刻与 event_date_utc 全链路 UTC；日界为世界时零点。
-- 幂等性: 本表完全由 dwd_events 决定；按 event_date_utc 整日/全量重建即可。
-- 未知访客桶: DWD 中 visitor_id IS NULL（原始哨兵 "0"）的行在此聚合为一行
--   visitor_id=0 + visitor_zero=1（真实访客 id >= 1，见 dwd 构建约束，无冲突）。
--   该桶计入行为计数（PV），不计入任何去重用户口径（UV / DAU 等）。
-- 来源: dwd_events（去重后标准明细）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dws_user_daily (
    event_date_utc   DATE            NOT NULL COMMENT '事件 UTC 日（分区键，日界=世界时零点）',
    visitor_id       BIGINT          NOT NULL COMMENT '访客 ID（>=1 为真实访客；0 = 未知访客桶）',
    visitor_zero     TINYINT         NOT NULL DEFAULT 0
                     COMMENT '1 = 未知访客桶（visitor_id=0，DWD 中为 NULL / 原始哨兵 "0"）；不计入任何去重用户指标',
    view_cnt         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 view 事件行数（不去重）',
    add_to_cart_cnt  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 add_to_cart 事件行数',
    transaction_cnt  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 transaction 事件行数',
    order_cnt        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该访客 distinct(transaction_id) 数（用户级口径；全局订单数不得对其求和，见 ads_funnel_daily 说明）',
    active_flag      TINYINT         NOT NULL COMMENT '当日是否有任意事件（有事件必有本行，故恒为 1；供按人计数直接过滤）',
    build_id         BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at        DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, visitor_id),
    KEY idx_uds_user (visitor_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWS：用户行为日汇总（日 + visitor_id；view/加购/交易计数、活跃标致、未知访客桶），一行 = 一访客一日';