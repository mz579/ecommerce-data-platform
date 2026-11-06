-- ============================================================================
-- ADS 应用层：站内流量日报（Phase 4）
--
-- 层级: ADS（Application Data Store / 应用层）。
-- 用途: 每日 PV / UV / DAU 一行，直接供报表查询；口径见 docs/metric_definitions.md。
-- 口径速记（均为 UTC 日口径）:
--   PV  = 当日所有 view 事件行数（不含精确重复行，含未知访客桶，不去重）；
--   UV  = 当日发生过 view 的去重访客数（排除未知访客桶 visitor_zero=1）；
--   DAU = 当日发生过任意事件的去重访客数（排除未知访客桶）。
-- 部分日: is_full_day=1 表示 [完整窗口起点, 完整窗口终点] 内的完整自然日；
--   首末日（2015-05-03、2015-09-18）为部分日（is_full_day=0），展示时剔除或打标。
-- 来源: dws_user_daily（DWS 人日汇总）。
-- 幂等性: 按 event_date_utc 整日/全量重建即可；重复执行结果一致。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_traffic_daily (
    event_date_utc  DATE            NOT NULL COMMENT 'UTC 日（主键，日界=世界时零点）',
    pv              BIGINT UNSIGNED NOT NULL COMMENT 'PV：当日 view 事件行数（不去重，含未知访客桶）',
    uv              BIGINT UNSIGNED NOT NULL COMMENT 'UV：当日发生过 view 的去重访客数（口径：event_type=view & visitor_id IS NOT NULL 去重）',
    dau             BIGINT UNSIGNED NOT NULL COMMENT 'DAU：当日任意事件去重访客数（exclude visitor_zero 桶）',
    is_full_day      TINYINT         NOT NULL COMMENT '1=完整自然日；0=部分日（首末日，数据窗口不完整）',
    build_id         BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at        DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：站内流量日报（日维度 PV / UV / DAU，附完整日标致），一行 = 一日';