-- ============================================================================
-- ADS 应用层：商品热度日排行 Top-N（Phase 4）
--
-- 层级: ADS（Application Data Store / 应用层）。
-- 用途: 每日商品「行为量热度」榜（默认 Daily Top-100，可由构建参数调整）。
--       排行依据：view_cnt 降序 → add_to_cart_cnt 降序 → transaction_cnt 降序
--       → item_id 降序（确定性平局裁决）；全量行为统计旁挂 dws_item_daily。
-- 粒度: 一行 = 一个商品在一个 UTC 日的上榜记录（仅保留当日 Top-N）。
-- 时区: 全链路 UTC；event_date_utc 日界 = 世界时零点。
-- 幂等性: 排行完全由 dws_item_daily 决定；按 event_date_utc 整日/全量重建即可。
-- 部分日: is_full_day 语义同 ads_traffic_daily（榜首列出也遵循同一完整日标致）。
-- 来源: dws_item_daily（DWS 商品日汇总）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_item_ranking_daily (
    event_date_utc   DATE            NOT NULL COMMENT 'UTC 日（排行窗口，日界=世界时零点）',
    item_id          BIGINT          NOT NULL COMMENT '商品 ID',
    rank_no          INT UNSIGNED    NOT NULL COMMENT '当日行为量排名（1=热度最高；平局按加购/交易/商品 ID 裁决）',
    view_cnt         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该商品 view 事件行数',
    add_to_cart_cnt  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该商品 add_to_cart 事件行数',
    transaction_cnt  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该商品 transaction 事件行数',
    view_uv          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日浏览该商品去重访客数',
    add_to_cart_uv   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日加购该商品去重访客数',
    transaction_uv   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日成交该商品去重访客数',
    total_events     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该商品三类事件合计',
    is_full_day       TINYINT         NOT NULL COMMENT '1=完整自然日；0=部分日',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at         DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, item_id),
    UNIQUE KEY uk_rank_day (event_date_utc, rank_no),
    KEY idx_rank_item (item_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：商品热度日排行 Top-N（日 + item_id；按行为量排序的行为量与统计透视），一行 = 一商品一日的上榜条目';