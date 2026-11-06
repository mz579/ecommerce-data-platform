-- ============================================================================
-- DWS 汇总层：商品行为日汇总（Phase 4）
--
-- 层级: DWS（Data Warehouse Summary / 汇总层）。
-- 用途: 从 dwd_events 按「UTC 日 + 商品」聚合行为量，供商详情类分析及
--       ADS 商品排行榜（ads_item_ranking_daily）取数。
-- 粒度: 一行 = 一个商品在一个 UTC 日的去重汇总。
-- 时区: 全链路 UTC；event_date_utc 日界 = 世界时零点。
-- 幂等性: 完全由 dwd_events 决定；按 event_date_utc 整日/全量重建即可。
-- 去重口径: view_uv 等为「该行为且 visitor_id IS NOT NULL」的 distinct visitor；
--           同一访客跨商品可重复计入（商品维度的行内去重，不跨商品）。
-- 来源: dwd_events（去重后标准明细）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dws_item_daily (
    event_date_utc    DATE            NOT NULL COMMENT '事件 UTC 日（分区键）',
    item_id           BIGINT          NOT NULL COMMENT '商品 ID（DWD 标准 BIGINT）',
    view_cnt          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 view 事件行数（不去重）',
    add_to_cart_cnt   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 add_to_cart 事件行数',
    transaction_cnt   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日 transaction 事件行数',
    view_uv           BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日浏览该商品（view）的去重访客数（visitor_id IS NOT NULL）',
    add_to_cart_uv    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日加购该商品（add_to_cart）的去重访客数',
    transaction_uv    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日成交该商品（transaction）的去重访客数',
    order_cnt         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该商品 distinct(transaction_id) 数（跨访客去重）',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at         DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, item_id),
    KEY idx_ids_item (item_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWS：商品行为日汇总（日 + item_id；view/加购/交易事件数 + 去重用户数 + 订单数），一行 = 一商品一日';