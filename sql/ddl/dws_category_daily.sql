-- ============================================================================
-- DWS 汇总层：类目行为日汇总（Phase 4）
--
-- 层级: DWS（Data Warehouse Summary / 汇总层）。
-- 用途: 类目维度的行为量 / 用户量日汇总。商品 → 类目的归属走 item_properties
--       的 categoryid 属性，按 as-of 快照语义：取事件日当天或之前最近的
--       categoryid 快照，绝不使用未来属性（见 docs/dws_ads_design.md §3.3）。
-- 粒度: 一行 = 一个类目在一个 UTC 日的去重汇总。
-- 时区: 全链路 UTC；快照与事件的日界 = 世界时零点。
-- 幂等性: 完全由 dwd_events + ods_item_properties_raw 决定；按 event_date_utc
--         整日/全量重建即可。
-- 边界: category_id = 0 表示「该商品在事件日当天及之前没有任何 categoryid
--       快照」（含事件早于属性窗口 2015-05-10 的情况），不是真实类目。
--       categoryid 属性值有 30 个不落在 category_tree 上，仍按属性值原样聚合
--       （不校验类目树，保留脏值可审计）。
-- 来源: dwd_events（行为）+ ods_item_properties_raw（categoryid 快照，as-of）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dws_category_daily (
    event_date_utc    DATE            NOT NULL COMMENT '事件 UTC 日（分区键）',
    category_id       BIGINT          NOT NULL COMMENT '类目 ID（as-of 命中 categoryid 属性值；0 = 无可用快照的未分类桶）',
    view_cnt          BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该类目 view 事件行数（不去重）',
    add_to_cart_cnt   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该类目 add_to_cart 事件行数',
    transaction_cnt   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该类目 transaction 事件行数',
    view_uv           BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日浏览该类目商品去重访客数（visitor_id IS NOT NULL）',
    add_to_cart_uv    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日加购该类目商品去重访客数',
    transaction_uv    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日成交该类目商品去重访客数',
    order_cnt         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当日该类目 distinct(transaction_id) 数',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at         DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, category_id),
    KEY idx_cds_category (category_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWS：类目行为日汇总（日 + category_id；行为量 + 去重用户量，商品归属按 categoryid 属性 as-of 快照），一行 = 一类目一日';