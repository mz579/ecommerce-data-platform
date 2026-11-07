-- ============================================================================
-- ADS 应用层：商品行为去重商品数日报——物化表（Phase 7 R16）
--
-- 背景: 原 v_ads_item_behavior_daily 视图对 dwd_events 做整窗按日 distinct item
--       统计，冷启动约 10.0s/次；产品分析看板（top 商品行为）每次加载都重算。
--       R16 要求把昂贵聚合预计算进 ADS 物理表、随构建幂等重建。
-- 粒度: 一行 = 一个 UTC 日（event_date_utc），139 行 = 生产完整窗口 139 天。
-- 口径: 与 docs/metric_definitions.md §9 及原视图完全一致：
--   items_viewed = 当日 view    行为的 COUNT(DISTINCT item_id)
--   items_carted = 当日 add_to_cart 行为的 COUNT(DISTINCT item_id)
--   items_bought = 当日 transaction 行为的 COUNT(DISTINCT item_id)
--   is_full_day  = 当日是否完整日（完整日集合内 = 1，边界渲染日 = 0），
--                  含义与 ads_traffic_daily.is_full_day 一致
-- 来源: dwd_events，仅统计 item_id IS NOT NULL 的事件（与源视图同口径）。
-- 构建: 通过 tmp_agg_days 限定在当批构建范围内做按日聚合（FULL/DATES 通用）。
-- 幂等: delete-scope + re-insert（按 event_date_utc IN (...) 精确覆盖）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；列结构变更请走迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_item_behavior_daily (
    event_date_utc  DATE              NOT NULL COMMENT 'UTC 日（PK）',
    items_viewed    BIGINT UNSIGNED   NOT NULL COMMENT '当日 view 行为 distinct item 数',
    items_carted    BIGINT UNSIGNED   NOT NULL COMMENT '当日 add_to_cart 行为 distinct item 数',
    items_bought    BIGINT UNSIGNED   NOT NULL COMMENT '当日 transaction 行为 distinct item 数',
    is_full_day     TINYINT           NOT NULL COMMENT '完整日标致：完整日集合内 = 1，否则 0',
    build_id        BIGINT UNSIGNED   NOT NULL COMMENT 'dws_ads_build_log.build_id（产生本行的批次）',
    loaded_at       DATETIME(3)       NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：商品行为去重商品数日报物化表（139 行，top 商品行为直接读取，0.00s 冷启动）'