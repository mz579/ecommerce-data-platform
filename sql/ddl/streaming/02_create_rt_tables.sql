-- ============================================================================
-- 实时结果库 ecommerce_rt：5 张结果/审计表（Phase 9.2）
--
-- 设计依据：docs/streaming_design.md §7.2（表结构）与 §7.3（幂等 upsert）。
-- 本文件不指定 database 前缀，由 scripts/apply_rt_ddl.py 连接到目标库后执行；
-- 目标库硬性限制为 `ecommerce_rt` 或以 `_test` 结尾（禁止 ecommerce_dw）。
-- 全部为 CREATE TABLE IF NOT EXISTS，可重复执行。
-- ============================================================================

-- 日流量结果：PV / UV / DAU（UTC 日窗口，主键 = event_date_utc）
CREATE TABLE IF NOT EXISTS rt_traffic_daily (
    event_date_utc   DATE             NOT NULL COMMENT '事件 UTC 日（窗口 [D 00:00, D+1 00:00)）',
    pv               BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '页面浏览事件数（含 visitor_id IS NULL）',
    uv               BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '浏览去重访客数（排除 NULL visitor）',
    dau              BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '活跃去重访客数（任意事件，排除 NULL visitor）',
    window_start     DATETIME(3)      NOT NULL COMMENT '窗口起点（UTC）',
    window_end       DATETIME(3)      NOT NULL COMMENT '窗口终点（UTC，不含）',
    source_watermark DATETIME(3)      NULL COMMENT '生成该结果时的水位线（UTC）',
    is_final         TINYINT          NOT NULL DEFAULT 0 COMMENT '1=窗口已越过 day_end+allowed_lateness',
    updated_at       DATETIME(3)      NOT NULL COMMENT '结果写入时刻（UTC）',
    PRIMARY KEY (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '实时：日流量 PV/UV/DAU（幂等 upsert）';

-- 日漏斗结果长表：每个指标一行（主键 = event_date_utc + metric_name）
CREATE TABLE IF NOT EXISTS rt_funnel_daily (
    event_date_utc   DATE             NOT NULL COMMENT '事件 UTC 日',
    metric_name      VARCHAR(48)      NOT NULL COMMENT '指标名：view_users/cart_users/buyer_users/orders/add_to_cart_rate/conv_strict_rate/conv_global_rate',
    metric_value     DECIMAL(20, 6)   NULL COMMENT '指标值；比率类可为 NULL（分母为 0）',
    window_start     DATETIME(3)      NOT NULL COMMENT '窗口起点（UTC）',
    window_end       DATETIME(3)      NOT NULL COMMENT '窗口终点（UTC，不含）',
    source_watermark DATETIME(3)      NULL COMMENT '生成该结果时的水位线（UTC）',
    is_final         TINYINT          NOT NULL DEFAULT 0 COMMENT '1=窗口已越过 day_end+allowed_lateness',
    updated_at       DATETIME(3)      NOT NULL COMMENT '结果写入时刻（UTC）',
    PRIMARY KEY (event_date_utc, metric_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '实时：日漏斗指标长表（幂等 upsert）';

-- 日商品行为结果：按事件类型去重的商品数
CREATE TABLE IF NOT EXISTS rt_item_behavior_daily (
    event_date_utc   DATE             NOT NULL COMMENT '事件 UTC 日',
    items_viewed     BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '当日被浏览的去重商品数',
    items_carted     BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '当日被加购的去重商品数',
    items_bought     BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '当日被购买的去重商品数',
    window_start     DATETIME(3)      NOT NULL COMMENT '窗口起点（UTC）',
    window_end       DATETIME(3)      NOT NULL COMMENT '窗口终点（UTC，不含）',
    is_final         TINYINT          NOT NULL DEFAULT 0 COMMENT '1=窗口已越过 day_end+allowed_lateness',
    updated_at       DATETIME(3)      NOT NULL COMMENT '结果写入时刻（UTC）',
    PRIMARY KEY (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '实时：日商品行为去重商品数（幂等 upsert）';

-- 对账审计：实时值与离线 ADS 值逐指标对比
CREATE TABLE IF NOT EXISTS rt_reconciliation_daily (
    event_date_utc   DATE             NOT NULL COMMENT '对账事件 UTC 日',
    metric_name      VARCHAR(64)      NOT NULL COMMENT '对账指标名（pv/uv/dau/orders/...）',
    offline_value    DECIMAL(20, 6)   NULL COMMENT '离线 ADS 值',
    realtime_value   DECIMAL(20, 6)   NULL COMMENT '实时结果表值',
    abs_diff         DECIMAL(20, 6)   NULL COMMENT 'realtime - offline',
    rel_diff         DECIMAL(12, 6)   NULL COMMENT '相对差异；offline=0 时为 NULL',
    status           VARCHAR(16)      NOT NULL COMMENT 'MATCH / DIFF / SKIP',
    checked_at       DATETIME(3)      NOT NULL COMMENT '对账执行时刻（UTC）',
    PRIMARY KEY (event_date_utc, metric_name)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '实时：批流对账审计（一次对账一行指标）';

-- 迟到旁路：超过 allowed lateness 的事件原文，供审计与差异解释
CREATE TABLE IF NOT EXISTS rt_late_events (
    event_id         VARCHAR(128)     NOT NULL COMMENT '事件确定性 ID（= DWD dedup_key）',
    event_time_utc   DATETIME(3)      NOT NULL COMMENT '事件业务时间（UTC）',
    detected_at      DATETIME(3)      NOT NULL COMMENT '检测到迟到的时刻（UTC）',
    lateness_ms      BIGINT           NOT NULL COMMENT '迟到毫秒数',
    reason           VARCHAR(64)      NOT NULL COMMENT '迟到原因分类',
    payload          JSON             NOT NULL COMMENT '事件原始 JSON',
    PRIMARY KEY (event_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '实时：超过 allowed lateness 的事件旁路审计';
