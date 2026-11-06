-- ============================================================================
-- DWD 明细标准化层：重复事件隔离表（Phase 3）
--
-- 用途: 存放「按业务键去重时被去掉的那些原始行」。恒等式要求：
--       ODS 行数 = dwd_events 行数 + dwd_events_duplicate 行数 + dwd_events_reject 行数。
--       被去掉的行绝不丢弃——保留原始 row_key、原始 5 字段、转换后字段与 dedup_key
--       （可与 dwd_events 中保留的那一行按 dedup_key 关联），可审计、可重放。
-- 主键: row_key（来源行血缘），与 dwd_events / dwd_events_reject 共用同一行键域。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwd_events_duplicate (
    row_key           BINARY(32)      NOT NULL
                      COMMENT 'ODS 确定性代理键（这条被去掉的原始行的血缘）',
    dedup_key         CHAR(64)        NOT NULL
                      COMMENT '业务键 SHA-256；同一 dedup_key 的「保留行」在 dwd_events 中',
    raw_timestamp     VARCHAR(64)     NOT NULL COMMENT '原始字段：时间戳（字符串保真）',
    raw_visitorid     VARCHAR(64)     NOT NULL COMMENT '原始字段：访客 ID（字符串保真，含哨兵 0）',
    raw_event         VARCHAR(32)     NOT NULL COMMENT '原始字段：事件类型（字符串保真）',
    raw_itemid        VARCHAR(64)     NOT NULL COMMENT '原始字段：商品 ID（字符串保真）',
    raw_transactionid VARCHAR(64)     NOT NULL COMMENT '原始字段：交易 ID（字符串保真，非交易为空串）',
    visitor_id        BIGINT          NULL COMMENT '转换后访客 ID（原始 0 → NULL）',
    item_id           BIGINT          NOT NULL COMMENT '转换后商品 ID',
    raw_event_type    VARCHAR(32)     NOT NULL COMMENT 'ODS 原始事件类型',
    event_type        VARCHAR(32)     NOT NULL COMMENT 'DWD 标准事件类型',
    event_time_utc    DATETIME(3)     NOT NULL COMMENT '事件时刻（UTC）',
    event_date_utc    DATE            NOT NULL COMMENT '事件 UTC 日',
    transaction_id    BIGINT          NULL COMMENT '订单级标识',
    quality_flags     VARCHAR(128)    NOT NULL DEFAULT 'ok' COMMENT '与 dwd_events 相同的转换 flags',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dwd_build_log.build_id',
    ingest_batch_id   BIGINT UNSIGNED NOT NULL COMMENT '来源 ODS 行所属批号',
    source_file       VARCHAR(255)    NOT NULL,
    source_row_number BIGINT UNSIGNED NOT NULL,
    loaded_at         DATETIME(3)     NOT NULL COMMENT 'DWD 写入时刻（UTC）',
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_dwd_dup_source (source_file, source_row_number),
    KEY idx_dwd_dup_dedup (dedup_key),
    KEY idx_dwd_dup_date_type (event_date_utc, event_type)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWD：被业务键去重掉的重复原始行（保留原值 + 血缘，供审计/重放，不得丢弃）';