-- ============================================================================
-- DWD 明细标准化层：异常隔离表（Phase 3）
--
-- 用途: 凡无法转换为标准事件的 ODS 原始行，整行带证据写入本表（异常隔离，
--       不丢弃、不用降级值填充）。reject_reason 为枚举，便于按原因排查：
--       invalid_timestamp / missing_item / missing_visitor / invalid_item_id /
--       invalid_visitor_id / unknown_event_type / missing_txn_on_transaction /
--       unexpected_txnid / invalid_transaction_id（完整口径见 docs/dwd_design.md）。
-- 血缘: 任意一行可凭 (source_file, source_row_number) 回查 ODS 与原始 CSV 原文。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwd_events_reject (
    row_key           BINARY(32)      NOT NULL
                      COMMENT 'ODS 确定性代理键（被拒原始行的血缘）',
    raw_timestamp     VARCHAR(64)     NOT NULL COMMENT '原始字段：时间戳（字符串保真）',
    raw_visitorid     VARCHAR(64)     NOT NULL COMMENT '原始字段：访客 ID（字符串保真）',
    raw_event         VARCHAR(32)     NOT NULL COMMENT '原始字段：事件类型（字符串保真）',
    raw_itemid        VARCHAR(64)     NOT NULL COMMENT '原始字段：商品 ID（字符串保真）',
    raw_transactionid VARCHAR(64)     NOT NULL COMMENT '原始字段：交易 ID（字符串保真）',
    event_type        VARCHAR(32)     NULL COMMENT '转换尝试结果：未知事件时为 unknown，其余为 NULL',
    event_time_utc    DATETIME(3)     NULL COMMENT '时间戳合法时的事件时刻（UTC），非法时间戳为 NULL',
    event_date_utc    DATE            NULL COMMENT '事件 UTC 日（时间戳合法时），非法时间戳为 NULL',
    reject_reason     VARCHAR(32)     NOT NULL COMMENT '拒绝原因枚举（见表注释）',
    reject_message    VARCHAR(255)    NULL COMMENT '人类可读的拒绝原因说明（定位用）',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dwd_build_log.build_id',
    ingest_batch_id   BIGINT UNSIGNED NOT NULL COMMENT '来源 ODS 行所属批号',
    source_file       VARCHAR(255)    NOT NULL,
    source_row_number BIGINT UNSIGNED NOT NULL,
    loaded_at         DATETIME(3)     NOT NULL COMMENT 'DWD 写入时刻（UTC）',
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_dwd_reject_source (source_file, source_row_number),
    KEY idx_dwd_reject_reason (reject_reason),
    KEY idx_dwd_reject_date (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWD：异常隔离表（拒绝的策略原样入此，保留 raw 字段与血量元数据，可审计可重放）';