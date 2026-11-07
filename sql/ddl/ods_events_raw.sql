-- ============================================================================
-- ODS 贴源层：原始行为事件表（Phase 2）
--
-- 层级: ODS（Operational Data Store / 贴源层），数仓分层第一层。
-- 用途: 原样保留 events.csv 的每一行（原始字段一律按字符串变量保真入库，
--       本层不做清洗、不去重、不做类型转换，即使字段值与契约相违背也照收），
--       同时附加血缘元数据列 source_file / source_row_number / ingest_batch_id /
--       loaded_at，支持按批次审计、回滚与重放。
-- 幂等性: 确定性代理键 row_key + 唯一键 (source_file, source_row_number)，
--         重复导入同一文件不会增加行数（规则见 docs/ods_design.md）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ods_events_raw (
    row_key          BINARY(32)    NOT NULL
                     COMMENT '确定性代理键 = SHA-256(UTF-8(source_file + U+001F + source_row_number)) 的 32 字节摘要，跨批次跨运行不变',
    `timestamp`      VARCHAR(64)   NOT NULL COMMENT '原始字段：事件时间戳（Unix 毫秒），字符串保真',
    visitorid        VARCHAR(64)   NOT NULL COMMENT '原始字段：访客 ID，字符串保真（含哨兵值 0）',
    event            VARCHAR(32)   NOT NULL COMMENT '原始字段：事件类型（view/addtocart/transaction）',
    itemid           VARCHAR(64)   NOT NULL COMMENT '原始字段：商品 ID，字符串保真',
    transactionid    VARCHAR(64)   NOT NULL COMMENT '原始字段：交易 ID，字符串保真（非交易事件为空串）',
    source_file      VARCHAR(255)  NOT NULL COMMENT '来源文件名（basename，如 events.csv）',
    source_row_number BIGINT UNSIGNED NOT NULL COMMENT 'CSV 物理行号（含表头偏移：表头为第 1 行，首条数据行为第 2 行）',
    ingest_batch_id  BIGINT UNSIGNED NOT NULL COMMENT 'etl_batch_log.batch_id，逻辑外键（不建物理 FK 以保证灌数性能）',
    loaded_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '本批次写入时刻（UTC）',
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_ods_events_raw_source (source_file, source_row_number),
    KEY idx_ods_events_raw_batch (ingest_batch_id),
    -- Phase 8 perf: quality rules GROUP BY (source_file, ingest_batch_id); this
    -- composite index makes ods_batch_log_rowcount/coverage index-only scans.
    -- NOTE: a functional index on CAST(`timestamp` AS UNSIGNED) was evaluated and
    -- rejected: it makes a raw non-numeric timestamp fail INSERT with error 3751,
    -- breaking the ODS contract that keeps dirty rows for DWD rejection.
    KEY idx_ods_events_raw_file_batch (source_file, ingest_batch_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ODS 贴源层：events.csv 原始行为事件（字符串保真、不清洗、不去重，可审计可重放）';