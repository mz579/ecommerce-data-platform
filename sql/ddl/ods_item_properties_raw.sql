-- ============================================================================
-- ODS 贴源层：商品属性原始表（Phase 2）
--
-- 层级: ODS（Operational Data Store / 贴源层）。
-- 用途: 原样保留 item_properties_part1.csv / item_properties_part2.csv 的每一行
--       （两个文件表头相同、可拼接，各自独立入库），原始字段全部作为字符串
--       VARCHAR/TEXT 保真，本层不做清洗 / 去重 / 类型转换（property 是自由文本
--       字典、value 长度不定，务必保持原样，见 Phase 1 数据字典 §2）。
-- 幂等性: row_key + 唯一键 (source_file, source_row_number)，同文件重复导入不增行。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ods_item_properties_raw (
    row_key          BINARY(32)    NOT NULL
                     COMMENT '确定性代理键 = SHA-256(UTF-8(source_file + U+001F + source_row_number)) 的 32 字节摘要',
    `timestamp`      VARCHAR(64)   NOT NULL COMMENT '原始字段：属性快照时间（Unix 毫秒），字符串保真',
    itemid           VARCHAR(64)   NOT NULL COMMENT '原始字段：商品 ID，字符串保真',
    property         VARCHAR(128)  NOT NULL COMMENT '原始字段：属性名（1,104 种自由文本，多为数字编码），字符串保真',
    `value`          TEXT          NOT NULL COMMENT '原始字段：属性值（长度不定，用 TEXT 保证不截断），字符串保真',
    source_file      VARCHAR(255)  NOT NULL COMMENT '来源文件名（basename，如 item_properties_part1.csv）',
    source_row_number BIGINT UNSIGNED NOT NULL COMMENT 'CSV 物理行号（含表头偏移：表头为第 1 行，首条数据行为第 2 行）',
    ingest_batch_id  BIGINT UNSIGNED NOT NULL COMMENT 'etl_batch_log.batch_id，逻辑外键',
    loaded_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '本批次写入时刻（UTC）',
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_ods_ip_raw_source (source_file, source_row_number),
    KEY idx_ods_ip_raw_batch (ingest_batch_id),
    -- Phase 8 perf: DWS category as-of fills scan property='categoryid' over 20M
    -- rows (62s -> 13s measured); quality batch grouping benefits from file_batch.
    KEY idx_ods_ip_raw_property (property),
    KEY idx_ods_ip_raw_file_batch (source_file, ingest_batch_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ODS 贴源层：item_properties 商品属性原始快照（part1/part2 各自整文件入库，字符串保真）';