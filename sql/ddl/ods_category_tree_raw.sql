-- ============================================================================
-- ODS 贴源层：类目树原始表（Phase 2）
--
-- 层级: ODS（Operational Data Store / 贴源层）。
-- 用途: 原样保留 category_tree.csv 的每一行，原始字段字符串保真，本层不做
--       清洗 / 去重 / 类型转换；根节点的空 parentid 以空串原样保留。
-- 幂等性: row_key + 唯一键 (source_file, source_row_number)，同文件重复导入不增行。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ods_category_tree_raw (
    row_key          BINARY(32)    NOT NULL
                     COMMENT '确定性代理键 = SHA-256(UTF-8(source_file + U+001F + source_row_number)) 的 32 字节摘要',
    categoryid       VARCHAR(64)   NOT NULL COMMENT '原始字段：类目 ID，字符串保真',
    parentid         VARCHAR(64)   NOT NULL COMMENT '原始字段：父类目 ID（根节点为空串），字符串保真',
    source_file      VARCHAR(255)  NOT NULL COMMENT '来源文件名（basename，如 category_tree.csv）',
    source_row_number BIGINT UNSIGNED NOT NULL COMMENT 'CSV 物理行号（含表头偏移：表头为第 1 行，首条数据行为第 2 行）',
    ingest_batch_id  BIGINT UNSIGNED NOT NULL COMMENT 'etl_batch_log.batch_id，逻辑外键',
    loaded_at        DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '本批次写入时刻（UTC）',
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_ods_ct_raw_source (source_file, source_row_number),
    KEY idx_ods_ct_raw_batch (ingest_batch_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ODS 贴源层：category_tree.csv 类目树原始记录（字符串保真，根节点 parentid 为空串）';