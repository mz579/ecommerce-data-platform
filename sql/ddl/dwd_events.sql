-- ============================================================================
-- DWD 明细标准化层：行为事件标准明细表（Phase 3）
--
-- 层级: DWD（Data Warehouse Detail / 明细标准化层）。
-- 用途: 由 ODS 贴源层 ods_events_raw 转换而来：时间标准化为 DATETIME(3) UTC、
--       事件类型映射为标准枚举 {view, add_to_cart, transaction}、ID 类型化为
--       BIGINT、按业务键去重、异常隔离。一行 = 一个标准行为事件（去重后）。
-- 幂等性: 本表内容完全由 ODS 决定，可整表/按 event_date 重建（见
--         docs/dwd_design.md）。row_key = ODS 确定性代理键（血缘），
--         dedup_key = 业务键 SHA-256（恒等去重依据），二者均唯一。
-- 来源:   ods_events_raw（events.csv @ Phase 2）。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwd_events (
    event_sk          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT
                      COMMENT '代理主键（本表内唯一即可，重建后值可变化；跨运行血缘由 row_key 保证）',
    row_key           BINARY(32)      NOT NULL
                      COMMENT 'ODS 确定性代理键（来源行血缘，跨运行不变）',
    dedup_key         CHAR(64)        NOT NULL
                      COMMENT "业务键 SHA-256 十六进制 = SHA-256(event_time_ms, visitor_id, event_type, item_id, COALESCE(transaction_id,0))",
    visitor_id        BIGINT          NULL
                      COMMENT '访客 ID（BIGINT）；原始 "0" 哨兵 → NULL + flag visitor_zero',
    item_id           BIGINT          NOT NULL COMMENT '商品 ID（BIGINT）',
    raw_event_type    VARCHAR(32)     NOT NULL COMMENT 'ODS 原始事件类型（view/addtocart/transaction），主方案要求保留原值',
    event_type        VARCHAR(32)     NOT NULL COMMENT 'DWD 标准事件类型（view/add_to_cart/transaction）',
    event_time_utc    DATETIME(3)     NOT NULL COMMENT '事件时刻（毫秒时间戳 → UTC，DATETIME(3) 保毫秒位）',
    event_date_utc    DATE            NOT NULL COMMENT '事件 UTC 日（分区键，event_time_utc 派生）',
    transaction_id    BIGINT          NULL COMMENT '订单级标识（仅 transaction 事件非空；非行级唯一）',
    quality_flags     VARCHAR(128)    NOT NULL DEFAULT 'ok'
                      COMMENT '逗号分隔异常 flag；无异常为 "ok"。取值见 docs/dwd_design.md',
    build_id          BIGINT UNSIGNED NOT NULL COMMENT 'dwd_build_log.build_id（本条由哪个 DWD 批次产生）',
    ingest_batch_id   BIGINT UNSIGNED NOT NULL COMMENT '来源 ODS 行所属 etl_batch_log.batch_id（上游血缘）',
    source_file       VARCHAR(255)    NOT NULL COMMENT '来源文件名（events.csv）',
    source_row_number BIGINT UNSIGNED NOT NULL COMMENT '来源 CSV 物理行号（表头=1），sed -n 可回查',
    loaded_at         DATETIME(3)     NOT NULL COMMENT 'DWD 写入时刻（UTC）',
    PRIMARY KEY (event_sk),
    UNIQUE KEY uk_dwd_events_row_key (row_key),
    UNIQUE KEY uk_dwd_events_dedup (dedup_key),
    UNIQUE KEY uk_dwd_events_source (source_file, source_row_number),
    KEY idx_dwd_events_date_type (event_date_utc, event_type),
    KEY idx_dwd_events_date_visitor (event_date_utc, visitor_id),
    KEY idx_dwd_events_date_item (event_date_utc, item_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWD：行为事件标准明细（时间标准化 + 事件映射 + 业务键去重 + 标志位），一行 = 一个去重后标准事件';