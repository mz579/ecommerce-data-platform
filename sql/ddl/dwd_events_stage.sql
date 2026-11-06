-- ============================================================================
-- DWD 明细标准化层：转换暂存表（Phase 3，内部临时产物）
--
-- 用途: 从 ODS 流式读取 → 纯函数转换的结果统一暂存此处（OK 与 REJECT 都进），
--       再由一个 SQL 窗口步骤（dwd_events_stage → dwd_events / _duplicate / _reject
--       三表分发）完成「业务键去重 + 异常隔离」。stage 是本批次的转换中间态，
--       构建完成后**保留**到 cleanup_build（或下一次同 scope 重建的 delete-scope
--       阶段）才被清空，不是最终明细；保留是为了断点续跑可直接复用它已有行数。
-- 好处: ① 转换 pass 与去重 pass 解耦，转换只是「行级纯函数」，无全局状态；
--       ② 幂等/断点续跑以 row_key 主键 + 计数实现（同 Phase 2 ODS 加载器思路）；
--       ③ 去重判定交给 MySQL 窗口函数，跨 275 万行也精确、不占 Python 内存。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwd_events_stage (
    row_key           BINARY(32)      NOT NULL
                      COMMENT 'ODS 确定性代理键（跨 pass 的唯一载体）',
    raw_timestamp     VARCHAR(64)     NOT NULL,
    raw_visitorid     VARCHAR(64)     NOT NULL,
    raw_event         VARCHAR(32)     NOT NULL,
    raw_itemid        VARCHAR(64)     NOT NULL,
    raw_transactionid VARCHAR(64)     NOT NULL,
    visitor_id        BIGINT          NULL,
    item_id           BIGINT          NULL,
    raw_event_type    VARCHAR(32)     NOT NULL,
    event_type        VARCHAR(32)     NULL COMMENT 'OK 行=标准事件；REJECT 行中未知事件存 unknown，其余 NULL',
    event_time_utc    DATETIME(3)     NULL,
    event_date_utc    DATE            NULL,
    transaction_id    BIGINT          NULL,
    dedup_key         CHAR(64)        NULL COMMENT 'OK 行业务键 SHA-256；REJECT 行为 NULL',
    quality_flags     VARCHAR(128)    NOT NULL DEFAULT 'ok',
    record_status     VARCHAR(8)      NOT NULL COMMENT 'OK = 标准通道候选；REJECT = 异常通道',
    reject_reason     VARCHAR(32)     NULL,
    reject_message    VARCHAR(255)    NULL,
    ingest_batch_id   BIGINT UNSIGNED NOT NULL,
    source_file       VARCHAR(255)    NOT NULL,
    source_row_number BIGINT UNSIGNED NOT NULL,
    loaded_at         DATETIME(3)     NOT NULL,
    build_id          BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (row_key),
    UNIQUE KEY uk_dwd_stage_source (source_file, source_row_number),
    KEY idx_dwd_stage_dedup (dedup_key, source_row_number),
    KEY idx_dwd_stage_date (event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWD 内部暂存：ODS→DWD 转换中间态（OK/REJECT），构建后保留至 cleanup_build/同 scope 重建';