-- ============================================================================
-- ODS 批次日志表（Phase 2）
--
-- 层级: 数仓公共服务层（非贴源层），为 ODS 入库过程提供批次级审计。
-- 用途: 每次入库一个源文件登记一行，记录文件 SHA-256、目标表、提交行数、
--       起止时间与状态，是幂等跳单、失败恢复、断点续跑的依据。
-- 状态机: PENDING -> LOADED | FAILED；FAILED/PENDING 再经 cleanup 置为 CLEANED。
-- 重复批次检测: 同一 (source_file, file_sha256) 已有 LOADED 即视为已入库。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本（不要手工改表）。
-- ============================================================================
CREATE TABLE IF NOT EXISTS etl_batch_log (
    batch_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    source_file   VARCHAR(255) NOT NULL COMMENT '来源文件名（basename，如 events.csv）',
    file_sha256   CHAR(64)      NOT NULL COMMENT '源文件整体 SHA-256（16 进制），用于重复批次检测',
    table_name    VARCHAR(64)   NOT NULL COMMENT '目标 ODS 表名',
    status        VARCHAR(16)   NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING/LOADED/FAILED/CLEANED',
    row_count     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本批次实际提交的原始行数（失败时是失败前已提交的部分行数）',
    started_at    DATETIME(3)   NOT NULL COMMENT '批次运行开始（UTC）',
    finished_at   DATETIME(3)   NULL COMMENT '批次运行结束（UTC）',
    error_summary TEXT          NULL COMMENT '失败原因（异常类型 + 消息，已截断；不包含任何凭据）',
    created_at    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (batch_id),
    KEY idx_batch_source_sha (source_file, file_sha256),
    KEY idx_batch_status_started (status, started_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ODS 批次日志：每文件一行的入库审计记录（SHA256、行数、状态、时长），驱动幂等与断点恢复';