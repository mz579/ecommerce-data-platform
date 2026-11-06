-- ============================================================================
-- DWD 构建批次日志（Phase 3）
--
-- 用途: 每次 DWD 构建（一个 scope：全量或一组 UTC 日）登记一行，记录 scope、
--       transform 版本、处理/标准/重复/拒绝四类行数与起止时间，是幂等重建、
--       断点续跑、失败清理的依据。
-- 状态机: RUNNING -> LOADED | FAILED；FAILED/遗留 RUNNING 经 cleanup 置 CLEANED。
-- 与 etl_batch_log 的区别: etl_batch_log 以「源文件 + SHA-256」为幂等键、无
--   行数分类；本表以「scope（event_date 集合）」为键、带 ok/duplicate/reject
--   细分计数，语义独立，故单独建表（见 docs/dwd_design.md §5 的取舍说明）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dwd_build_log (
    build_id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_type        VARCHAR(16)     NOT NULL COMMENT 'FULL = 全量重建；DATES = 按 UTC 日重建',
    dates_csv         VARCHAR(255)    NULL COMMENT 'scope_type=DATES 时的逗号分隔日期列表（UTC），FULL 为 NULL',
    status            VARCHAR(16)     NOT NULL DEFAULT 'RUNNING' COMMENT 'RUNNING/LOADED/FAILED/CLEANED',
    transform_version VARCHAR(32)     NOT NULL COMMENT '转换规则版本（逻辑变更须强制全量重建）',
    processed_rows    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本 scope ODS 处理行数（= OK+重复+拒绝）',
    ok_rows           BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '进入 dwd_events 的标准事件行数',
    duplicate_rows    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '进入 dwd_events_duplicate 的重复行数',
    reject_rows       BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '进入 dwd_events_reject 的拒绝行数',
    started_at        DATETIME(3)     NOT NULL COMMENT '构建启动（UTC）',
    finished_at       DATETIME(3)     NULL,
    error_summary     TEXT            NULL COMMENT '失败原因（异常类型 + 消息，不含凭据）',
    created_at        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (build_id),
    KEY idx_build_scope (scope_type, dates_csv),
    KEY idx_build_status (status, started_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWD 构建日志：每次构建一行（scope、版本、OK/重复/拒绝行数、起止、状态），驱动幂等重建与断点续跑';