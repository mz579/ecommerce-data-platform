-- ============================================================================
-- DWS/ADS 聚合批次日志（Phase 4）
--
-- 用途: 每次 DWS/ADS 构建（一个 scope：全量或一组 UTC 日）登记一行，记录 scope、
--       聚合版本、各表写出行数（JSON）与起止时间，是幂等重建、状态判定的依据。
-- 状态机: RUNNING -> LOADED | FAILED；FAILED/遗留 RUNNING 经 cleanup(-force) 置 CLEANED。
-- 为什么不复用 dwd_build_log / etl_batch_log:
--   etl_batch_log 以「源文件 + SHA-256」为幂等键；dwd_build_log 以 transform 版本 +
--   ok/duplicate/reject 三分类计数为语义，两者都不描述「聚合批次 → 7 张输出表写入行数」
--   的形态。DWS/ADS 构建读的是 dwd_events（已标准化），不涉及文件批与三分类，
--   故独立建表（详见 docs/dws_ads_design.md §5 的取舍说明）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS dws_ads_build_log (
    build_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_type    VARCHAR(16)     NOT NULL COMMENT 'FULL = 全量重建；DATES = 按 UTC 日重建',
    dates_csv     VARCHAR(255)    NULL COMMENT 'scope_type=DATES 时的逗号分隔日期列表（UTC），FULL 为 NULL',
    status        VARCHAR(16)     NOT NULL DEFAULT 'RUNNING' COMMENT 'RUNNING/LOADED/FAILED/CLEANED',
    agg_version   VARCHAR(32)     NOT NULL COMMENT '聚合口径版本（口径/规则变更须重新全量构建）',
    table_stats   TEXT            NULL COMMENT 'JSON：{表名: 写入行数}，本批次各输出表行数',
    rows_written  BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '本批次全部表写入行数合计',
    started_at    DATETIME(3)     NOT NULL COMMENT '构建启动（UTC）',
    finished_at   DATETIME(3)     NULL,
    error_summary TEXT            NULL COMMENT '失败原因（异常类型 + 消息，不含凭据）',
    created_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (build_id),
    KEY idx_agg_scope (scope_type, dates_csv),
    KEY idx_agg_status (status, started_at)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'DWS/ADS 聚合日志：每次构建一行（scope、聚合版本、各表行数、起止、状态），驱动幂等重建与审计';