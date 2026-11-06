-- ============================================================================
-- 数据质量门禁：检查运行头（Phase 5）
--
-- 一次 `quality run`（一个检查范围：FULL 或一组 UTC 日）登记一行，记录运行范围、
-- 通过/失败规则数与比对项数、起止时间。门禁语义（hard gate）：
--   任何规则 FAIL -> 本运行 status = 'FAIL' -> CLI 非零退出；下游 publish 拒绝执行。
-- 每次运行的逐规则明细落在 quality_check_result（run_id 逻辑关联，不建物理外键，
-- 与既有审计表保持一致风格）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS quality_check_run (
    run_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_type   VARCHAR(16)     NOT NULL COMMENT 'FULL = 全库；DATES = 按 UTC 日集合',
    dates_csv    VARCHAR(255)    NULL COMMENT 'scope_type=DATES 时的日期列表，FULL 为 NULL',
    window_start DATE            NULL COMMENT '本次门禁的合法日期窗口下界（校验用）',
    window_end   DATE            NULL COMMENT '本次门禁的合法日期窗口上界',
    status       VARCHAR(16)     NOT NULL COMMENT 'PASS / FAIL（有任一规则 FAIL 即 FAIL）',
    rules_total  INT UNSIGNED    NOT NULL COMMENT '执行的规则总数',
    rules_passed INT UNSIGNED    NOT NULL COMMENT '通过的规则数',
    rules_failed INT UNSIGNED    NOT NULL COMMENT '失败的规则数',
    checks_total BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '全部规则比对的对象行数之和',
    started_at   DATETIME(3)     NOT NULL COMMENT '运行启动（UTC）',
    finished_at  DATETIME(3)     NULL,
    PRIMARY KEY (run_id),
    KEY idx_qr_run_status (status, started_at),
    KEY idx_qr_run_scope (scope_type, dates_csv)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '数据质量门禁运行头：一次质量检查一行（范围、通过/失败规则数、结果状态、耗时）';