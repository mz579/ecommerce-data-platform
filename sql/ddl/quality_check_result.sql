-- ============================================================================
-- 数据质量门禁：逐规则结果明细（Phase 5）
--
-- 每次 quality_check_run 的每条规则一行，记录该规则通过/失败、比对对象行数、
-- 违规行数、失败详情（前几条）与耗时。可跨历史查询任意运行的门禁结果。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS quality_check_result (
    result_id   BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    run_id      BIGINT UNSIGNED NOT NULL COMMENT '所属 quality_check_run.run_id（逻辑关联）',
    rule_key    VARCHAR(128)    NOT NULL COMMENT '规则键（quality/rules.py 中的稳定键）',
    category    VARCHAR(32)     NOT NULL COMMENT 'completeness/uniqueness/validity/range/reconciliation',
    status      VARCHAR(16)     NOT NULL COMMENT 'PASS / FAIL',
    checked     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '该规则比对的对象行数',
    failed_rows BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '违规行数',
    detail      TEXT            NULL COMMENT 'FAIL 时的失败详情（前几条），PASS 时为空',
    duration_ms INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '该规则执行耗时（毫秒）',
    PRIMARY KEY (result_id),
    KEY idx_qcr_run (run_id),
    KEY idx_qcr_rule (rule_key)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '数据质量门禁：逐规则结果明细（可查询历史门禁结果）';