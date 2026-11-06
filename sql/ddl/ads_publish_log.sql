-- ============================================================================
-- ADS 发布日志（Phase 5）
--
-- 用途: 下游「ADS 发布 / publish 标记」的唯一落点。只有门禁（quality gate）通过
--   （有任一规则 FAIL 时 publish 直接拒绝、绝不动本表）才会写入一行，记录发布范围
--   （FULL 或一组 UTC 日）、批准本发布的门禁运行号与发布时间。抽象层面：
--   质量失败必须阻止下游发布（不允许只打 warning 继续）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_publish_log (
    publish_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scope_type     VARCHAR(16)     NOT NULL COMMENT 'FULL = 全库；DATES = 按 UTC 日集合',
    dates_csv      VARCHAR(255)    NULL COMMENT 'scope_type=DATES 时的日期列表，FULL 为 NULL',
    quality_run_id BIGINT UNSIGNED NOT NULL COMMENT '批准本次发布的门禁运行号（必须为 PASS）',
    quality_status VARCHAR(16)     NOT NULL COMMENT '恒为 PASS（未通过门禁不会产生本表行）',
    published_at   DATETIME(3)     NOT NULL COMMENT '发布时刻（UTC）',
    PRIMARY KEY (publish_id),
    KEY idx_adv_scope (scope_type, dates_csv),
    KEY idx_adv_run (quality_run_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS 发布日志：门禁通过后每次发布一行（发布范围、批准的门禁运行号、发布时刻）';