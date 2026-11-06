-- ============================================================================
-- Airflow 流水线运行控制表（Phase 6）
--
-- 用途: 每次 `ecommerce_daily_pipeline` DAG 运行在此登记一个可审计的运行批次
--   （batch_id），并记录该次窗口的规划结果（dwd_dates / agg_dates）与收尾
--   状态（门禁 run id、发布 id、错误摘要）。任务日志可凭 batch_id 检索。
--   这是**控制/审计表**（与 etl_watermark / quality_check_run / ads_publish_log
--   同层），不含任何业务数据，不影响 ODS/DWD/DWS/ADS 生产不变量。
--
-- 语义约定见 docs/airflow_design.md。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS airflow_pipeline_run (
    run_id           BIGINT AUTO_INCREMENT
                     COMMENT '运行批次号（batch_id）',
    pipeline         VARCHAR(64)        NOT NULL
                     COMMENT '处理链标识（默认 incremental）',
    dag_run_id       VARCHAR(256)       NOT NULL
                     COMMENT 'Airflow DAG run id（dags list-runs 可检索）',
    mode             VARCHAR(8)         NOT NULL
                     COMMENT 'TAIL（水位线裁剪） | FORCE（conf 显式回填）',
    window_start     DATE               NOT NULL
                     COMMENT '本次请求窗口起点（业务日期，UTC）',
    window_end       DATE               NOT NULL
                     COMMENT '本次请求窗口终点（业务日期，UTC）',
    dwd_dates        VARCHAR(1024)      NULL
                     COMMENT '规划出的 DWD 重建日期（逗号分隔；空 = stable-state no-op）',
    agg_dates        VARCHAR(1024)      NULL
                     COMMENT 'DWS/ADS 重建 scope = dwd_dates ∪ 留存前驱日',
    status           VARCHAR(16)        NOT NULL
                     COMMENT 'RUNNING | SUCCESS | FAILED',
    published        VARCHAR(16)        NULL
                     COMMENT 'PUBLISHED | SKIPPED（stable-state 重跑不发布）',
    publish_id       BIGINT             NULL
                     COMMENT 'ads_publish_log.publish_id（若本轮发布）',
    quality_run_id   BIGINT             NULL
                     COMMENT 'quality_check_run.run_id（本轮门禁）',
    error_summary    VARCHAR(2048)      NULL
                     COMMENT '收尾时的错误摘要（失败回调写入）',
    started_at       DATETIME(3)        NOT NULL
                     COMMENT '登记时刻（UTC）',
    finished_at      DATETIME(3)        NULL
                     COMMENT '收尾时刻（UTC）',
    PRIMARY KEY (run_id),
    KEY idx_dag_run_id (dag_run_id),
    KEY idx_pipeline (pipeline)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Airflow 流水线运行控制表：每次 DAG 运行的规划与收尾审计';