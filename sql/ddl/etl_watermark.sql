-- ============================================================================
-- 增量处理水位线（Phase 5）
--
-- 用途: 每条处理流水线（dwd / dws_ads）按【业务日期】记录已处理到的水位：
--   - 每行 = <流水线, 日期> 该日期已被该流水线完整处理
--   - 该流水线的前沿 frontier = MAX(event_date_utc)（按业务日期推进，不依赖机器时间）
--   - max_ingest_batch 记录「该日期已消费的最大 ODS 装载批次」，用于迟到数据判定：
--     若某日期（在迟到窗口内）在 ODS 中出现了比已消费批次更新的行，则该日期视为
--     迟到受影响日期，需要重算。
-- 语义约定见 docs/incremental_and_quality.md（水位线模型 / 迟到策略 / 幂等重跑）。
-- 可重复执行: CREATE TABLE IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS etl_watermark (
    pipeline         VARCHAR(32)        NOT NULL
                     COMMENT '流水线标识：dwd / dws_ads',
    event_date_utc   DATE               NOT NULL
                     COMMENT '已处理到的业务日期（UTC）；该流水线前沿 = MAX(event_date_utc)',
    max_ingest_batch BIGINT UNSIGNED    NOT NULL DEFAULT 0
                     COMMENT '该日期已消费的最大 ODS 载入批次（迟到判定依据）',
    processed_at     DATETIME(3)        NOT NULL
                     COMMENT '本轮写入时刻（UTC）',
    PRIMARY KEY (pipeline, event_date_utc)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = '增量水位线：按业务日期记录每条流水线已处理到的位置与已消费批次';