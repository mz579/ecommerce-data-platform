-- ============================================================================
-- ADS 应用层：次日留存 cohort 表（Phase 4）
--
-- 层级: ADS（Application Data Store / 应用层）。
-- 用途: 以「首次观测日」为 cohort（= min(event_date_utc) per visitor），统计 cohort
--       在 age_day 天后再次发生任意事件的用户数与比例。
-- 粒度: 一行 = 一个 cohort 日 + 一个年龄日（age_day=0 为 cohort 自身规模）。
-- 口径: age_day=1 即次日留存（D1）。分子 = first_seen 于 cohort_date 的访客 ∩ 在
--   cohort_date+age_day 发生任意事件的访客；分母 = cohort 规模。visitor_id 去重，
--   排除未知访客桶。retention_rate = retained/cohort_size（分母 0 → NULL）。
-- 窗口不完整标记: window_status='full' 表示 cohort 日与回访日都在完整窗口内
--   （生产库 [2015-05-04, 2015-09-17]，cohort D0 ∈ [05-04, 09-16]）；否则为
--   'partial'——部分日或窗口末尾不足的 cohort 必须按标记解读，不放 0。
-- 来源: dwd_events（首次观测日 + 回访行为，全量窗口计算）。
-- 幂等性: 按 cohort_date 整日/全量重建即可；重复执行结果一致。
-- 可重复执行: 使用 IF NOT EXISTS；如需改动列结构请新建迁移脚本。
-- ============================================================================
CREATE TABLE IF NOT EXISTS ads_retention_cohort (
    cohort_date    DATE            NOT NULL COMMENT '首次观测日（UTC；= min(event_date_utc) per visitor）',
    age_day        TINYINT UNSIGNED NOT NULL COMMENT '回访年龄日（0 = cohort 规模行；1 = 次日留存）',
    cohort_size    BIGINT UNSIGNED NOT NULL COMMENT '分母：首次观测日为 cohort_date 的去重访客数',
    retained_users BIGINT UNSIGNED NOT NULL COMMENT '分子：cohort 中在 cohort_date+age_day 发生任意事件的去重访客数',
    retention_rate DECIMAL(12,6)   NULL COMMENT '留存率 = retained_users / cohort_size（小数，如 0.123456；分母 0 → NULL）',
    window_status  VARCHAR(16)     NOT NULL COMMENT "回访窗口完整性：'full'（cohort 与回访日均完整）/'partial'（部分日或窗口末不足，不得当完整率使用）",
    build_id       BIGINT UNSIGNED NOT NULL COMMENT 'dws_ads_build_log.build_id（本行由哪个聚合批次产生）',
    loaded_at      DATETIME(3)     NOT NULL COMMENT '写入时刻（UTC）',
    PRIMARY KEY (cohort_date, age_day)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'ADS：次日留存 cohort 表（cohort_date + age_day；首次观测 cohort 规模与 D+1 回流），一行 = 一 cohort 一年龄日';