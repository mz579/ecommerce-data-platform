-- ============================================================================
-- Phase 7（Superset）受控只读视图：ADS 发布新鲜度
--
-- 用途: 给「数据发布 / 新鲜度」KPI 提供单一来源。仅读链上审计表
--       ads_publish_log：每次门禁通过的真实发布写一行（publish_id 自增、
--       published_at 为发布时刻 UTC）。发布只写审计日志、不动任何业务表，
--       因此本视图是「刷新后数据更新」证据的载体：用既有 CLI
--       （publish --run <PASS run>）追加一行后，看板刷新即见变化。
-- 输出: 发布累计数、最新发布 id/时刻/范围/批准门禁号。
-- 安全: DEFINER 视图，访问方只见最终统计，不暴露控制表结构。
-- 可重复执行: CREATE OR REPLACE VIEW。
-- ============================================================================
CREATE OR REPLACE VIEW v_ads_publish_status AS
SELECT (SELECT COUNT(*) FROM ads_publish_log)                                      AS publish_count,
       (SELECT publish_id   FROM ads_publish_log ORDER BY publish_id DESC LIMIT 1)  AS latest_publish_id,
       (SELECT published_at FROM ads_publish_log ORDER BY publish_id DESC LIMIT 1)  AS latest_published_at,
       (SELECT scope_type   FROM ads_publish_log ORDER BY publish_id DESC LIMIT 1)  AS latest_scope_type,
       (SELECT dates_csv    FROM ads_publish_log ORDER BY publish_id DESC LIMIT 1)  AS latest_dates_csv,
       (SELECT quality_run_id FROM ads_publish_log ORDER BY publish_id DESC LIMIT 1) AS latest_quality_run_id