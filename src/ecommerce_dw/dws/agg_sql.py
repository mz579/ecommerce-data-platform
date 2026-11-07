"""SQL statements for the DWS/ADS aggregation build (Phase 4).

Design notes:
- Every statement filters its scope through a per-build temp table
  ``tmp_agg_days`` (one row per UTC day in scope), joined by the day key, so
  FULL and DATES scopes share one code path and the parameter lists stay
  positional and small (scope-day condition is never inlined into WHERE).
- Aggregations run fully server-side (INSERT ... SELECT over dwd_events /
  dws_* tables); Python never pulls the millions of rows into memory.
- All placeholders are ``%s`` (pymysql). Statements are idempotent by design:
  the build deletes the scope from all seven output tables first, then re-inserts.
"""

from __future__ import annotations

from datetime import date, datetime

# --------------------------------------------------------------------------- #
# scope days (FULL: all event days; DATES: the requested days)                 #
# --------------------------------------------------------------------------- #


DROP_DAYS_SQL = "DROP TEMPORARY TABLE IF EXISTS tmp_agg_days"

CREATE_DAYS_SQL = """
CREATE TEMPORARY TABLE tmp_agg_days (
    event_date_utc DATE NOT NULL PRIMARY KEY
) ENGINE = InnoDB
"""


def fill_days_full_sql() -> str:
    return "INSERT INTO tmp_agg_days SELECT DISTINCT event_date_utc FROM dwd_events"


def fill_days_dates_sql(day_count: int) -> str:
    marks = ", ".join(["(%s)"] * day_count)
    return f"INSERT INTO tmp_agg_days (event_date_utc) VALUES {marks}"


# --------------------------------------------------------------------------- #
# DWS 层                                                                        #
# --------------------------------------------------------------------------- #


def insert_user_daily_sql() -> str:
    return """
    INSERT INTO dws_user_daily
        (event_date_utc, visitor_id, visitor_zero, view_cnt, add_to_cart_cnt,
         transaction_cnt, order_cnt, active_flag, build_id, loaded_at)
    SELECT ev.event_date_utc,
           COALESCE(ev.visitor_id, 0) AS visitor_id,
           CASE WHEN ev.visitor_id IS NULL THEN 1 ELSE 0 END AS visitor_zero,
           SUM(ev.event_type = 'view') AS view_cnt,
           SUM(ev.event_type = 'add_to_cart') AS add_to_cart_cnt,
           SUM(ev.event_type = 'transaction') AS transaction_cnt,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.transaction_id END) AS order_cnt,
           1 AS active_flag,
           %s AS build_id, %s AS loaded_at
    FROM dwd_events ev
    JOIN tmp_agg_days d ON d.event_date_utc = ev.event_date_utc
    GROUP BY ev.event_date_utc, COALESCE(ev.visitor_id, 0),
             CASE WHEN ev.visitor_id IS NULL THEN 1 ELSE 0 END
    """


def insert_item_daily_sql() -> str:
    return """
    INSERT INTO dws_item_daily
        (event_date_utc, item_id, view_cnt, add_to_cart_cnt, transaction_cnt,
         view_uv, add_to_cart_uv, transaction_uv, order_cnt, build_id, loaded_at)
    SELECT ev.event_date_utc, ev.item_id,
           SUM(ev.event_type = 'view') AS view_cnt,
           SUM(ev.event_type = 'add_to_cart') AS add_to_cart_cnt,
           SUM(ev.event_type = 'transaction') AS transaction_cnt,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'view'
                               THEN ev.visitor_id END) AS view_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'add_to_cart'
                               THEN ev.visitor_id END) AS add_to_cart_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.visitor_id END) AS transaction_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.transaction_id END) AS order_cnt,
           %s AS build_id, %s AS loaded_at
    FROM dwd_events ev
    JOIN tmp_agg_days d ON d.event_date_utc = ev.event_date_utc
    GROUP BY ev.event_date_utc, ev.item_id
    """


def drop_category_snap_sql() -> str:
    return "DROP TEMPORARY TABLE IF EXISTS tmp_category_snap"


def create_category_snap_sql() -> str:
    return """
    CREATE TEMPORARY TABLE tmp_category_snap (
        category_snap_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        item_id          BIGINT       NOT NULL,
        snap_ms          BIGINT UNSIGNED NOT NULL,
        category_id      BIGINT       NOT NULL,
        PRIMARY KEY (category_snap_id),
        KEY idx_snap_item_ms (item_id, snap_ms)
    ) ENGINE = InnoDB
    """


def fill_category_snap_sql() -> str:
    return """
    INSERT INTO tmp_category_snap (item_id, snap_ms, category_id)
    SELECT CAST(itemid AS UNSIGNED),
           CAST(`timestamp` AS UNSIGNED),
           CAST(`value` AS UNSIGNED)
    FROM ods_item_properties_raw
    WHERE property = 'categoryid'
      AND `value` REGEXP '^[0-9]+$'
    """


def drop_item_days_sql() -> str:
    return "DROP TEMPORARY TABLE IF EXISTS tmp_item_days"


def create_item_days_sql() -> str:
    return """
    CREATE TEMPORARY TABLE tmp_item_days (
        event_date_utc DATE   NOT NULL,
        item_id        BIGINT NOT NULL,
        PRIMARY KEY (event_date_utc, item_id)
    ) ENGINE = InnoDB
    """


def fill_item_days_sql() -> str:
    return """
    INSERT INTO tmp_item_days (event_date_utc, item_id)
    SELECT DISTINCT ev.event_date_utc, ev.item_id
    FROM dwd_events ev
    JOIN tmp_agg_days d ON d.event_date_utc = ev.event_date_utc
    """


def drop_item_category_sql() -> str:
    return "DROP TEMPORARY TABLE IF EXISTS tmp_item_category"


def create_item_category_sql() -> str:
    return """
    CREATE TEMPORARY TABLE tmp_item_category (
        event_date_utc DATE   NOT NULL,
        item_id        BIGINT NOT NULL,
        category_id    BIGINT NOT NULL,
        PRIMARY KEY (event_date_utc, item_id)
    ) ENGINE = InnoDB
    """


def fill_item_category_sql() -> str:
    """As-of 商品类目映射：每个 (事件日, 商品) 一个 categoryid。

    取当日或之前最近快照（snap_ms < 次日 0 点的毫秒数），平局用
    category_id 降序裁决保证确定性；未来属性绝不参与。逐日边界直接内联，
    每个 CTE/临时表只被引用一次（MySQL 不允许同一语句内重开派生表）。
    """
    return """
    INSERT INTO tmp_item_category (event_date_utc, item_id, category_id)
    WITH asof AS (
        SELECT id.event_date_utc, id.item_id, c.category_id,
               ROW_NUMBER() OVER (
                   PARTITION BY id.event_date_utc, id.item_id
                   ORDER BY c.snap_ms DESC, c.category_id DESC
               ) AS rn
        FROM tmp_item_days id
        JOIN tmp_category_snap c
             ON c.item_id = id.item_id
            AND c.snap_ms < UNIX_TIMESTAMP(id.event_date_utc + INTERVAL 1 DAY) * 1000
    )
    SELECT event_date_utc, item_id, category_id FROM asof WHERE rn = 1
    """


def insert_category_daily_sql() -> str:
    return """
    INSERT INTO dws_category_daily
        (event_date_utc, category_id, view_cnt, add_to_cart_cnt, transaction_cnt,
         view_uv, add_to_cart_uv, transaction_uv, order_cnt, build_id, loaded_at)
    SELECT ev.event_date_utc,
           COALESCE(ic.category_id, 0) AS category_id,
           SUM(ev.event_type = 'view') AS view_cnt,
           SUM(ev.event_type = 'add_to_cart') AS add_to_cart_cnt,
           SUM(ev.event_type = 'transaction') AS transaction_cnt,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'view'
                               THEN ev.visitor_id END) AS view_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'add_to_cart'
                               THEN ev.visitor_id END) AS add_to_cart_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.visitor_id END) AS transaction_uv,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.transaction_id END) AS order_cnt,
           %s AS build_id, %s AS loaded_at
    FROM dwd_events ev
    JOIN tmp_agg_days d ON d.event_date_utc = ev.event_date_utc
    LEFT JOIN tmp_item_category ic
           ON ic.event_date_utc = ev.event_date_utc AND ic.item_id = ev.item_id
    GROUP BY ev.event_date_utc, COALESCE(ic.category_id, 0)
    """


# --------------------------------------------------------------------------- #
# ADS 层                                                                        #
# --------------------------------------------------------------------------- #


def insert_traffic_daily_sql() -> str:
    return """
    INSERT INTO ads_traffic_daily
        (event_date_utc, pv, uv, dau, is_full_day, build_id, loaded_at)
    SELECT u.event_date_utc,
           SUM(u.view_cnt) AS pv,
           SUM(u.view_cnt > 0 AND u.visitor_zero = 0) AS uv,
           SUM(u.active_flag = 1 AND u.visitor_zero = 0) AS dau,
           CASE WHEN u.event_date_utc BETWEEN %s AND %s THEN 1 ELSE 0 END,
           %s AS build_id, %s AS loaded_at
    FROM dws_user_daily u
    JOIN tmp_agg_days d ON d.event_date_utc = u.event_date_utc
    GROUP BY u.event_date_utc
    """


def drop_funnel_day_sql() -> str:
    return "DROP TEMPORARY TABLE IF EXISTS tmp_funnel_day"


def create_funnel_day_sql() -> str:
    return """
    CREATE TEMPORARY TABLE tmp_funnel_day (
        event_date_utc    DATE NOT NULL PRIMARY KEY,
        view_users        BIGINT UNSIGNED NOT NULL,
        cart_users        BIGINT UNSIGNED NOT NULL,
        buyer_users       BIGINT UNSIGNED NOT NULL,
        conv_strict_users BIGINT UNSIGNED NOT NULL,
        conv_global_users BIGINT UNSIGNED NOT NULL,
        is_full_day_f     TINYINT NOT NULL
    ) ENGINE = InnoDB
    """


def fill_funnel_day_sql() -> str:
    """逐日漏斗分子/分母（不含 orders）。窗口标记在此一次性定型，供 UNION 各分支复用。"""
    return """
    INSERT INTO tmp_funnel_day
        (event_date_utc, view_users, cart_users, buyer_users,
         conv_strict_users, conv_global_users, is_full_day_f)
    SELECT u.event_date_utc,
           SUM(u.view_cnt > 0 AND u.visitor_zero = 0) AS view_users,
           SUM(u.add_to_cart_cnt > 0 AND u.visitor_zero = 0) AS cart_users,
           SUM(u.transaction_cnt > 0 AND u.visitor_zero = 0) AS buyer_users,
           SUM(u.view_cnt > 0 AND u.transaction_cnt > 0
               AND u.visitor_zero = 0) AS conv_strict_users,
           SUM(u.transaction_cnt > 0 AND u.visitor_zero = 0) AS conv_global_users,
           CASE WHEN u.event_date_utc BETWEEN %s AND %s THEN 1 ELSE 0 END
    FROM dws_user_daily u
    JOIN tmp_agg_days d ON d.event_date_utc = u.event_date_utc
    GROUP BY u.event_date_utc
    """


def insert_funnel_daily_sql() -> str:
    """漏斗长表：9 个口径行/日。

    按人口径全部取自 tmp_funnel_day（仅被引用一次）；orders 单独从 dwd_events
    全局去重（一个 transaction_id 可跨日/跨行，对用户行求和会高估订单数——
    生产实测 17674 > 17672）。指标行由 9 行的 metric 源表 CROSS JOIN 展开，
    避免同一语句内多次重开临时表 / CTE（MySQL 会报 Can't reopen table）。
    """
    return """
    INSERT INTO ads_funnel_daily
        (event_date_utc, metric_name, metric_value, is_full_day, build_id, loaded_at)
    WITH m AS (
        SELECT 0 AS idx, 'view_users' AS metric_name
        UNION ALL SELECT 1, 'cart_users'
        UNION ALL SELECT 2, 'buyer_users'
        UNION ALL SELECT 3, 'orders'
        UNION ALL SELECT 4, 'add_to_cart_rate'
        UNION ALL SELECT 5, 'conv_strict_users'
        UNION ALL SELECT 6, 'conv_global_users'
        UNION ALL SELECT 7, 'conv_strict_rate'
        UNION ALL SELECT 8, 'conv_global_rate'
    ),
    o AS (
        SELECT ev.event_date_utc, COUNT(DISTINCT ev.transaction_id) AS orders
        FROM dwd_events ev
        WHERE ev.event_type = 'transaction'
        GROUP BY ev.event_date_utc
    )
    SELECT f.event_date_utc, m.metric_name,
           CASE m.idx
               WHEN 0 THEN f.view_users
               WHEN 1 THEN f.cart_users
               WHEN 2 THEN f.buyer_users
               WHEN 3 THEN COALESCE(o.orders, 0)
               WHEN 4 THEN
                   CASE WHEN f.view_users = 0 THEN NULL
                        ELSE f.cart_users / f.view_users END
               WHEN 5 THEN f.conv_strict_users
               WHEN 6 THEN f.conv_global_users
               WHEN 7 THEN
                   CASE WHEN f.view_users = 0 THEN NULL
                        ELSE f.conv_strict_users / f.view_users END
               WHEN 8 THEN
                   CASE WHEN f.view_users = 0 THEN NULL
                        ELSE f.conv_global_users / f.view_users END
           END AS metric_value,
           f.is_full_day_f,
           %s AS build_id, %s AS loaded_at
    FROM tmp_funnel_day f
    CROSS JOIN m
    LEFT JOIN o ON o.event_date_utc = f.event_date_utc
    """


def insert_item_ranking_daily_sql() -> str:
    return """
    INSERT INTO ads_item_ranking_daily
        (event_date_utc, item_id, rank_no, view_cnt, add_to_cart_cnt,
         transaction_cnt, view_uv, add_to_cart_uv, transaction_uv, total_events,
         is_full_day, build_id, loaded_at)
    SELECT event_date_utc, item_id, rank_no, view_cnt, add_to_cart_cnt,
           transaction_cnt, view_uv, add_to_cart_uv, transaction_uv, total_events,
           CASE WHEN event_date_utc BETWEEN %s AND %s THEN 1 ELSE 0 END,
           %s AS build_id, %s AS loaded_at
    FROM (
        SELECT t.event_date_utc, t.item_id, t.view_cnt, t.add_to_cart_cnt,
               t.transaction_cnt, t.view_uv, t.add_to_cart_uv, t.transaction_uv,
               t.view_cnt + t.add_to_cart_cnt + t.transaction_cnt AS total_events,
               ROW_NUMBER() OVER (
                   PARTITION BY t.event_date_utc
                   ORDER BY t.view_cnt DESC, t.add_to_cart_cnt DESC,
                            t.transaction_cnt DESC, t.item_id DESC
               ) AS rank_no
        FROM dws_item_daily t
        JOIN tmp_agg_days d ON d.event_date_utc = t.event_date_utc
    ) ranked
    WHERE ranked.rank_no <= %s
    """


def insert_item_behavior_daily_sql() -> str:
    """商品行为去重商品数日报：每日 view / add_to_cart / transaction 各自的
    distinct item 数（与 v_ads_item_behavior_daily 原口径一致，仅统计
    item_id IS NOT NULL 的事件）。is_full_day 标致与 traffic/ranking 同规则。
    """
    return """
    INSERT INTO ads_item_behavior_daily
        (event_date_utc, items_viewed, items_carted, items_bought,
         is_full_day, build_id, loaded_at)
    SELECT ev.event_date_utc,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'view'
                               THEN ev.item_id END) AS items_viewed,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'add_to_cart'
                               THEN ev.item_id END) AS items_carted,
           COUNT(DISTINCT CASE WHEN ev.event_type = 'transaction'
                               THEN ev.item_id END) AS items_bought,
           CASE WHEN ev.event_date_utc BETWEEN %s AND %s THEN 1 ELSE 0 END,
           %s AS build_id, %s AS loaded_at
    FROM dwd_events ev
    JOIN tmp_agg_days d ON d.event_date_utc = ev.event_date_utc
    WHERE ev.item_id IS NOT NULL
    GROUP BY ev.event_date_utc
    """


def insert_funnel_interval_sql() -> str:
    """区间（完整窗口）用户级漏斗单行表（FULL 构建专用，id 恒为 1）。

    窗口由参数 window_start / window_end 给定，与构建的完整窗口标记一致
    （生产 = FULL_WINDOW_START/END 常量）。语义与旧 v_ads_funnel_interval 视图
    完全一致：每位访问者折叠成 4 个 0/1 用户标志再求窗口汇总；
    conv_strict = 交易 ∩ 浏览 = MAX(view) * MAX(transaction)；orders 在窗口内
    对 transaction_id 全局去重。仅 FULL 构建调用；DATES 构建不触碰本表。
    """
    return """
    INSERT INTO ads_funnel_interval
        (id, window_start, window_end, view_users, cart_users, buyer_users, orders,
         conv_strict_users, conv_global_users, add_to_cart_rate, conv_strict_rate,
         conv_global_rate, build_id, loaded_at)
    SELECT 1,
           %s, %s,
           u.view_users, u.cart_users, u.buyer_users, o.orders,
           u.conv_strict_users, u.conv_global_users,
           CASE WHEN u.view_users = 0 THEN NULL
                ELSE u.cart_users / u.view_users END,
           CASE WHEN u.view_users = 0 THEN NULL
                ELSE u.conv_strict_users / u.view_users END,
           CASE WHEN u.view_users = 0 THEN NULL
                ELSE u.conv_global_users / u.view_users END,
           %s AS build_id, %s AS loaded_at
    FROM (
        SELECT SUM(v.viewed) AS view_users,
               SUM(v.carted) AS cart_users,
               SUM(v.bought) AS buyer_users,
               SUM(v.viewed * v.bought) AS conv_strict_users,
               SUM(v.bought) AS conv_global_users
        FROM (
            SELECT visitor_id,
                   MAX(event_type = 'view') AS viewed,
                   MAX(event_type = 'add_to_cart') AS carted,
                   MAX(event_type = 'transaction') AS bought
            FROM dwd_events
            WHERE visitor_id IS NOT NULL
              AND event_type IN ('view', 'add_to_cart', 'transaction')
              AND event_date_utc BETWEEN %s AND %s
            GROUP BY visitor_id
        ) v
    ) u
    CROSS JOIN (
        SELECT COUNT(DISTINCT transaction_id) AS orders
        FROM dwd_events
        WHERE event_type = 'transaction'
          AND event_date_utc BETWEEN %s AND %s
    ) o
    """


def insert_retention_cohort_sql() -> str:
    """次日（及当日规模）留存 cohort：age_day ∈ {0, 1}。

    cohort = 首次观测日；回访 = 任意事件发生在 cohort_date + age_day。
    窗口完整性标记 window_status（参数为完整窗口起止），不全的 cohort 标记
    'partial' 而非填 0。每个 CTE 在语句内只引用一次。
    """
    return """
    INSERT INTO ads_retention_cohort
        (cohort_date, age_day, cohort_size, retained_users, retention_rate,
         window_status, build_id, loaded_at)
    WITH fs AS (
        SELECT ev.visitor_id, MIN(ev.event_date_utc) AS cohort_date
        FROM dwd_events ev
        WHERE ev.visitor_id IS NOT NULL
        GROUP BY ev.visitor_id
    ),
    ad AS (
        SELECT DISTINCT ev.visitor_id, ev.event_date_utc
        FROM dwd_events ev
        WHERE ev.visitor_id IS NOT NULL
    ),
    age AS (
        SELECT 0 AS age_day UNION ALL SELECT 1
    ),
    joined AS (
        SELECT f.cohort_date, g.age_day, f.visitor_id,
               a.visitor_id AS returned_visitor
        FROM fs f
        JOIN tmp_agg_days d ON d.event_date_utc = f.cohort_date
        CROSS JOIN age g
        LEFT JOIN ad a
               ON a.visitor_id = f.visitor_id
              AND a.event_date_utc =
                  DATE_ADD(f.cohort_date, INTERVAL g.age_day DAY)
    )
    SELECT cohort_date, age_day,
           COUNT(DISTINCT visitor_id) AS cohort_size,
           COUNT(DISTINCT CASE WHEN returned_visitor IS NOT NULL
                               THEN visitor_id END) AS retained_users,
           CASE WHEN COUNT(DISTINCT visitor_id) = 0
                THEN NULL
                ELSE COUNT(DISTINCT CASE WHEN returned_visitor IS NOT NULL THEN visitor_id END)
                     / COUNT(DISTINCT visitor_id)
           END AS retention_rate,
           CASE WHEN cohort_date BETWEEN %s AND %s
                     AND DATE_ADD(cohort_date, INTERVAL age_day DAY) BETWEEN %s AND %s
                THEN 'full' ELSE 'partial'
           END AS window_status,
           %s AS build_id, %s AS loaded_at
    FROM joined
    GROUP BY cohort_date, age_day
    """


# --------------------------------------------------------------------------- #
# params helpers                                                               #
# --------------------------------------------------------------------------- #


def lineage_params(build_id: int, loaded_at: datetime) -> list:
    """Params for statements that carry only build_id + loaded_at (DWS tables)."""
    return [build_id, loaded_at]


def base_params(
    build_id: int,
    loaded_at: datetime,
    window_start: date,
    window_end: date,
) -> list:
    """Params for statements that embed the full-window marker + lineage."""
    return [window_start, window_end, build_id, loaded_at]


def retention_params(
    build_id: int,
    loaded_at: datetime,
    window_start: date,
    window_end: date,
) -> list:
    return [window_start, window_end, window_start, window_end, build_id, loaded_at]


# param layout for the funnel INSERT: only the lineage pair (build_id, loaded_at)
# appears in the statement; the two window placeholders live in the
# tmp_funnel_day fill (fill_funnel_day_sql).
def funnel_day_params(
    window_start: date,
    window_end: date,
) -> list:
    return [window_start, window_end]


def ranking_params(
    build_id: int,
    loaded_at: datetime,
    window_start: date,
    window_end: date,
    top_n: int,
) -> list:
    params = [window_start, window_end, build_id, loaded_at, top_n]
    return params


def funnel_interval_params(
    build_id: int,
    loaded_at: datetime,
    window_start: date,
    window_end: date,
) -> list:
    """Params for the interval-funnel INSERT: the two window columns + lineage,
    then the window pair twice more for the user-level and orders subqueries."""
    return [window_start, window_end, build_id, loaded_at,
            window_start, window_end, window_start, window_end]
