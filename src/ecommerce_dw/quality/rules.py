"""Quality-gate rule catalog (Phase 5).

Covers the five required categories with hard (``error``) rules only:

- **completeness**  : ODS batch-log row counts, batch coverage, DWD identity
  ``ODS = dwd_events + duplicates + rejects`` per source file.
- **uniqueness**    : ODS surrogate keys / source positions, DWD row & business
  keys, DWS and ADS primary keys.
- **validity**      : DWD standard event enum, business-date window, the precise
  ``visitor_id NULL <-> visitor_zero`` rule, DWD quality flags, and the raw ODS
  contract columns (event / timestamp / visitorid / itemid / transactionid).
- **range**         : positive ids, non-negative counters, UV <= DAU <= user rows,
  UV <= PV, funnel rate bounds, contiguous ranking ranks, retention rate range.
- **reconciliation**: per-day DWS totals vs DWD detail (user/item/category x
  view/cart/txn), ADS traffic/funnel vs DWS rows, funnel rates recomputed, funnel
  orders vs distinct transaction ids, retention invariants, ranking vs item table.

Every rule returns ``(total, bad)`` — the gate fails a rule iff ``bad > 0``.
"""

from __future__ import annotations

from datetime import date

from ecommerce_dw.quality.models import CheckRule, SqlCountRule

# Real dataset window (production): first/last UTC days with events.
_VALID_WINDOW: tuple[date, date] = (date(2015, 5, 3), date(2015, 9, 18))


def _pk_unique_rule(
    key: str, description: str, table: str, key_cols: list[str], date_col: str | None
) -> CheckRule:
    cols = ", ".join(f"`{c}`" for c in key_cols)
    sql = (
        "SELECT COUNT(*) AS total,\n"
        f"       (COUNT(*) - COUNT(DISTINCT {cols})) AS bad\n"
        f"FROM `{table}`\n"
        "WHERE 1 = 1{date_filter}"
    )
    return SqlCountRule(
        key=key,
        category="uniqueness",
        description=description,
        sql=sql,
        date_column=date_col,
    )


def build_rule_catalog(
    valid_window: tuple[date, date] = _VALID_WINDOW,
) -> tuple[CheckRule, ...]:
    """Return the immutable quality-gate rule catalog.

    ``valid_window`` pins the dataset's allowed business-date range checked by
    ``dwd_event_date_in_window``; integrations on synthetic test data override it
    (production callers keep the real 2015 window).
    """
    rules: list[CheckRule] = []

    # ------------------------------------------------------------------ #
    # completeness                                                        #
    # ------------------------------------------------------------------ #
    rules.append(
        SqlCountRule(
            key="ods_batch_log_rowcount",
            category="completeness",
            description=(
                "Each LOADED etl_batch_log row_count equals the actual ODS row count "
                "for that (table, file, batch)"
            ),
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(COALESCE(c.cnt, 0) <> b.row_count) AS bad
                FROM etl_batch_log b
                LEFT JOIN (
                    SELECT table_name, source_file, ingest_batch_id, cnt
                    FROM (
                        SELECT 'ods_events_raw' AS table_name, source_file, ingest_batch_id,
                               COUNT(*) AS cnt
                        FROM ods_events_raw GROUP BY source_file, ingest_batch_id
                        UNION ALL
                        SELECT 'ods_item_properties_raw', source_file, ingest_batch_id, COUNT(*)
                        FROM ods_item_properties_raw GROUP BY source_file, ingest_batch_id
                        UNION ALL
                        SELECT 'ods_category_tree_raw', source_file, ingest_batch_id, COUNT(*)
                        FROM ods_category_tree_raw GROUP BY source_file, ingest_batch_id
                    ) u
                ) c
                  ON c.table_name = b.table_name
                 AND c.source_file = b.source_file
                 AND c.ingest_batch_id = b.batch_id
                WHERE b.status = 'LOADED'
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_batch_coverage",
            category="completeness",
            description="Every ODS (table, file, batch) group is backed by an etl_batch_log row",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(b.batch_id IS NULL) AS bad
                FROM (
                    SELECT 'ods_events_raw' AS table_name, source_file, ingest_batch_id,
                           COUNT(*) AS cnt
                    FROM ods_events_raw GROUP BY source_file, ingest_batch_id
                    UNION ALL
                    SELECT 'ods_item_properties_raw', source_file, ingest_batch_id, COUNT(*)
                    FROM ods_item_properties_raw GROUP BY source_file, ingest_batch_id
                    UNION ALL
                    SELECT 'ods_category_tree_raw', source_file, ingest_batch_id, COUNT(*)
                    FROM ods_category_tree_raw GROUP BY source_file, ingest_batch_id
                ) g
                LEFT JOIN etl_batch_log b ON b.batch_id = g.ingest_batch_id
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_identity",
            category="completeness",
            description=(
                "Per source file: ODS rows == dwd_events + duplicates + rejects "
                "(nothing silently lost)"
            ),
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(COALESCE(d.cnt, 0) + COALESCE(p.cnt, 0) + COALESCE(r.cnt, 0)
                           <> o.cnt) AS bad
                FROM (SELECT source_file, COUNT(*) AS cnt
                      FROM ods_events_raw GROUP BY source_file) o
                LEFT JOIN (SELECT source_file, COUNT(*) AS cnt
                           FROM dwd_events GROUP BY source_file) d USING (source_file)
                LEFT JOIN (SELECT source_file, COUNT(*) AS cnt
                           FROM dwd_events_duplicate GROUP BY source_file) p USING (source_file)
                LEFT JOIN (SELECT source_file, COUNT(*) AS cnt
                           FROM dwd_events_reject GROUP BY source_file) r USING (source_file)
            """,
        )
    )

    # ------------------------------------------------------------------ #
    # uniqueness                                                          #
    # ------------------------------------------------------------------ #
    rules.append(
        SqlCountRule(
            key="ods_row_key_unique",
            category="uniqueness",
            description="ODS surrogate row_key is unique in every ODS table",
            sql="""
                SELECT SUM(u.total) AS total, SUM(u.bad) AS bad
                FROM (
                    SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) AS bad
                    FROM ods_events_raw
                    UNION ALL
                    SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) AS bad
                    FROM ods_item_properties_raw
                    UNION ALL
                    SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) AS bad
                    FROM ods_category_tree_raw
                ) u
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_source_path_unique",
            category="uniqueness",
            description="ODS (source_file, source_row_number) is unique in every ODS table",
            sql="""
                SELECT SUM(u.total) AS total, SUM(u.bad) AS bad
                FROM (
                    SELECT COUNT(*) AS total,
                           (COUNT(*) - COUNT(DISTINCT source_file, source_row_number)) AS bad
                    FROM ods_events_raw
                    UNION ALL
                    SELECT COUNT(*) AS total,
                           (COUNT(*) - COUNT(DISTINCT source_file, source_row_number)) AS bad
                    FROM ods_item_properties_raw
                    UNION ALL
                    SELECT COUNT(*) AS total,
                           (COUNT(*) - COUNT(DISTINCT source_file, source_row_number)) AS bad
                    FROM ods_category_tree_raw
                ) u
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_events_row_key_unique",
            category="uniqueness",
            description="dwd_events.row_key is unique",
            sql=(
                "SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) "
                "AS bad FROM dwd_events"
            ),
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_dedup_key_unique",
            category="uniqueness",
            description="dwd_events.dedup_key (business key) is unique - no exact duplicates",
            sql=(
                "SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT dedup_key)) "
                "AS bad FROM dwd_events"
            ),
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_aux_row_key_unique",
            category="uniqueness",
            description="dwd_events_duplicate / dwd_events_reject row_key is unique",
            sql="""
                SELECT SUM(u.total) AS total, SUM(u.bad) AS bad
                FROM (
                    SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) AS bad
                    FROM dwd_events_duplicate
                    UNION ALL
                    SELECT COUNT(*) AS total, (COUNT(*) - COUNT(DISTINCT row_key)) AS bad
                    FROM dwd_events_reject
                ) u
            """,
        )
    )
    rules.extend(
        [
            _pk_unique_rule(
                "dws_user_daily_pk_unique",
                "dws_user_daily primary key (event_date_utc, visitor_id) is unique",
                "dws_user_daily",
                ["event_date_utc", "visitor_id"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "dws_item_daily_pk_unique",
                "dws_item_daily primary key (event_date_utc, item_id) is unique",
                "dws_item_daily",
                ["event_date_utc", "item_id"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "dws_category_daily_pk_unique",
                "dws_category_daily primary key (event_date_utc, category_id) is unique",
                "dws_category_daily",
                ["event_date_utc", "category_id"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "ads_traffic_daily_pk_unique",
                "ads_traffic_daily primary key (event_date_utc) is unique",
                "ads_traffic_daily",
                ["event_date_utc"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "ads_funnel_daily_pk_unique",
                "ads_funnel_daily primary key (event_date_utc, metric_name) is unique",
                "ads_funnel_daily",
                ["event_date_utc", "metric_name"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "ads_item_ranking_daily_pk_unique",
                "ads_item_ranking_daily primary key (event_date_utc, rank_no) is unique",
                "ads_item_ranking_daily",
                ["event_date_utc", "rank_no"],
                "event_date_utc",
            ),
            _pk_unique_rule(
                "ads_retention_cohort_pk_unique",
                "ads_retention_cohort primary key (cohort_date, age_day) is unique",
                "ads_retention_cohort",
                ["cohort_date", "age_day"],
                "cohort_date",
            ),
        ]
    )

    # ------------------------------------------------------------------ #
    # validity                                                            #
    # ------------------------------------------------------------------ #
    rules.append(
        SqlCountRule(
            key="dwd_event_type_valid",
            category="validity",
            description="DWD event_type is in the standard enum {view, add_to_cart, transaction}",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(ev.event_type NOT IN ('view', 'add_to_cart', 'transaction')) AS bad
                FROM dwd_events ev
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_event_date_in_window",
            category="validity",
            description=f"DWD event_date_utc is inside the dataset business window "
            f"[{valid_window[0]}, {valid_window[1]}]",
            extra_params=valid_window,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(ev.event_date_utc IS NULL OR ev.event_date_utc < %s
                           OR ev.event_date_utc > %s) AS bad
                FROM dwd_events ev
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_visitor_null_flag_consistent",
            category="validity",
            description=(
                "Precise visitor rule: visitor_id IS NULL exactly when quality_flags carries "
                "visitor_zero (the '0' sentinel), otherwise non-null"
            ),
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(
                           (ev.visitor_id IS NULL
                            AND (ev.quality_flags IS NULL
                                 OR FIND_IN_SET('visitor_zero', ev.quality_flags) = 0))
                           OR
                           (ev.visitor_id IS NOT NULL
                            AND ev.quality_flags IS NOT NULL
                            AND FIND_IN_SET('visitor_zero', ev.quality_flags) > 0)
                       ) AS bad
                FROM dwd_events ev
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="dwd_quality_flags_valid",
            category="validity",
            description="DWD quality_flags only holds {ok, visitor_zero}",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(ev.quality_flags NOT IN ('ok', 'visitor_zero')) AS bad
                FROM dwd_events ev
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_event_type_valid",
            category="validity",
            description="Raw ODS event column only holds the contract values "
            "{view, addtocart, transaction}",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(o.event NOT IN ('view', 'addtocart', 'transaction')) AS bad
                FROM ods_events_raw o
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_timestamp_valid",
            category="validity",
            description="Raw ODS timestamp is a positive integer millisecond string",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(o.`timestamp` NOT REGEXP '^[1-9][0-9]*$') AS bad
                FROM ods_events_raw o
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_visitorid_valid",
            category="validity",
            description="Raw ODS visitorid is non-empty and integer-shaped ('0' sentinel allowed)",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(o.visitorid = '' OR o.visitorid NOT REGEXP '^-?[0-9]+$') AS bad
                FROM ods_events_raw o
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_itemid_valid",
            category="validity",
            description="Raw ODS itemid is non-empty and integer-shaped",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(o.itemid = '' OR o.itemid NOT REGEXP '^-?[0-9]+$') AS bad
                FROM ods_events_raw o
            """,
        )
    )
    rules.append(
        SqlCountRule(
            key="ods_transactionid_valid",
            category="validity",
            description="transaction events carry transactionid; non-transaction events do not",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM((o.event = 'transaction' AND o.transactionid = '')
                           OR (o.event <> 'transaction' AND o.transactionid <> '')) AS bad
                FROM ods_events_raw o
            """,
        )
    )

    # ------------------------------------------------------------------ #
    # range                                                               #
    # ------------------------------------------------------------------ #
    rules.append(
        SqlCountRule(
            key="dwd_item_id_positive",
            category="range",
            description="DWD item_id is non-null and positive",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(ev.item_id IS NULL OR ev.item_id <= 0) AS bad
                FROM dwd_events ev
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="dws_user_visitor_non_negative",
            category="range",
            description="dws_user_daily.visitor_id >= 0 (0 is the documented unknown bucket)",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.visitor_id < 0) AS bad
                FROM dws_user_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="dws_user_bucket_consistent",
            category="range",
            description="visitor_zero flag exactly identifies the visitor_id = 0 unknown bucket",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM((t.visitor_id = 0 AND t.visitor_zero = 0)
                           OR (t.visitor_id <> 0 AND t.visitor_zero = 1)) AS bad
                FROM dws_user_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="dws_counts_non_negative",
            category="range",
            description="DWS counters are non-negative and active_flag is binary",
            date_filter_repeats=3,
            sql="""
                SELECT SUM(u.total) AS total, SUM(u.bad) AS bad
                FROM (
                    SELECT COUNT(*) AS total,
                           SUM(t.view_cnt < 0 OR t.add_to_cart_cnt < 0
                               OR t.transaction_cnt < 0 OR t.order_cnt < 0
                               OR t.active_flag NOT IN (0, 1)) AS bad
                    FROM dws_user_daily t WHERE 1 = 1{date_filter}
                    UNION ALL
                    SELECT COUNT(*) AS total,
                           SUM(t.view_cnt < 0 OR t.add_to_cart_cnt < 0
                               OR t.transaction_cnt < 0 OR t.order_cnt < 0
                               OR t.view_uv < 0 OR t.add_to_cart_uv < 0
                               OR t.transaction_uv < 0) AS bad
                    FROM dws_item_daily t WHERE 1 = 1{date_filter}
                    UNION ALL
                    SELECT COUNT(*) AS total,
                           SUM(t.view_cnt < 0 OR t.add_to_cart_cnt < 0
                               OR t.transaction_cnt < 0 OR t.order_cnt < 0
                               OR t.view_uv < 0 OR t.add_to_cart_uv < 0
                               OR t.transaction_uv < 0) AS bad
                    FROM dws_category_daily t WHERE 1 = 1{date_filter}
                ) u
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_traffic_non_negative",
            category="range",
            description="ADS traffic pv/uv/dau are non-negative and is_full_day is binary",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.pv < 0 OR t.uv < 0 OR t.dau < 0
                           OR t.is_full_day NOT IN (0, 1)) AS bad
                FROM ads_traffic_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_funnel_metric_non_negative",
            category="range",
            description="ADS funnel metric values are non-negative()",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.metric_value < 0) AS bad
                FROM ads_funnel_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_funnel_rate_upper_bound",
            category="range",
            description=(
                "funnel normalized rates (add_to_cart / conv_strict) are <= 1; "
                "conv_global_rate may validly exceed 1 and is excluded"
            ),
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.metric_name IN ('add_to_cart_rate', 'conv_strict_rate')
                           AND t.metric_value > 1) AS bad
                FROM ads_funnel_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_retention_rate_in_range",
            category="range",
            description="retention_rate is NULL iff cohort_size = 0, otherwise in [0, 1]",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(NOT (
                           (t.cohort_size = 0 AND t.retention_rate IS NULL)
                           OR (t.cohort_size > 0 AND t.retention_rate >= 0
                               AND t.retention_rate <= 1)
                       )) AS bad
                FROM ads_retention_cohort t
                WHERE 1 = 1{date_filter}
            """,
            date_column="cohort_date",
        )
    )
    rules.append(
        SqlCountRule(
            key="traffic_uv_le_dau",
            category="range",
            description="per day UV (view visitors) <= DAU (any-event visitors)",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.uv > t.dau) AS bad
                FROM ads_traffic_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="traffic_uv_le_pv",
            category="range",
            description="per day UV <= PV (every view visitor contributes a view row)",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.uv > t.pv) AS bad
                FROM ads_traffic_daily t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="traffic_dau_le_user_rows",
            category="range",
            description=(
                "per day DAU <= the number of dws_user_daily rows (one row per active user)"
            ),
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.dau > COALESCE(r.rows_cnt, 0)) AS bad
                FROM ads_traffic_daily t
                LEFT JOIN (
                    SELECT event_date_utc AS day_utc, COUNT(*) AS rows_cnt
                    FROM dws_user_daily GROUP BY event_date_utc
                ) r ON r.day_utc = t.event_date_utc
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ranking_ranks_contiguous",
            category="range",
            description="per day ranking ranks are 1..N with no gaps or duplicates",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.dc <> t.mx OR t.dc <> t.c OR t.mn <> 1) AS bad
                FROM (
                    SELECT event_date_utc, COUNT(*) AS c, MIN(rank_no) AS mn,
                           MAX(rank_no) AS mx, COUNT(DISTINCT rank_no) AS dc
                    FROM ads_item_ranking_daily
                    GROUP BY event_date_utc
                ) t
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )

    # ------------------------------------------------------------------ #
    # reconciliation (per-day, per-metric cross-layer)                    #
    # ------------------------------------------------------------------ #
    for table, label in (
        ("dws_user_daily", "user"),
        ("dws_item_daily", "item"),
        ("dws_category_daily", "category"),
    ):
        for col, event, metric in (
            ("view_cnt", "view", "views"),
            ("add_to_cart_cnt", "add_to_cart", "carts"),
            ("transaction_cnt", "transaction", "txns"),
        ):
            rules.append(
                SqlCountRule(
                    key=f"dws_{label}_sum_{metric}_eq_dwd",
                    category="reconciliation",
                    description=f"daily SUM({table}.{col}) equals the DWD {event} row count "
                    "- DWS grain adds up exactly to DWD detail",
                    date_filter_repeats=2,
                    sql=f"""
                        SELECT COUNT(*) AS total,
                               SUM(g.cnt <> d.cnt) AS bad
                        FROM (
                            SELECT event_date_utc, SUM(`{col}`) AS cnt
                            FROM `{table}`
                            WHERE 1 = 1{{date_filter}}
                            GROUP BY event_date_utc
                        ) g
                        JOIN (
                            SELECT event_date_utc, COUNT(*) AS cnt
                            FROM dwd_events
                            WHERE event_type = '{event}'
                              AND 1 = 1{{date_filter}}
                            GROUP BY event_date_utc
                        ) d USING (event_date_utc)
                    """,
                    date_column="event_date_utc",
                )
            )
    rules.append(
        SqlCountRule(
            key="ads_traffic_pv_eq_user_sum",
            category="reconciliation",
            description="ads_traffic_daily.pv equals the daily SUM of dws_user_daily.view_cnt",
            date_filter_repeats=2,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.pv <> g.cnt) AS bad
                FROM ads_traffic_daily t
                JOIN (
                    SELECT event_date_utc AS day_utc, SUM(view_cnt) AS cnt
                    FROM dws_user_daily
                    WHERE 1 = 1{date_filter}
                    GROUP BY event_date_utc
                ) g ON g.day_utc = t.event_date_utc
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_traffic_uv_eq_user_views",
            category="reconciliation",
            description=(
                "ads_traffic_daily.uv equals the unique-view-user row count in dws_user_daily"
            ),
            date_filter_repeats=2,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.uv <> g.cnt) AS bad
                FROM ads_traffic_daily t
                JOIN (
                    SELECT event_date_utc AS day_utc, COUNT(*) AS cnt
                    FROM dws_user_daily
                    WHERE view_cnt > 0 AND visitor_zero = 0{date_filter}
                    GROUP BY event_date_utc
                ) g ON g.day_utc = t.event_date_utc
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_traffic_dau_eq_user_actives",
            category="reconciliation",
            description="ads_traffic_daily.dau equals the any-event active-user row count",
            date_filter_repeats=2,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.dau <> g.cnt) AS bad
                FROM ads_traffic_daily t
                JOIN (
                    SELECT event_date_utc AS day_utc, COUNT(*) AS cnt
                    FROM dws_user_daily
                    WHERE active_flag = 1 AND visitor_zero = 0{date_filter}
                    GROUP BY event_date_utc
                ) g ON g.day_utc = t.event_date_utc
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    for metric, condition in (
        ("view_users", "view_cnt > 0"),
        ("cart_users", "add_to_cart_cnt > 0"),
        ("buyer_users", "transaction_cnt > 0"),
        ("conv_strict_users", "view_cnt > 0 AND transaction_cnt > 0"),
    ):
        rules.append(
            SqlCountRule(
                key=f"ads_funnel_{metric.lower()}_eq_dws",
                category="reconciliation",
                description=f"ads_funnel_daily.{metric} equals the {condition} user row count",
                date_filter_repeats=2,
                sql=f"""
                    SELECT COUNT(*) AS total,
                           SUM(f.metric_value <> g.cnt) AS bad
                    FROM ads_funnel_daily f
                    JOIN (
                        SELECT event_date_utc AS day_utc, COUNT(*) AS cnt
                        FROM dws_user_daily
                        WHERE {condition} AND visitor_zero = 0{{date_filter}}
                        GROUP BY event_date_utc
                    ) g ON g.day_utc = f.event_date_utc
                    WHERE f.metric_name = '{metric}'
                      AND 1 = 1{{date_filter}}
                """,
                date_column="event_date_utc",
            )
        )
    rules.append(
        SqlCountRule(
            key="ads_funnel_orders_eq_dwd_distinct",
            category="reconciliation",
            description=(
                "ads_funnel_daily.orders equals DISTINCT transaction_id per day in dwd_events"
            ),
            date_filter_repeats=2,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(f.metric_value <> g.cnt) AS bad
                FROM ads_funnel_daily f
                JOIN (
                    SELECT event_date_utc AS day_utc, COUNT(DISTINCT transaction_id) AS cnt
                    FROM dwd_events
                    WHERE event_type = 'transaction'{date_filter}
                    GROUP BY event_date_utc
                ) g ON g.day_utc = f.event_date_utc
                WHERE f.metric_name = 'orders'
                  AND 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_funnel_conv_global_users_eq_dwd",
            category="reconciliation",
            description=(
                "ads_funnel_daily.conv_global_users equals the DISTINCT transaction "
                "visitor count per day in dwd_events (visitor_id IS NOT NULL) - an "
                "independent DWD-side check, not derived from the DWS build"
            ),
            date_filter_repeats=2,
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(f.metric_value <> g.cnt) AS bad
                FROM ads_funnel_daily f
                JOIN (
                    SELECT event_date_utc AS day_utc, COUNT(DISTINCT visitor_id) AS cnt
                    FROM dwd_events
                    WHERE event_type = 'transaction' AND visitor_id IS NOT NULL{date_filter}
                    GROUP BY event_date_utc
                ) g ON g.day_utc = f.event_date_utc
                WHERE f.metric_name = 'conv_global_users'
                  AND 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )
    for key, metric, numerator in (
        ("add_to_cart_rate", "add_to_cart_rate", "add_to_cart_cnt > 0"),
        ("conv_strict_rate", "conv_strict_rate", "view_cnt > 0 AND transaction_cnt > 0"),
        ("conv_global_rate", "conv_global_rate", "transaction_cnt > 0"),
    ):
        rules.append(
            SqlCountRule(
                key=f"ads_funnel_{key}_eq_dws",
                category="reconciliation",
                description=(
                    f"ads_funnel_daily.{metric} equals numerator/denominator recomputed from "
                    "dws_user_daily (tolerance 1e-6, denominator 0 -> NULL)"
                ),
                date_filter_repeats=2,
                sql=f"""
                    SELECT COUNT(*) AS total,
                           SUM(NOT (
                               (g.views = 0 AND f.metric_value IS NULL)
                               OR (g.views <> 0
                                   AND ABS(f.metric_value - (g.num / g.views)) < 1e-6)
                           )) AS bad
                    FROM ads_funnel_daily f
                    JOIN (
                        SELECT event_date_utc AS day_utc,
                               SUM(view_cnt > 0 AND visitor_zero = 0) AS views,
                               SUM({numerator} AND visitor_zero = 0) AS num
                        FROM dws_user_daily
                        WHERE 1 = 1{{date_filter}}
                        GROUP BY event_date_utc
                    ) g ON g.day_utc = f.event_date_utc
                    WHERE f.metric_name = '{metric}'
                      AND 1 = 1{{date_filter}}
                """,
                date_column="event_date_utc",
            )
        )
    rules.append(
        SqlCountRule(
            key="ads_retention_age0_consistent",
            category="reconciliation",
            description="retention age 0 rows satisfy retained == cohort_size and rate == 1",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.age_day = 0
                           AND (t.retained_users <> t.cohort_size
                                OR t.retention_rate <> 1)) AS bad
                FROM ads_retention_cohort t
                WHERE 1 = 1{date_filter}
            """,
            date_column="cohort_date",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_retention_age1_le_cohort",
            category="reconciliation",
            description="retention age 1 retained_users never exceeds its cohort_size",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.age_day = 1 AND t.retained_users > t.cohort_size) AS bad
                FROM ads_retention_cohort t
                WHERE 1 = 1{date_filter}
            """,
            date_column="cohort_date",
        )
    )
    rules.append(
        SqlCountRule(
            key="ads_item_ranking_matches_dws_item",
            category="reconciliation",
            description="ads_item_ranking_daily rows are a consistent subset of dws_item_daily",
            sql="""
                SELECT COUNT(*) AS total,
                       SUM(t.view_cnt <> r.view_cnt
                           OR t.add_to_cart_cnt <> r.add_to_cart_cnt
                           OR t.transaction_cnt <> r.transaction_cnt) AS bad
                FROM (
                    SELECT event_date_utc AS day_utc, item_id,
                           view_cnt, add_to_cart_cnt, transaction_cnt
                    FROM ads_item_ranking_daily
                ) r
                JOIN dws_item_daily t
                  ON t.event_date_utc = r.day_utc
                 AND t.item_id = r.item_id
                WHERE 1 = 1{date_filter}
            """,
            date_column="event_date_utc",
        )
    )

    keys = [r.key for r in rules]
    if len(keys) != len(set(keys)):
        dupes = {k for k in keys if keys.count(k) > 1}
        raise ValueError(f"duplicate rule keys in catalog: {sorted(dupes)}")
    return tuple(rules)
