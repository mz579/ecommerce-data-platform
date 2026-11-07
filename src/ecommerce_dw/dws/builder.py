"""DWS/ADS aggregation builder: dwd_events -> DWS -> ADS (Phase 4).

Implements the design in ``docs/dws_ads_design.md``:

- **SQL-first, memory-bounded**: every aggregation is an ``INSERT ... SELECT``
  executed server-side over ``dwd_events`` / the DWS tables. Python never reads
  the million-row inputs into RAM.
- **scope = temp day table**: both FULL and DATES scopes materialize the UTC
  days under build into ``tmp_agg_days`` and every statement joins it, so the
  two scopes share one code path and statements stay positionally parameterized.
- **idempotent**: a scope rebuild deletes the scope from the output tables
  first, then re-inserts deterministically; re-running a LOADED scope is allowed
  and reproduces identical content (volatile lineage columns aside).
- **actual/per-day rebuild**: ``--dates`` covers the same day set for DWS/ADS
  day tables and the same cohort dates for retention.
- **interval single-row table**: ``ads_funnel_interval`` (Phase 7 R16) is the
  full-window user-funnel materialized into one row; it is only rebuilt on a
  FULL scope and is left untouched by DATES builds.
- **audited**: every run registers a ``dws_ads_build_log`` row (per-table row
  counts as JSON) and verifies invariants against the ADS output before marking
  LOADED. ``verify_dws_ads`` rechecks every metric with independent SQL written
  differently from the build statements.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pymysql
import pymysql.cursors

from ecommerce_dw.config import DbConfig
from ecommerce_dw.dws import agg_sql
from ecommerce_dw.dws.build_log import AggBuildLogRepo
from ecommerce_dw.dws.constants import (
    AGG_VERSION,
    DEFAULT_TOP_N,
    FULL_WINDOW_END,
    FULL_WINDOW_START,
)

logger = logging.getLogger(__name__)

_ERROR_SUMMARY_MAX = 4_000

_OUTPUT_TABLES = (
    "dws_user_daily",
    "dws_item_daily",
    "dws_category_daily",
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
    "ads_item_behavior_daily",
    "ads_funnel_interval",
)
_REQUIRED_TABLES = _OUTPUT_TABLES + ("dws_ads_build_log",)
# Tables whose grain day column is event_date_utc (retention uses cohort_date;
# the interval funnel is a single window row, never day-keyed).
_DAY_KEYED_TABLES = tuple(
    t for t in _OUTPUT_TABLES if t not in ("ads_retention_cohort", "ads_funnel_interval")
)
# The one-row full-window funnel is FULL-only; DATES builds must not touch it.
_INTERVAL_TABLE = "ads_funnel_interval"


class AggBuildError(RuntimeError):
    """Raised when an aggregation build cannot start or an invariant is violated."""


@dataclass(frozen=True)
class Scope:
    """A DWS/ADS build target: the UTC day (or cohort day) set under build."""

    dates: tuple[date, ...]  # empty tuple == FULL

    def __post_init__(self) -> None:
        object.__setattr__(self, "dates", tuple(sorted(set(self.dates))))

    @property
    def scope_type(self) -> str:
        return "FULL" if not self.dates else "DATES"

    @property
    def dates_csv(self) -> str | None:
        return ",".join(d.isoformat() for d in self.dates) if self.dates else None


@dataclass(frozen=True)
class AggBuildResult:
    build_id: int
    scope_type: str
    dates_csv: str | None
    status: str
    table_stats: dict[str, int]
    rows_written: int
    duration_seconds: float


def connect(cfg: DbConfig) -> pymysql.connections.Connection:
    """Open a connection; long read timeout for the heavy aggregation statements."""
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=3600,
        autocommit=False,
        cursorclass=pymysql.cursors.Cursor,
    )


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _truncate_summary(text: str, limit: int = _ERROR_SUMMARY_MAX) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


# --------------------------------------------------------------------------- #
# the build                                                                   #
# --------------------------------------------------------------------------- #


def build_dws_ads(
    cfg: DbConfig,
    *,
    dates: list[date] | tuple[date, ...] | None = None,
    top_n: int = DEFAULT_TOP_N,
    force: bool = False,
    window_start: date = FULL_WINDOW_START,
    window_end: date = FULL_WINDOW_END,
) -> AggBuildResult:
    """Build (or rebuild) the DWS/ADS layers for the requested scope.

    - ``dates`` empty/None -> FULL rebuild of all output tables (including the
      single-row interval funnel); otherwise the given UTC days are rebuilt and
      the interval funnel is preserved untouched.
    - ``top_n`` limits rows in ``ads_item_ranking_daily`` per day.
    - ``force=True`` discards an existing failed/crashed build for the scope.
    - ``window_start`` / ``window_end`` define the "full window" used for the
      partial-day / retention-window markers (defaults are the pinned constants).
    """
    scope = Scope(tuple(sorted(set(dates or ()))))
    conn = connect(cfg)
    started_at = _now()
    dur_start = time.monotonic()
    build_id: int | None = None
    table_stats: dict[str, int] = {}
    try:
        _ensure_tables(conn)
        repo = AggBuildLogRepo(conn)
        existing = repo.find_for_scope(scope.scope_type, scope.dates_csv)
        running = [e for e in existing if e.status == "RUNNING"]
        if running:
            if running[0].agg_version != AGG_VERSION:
                raise AggBuildError(
                    f"build #{running[0].build_id} used agg version "
                    f"{running[0].agg_version!r} != current {AGG_VERSION!r}; "
                    "aggregation rules changed, force a clean rebuild with --force"
                )
            # A crashed run leaves RUNNING + possibly half-written rows; the
            # whole scope is re-deleted and rebuilt below, so just sweep it.
            logger.warning(
                "sweeping crashed aggregation build #%d and restarting the scope fresh",
                running[0].build_id,
            )
            repo.mark_cleaned(running[0].build_id, _now())
        else:
            failed = [e for e in existing if e.status == "FAILED"]
            if failed and not force:
                raise AggBuildError(
                    f"build #{failed[0].build_id} for this scope FAILED; clean it first "
                    f"(`cleanup --build {failed[0].build_id}`) or pass --force"
                )
            if force and existing:
                for entry in existing:
                    if entry.status in ("RUNNING", "FAILED"):
                        repo.mark_cleaned(entry.build_id, _now())
        build_id = repo.register(scope.scope_type, scope.dates_csv, AGG_VERSION, started_at)

        _delete_scope_rows(conn, scope)
        _fill_agg_days(conn, scope)
        table_stats = _build_all_tables(
            conn,
            build_id=build_id,
            loaded_at=started_at,
            top_n=top_n,
            window_start=window_start,
            window_end=window_end,
            scope=scope,
        )
        _verify_build(conn, scope, table_stats)
        duration = time.monotonic() - dur_start
        repo.mark_loaded(build_id, table_stats=table_stats, finished_at=_now())
        logger.info(
            "[aggregation #%d] %s loaded: %d rows across %d tables in %.1fs",
            build_id,
            scope.scope_type,
            sum(table_stats.values()),
            len(table_stats),
            duration,
        )
        return AggBuildResult(
            build_id=build_id,
            scope_type=scope.scope_type,
            dates_csv=scope.dates_csv,
            status="LOADED",
            table_stats=table_stats,
            rows_written=sum(table_stats.values()),
            duration_seconds=duration,
        )
    except Exception as exc:
        _safe_rollback(conn)
        if build_id is not None:
            try:
                AggBuildLogRepo(conn).mark_failed(
                    build_id, _truncate_summary(f"{type(exc).__name__}: {exc}"), _now()
                )
            except Exception:  # failing to record FAILED must not mask the real error
                logger.exception("could not record FAILED status on build #%s", build_id)
        raise AggBuildError(f"DWS/ADS build failed: {exc}") from exc
    finally:
        conn.close()


def _ensure_tables(conn: pymysql.connections.Connection) -> None:
    names = _list_table_names(conn)
    missing = [t for t in _REQUIRED_TABLES if t not in names]
    if missing:
        raise AggBuildError(
            f"DWS/ADS DDL not applied: missing {missing}; "
            "run `python scripts/apply_dws_ads_ddl.py` first"
        )


def _list_table_names(conn: pymysql.connections.Connection) -> set[str]:
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW TABLES")
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()


def _delete_scope_rows(conn: pymysql.connections.Connection, scope: Scope) -> None:
    """Wipe the scope before a fresh rebuild.

    FULL wipes every output table; DATES wipes only the day-keyed rows (and the
    same cohort dates for retention) and never touches ads_funnel_interval — the
    single-row window table is rebuilt by FULL scopes only.
    """
    cursor = conn.cursor()
    try:
        if scope.scope_type == "FULL":
            for table in _OUTPUT_TABLES:
                cursor.execute(f"DELETE FROM `{table}`")
        else:
            dates = list(scope.dates)
            marks = ", ".join(["%s"] * len(dates))
            for table in _DAY_KEYED_TABLES:
                cursor.execute(f"DELETE FROM `{table}` WHERE event_date_utc IN ({marks})", dates)
            cursor.execute(
                f"DELETE FROM `ads_retention_cohort` WHERE cohort_date IN ({marks})", dates
            )
        conn.commit()
    finally:
        cursor.close()


def _fill_agg_days(conn: pymysql.connections.Connection, scope: Scope) -> None:
    """Materialize the scope's UTC days into ``tmp_agg_days`` (FULL = all days)."""
    cursor = conn.cursor()
    try:
        cursor.execute(agg_sql.DROP_DAYS_SQL)
        cursor.execute(agg_sql.CREATE_DAYS_SQL)
        if scope.scope_type == "FULL":
            cursor.execute(agg_sql.fill_days_full_sql())
        else:
            cursor.execute(agg_sql.fill_days_dates_sql(len(scope.dates)), tuple(scope.dates))
        conn.commit()
    finally:
        cursor.close()


def _build_all_tables(
    conn: pymysql.connections.Connection,
    *,
    build_id: int,
    loaded_at: datetime,
    top_n: int,
    window_start: date,
    window_end: date,
    scope: Scope,
) -> dict[str, int]:
    """Run the nine output-table builds in dependency order; return per-table rows."""
    stats: dict[str, int] = {}

    stats["dws_user_daily"] = _exec_insert(
        conn, agg_sql.insert_user_daily_sql(), [build_id, loaded_at]
    )
    logger.info("dws_user_daily <- %s rows", stats["dws_user_daily"])

    stats["dws_item_daily"] = _exec_insert(
        conn, agg_sql.insert_item_daily_sql(), [build_id, loaded_at]
    )
    logger.info("dws_item_daily <- %s rows", stats["dws_item_daily"])

    stats["dws_category_daily"] = _build_category_daily(conn, build_id, loaded_at)
    logger.info("dws_category_daily <- %s rows", stats["dws_category_daily"])

    stats["ads_traffic_daily"] = _exec_insert(
        conn,
        agg_sql.insert_traffic_daily_sql(),
        agg_sql.base_params(build_id, loaded_at, window_start, window_end),
    )
    logger.info("ads_traffic_daily <- %s rows", stats["ads_traffic_daily"])

    stats["ads_funnel_daily"] = _build_funnel_daily(
        conn,
        build_id=build_id,
        loaded_at=loaded_at,
        window_start=window_start,
        window_end=window_end,
    )
    logger.info("ads_funnel_daily <- %s rows", stats["ads_funnel_daily"])

    stats["ads_item_ranking_daily"] = _exec_insert(
        conn,
        agg_sql.insert_item_ranking_daily_sql(),
        agg_sql.ranking_params(build_id, loaded_at, window_start, window_end, top_n),
    )
    logger.info(
        "ads_item_ranking_daily <- %s rows (top-%d/day)", stats["ads_item_ranking_daily"], top_n
    )

    stats["ads_retention_cohort"] = _exec_insert(
        conn,
        agg_sql.insert_retention_cohort_sql(),
        agg_sql.retention_params(build_id, loaded_at, window_start, window_end),
    )
    logger.info("ads_retention_cohort <- %s rows", stats["ads_retention_cohort"])

    stats["ads_item_behavior_daily"] = _exec_insert(
        conn,
        agg_sql.insert_item_behavior_daily_sql(),
        agg_sql.base_params(build_id, loaded_at, window_start, window_end),
    )
    logger.info("ads_item_behavior_daily <- %s rows", stats["ads_item_behavior_daily"])

    # The single-row interval funnel is FULL-window only: it aggregates the whole
    # window and would be wrong to rebuild from a partial day set, so DATES
    # scopes leave the previously built row (and its build_id lineage) intact.
    if scope.scope_type == "FULL":
        stats[_INTERVAL_TABLE] = _exec_insert(
            conn,
            agg_sql.insert_funnel_interval_sql(),
            agg_sql.funnel_interval_params(build_id, loaded_at, window_start, window_end),
        )
        logger.info("ads_funnel_interval <- %s rows", stats[_INTERVAL_TABLE])

    return stats


def _build_category_daily(
    conn: pymysql.connections.Connection, build_id: int, loaded_at: datetime
) -> int:
    """类目日汇总：先建 as-of 商品类目映射（三张临时表），再按类目聚合。

    临时表在连接结束时自动销毁；同连接内的唯一性由 DROP IF EXISTS 兜底。
    """
    cursor = conn.cursor()
    try:
        cursor.execute(agg_sql.drop_category_snap_sql())
        cursor.execute(agg_sql.create_category_snap_sql())
        cursor.execute(agg_sql.fill_category_snap_sql())
        conn.commit()
        logger.info("tmp_category_snap filled")

        cursor.execute(agg_sql.drop_item_days_sql())
        cursor.execute(agg_sql.create_item_days_sql())
        cursor.execute(agg_sql.fill_item_days_sql())
        conn.commit()
        logger.info("tmp_item_days filled (scope (day, item) set)")

        cursor.execute(agg_sql.drop_item_category_sql())
        cursor.execute(agg_sql.create_item_category_sql())
        cursor.execute(agg_sql.fill_item_category_sql())
        conn.commit()
        logger.info("tmp_item_category filled (as-of mapping)")
    finally:
        cursor.close()

    return _exec_insert(conn, agg_sql.insert_category_daily_sql(), [build_id, loaded_at])


def _build_funnel_daily(
    conn: pymysql.connections.Connection,
    *,
    build_id: int,
    loaded_at: datetime,
    window_start: date,
    window_end: date,
) -> int:
    """漏斗长表：先把逐日分子/分母落到 tmp_funnel_day（窗口标记定型），
    再以 9 个 UNION 分支写成 ads_funnel_daily。"""
    cursor = conn.cursor()
    try:
        cursor.execute(agg_sql.drop_funnel_day_sql())
        cursor.execute(agg_sql.create_funnel_day_sql())
        cursor.execute(
            agg_sql.fill_funnel_day_sql(),
            agg_sql.funnel_day_params(window_start, window_end),
        )
        conn.commit()
        logger.info("tmp_funnel_day filled")
    finally:
        cursor.close()

    return _exec_insert(
        conn,
        agg_sql.insert_funnel_daily_sql(),
        agg_sql.lineage_params(build_id, loaded_at),
    )


def _exec_insert(conn: pymysql.connections.Connection, sql: str, params: list) -> int:
    """Run one INSERT ... SELECT and return the number of inserted rows."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql, tuple(params))
        affected = cursor.rowcount
        conn.commit()
        return int(affected)
    finally:
        cursor.close()


def _verify_build(
    conn: pymysql.connections.Connection, scope: Scope, table_stats: dict[str, int]
) -> None:
    """Post-build invariants over the ADS output (fail loud, never silently)."""
    cursor = conn.cursor()
    try:
        # 1) traffic.uv == funnel.view_users (same口径, two tables).
        cursor.execute(
            """
            SELECT t.event_date_utc
            FROM ads_traffic_daily t
            JOIN tmp_agg_days d ON d.event_date_utc = t.event_date_utc
            JOIN ads_funnel_daily f
              ON f.event_date_utc = t.event_date_utc AND f.metric_name = 'view_users'
            WHERE t.uv <> f.metric_value
            """
        )
        mismatch = [row[0] for row in cursor.fetchall()]
        if mismatch:
            raise AggBuildError(
                f"invariant violated: traffic.uv != funnel.view_users on {mismatch[:5]}"
            )

        # 2) funnel.conv_global_users == buyer_users (same set built via two columns).
        cursor.execute(
            """
            SELECT f1.event_date_utc
            FROM ads_funnel_daily f1
            JOIN ads_funnel_daily f2
              ON f2.event_date_utc = f1.event_date_utc AND f2.metric_name = 'buyer_users'
            WHERE f1.metric_name = 'conv_global_users'
              AND f1.metric_value <> f2.metric_value
            """
        )
        mismatch = [row[0] for row in cursor.fetchall()]
        if mismatch:
            raise AggBuildError(
                f"invariant violated: conv_global_users != buyer_users on {mismatch[:5]}"
            )

        # 3) funnel.conv_strict_users <= conv_global_users (subset invariant).
        cursor.execute(
            """
            SELECT f1.event_date_utc
            FROM ads_funnel_daily f1
            JOIN ads_funnel_daily f2
              ON f2.event_date_utc = f1.event_date_utc AND f2.metric_name = 'conv_global_users'
            WHERE f1.metric_name = 'conv_strict_users'
              AND f1.metric_value > f2.metric_value
            """
        )
        mismatch = [row[0] for row in cursor.fetchall()]
        if mismatch:
            raise AggBuildError(
                f"invariant violated: conv_strict_users > conv_global_users on {mismatch[:5]}"
            )

        # 4) retention age 0: retained == cohort and rate == 1.
        cursor.execute(
            "SELECT cohort_date FROM ads_retention_cohort "
            "WHERE age_day = 0 AND (retained_users <> cohort_size OR retention_rate <> 1.0)"
        )
        mismatch = [row[0] for row in cursor.fetchall()]
        if mismatch:
            raise AggBuildError(
                f"invariant violated: retention age0 retained != size on {mismatch[:5]}"
            )

        # 5) expected per-table row parity: re-count built rows for the scope.
        #    ads_funnel_interval is a single window row with no day key; it only
        #    appears in table_stats on FULL scopes, so the plain table count matches.
        for table, written in table_stats.items():
            if table == "ads_retention_cohort":
                cursor.execute(
                    "SELECT COUNT(*) FROM ads_retention_cohort "
                    "JOIN tmp_agg_days d ON d.event_date_utc = ads_retention_cohort.cohort_date"
                )
            elif table == "ads_funnel_interval":
                cursor.execute("SELECT COUNT(*) FROM ads_funnel_interval")
            else:
                cursor.execute(
                    f"SELECT COUNT(*) FROM `{table}` "
                    "JOIN tmp_agg_days d ON d.event_date_utc = " + f"`{table}`.event_date_utc"
                )
            present = int(cursor.fetchone()[0])
            if present != written:
                raise AggBuildError(
                    f"row parity violated: {table} has {present} rows in scope, "
                    f"build reported {written}"
                )

        # 6) interval funnel sanity: exactly one window row, non-degenerate, and
        #    strict conversion is a subset of the global transaction user set.
        if scope.scope_type == "FULL":
            cursor.execute(
                "SELECT COUNT(*) FROM ads_funnel_interval "
                "WHERE view_users = 0 "
                "   OR conv_strict_users > conv_global_users "
                "   OR add_to_cart_rate < 0 OR conv_strict_rate < 0 OR conv_global_rate < 0"
            )
            bad = int(cursor.fetchone()[0])
            if bad:
                raise AggBuildError(
                    f"interval funnel invariant violated on {bad} row(s): "
                    "degenerate or inconsistent window-level values"
                )
    finally:
        cursor.close()


def cleanup_build(cfg: DbConfig, build_id: int) -> int:
    """Delete the rows an aggregation build produced and mark it CLEANED."""
    conn = connect(cfg)
    try:
        repo = AggBuildLogRepo(conn)
        entries = repo.find_by_id(build_id)
        if not entries:
            raise AggBuildError(f"build #{build_id} not found in dws_ads_build_log")
        deleted = 0
        cursor = conn.cursor()
        try:
            for table in _OUTPUT_TABLES:
                cursor.execute(f"DELETE FROM `{table}` WHERE build_id = %s", (build_id,))
                deleted += cursor.rowcount
            conn.commit()
        finally:
            cursor.close()
        repo.mark_cleaned(build_id, _now())
        logger.info("aggregation build #%d cleaned: removed %d rows", build_id, deleted)
        return deleted
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# independent verification (different SQL written against the source)          #
# --------------------------------------------------------------------------- #


def _as_float(value: object) -> float | None:
    """float() with NULL/None preserved (for ADS metric columns)."""
    return None if value is None else float(value)


@dataclass(frozen=True)
class MetricCheck:
    """One ADS metric value versus an independent recomputation."""

    metric: str
    day: date
    ads_value: object
    direct_value: object

    @property
    def ok(self) -> bool:
        if self.ads_value is None or self.direct_value is None:
            return self.ads_value is None and self.direct_value is None
        try:
            # ADS rates are stored at DECIMAL(...,6): a stored value can be off by
            # up to 5e-7 from the exact quotient, so the tolerance is column-scale
            # aware (integer metrics compare exact to 1e-9 < 1e-6 anyway).
            return abs(float(self.ads_value) - float(self.direct_value)) < 1e-6
        except (TypeError, ValueError):
            return False


@dataclass(frozen=True)
class AggVerifyReport:
    checks: tuple[MetricCheck, ...]

    @property
    def mismatches(self) -> tuple[MetricCheck, ...]:
        return tuple(c for c in self.checks if not c.ok)


_ROW_SELECTS = {
    "pv": (
        "SELECT event_date_utc, COUNT(*) AS v FROM dwd_events "
        "WHERE event_type = 'view' GROUP BY event_date_utc"
    ),
    "uv": (
        "SELECT event_date_utc, COUNT(DISTINCT visitor_id) AS v FROM dwd_events "
        "WHERE event_type = 'view' AND visitor_id IS NOT NULL GROUP BY event_date_utc"
    ),
    "dau": (
        "SELECT event_date_utc, COUNT(DISTINCT visitor_id) AS v FROM dwd_events "
        "WHERE visitor_id IS NOT NULL GROUP BY event_date_utc"
    ),
    "cart": (
        "SELECT event_date_utc, COUNT(DISTINCT visitor_id) AS v FROM dwd_events "
        "WHERE event_type = 'add_to_cart' AND visitor_id IS NOT NULL GROUP BY event_date_utc"
    ),
    "buyer": (
        "SELECT event_date_utc, COUNT(DISTINCT visitor_id) AS v FROM dwd_events "
        "WHERE event_type = 'transaction' AND visitor_id IS NOT NULL GROUP BY event_date_utc"
    ),
    "orders": (
        "SELECT event_date_utc, COUNT(DISTINCT transaction_id) AS v FROM dwd_events "
        "WHERE event_type = 'transaction' GROUP BY event_date_utc"
    ),
}


_CONV_STRICT_SQL = """
SELECT v.event_date_utc, COUNT(DISTINCT v.visitor_id) AS v
FROM (
    SELECT DISTINCT event_date_utc, visitor_id FROM dwd_events
    WHERE event_type = 'view' AND visitor_id IS NOT NULL
) v
JOIN (
    SELECT DISTINCT event_date_utc, visitor_id FROM dwd_events
    WHERE event_type = 'transaction' AND visitor_id IS NOT NULL
) b ON b.visitor_id = v.visitor_id AND b.event_date_utc = v.event_date_utc
GROUP BY v.event_date_utc
"""

_RETENTION_D1_SQL = """
SELECT f.cohort_date,
       COUNT(DISTINCT f.visitor_id) AS sz,
       COUNT(DISTINCT
             CASE WHEN r.event_date_utc = DATE_ADD(f.cohort_date, INTERVAL 1 DAY)
                  THEN f.visitor_id END) AS d1
FROM (
    SELECT visitor_id, MIN(event_date_utc) AS cohort_date
    FROM dwd_events WHERE visitor_id IS NOT NULL GROUP BY visitor_id
) f
LEFT JOIN (
    SELECT DISTINCT visitor_id, event_date_utc
    FROM dwd_events WHERE visitor_id IS NOT NULL
) r USING (visitor_id)
GROUP BY f.cohort_date
"""

# Phase 7 R16: independent recomputation of the single-row interval funnel.
# Written differently from the build INSERT (set-membership DISTINCT counts +
# a join-intersect for the strict converter) so the two paths cannot copy a bug.
_INTERVAL_DIRECT_SQL = """
SELECT
    (SELECT COUNT(DISTINCT visitor_id) FROM dwd_events
      WHERE event_type = 'view' AND visitor_id IS NOT NULL
        AND event_date_utc BETWEEN %s AND %s) AS view_users,
    (SELECT COUNT(DISTINCT visitor_id) FROM dwd_events
      WHERE event_type = 'add_to_cart' AND visitor_id IS NOT NULL
        AND event_date_utc BETWEEN %s AND %s) AS cart_users,
    (SELECT COUNT(DISTINCT visitor_id) FROM dwd_events
      WHERE event_type = 'transaction' AND visitor_id IS NOT NULL
        AND event_date_utc BETWEEN %s AND %s) AS buyer_users,
    (SELECT COUNT(DISTINCT transaction_id) FROM dwd_events
      WHERE event_type = 'transaction' AND event_date_utc BETWEEN %s AND %s) AS orders,
    (SELECT COUNT(*) FROM (
        SELECT DISTINCT visitor_id FROM dwd_events
        WHERE event_type = 'view' AND visitor_id IS NOT NULL
          AND event_date_utc BETWEEN %s AND %s
     ) iv
     JOIN (
        SELECT DISTINCT visitor_id FROM dwd_events
        WHERE event_type = 'transaction' AND visitor_id IS NOT NULL
          AND event_date_utc BETWEEN %s AND %s
     ) ib USING (visitor_id)) AS conv_strict_users,
    (SELECT COUNT(DISTINCT visitor_id) FROM dwd_events
      WHERE event_type = 'transaction' AND visitor_id IS NOT NULL
        AND event_date_utc BETWEEN %s AND %s) AS conv_global_users
"""

# Phase 7 R16: independent per-day distinct-item counts (ads_item_behavior_daily).
# Window-unfiltered like the day-keyed _ROW_SELECTS above, so a DATES-scoped
# verify still compares every built day (incl. window-edge partial days).
_ITEM_BEHAVIOR_DIRECT_SQL = """
SELECT event_date_utc,
       COUNT(DISTINCT CASE WHEN event_type = 'view' THEN item_id END) AS viewed,
       COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN item_id END) AS carted,
       COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN item_id END) AS bought
FROM dwd_events
WHERE item_id IS NOT NULL
GROUP BY event_date_utc
"""


def verify_dws_ads(
    cfg: DbConfig,
    *,
    dates: list[date] | None = None,
    window_start: date = FULL_WINDOW_START,
    window_end: date = FULL_WINDOW_END,
) -> AggVerifyReport:
    """Recompute every ADS metric directly from ``dwd_events`` with SQL written
    differently from the build statements, and diff against the ADS output.

    Read-only. Raises if the expected ADS tables are missing.
    """
    conn = connect(cfg)
    try:
        _ensure_tables(conn)
        cursor = conn.cursor()
        try:
            day_set: set[date] | None = set(dates) if dates else None

            direct: dict[str, dict[date, float]] = {}
            for name, sql in _ROW_SELECTS.items():
                direct[name] = {}
                cursor.execute(sql)
                for row in cursor.fetchall():
                    direct[name][row[0]] = float(row[1])
            direct["conv_strict"] = {}
            cursor.execute(_CONV_STRICT_SQL)
            for row in cursor.fetchall():
                direct["conv_strict"][row[0]] = float(row[1])

            # ADS values.
            ads_traffic: dict[date, dict[str, float]] = {}
            ads_funnel: dict[date, dict[str, float]] = {}
            cursor.execute("SELECT event_date_utc, pv, uv, dau FROM ads_traffic_daily")
            for row in cursor.fetchall():
                day = row[0]
                if day_set is not None and day not in day_set:
                    continue
                ads_traffic[day] = {"pv": float(row[1]), "uv": float(row[2]), "dau": float(row[3])}
            cursor.execute("SELECT event_date_utc, metric_name, metric_value FROM ads_funnel_daily")
            for row in cursor.fetchall():
                day, name, value = row[0], row[1], row[2]
                if day_set is not None and day not in day_set:
                    continue
                ads_funnel.setdefault(day, {})[name] = None if value is None else float(value)

            checks: list[MetricCheck] = []
            for day in sorted(ads_traffic):
                t = ads_traffic[day]
                dct = direct
                checks.append(MetricCheck("pv", day, t["pv"], dct["pv"].get(day, 0.0)))
                checks.append(MetricCheck("uv", day, t["uv"], dct["uv"].get(day, 0.0)))
                checks.append(MetricCheck("dau", day, t["dau"], dct["dau"].get(day, 0.0)))
                f = ads_funnel.get(day, {})
                for metric, key in (
                    ("cart_users", "cart"),
                    ("buyer_users", "buyer"),
                    ("orders", "orders"),
                    ("conv_strict_users", "conv_strict"),
                    ("conv_global_users", "buyer"),
                ):
                    checks.append(MetricCheck(metric, day, f.get(metric), dct[key].get(day, 0.0)))

                # Rates: recompute from the direct numerators/denominators.
                vusers = f.get("view_users")
                if vusers is not None:
                    denom = dct["uv"].get(day, 0.0)
                    for metric, num_key in (
                        ("add_to_cart_rate", "cart"),
                        ("conv_strict_rate", "conv_strict"),
                        ("conv_global_rate", "buyer"),
                    ):
                        num = dct[num_key].get(day, 0.0)
                        expected = None if denom == 0 else num / denom
                        checks.append(MetricCheck(metric, day, f.get(metric), expected))
                if "view_users" in f and f["view_users"] != ads_traffic[day]["uv"]:
                    checks.append(MetricCheck("view_users", day, f["view_users"], t["uv"]))

            # Retention D+1: compare ADS age_day=1 vs independent formulation.
            cursor.execute(_RETENTION_D1_SQL)
            ret_direct = {}
            for row in cursor.fetchall():
                sz, d1 = float(row[1]), float(row[2])
                ret_direct[row[0]] = None if sz == 0 else d1 / sz
            cursor.execute(
                "SELECT cohort_date, cohort_size, retained_users, retention_rate "
                "FROM ads_retention_cohort WHERE age_day = 1"
            )
            for row in cursor.fetchall():
                if day_set is not None and row[0] not in day_set:
                    continue
                ads_rate = None if row[3] is None else float(row[3])
                checks.append(MetricCheck("retention_d1", row[0], ads_rate, ret_direct.get(row[0])))

            # Interval funnel (single window row) vs independent recomputation.
            cursor.execute(_INTERVAL_DIRECT_SQL, [window_start, window_end] * 7)
            irow = cursor.fetchone()
            if irow is not None:
                direct_interval = {
                    "view_users": float(irow[0]),
                    "cart_users": float(irow[1]),
                    "buyer_users": float(irow[2]),
                    "orders": float(irow[3]),
                    "conv_strict_users": float(irow[4]),
                    "conv_global_users": float(irow[5]),
                }
                cursor.execute(
                    "SELECT view_users, cart_users, buyer_users, orders, "
                    "conv_strict_users, conv_global_users, add_to_cart_rate, "
                    "conv_strict_rate, conv_global_rate FROM ads_funnel_interval"
                )
                iads = cursor.fetchone()
                if iads is not None:
                    for name in (
                        "view_users",
                        "cart_users",
                        "buyer_users",
                        "orders",
                        "conv_strict_users",
                        "conv_global_users",
                    ):
                        col = (
                            "view_users",
                            "cart_users",
                            "buyer_users",
                            "orders",
                            "conv_strict_users",
                            "conv_global_users",
                        ).index(name)
                        checks.append(
                            MetricCheck(
                                f"interval.{name}",
                                window_end,
                                _as_float(iads[col]),
                                direct_interval[name],
                            )
                        )
                    denom = direct_interval["view_users"]
                    for name, num_key, col in (
                        ("add_to_cart_rate", "cart_users", 6),
                        ("conv_strict_rate", "conv_strict_users", 7),
                        ("conv_global_rate", "conv_global_users", 8),
                    ):
                        num = direct_interval[num_key]
                        expected = None if denom == 0 else num / denom
                        checks.append(
                            MetricCheck(
                                f"interval.{name}",
                                window_end,
                                _as_float(iads[col]),
                                expected,
                            )
                        )

            # Daily distinct item counts (ads_item_behavior_daily) vs direct.
            direct_items: dict[date, dict[str, float]] = {}
            cursor.execute(_ITEM_BEHAVIOR_DIRECT_SQL)
            for day, viewed, carted, bought in cursor.fetchall():
                direct_items[day] = {
                    "viewed": float(viewed),
                    "carted": float(carted),
                    "bought": float(bought),
                }
            cursor.execute(
                "SELECT event_date_utc, items_viewed, items_carted, items_bought "
                "FROM ads_item_behavior_daily"
            )
            for day, viewed, carted, bought in cursor.fetchall():
                if day_set is not None and day not in day_set:
                    continue
                d = direct_items.get(day)
                if d is None:
                    continue
                for name, ads_v, direct_v in (
                    ("viewed", viewed, d["viewed"]),
                    ("carted", carted, d["carted"]),
                    ("bought", bought, d["bought"]),
                ):
                    checks.append(
                        MetricCheck(
                            f"item_behavior.{name}",
                            day,
                            None if ads_v is None else float(ads_v),
                            direct_v,
                        )
                    )

            return AggVerifyReport(tuple(checks))
        finally:
            cursor.close()
    finally:
        conn.close()


def _safe_rollback(conn: pymysql.connections.Connection) -> None:
    try:
        conn.rollback()
    except Exception:
        pass
