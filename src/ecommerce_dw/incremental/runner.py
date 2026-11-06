"""Incremental runner: watermark check -> DWD dates -> DWS/ADS dates+predecessors
-> watermark advance (Phase 5).

The runner orchestrates the existing Phase 2-4 builders over a *business date*
scope derived purely from the watermark:

1. read the current ODS date -> max ingest batch map and the recorded watermark;
2. plan (pure logic in ``planner.py``) fresh tail + in-window late dates, or an
   explicit ``--from/--to`` backfill;
3. rebuild these dates in DWD (events detail) and DWS/ADS (widened by retention
   predecessors), both idempotent builders;
4. only on success advance the watermark -- a run that dies mid-way simply
   replans the same dates next time and reprocesses them identically.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pymysql

from ecommerce_dw.config import DbConfig
from ecommerce_dw.dwd import builder as dwd_builder
from ecommerce_dw.dws import builder as dws_builder
from ecommerce_dw.dws.constants import FULL_WINDOW_END, FULL_WINDOW_START
from ecommerce_dw.incremental.planner import (
    plan_backfill,
    plan_incremental,
)
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE, WatermarkRepo

logger = logging.getLogger(__name__)

# SQL that maps a utc millisecond timestamp to its UTC calendar day (tz-safe).
_DAY_SQL = "DATE('1970-01-01') + INTERVAL (CAST(`timestamp` AS UNSIGNED) DIV 86400000) DAY"


@dataclass(frozen=True)
class IncrementalResult:
    pipeline: str
    scope_type: str
    dates_csv: str | None
    dates_built: tuple[date, ...]
    late_dates: tuple[date, ...]
    out_of_window_dates: tuple[date, ...]
    dwd_build_ids: tuple[int, ...]
    dwd_rows: int
    agg_build_id: int | None
    agg_rows: int
    watermark_upserted: int
    duration_seconds: float
    dry_run: bool = False


class IncrementalError(RuntimeError):
    """Raised when the incremental chain cannot start (e.g. empty watermark)."""


def connect(cfg: DbConfig) -> pymysql.connections.Connection:
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
    )


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _ensure_watermark(conn: pymysql.connections.Connection) -> None:
    ddl = Path(__file__).resolve().parents[3] / "sql" / "ddl" / "etl_watermark.sql"
    if ddl.is_file():
        with conn.cursor() as cur:
            cur.execute(ddl.read_text(encoding="utf-8"))
            conn.commit()
    else:
        logger.warning("missing etl_watermark DDL at %s - relying on caller", ddl)


def current_batch_by_date(conn: pymysql.connections.Connection) -> dict[date, int]:
    """``{business_date -> max ODS ingest batch}`` from the raw events table."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_DAY_SQL} AS ev_day, MAX(ingest_batch_id) AS max_batch "
            "FROM ods_events_raw GROUP BY ev_day"
        )
        return {row[0]: int(row[1]) for row in cur.fetchall()}


def _odv_source_files(conn: pymysql.connections.Connection, days) -> list[str]:
    if not days:
        return []
    marks = ", ".join(["%s"] * len(days))
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT source_file FROM ods_events_raw WHERE {_DAY_SQL} IN ({marks})",
            tuple(days),
        )
        return [row[0] for row in cur.fetchall()]


def _dwd_days_present(conn: pymysql.connections.Connection) -> set[date]:
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT event_date_utc FROM dwd_events")
        return {row[0] for row in cur.fetchall()}


def _agg_scope(days, dwd_days: set[date]) -> tuple[date, ...]:
    """Affected dates plus their retention predecessors that hold DWD rows."""
    affected = set(days)
    for day in days:
        pred = day - timedelta(days=1)
        if pred in dwd_days:
            affected.add(pred)
    return tuple(sorted(affected))


def init_watermark(
    cfg: DbConfig,
    *,
    pipeline: str = DEFAULT_PIPELINE,
    to_date: date | None = None,
    window_start: date = FULL_WINDOW_START,
    window_end: date = FULL_WINDOW_END,
) -> int:
    """Bootstrap the watermark for business dates already built (e.g. a Phase 4
    FULL build). Dates after ``to_date`` are intentionally left unrecorded so the
    next incremental run picks them up as fresh tail dates.
    """
    conn = connect(cfg)
    try:
        _ensure_watermark(conn)
        current = current_batch_by_date(conn)
        upper = to_date or window_end
        entries = {d: b for d, b in current.items() if d <= upper and b > 0}
        written = WatermarkRepo(conn).upsert_many(pipeline, entries, processed_at=_now())
        latest = max(entries) if entries else None
        logger.info(
            "watermark init pipeline=%s dates=%d (oldest=%s newest=%s to=%s)",
            pipeline,
            len(entries),
            min(entries) if entries else "-",
            latest,
            upper,
        )
        return written
    finally:
        conn.close()


def run_incremental(
    cfg: DbConfig,
    *,
    from_date: date | None = None,
    to_date: date | None = None,
    pipeline: str = DEFAULT_PIPELINE,
    late_window_days: int = 7,
    dry_run: bool = False,
    force: bool = False,
    top_n: int = dws_builder.DEFAULT_TOP_N,
    window_start: date = FULL_WINDOW_START,
    window_end: date = FULL_WINDOW_END,
) -> IncrementalResult:
    """Plan and execute one incremental step against the current warehouse state.

    With ``from_date``/``to_date`` an explicit backfill is forced for that range;
    otherwise the plan is derived from the watermark (tail + in-window late).
    """
    if late_window_days < 1:
        raise ValueError("late_window_days must be >= 1")
    if (from_date is None) != (to_date is None):
        raise ValueError("--from and --to must be given together")
    if from_date and to_date and from_date > to_date:
        raise ValueError("--from must be <= --to")

    conn = connect(cfg)
    try:
        _ensure_watermark(conn)
        repo = WatermarkRepo(conn)
        watermark = repo.load(pipeline)
        current = current_batch_by_date(conn)
        if not current:
            raise IncrementalError("ods_events_raw has no business dates to process")

        if from_date is not None:
            plan = plan_backfill(
                from_date, to_date, current, watermark, late_window_days=late_window_days
            )
        else:
            plan = plan_incremental(current, watermark, late_window_days=late_window_days)

        days = plan.affected
        if not days:
            return IncrementalResult(
                pipeline=pipeline,
                scope_type="FULL" if not days else "DATES",
                dates_csv=None,
                dates_built=(),
                late_dates=plan.late_dates,
                out_of_window_dates=plan.out_of_window_dates,
                dwd_build_ids=(),
                dwd_rows=0,
                agg_build_id=None,
                agg_rows=0,
                watermark_upserted=0,
                duration_seconds=0.0,
            )

        directory_dates = sorted(days)
        dates_csv = ",".join(d.isoformat() for d in directory_dates)
        logger.info(
            "incremental plan pipeline=%s dates=%s (%s)",
            pipeline,
            dates_csv,
            plan.label,
        )
        scope_type = "DATES"

        if dry_run:
            return IncrementalResult(
                pipeline=pipeline,
                scope_type=scope_type,
                dates_csv=dates_csv,
                dates_built=tuple(directory_dates),
                late_dates=plan.late_dates,
                out_of_window_dates=plan.out_of_window_dates,
                dwd_build_ids=(),
                dwd_rows=0,
                agg_build_id=None,
                agg_rows=0,
                watermark_upserted=0,
                duration_seconds=0.0,
                dry_run=True,
            )

        # ---- DWD: rebuild affected dates for every ODS source file ---------- #
        dur_start = time.monotonic()
        dwd_build_ids: list[int] = []
        dwd_rows = 0
        for source_file in _odv_source_files(conn, directory_dates):
            result = dwd_builder.build_dwd(
                cfg,
                dates=directory_dates,
                source_file=source_file,
                force=force,
            )
            dwd_build_ids.append(result.build_id)
            dwd_rows += result.ok_rows
            logger.info(
                "dwd incremental: build #%d source=%s dates=%s ok_rows=%d",
                result.build_id,
                source_file,
                dates_csv,
                result.ok_rows,
            )

        # ---- DWS/ADS: affected dates plus retention predecessors ------------ #
        dwd_days = _dwd_days_present(conn)
        agg_dates = _agg_scope(directory_dates, dwd_days)
        agg = dws_builder.build_dws_ads(
            cfg,
            dates=agg_dates,
            top_n=top_n,
            force=force,
            window_start=window_start,
            window_end=window_end,
        )
        logger.info(
            "dws/ads incremental: build #%d dates=%s rows=%d",
            agg.build_id,
            agg.dates_csv,
            agg.rows_written,
        )

        # ---- watermark advance (only after every layer succeeded) ----------- #
        entries = {d: current[d] for d in directory_dates if d in current}
        written = repo.upsert_many(pipeline, entries, processed_at=_now())

        return IncrementalResult(
            pipeline=pipeline,
            scope_type=scope_type,
            dates_csv=dates_csv,
            dates_built=tuple(directory_dates),
            late_dates=plan.late_dates,
            out_of_window_dates=plan.out_of_window_dates,
            dwd_build_ids=tuple(dwd_build_ids),
            dwd_rows=dwd_rows,
            agg_build_id=agg.build_id,
            agg_rows=agg.rows_written,
            watermark_upserted=written,
            duration_seconds=time.monotonic() - dur_start,
        )
    finally:
        conn.close()
