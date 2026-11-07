"""Chunked, idempotent, resumable DWD build: ODS events -> DWD detail (Phase 3).

Implements the design in ``docs/dwd_design.md``:

- **memory-bounded**: ODS is streamed via ``fetchmany`` chunks (never the whole
  2.7M rows in RAM); the transform is a pure function per row.
- **two passes, single source of truth for dedup**: pass 1 writes every
  transformed row (OK *and* REJECT) into the ``dwd_events_stage`` temporary
  table; pass 2 runs one SQL window step per output table
  (``dwd_events`` / ``dwd_events_duplicate`` / ``dwd_events_reject``) that
  classifies the business-key winner and isolates anomalies. Dedup is therefore
  exact — no approximate in-memory key set.
- **idempotent / rebuildable**: a ``FULL`` rebuild clears the events DWD tables
  and reproduces them from ODS; ``--dates`` rebuilds a subset of UTC days while
  every other day is preserved — that still respects cross-day dedup because the
  split ranks rebuilt rows against the preserved winners already in ``dwd_events``.
- **resumable**: a crashed run leaves ``dwd_build_log.RUNNING`` plus committed
  stage rows; the next run counts them and continues where the last committed
  chunk ended. If the transform version changed, resume is refused (use
  ``--force`` for a clean rebuild).
- **audited**: every run registers a ``dwd_build_log`` row and verifies the
  identity ``ODS = dwd_events + dwd_events_duplicate + dwd_events_reject``
  before marking ``LOADED``.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pymysql
import pymysql.cursors

from ecommerce_dw.config import DbConfig
from ecommerce_dw.dwd.build_log import BuildLogRepo
from ecommerce_dw.dwd.transform import (
    TRANSFORM_VERSION,
    TransformOutcome,
    date_ms_range,
    transform_event,
)

SOURCE_FILE = "events.csv"

DEFAULT_CHUNK_SIZE = 20_000
DEFAULT_MAX_RESUMES = 20
DEFAULT_RESUME_WAIT_SECONDS = 300
_PROGRESS_LOG_EVERY = 5
_MAX_STMT_LENGTH = 8 * 1024 * 1024
_ERROR_SUMMARY_MAX = 4_000

logger = logging.getLogger(__name__)


class BuildError(RuntimeError):
    """Raised when a DWD build cannot start or its invariants are violated."""


# --------------------------------------------------------------------------- #
# scope (what part of the events DWD a build covers)                          #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Scope:
    """A DWD build target: the events ODS source plus a subset of UTC days."""

    dates: tuple[date, ...]  # empty tuple == FULL (all dates)
    source: str = SOURCE_FILE

    def __post_init__(self) -> None:
        # Normalize so identical scopes compare equal regardless of input order.
        object.__setattr__(self, "dates", tuple(sorted(set(self.dates))))

    @property
    def scope_type(self) -> str:
        return "FULL" if not self.dates else "DATES"

    @property
    def dates_csv(self) -> str | None:
        return ",".join(d.isoformat() for d in self.dates) if self.dates else None


# --------------------------------------------------------------------------- #
# outcome of a build run                                                      #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class BuildResult:
    build_id: int
    scope_type: str
    dates_csv: str | None
    status: str
    ods_count: int
    processed_rows: int
    ok_rows: int
    duplicate_rows: int
    reject_rows: int
    duration_seconds: float
    resumed_from: int


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _truncate_summary(text: str, limit: int = _ERROR_SUMMARY_MAX) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def connect(cfg: DbConfig) -> pymysql.connections.Connection:
    """Open a connection; long read timeout for the one-shot split statements."""
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


# --------------------------------------------------------------------------- #
# SQL fragments shared by the passes                                          #
# --------------------------------------------------------------------------- #

_STAGE_COLUMNS = (
    "row_key",
    "raw_timestamp",
    "raw_visitorid",
    "raw_event",
    "raw_itemid",
    "raw_transactionid",
    "visitor_id",
    "item_id",
    "raw_event_type",
    "event_type",
    "event_time_utc",
    "event_date_utc",
    "transaction_id",
    "dedup_key",
    "quality_flags",
    "record_status",
    "reject_reason",
    "reject_message",
    "ingest_batch_id",
    "source_file",
    "source_row_number",
    "loaded_at",
    "build_id",
)

_DWD_EVENTS_COLUMNS = (
    "row_key",
    "dedup_key",
    "visitor_id",
    "item_id",
    "raw_event_type",
    "event_type",
    "event_time_utc",
    "event_date_utc",
    "transaction_id",
    "quality_flags",
    "build_id",
    "ingest_batch_id",
    "source_file",
    "source_row_number",
    "loaded_at",
)

_DWD_DUPLICATE_COLUMNS = _DWD_EVENTS_COLUMNS + (
    "raw_timestamp",
    "raw_visitorid",
    "raw_event",
    "raw_itemid",
    "raw_transactionid",
)

_DWD_REJECT_COLUMNS = (
    "row_key",
    "raw_timestamp",
    "raw_visitorid",
    "raw_event",
    "raw_itemid",
    "raw_transactionid",
    "event_type",
    "event_time_utc",
    "event_date_utc",
    "reject_reason",
    "reject_message",
    "build_id",
    "ingest_batch_id",
    "source_file",
    "source_row_number",
    "loaded_at",
)


def _stage_insert_sql() -> str:
    identifiers = ", ".join(f"`{c}`" for c in _STAGE_COLUMNS)
    placeholders = ", ".join(["%s"] * len(_STAGE_COLUMNS))
    return f"INSERT IGNORE INTO `dwd_events_stage` ({identifiers}) VALUES ({placeholders})"


def _ods_where(scope: Scope) -> tuple[str, list]:
    """WHERE over ``ods_events_raw`` selecting the scope's rows.

    For a ``DATES`` scope the UTC-day range is expressed on the numeric ms
    timestamp. Invalid timestamps (empty / non-numeric / negative / ``"0"``) all
    CAST to ``0``; they have no UTC day, so every date scope must ALSO pick them
    up — the transform rejects them, they are never silently dropped, and the
    scope identity ``ODS = ok + dup + reject`` stays exact.
    """
    if scope.scope_type == "FULL":
        return "source_file = %s", [scope.source]
    conds: list[str] = []
    params: list = [scope.source]
    for d in scope.dates:
        start, end = date_ms_range(d)
        conds.append("(CAST(`timestamp` AS UNSIGNED) >= %s AND CAST(`timestamp` AS UNSIGNED) < %s)")
        params.extend([start, end])
    return (
        "source_file = %s AND (" + " OR ".join(conds) + " OR CAST(`timestamp` AS UNSIGNED) = 0)",
        params,
    )


def _dwd_where(scope: Scope) -> tuple[str, list]:
    """WHERE over the DWD tables (all of them carry ``event_date_utc``).

    Reject rows for invalid timestamps have ``event_date_utc`` NULL (no UTC day
    exists for them). They are date-less, so a ``DATES`` scope must include them
    as well, both for the delete-then-rebuild and for the identity check.
    """
    if scope.scope_type == "FULL":
        return "source_file = %s", [scope.source]
    marks = ", ".join(["%s"] * len(scope.dates))
    return (
        f"source_file = %s AND (event_date_utc IN ({marks}) OR event_date_utc IS NULL)",
        [scope.source, *list(scope.dates)],
    )


def _ods_select_sql(scope: Scope) -> tuple[str, list]:
    where, params = _ods_where(scope)
    return (
        "SELECT row_key, `timestamp`, visitorid, event, itemid, transactionid, "
        f"ingest_batch_id, source_row_number FROM `ods_events_raw` WHERE {where} "
        "ORDER BY source_row_number",
        params,
    )


# --------------------------------------------------------------------------- #
# DWD split statements (one atomic statement per output table)                #
# --------------------------------------------------------------------------- #

_WINNER_SELECT_SQL = """
INSERT IGNORE INTO `dwd_events` ({cols})
SELECT s.row_key, s.dedup_key, s.visitor_id, s.item_id, s.raw_event_type,
       s.event_type, s.event_time_utc, s.event_date_utc, s.transaction_id,
       s.quality_flags, s.build_id, s.ingest_batch_id, s.source_file,
       s.source_row_number, s.loaded_at
FROM (
    SELECT st.row_key, st.dedup_key, st.visitor_id, st.item_id, st.raw_event_type,
           st.event_type, st.event_time_utc, st.event_date_utc, st.transaction_id,
           st.quality_flags, st.build_id, st.ingest_batch_id, st.source_file,
           st.source_row_number, st.loaded_at,
           ROW_NUMBER() OVER (PARTITION BY st.dedup_key ORDER BY st.source_row_number) AS rn
    FROM `dwd_events_stage` st
    WHERE st.record_status = 'OK' AND st.build_id = %s
) s
-- Phase 8 perf: join the UNIQUE dedup_key index directly (eq_ref). The previous
-- "LEFT JOIN (SELECT DISTINCT dedup_key FROM dwd_events)" materialised every
-- 2.7M key on each split statement; semantics are identical without DISTINCT.
LEFT JOIN `dwd_events` e ON e.dedup_key = s.dedup_key
WHERE s.rn = 1 AND e.dedup_key IS NULL
"""

_DUPLICATE_SELECT_SQL = """
INSERT IGNORE INTO `dwd_events_duplicate` ({cols})
SELECT s.row_key, s.dedup_key, s.visitor_id, s.item_id, s.raw_event_type,
       s.event_type, s.event_time_utc, s.event_date_utc, s.transaction_id,
       s.quality_flags, s.build_id, s.ingest_batch_id, s.source_file,
       s.source_row_number, s.loaded_at,
       s.raw_timestamp, s.raw_visitorid, s.raw_event, s.raw_itemid, s.raw_transactionid
FROM (
    SELECT st.*,
           ROW_NUMBER() OVER (PARTITION BY st.dedup_key ORDER BY st.source_row_number) AS rn
    FROM `dwd_events_stage` st
    WHERE st.record_status = 'OK' AND st.build_id = %s
) s
-- Same Phase 8 perf rewrite as the winner split (UNIQUE dedup_key eq_ref join).
LEFT JOIN `dwd_events` e ON e.dedup_key = s.dedup_key
WHERE s.rn > 1 OR e.dedup_key IS NOT NULL
"""

_REJECT_SELECT_SQL = """
INSERT IGNORE INTO `dwd_events_reject` ({cols})
SELECT st.row_key, st.raw_timestamp, st.raw_visitorid, st.raw_event, st.raw_itemid,
       st.raw_transactionid, st.event_type, st.event_time_utc, st.event_date_utc,
       st.reject_reason, st.reject_message, st.build_id, st.ingest_batch_id,
       st.source_file, st.source_row_number, st.loaded_at
FROM `dwd_events_stage` st
WHERE st.record_status = 'REJECT' AND st.build_id = %s
"""


def _stage_row_params(
    row_key: bytes,
    raw_timestamp: str,
    raw_visitorid: str,
    raw_event: str,
    raw_itemid: str,
    raw_transactionid: str,
    source_row_number: int,
    ingest_batch_id: int,
    outcome: TransformOutcome,
    *,
    source_file: str,
    build_id: int,
    loaded_at: datetime,
) -> tuple:
    return (
        row_key,
        raw_timestamp,
        raw_visitorid,
        raw_event,
        raw_itemid,
        raw_transactionid,
        outcome.visitor_id,
        outcome.item_id,
        raw_event,
        outcome.event_type,
        outcome.event_time_utc,
        outcome.event_date_utc,
        outcome.transaction_id,
        outcome.dedup_key,
        outcome.quality_flags_csv,
        outcome.status,
        outcome.reject_reason,
        outcome.reject_message,
        ingest_batch_id,
        source_file,
        source_row_number,
        loaded_at,
        build_id,
    )


# --------------------------------------------------------------------------- #
# the build                                                                   #
# --------------------------------------------------------------------------- #


def build_dwd(
    cfg: DbConfig,
    *,
    dates: list[date] | tuple[date, ...] | None = None,
    source_file: str = SOURCE_FILE,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    force: bool = False,
    max_resumes: int = DEFAULT_MAX_RESUMES,
    resume_wait_seconds: int = DEFAULT_RESUME_WAIT_SECONDS,
) -> BuildResult:
    """Build (or rebuild) the DWD events detail for the requested scope.

    - ``dates`` empty/None -> FULL rebuild of the events DWD from ODS.
    - otherwise only the given UTC days are rebuilt; all other days are preserved
      and their dedup winners still count during the split.
    - ``force=True`` discards an existing failed/crashed build cleanly.
    """
    scope = Scope(tuple(sorted(set(dates or ()))), source=source_file)
    conn = connect(cfg)
    started_at = _now()
    dur_start = time.monotonic()
    build_id: int | None = None
    resumed_from = 0
    try:
        _ensure_tables(conn)
        repo = BuildLogRepo(conn)
        existing = repo.find_for_scope(scope.scope_type, scope.dates_csv)
        running = [e for e in existing if e.status == "RUNNING"]
        if running:
            candidate = running[0]
            if candidate.transform_version != TRANSFORM_VERSION:
                raise BuildError(
                    f"build #{candidate.build_id} used transform version "
                    f"{candidate.transform_version!r} != current {TRANSFORM_VERSION!r}; "
                    "rules changed, force a clean rebuild with --force"
                )
            build_id = candidate.build_id
            resumed_from = _count_stage_scope(conn, scope)
            logger.info(
                "resume build #%d (%s): %d rows already staged by the interrupted run",
                build_id,
                scope.scope_type,
                resumed_from,
            )
        else:
            if not force:
                failed = [e for e in existing if e.status == "FAILED"]
                if failed:
                    raise BuildError(
                        f"build #{failed[0].build_id} for this scope FAILED; clean it first "
                        f"(`cleanup --build {failed[0].build_id}`) or pass --force"
                    )
            if force and existing:
                for entry in existing:
                    if entry.status in ("RUNNING", "FAILED"):
                        repo.mark_cleaned(entry.build_id, _now())
            build_id = repo.register(
                scope.scope_type, scope.dates_csv, TRANSFORM_VERSION, started_at
            )
            _delete_scope_rows(conn, scope)

        _stage_all(
            conn,
            cfg,
            scope,
            skip_count=resumed_from,
            build_id=build_id,
            loaded_at=started_at,
            chunk_size=chunk_size,
            max_resumes=max_resumes,
            resume_wait_seconds=resume_wait_seconds,
        )
        _split_stage_to_tables(
            conn,
            cfg,
            build_id=build_id,
            resume_wait_seconds=resume_wait_seconds,
            max_resumes=max_resumes,
        )
        counts = _verify_identity(conn, scope)
        duration = time.monotonic() - dur_start
        repo.mark_loaded(
            build_id,
            processed=counts["processed"],
            ok_rows=counts["ok"],
            duplicate_rows=counts["duplicate"],
            reject_rows=counts["reject"],
            finished_at=_now(),
        )
        logger.info(
            "[build #%d] %s loaded: ods=%d ok=%d dup=%d reject=%d in %.1fs (resumed %d rows)",
            build_id,
            scope.scope_type,
            counts["ods"],
            counts["ok"],
            counts["duplicate"],
            counts["reject"],
            duration,
            resumed_from,
        )
        return BuildResult(
            build_id=build_id,
            scope_type=scope.scope_type,
            dates_csv=scope.dates_csv,
            status="LOADED",
            ods_count=counts["ods"],
            processed_rows=counts["processed"],
            ok_rows=counts["ok"],
            duplicate_rows=counts["duplicate"],
            reject_rows=counts["reject"],
            duration_seconds=duration,
            resumed_from=resumed_from,
        )
    except Exception as exc:
        _safe_rollback(conn)
        if build_id is not None:
            try:
                BuildLogRepo(conn).mark_failed(
                    build_id,
                    _truncate_summary(f"{type(exc).__name__}: {exc}"),
                    _now(),
                )
            except Exception:  # failing to record FAILED must not mask the real error
                logger.exception("could not record FAILED status on build #%s", build_id)
        raise BuildError(f"DWD build failed: {exc}") from exc
    finally:
        conn.close()


def _ensure_tables(conn: pymysql.connections.Connection) -> None:
    names = _list_table_names(conn)
    required = (
        "dwd_events",
        "dwd_events_duplicate",
        "dwd_events_reject",
        "dwd_events_stage",
        "dwd_build_log",
    )
    missing = [t for t in required if t not in names]
    if missing:
        raise BuildError(
            f"DWD DDL not applied: missing {missing}; run `python scripts/apply_dwd_ddl.py` first"
        )


def _list_table_names(conn: pymysql.connections.Connection) -> set[str]:
    cursor = conn.cursor()
    try:
        cursor.execute("SHOW TABLES")
        return {row[0] for row in cursor.fetchall()}
    finally:
        cursor.close()


def _delete_scope_rows(conn: pymysql.connections.Connection, scope: Scope) -> None:
    """Wipe the scope from the four DWD tables before a fresh rebuild."""
    where, params = _dwd_where(scope)
    cursor = conn.cursor()
    try:
        for table in (
            "dwd_events",
            "dwd_events_duplicate",
            "dwd_events_reject",
            "dwd_events_stage",
        ):
            cursor.execute(f"DELETE FROM `{table}` WHERE {where}", params)
        conn.commit()
    finally:
        cursor.close()


def _count_stage_scope(conn: pymysql.connections.Connection, scope: Scope) -> int:
    where, params = _dwd_where(scope)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `dwd_events_stage` WHERE {where}", params)
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _stage_all(
    conn: pymysql.connections.Connection,
    cfg: DbConfig,
    scope: Scope,
    *,
    skip_count: int,
    build_id: int,
    loaded_at: datetime,
    chunk_size: int,
    max_resumes: int,
    resume_wait_seconds: int,
) -> int:
    """Stream ODS rows in scope through the transform into the stage table.

    Memory is bounded by streaming the SELECT with a client-side unbuffered
    ``SSCursor``, and the INSERTs go out over a separate connection. A single
    socket cannot safely stream a result and run a concurrent INSERT, and
    pymysql's default buffered ``Cursor`` would pull the whole scope (up to
    2.7M rows) into RAM at ``execute()`` — and its ``executemany`` replaces the
    pending SELECT result mid-fetch, silently truncating the stream.

    Returns the number of stage rows committed for the scope (== ODS rows in
    scope once complete). On connection loss it reconnects both connections,
    recounts the committed stage rows and continues, so an interrupted run
    resumes.
    """
    if chunk_size < 1:
        raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
    select_sql, select_params = _ods_select_sql(scope)
    insert_sql = _stage_insert_sql()
    total_in_scope = _ods_scope_count(conn, scope)
    processed = skip_count
    resumed = 0
    chunk_index = 0
    writer = connect(cfg)
    try:
        while True:
            reader = conn.cursor(pymysql.cursors.SSCursor)
            try:
                reader.execute(select_sql, select_params)
                remaining = skip_count
                while True:
                    chunk = reader.fetchmany(chunk_size)
                    if not chunk:
                        break
                    if remaining:
                        if remaining >= len(chunk):
                            remaining -= len(chunk)
                            continue
                        chunk = chunk[remaining:]
                        remaining = 0
                    params = [
                        _stage_row_params(
                            row_key,
                            raw_ts,
                            raw_visit,
                            raw_event,
                            raw_item,
                            raw_txn,
                            source_row,
                            ingest_batch,
                            transform_event(raw_ts, raw_visit, raw_event, raw_item, raw_txn),
                            source_file=scope.source,
                            build_id=build_id,
                            loaded_at=loaded_at,
                        )
                        for (
                            row_key,
                            raw_ts,
                            raw_visit,
                            raw_event,
                            raw_item,
                            raw_txn,
                            ingest_batch,
                            source_row,
                        ) in chunk
                    ]
                    writer_cursor = writer.cursor()
                    writer_cursor.max_stmt_length = _MAX_STMT_LENGTH
                    try:
                        writer_cursor.executemany(insert_sql, params)
                    finally:
                        writer_cursor.close()
                    writer.commit()
                    processed += len(params)
                    chunk_index += 1
                    if chunk_index % max(1, _PROGRESS_LOG_EVERY) == 0:
                        pct = 100.0 * processed / max(1, total_in_scope)
                        logger.info("[build #%d] staged %d rows (%.1f%%)", build_id, processed, pct)
                return processed
            except pymysql.err.OperationalError as exc:
                _safe_rollback(conn)
                _safe_rollback(writer)
                resumed += 1
                if resumed > max_resumes:
                    raise BuildError(
                        f"MySQL unreachable after {resumed - 1} resume(s) during staging: {exc}"
                    ) from exc
                logger.warning(
                    "[build #%d] connection lost during staging (%s); reconnecting and "
                    "resuming from last committed stage row (attempt %d/%d)",
                    build_id,
                    exc,
                    resumed,
                    max_resumes,
                )
                conn = _wait_and_reconnect(cfg, resume_wait_seconds, resumed, max_resumes)
                writer.close()
                writer = _wait_and_reconnect(cfg, resume_wait_seconds, resumed, max_resumes)
                skip_count = _count_stage_scope(conn, scope)
                processed = skip_count
                chunk_index = 0
            finally:
                reader.close()
    finally:
        writer.close()


def _split_stage_to_tables(
    conn: pymysql.connections.Connection,
    cfg: DbConfig,
    *,
    build_id: int,
    resume_wait_seconds: int,
    max_resumes: int,
) -> None:
    """Run the three atomic split statements, retrying across connection loss.

    Each statement is a single transaction that either lands whole or rolls back
    on disconnect, so retry-from-index is safe; INSERT IGNORE makes a retry after
    an ambiguous commit a no-op. Every statement filters ``st.build_id = %s`` so
    only THIS build's stage rows are classified — a build can never sweep up the
    un-split stage rows of another (e.g. an interrupted FULL) build.
    """
    # Order matters: the duplicate statement left-joins `dwd_events` to find
    # duplicates against preserved winners, so it must run BEFORE the winner
    # statement, otherwise it would re-classify the just-inserted winners of
    # this same build as duplicates.
    statements = [
        (_DUPLICATE_SELECT_SQL, "dwd_events_duplicate", _DWD_DUPLICATE_COLUMNS),
        (_WINNER_SELECT_SQL, "dwd_events", _DWD_EVENTS_COLUMNS),
        (_REJECT_SELECT_SQL, "dwd_events_reject", _DWD_REJECT_COLUMNS),
    ]
    index = 0
    resumed = 0
    while index < len(statements):
        sql_template, table, columns = statements[index]
        cols = ", ".join(f"`{c}`" for c in columns)
        cursor = conn.cursor()
        cursor.max_stmt_length = _MAX_STMT_LENGTH
        try:
            # Each statement filters its own record-status literal (hardcoded)
            # plus this build's id (parameterized). The old `("OK",)` argument
            # leaked the literal into the reject statement and silently dropped
            # every REJECT row, which is why only build_id is parameterized.
            cursor.execute(sql_template.format(cols=cols), (build_id,))
            conn.commit()
            index += 1
            logger.info("split -> %s committed", table)
        except pymysql.err.OperationalError as exc:
            _safe_rollback(conn)
            resumed += 1
            if resumed > max_resumes:
                raise BuildError(
                    f"MySQL unreachable after {resumed - 1} resume(s) during split: {exc}"
                ) from exc
            logger.warning(
                "connection lost during split -> %s (%s); reconnecting and retrying "
                "(attempt %d/%d)",
                table,
                exc,
                resumed,
                max_resumes,
            )
            conn = _wait_and_reconnect(cfg, resume_wait_seconds, resumed, max_resumes)
        finally:
            cursor.close()


def _verify_identity(conn: pymysql.connections.Connection, scope: Scope) -> dict[str, int]:
    """Check ``ODS = dwd_events + duplicates + rejects`` and dedup uniqueness."""
    where, params = _dwd_where(scope)
    ods_count = _ods_scope_count(conn, scope)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `dwd_events` WHERE {where}", params)
        ok = int(cursor.fetchone()[0])
        cursor.execute(f"SELECT COUNT(*) FROM `dwd_events_duplicate` WHERE {where}", params)
        dup = int(cursor.fetchone()[0])
        cursor.execute(f"SELECT COUNT(*) FROM `dwd_events_reject` WHERE {where}", params)
        rej = int(cursor.fetchone()[0])
        cursor.execute(f"SELECT COUNT(DISTINCT dedup_key) FROM `dwd_events` WHERE {where}", params)
        distinct_keys = int(cursor.fetchone()[0])
        cursor.execute(f"SELECT COUNT(*) FROM `dwd_events_stage` WHERE {where}", params)
        staged = int(cursor.fetchone()[0])
    finally:
        cursor.close()
    if ok != distinct_keys:
        raise BuildError(
            f"dedup invariant violated: dwd_events has {ok} rows but only "
            f"{distinct_keys} distinct dedup_key for the scope"
        )
    if ods_count != ok + dup + rej:
        raise BuildError(
            f"identity violated: ODS={ods_count} != dwd_events({ok}) + duplicate({dup}) "
            f"+ reject({rej})"
        )
    if staged != ok + dup + rej:
        raise BuildError(
            f"stage not fully consumed: staged={staged} != ok({ok}) + dup({dup}) + rej({rej})"
        )
    return {
        "processed": ods_count,
        "ods": ods_count,
        "ok": ok,
        "duplicate": dup,
        "reject": rej,
    }


def _ods_scope_count(conn: pymysql.connections.Connection, scope: Scope) -> int:
    where, params = _ods_where(scope)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM `ods_events_raw` WHERE {where}", params)
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _safe_rollback(conn: pymysql.connections.Connection) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def _wait_and_reconnect(cfg: DbConfig, wait_seconds: int, resumed: int, max_resumes: int):
    """Poll MySQL until it accepts connections, then return a fresh connection."""
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return connect(cfg)
        except pymysql.err.OperationalError:
            if time.monotonic() >= deadline:
                raise BuildError(
                    f"MySQL did not come back within {wait_seconds}s "
                    f"(resume {resumed}/{max_resumes})"
                ) from None
            logger.info("still waiting for MySQL (resume %d/%d)...", resumed, max_resumes)
            time.sleep(5)


def cleanup_build(cfg: DbConfig, build_id: int) -> int:
    """Delete the rows a DWD build produced and mark it CLEANED.

    Returns the total number of rows removed from the four DWD tables.
    """
    conn = connect(cfg)
    try:
        repo = BuildLogRepo(conn)
        entries = repo.find_by_id(build_id)
        if not entries:
            raise BuildError(f"build #{build_id} not found in dwd_build_log")
        deleted = 0
        cursor = conn.cursor()
        try:
            for table in (
                "dwd_events",
                "dwd_events_duplicate",
                "dwd_events_reject",
                "dwd_events_stage",
            ):
                cursor.execute(f"DELETE FROM `{table}` WHERE build_id = %s", (build_id,))
                deleted += cursor.rowcount
            conn.commit()
        finally:
            cursor.close()
        repo.mark_cleaned(build_id, _now())
        logger.info("build #%d cleaned: removed %d rows", build_id, deleted)
        return deleted
    finally:
        conn.close()


def verify_build(
    cfg: DbConfig, *, dates: list[date] | None = None, source_file: str = SOURCE_FILE
) -> dict[str, int]:
    """Re-run the identity check for a scope without rebuilding (read-only)."""
    scope = Scope(tuple(sorted(set(dates or ()))), source=source_file)
    conn = connect(cfg)
    try:
        _ensure_tables(conn)
        return _verify_identity(conn, scope)
    finally:
        conn.close()
