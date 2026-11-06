"""Chunked, idempotent, resumable CSV-to-ODS loader (Phase 2).

Implements the design documented in ``docs/ods_design.md``:

- memory-bounded streaming: at most ``chunk_size`` rows are buffered at a time
- auditable: every run registers an ``etl_batch_log`` row carrying the file
  SHA-256, committed row count, timestamps and status
- idempotent: re-loading a file whose SHA-256 is already ``LOADED`` is skipped;
  ``force=True`` deletes that file's rows first so a reload never duplicates
- resumable two ways:
  * automated: if MySQL disappears mid-run (connection loss), the loader
    reconnects (waiting for the DB to come back) and continues the SAME batch
    from the last committed row — ``INSERT IGNORE`` makes a chunk that was
    actually committed by the server right before the drop harmless on replay;
  * manual: ``cleanup_batch`` / ``cleanup_file`` wipe a failed batch's rows and
    set the log row to ``CLEANED`` so the file can be loaded again cleanly.

No cleaning, dedup or type conversion happens in this layer — every raw CSV cell
is stored as an exact string (empty cells become empty strings).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pymysql

from ecommerce_dw.config import DbConfig
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.chunk import CsvChunkReader, read_csv_header
from ecommerce_dw.ingestion.db import connect
from ecommerce_dw.ingestion.keys import make_row_key, sha256_file
from ecommerce_dw.ingestion.specs import TABLE_SPECS, TableSpec

DEFAULT_CHUNK_SIZE = 10_000
DEFAULT_MAX_RESUMES = 20
DEFAULT_RESUME_WAIT_SECONDS = 300
_ERROR_SUMMARY_MAX = 4_000
_PROGRESS_LOG_EVERY = 5  # log one progress line every N chunks
# pymysql batches a pymysql executemany chunk into multi-row INSERT statements;
# the default max_stmt_length (64KB) forces several round trips per chunk.
_MAX_STMT_LENGTH = 8 * 1024 * 1024  # one ~MB-sized INSERT per 10k-row chunk

logger = logging.getLogger(__name__)


class LoadError(RuntimeError):
    """Raised when a load run fails after a batch was registered."""


class BatchStateError(RuntimeError):
    """Raised when an existing batch blocks a fresh load (must be cleaned first)."""


@dataclass(frozen=True)
class LoadResult:
    """Outcome of a single load run (a skipped run reports no new rows)."""

    table_name: str
    source_file: str
    file_sha256: str
    batch_id: int
    rows_committed: int
    duration_seconds: float
    status: str = "LOADED"
    skipped: bool = False


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _truncate_summary(text: str, limit: int = _ERROR_SUMMARY_MAX) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def build_insert_sql(spec: TableSpec) -> str:
    """Insert covering the surrogate key, raw columns and lineage metadata.

    ``INSERT IGNORE`` is used so a replay after an ambiguous commit (the server
    committed a chunk just before a connection drop) is a no-op for rows that
    are already present, while genuine duplicate-key attempts still never
    increase the table (see docs/ods_design.md §4).
    """
    columns = (
        "row_key",
        *spec.raw_columns,
        "source_file",
        "source_row_number",
        "ingest_batch_id",
        "loaded_at",
    )
    identifiers = ", ".join(f"`{name}`" for name in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT IGNORE INTO `{spec.table_name}` ({identifiers}) VALUES ({placeholders})"


def load_csv(
    cfg: DbConfig,
    path: str | Path,
    table_name: str,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    force: bool = False,
    progress_every: int = _PROGRESS_LOG_EVERY,
    max_resumes: int = DEFAULT_MAX_RESUMES,
    resume_wait_seconds: int = DEFAULT_RESUME_WAIT_SECONDS,
) -> LoadResult:
    """Load one CSV file into an ODS raw table.

    - Skips (with a ``skipped=True`` result) if a batch with the same file SHA-256
      is already ``LOADED`` and ``force`` is False.
    - Raises ``BatchStateError`` if an incomplete (PENDING/FAILED) batch exists
      for the file and ``force`` is False.
    - With ``force=True`` deletes the file's rows and marks old batches CLEANED
      before loading a fresh batch.
    - Survives transient MySQL restarts: on connection loss it reconnects (up to
      ``max_resumes`` times, each wait bound by ``resume_wait_seconds``) and
      continues the same batch from the last committed row.
    """
    src = Path(path)
    try:
        spec = TABLE_SPECS[table_name]
    except KeyError as exc:
        raise ValueError(
            f"unknown ODS table {table_name!r}; choose one of {sorted(TABLE_SPECS)}"
        ) from exc
    source_file = src.name
    file_sha256 = sha256_file(src)
    # Header is validated before a batch is registered so a mismatched file
    # does not poison the batch log.
    read_csv_header(src, expected=spec.raw_columns)

    conn = connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        existing = repo.find_for_file(source_file)
        resume_batch = None
        resume_inserted = 0
        if existing and not force:
            loaded = [e for e in existing if e.status == "LOADED"]
            if loaded:
                best = loaded[0]
                logger.info(
                    "skip %s: already loaded as batch #%d (sha256=%s...)",
                    source_file,
                    best.batch_id,
                    best.file_sha256[:16],
                )
                return LoadResult(
                    table_name=table_name,
                    source_file=source_file,
                    file_sha256=file_sha256,
                    batch_id=best.batch_id,
                    rows_committed=0,
                    duration_seconds=0.0,
                    status="SKIPPED",
                    skipped=True,
                )
            # A PENDING batch with the same file SHA-256 is a previous run that
            # lost its process/connection mid-insert. Continue it in place
            # (counting its already-committed rows) so no committed work is
            # thrown away; the file's rows stay inside one single batch.
            pend = [e for e in existing if e.status == "PENDING" and e.file_sha256 == file_sha256]
            if pend:
                resume_batch = pend[-1]
                resume_inserted = _count_batch_rows(conn, spec.table_name, resume_batch.batch_id)
                logger.info(
                    "resume batch #%d for %s (%d rows already committed by the interrupted run)",
                    resume_batch.batch_id,
                    source_file,
                    resume_inserted,
                )
            needs_clean = [
                e
                for e in existing
                if e.status == "FAILED" or (e.status == "PENDING" and e.file_sha256 != file_sha256)
            ]
            if needs_clean:
                ids = ", ".join(str(e.batch_id) for e in needs_clean)
                raise BatchStateError(
                    f"file {source_file!r} has failed/changed batches [{ids}]; "
                    "clean them first, e.g. "
                    f"python -m ecommerce_dw.ingestion.cli "
                    f"cleanup --batch {needs_clean[0].batch_id}"
                )
        if existing and force:
            _delete_file_rows(conn, spec.table_name, source_file)
            for entry in existing:
                repo.mark_cleaned(entry.batch_id, _now())

        cursor = conn.cursor()
        cursor.max_stmt_length = _MAX_STMT_LENGTH
        batch_id: int | None = None
        inserted = 0
        insert_sql = build_insert_sql(spec)
        try:
            if resume_batch is not None:
                batch_id = resume_batch.batch_id
                inserted = resume_inserted
                started = resume_batch.started_at
            else:
                started = _now()
                batch_id = repo.register(source_file, file_sha256, spec.table_name, started)
            dur_start = time.monotonic()
            resumed = 0
            while True:
                try:
                    skip_rows = inserted
                    chunk_index = 0
                    reader = CsvChunkReader(
                        src, chunk_size=chunk_size, expected_header=spec.raw_columns
                    )
                    for chunk in reader.iter_chunks():
                        chunk_index += 1
                        base, rows = _suffix(chunk.rows, skip_rows, chunk.start_row_number)
                        skip_rows -= base - chunk.start_row_number
                        if not rows:
                            continue
                        params = _params_for(rows, base, spec, source_file, batch_id, started)
                        cursor.executemany(insert_sql, params)
                        conn.commit()
                        inserted += len(params)
                        if chunk_index % max(1, progress_every) == 0:
                            logger.info(
                                "[batch #%d] %s: committed %d rows so far",
                                batch_id,
                                source_file,
                                inserted,
                            )
                    break
                except pymysql.err.OperationalError as exc:
                    _safe_rollback(conn)
                    resumed += 1
                    if resumed > max_resumes:
                        raise LoadError(
                            f"MySQL unreachable after {resumed - 1} resume(s): {exc}"
                        ) from exc
                    logger.warning(
                        "[batch #%d] %s: connection lost (%s), waiting for MySQL "
                        "then resuming at row %d (attempt %d/%d)",
                        batch_id,
                        source_file,
                        exc,
                        inserted + 2,
                        resumed,
                        max_resumes,
                    )
                    conn = _wait_and_reconnect(cfg, resume_wait_seconds, resumed, max_resumes)
                    repo = BatchLogRepo(conn)
                    cursor = conn.cursor()
                    cursor.max_stmt_length = _MAX_STMT_LENGTH
            duration = time.monotonic() - dur_start
            repo.mark_loaded(batch_id, inserted, _now())
            logger.info(
                "[batch #%d] %s: done, committed %d rows in %.1fs",
                batch_id,
                source_file,
                inserted,
                duration,
            )
            return LoadResult(
                table_name=table_name,
                source_file=source_file,
                file_sha256=file_sha256,
                batch_id=batch_id,
                rows_committed=inserted,
                duration_seconds=duration,
            )
        except Exception as exc:
            _safe_rollback(conn)
            try:
                if batch_id is not None:
                    repo.mark_failed(
                        batch_id,
                        inserted,
                        _truncate_summary(f"{type(exc).__name__}: {exc}"),
                        _now(),
                    )
            except Exception:  # the failure signature must not mask the real error
                logger.exception("[batch #%s] could not record FAILED status", batch_id)
            raise LoadError(f"load of {source_file} failed: {exc}") from exc
        finally:
            cursor.close()
    finally:
        conn.close()


def _suffix(rows, skip_rows: int, start_row_number: int) -> tuple[int, tuple[tuple[str, ...], ...]]:
    """Return (base_row_number, rows) with the first ``skip_rows`` rows dropped."""
    keep = min(skip_rows, len(rows))
    return start_row_number + keep, rows[keep:]


def _params_for(
    rows,
    base_row_number: int,
    spec: TableSpec,
    source_file: str,
    batch_id: int,
    loaded_at: datetime,
) -> list[tuple]:
    params = []
    for offset, fields in enumerate(rows):
        row_number = base_row_number + offset
        params.append(
            (
                make_row_key(source_file, row_number),
                *fields,
                source_file,
                row_number,
                batch_id,
                loaded_at,
            )
        )
    return params


def _safe_rollback(conn: pymysql.connections.Connection) -> None:
    try:
        conn.rollback()
    except Exception:
        pass


def _count_batch_rows(conn: pymysql.connections.Connection, table_name: str, batch_id: int) -> int:
    """Return how many rows a batch already committed (for cross-process resume)."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM `{table_name}` WHERE ingest_batch_id = %s",
            (batch_id,),
        )
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _wait_and_reconnect(cfg: DbConfig, wait_seconds: int, resumed: int, max_resumes: int):
    """Poll the database until it accepts connections, then open a fresh one."""
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            return connect(cfg, connect_timeout=5)
        except pymysql.err.OperationalError:
            if time.monotonic() >= deadline:
                raise LoadError(
                    f"MySQL did not come back within {wait_seconds}s "
                    f"(resume {resumed}/{max_resumes})"
                ) from None
            logger.info("still waiting for MySQL (%d/%d resumes)...", resumed, max_resumes)
            time.sleep(5)


def cleanup_batch(cfg: DbConfig, batch_id: int) -> int:
    """Delete a batch's rows from its ODS table and mark it CLEANED.

    Returns the number of rows deleted. Re-running the file afterwards loads a
    fresh batch (the CLEANED marker plus a new SHA-256-aware skip check).
    """
    conn = connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        entries = repo.find_by_id(batch_id)
        if not entries:
            raise BatchStateError(f"batch #{batch_id} not found in etl_batch_log")
        entry = entries[0]
        deleted = _delete_batch_rows(conn, entry.table_name, batch_id)
        repo.mark_cleaned(batch_id, _now())
        logger.info(
            "batch #%d cleaned: removed %d rows from %s", batch_id, deleted, entry.table_name
        )
        return deleted
    finally:
        conn.close()


def cleanup_file(cfg: DbConfig, source_file: str) -> int:
    """Delete every batch's rows for a source file and mark them CLEANED."""
    conn = connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        entries = repo.find_for_file(source_file)
        total = 0
        for entry in entries:
            total += _delete_batch_rows(conn, entry.table_name, entry.batch_id)
            repo.mark_cleaned(entry.batch_id, _now())
        logger.info(
            "file %s cleaned: removed %d rows across %d batches", source_file, total, len(entries)
        )
        return total
    finally:
        conn.close()


def _delete_file_rows(
    conn: pymysql.connections.Connection, table_name: str, source_file: str
) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"DELETE FROM `{table_name}` WHERE source_file = %s",
            (source_file,),
        )
        conn.commit()
        return cursor.rowcount
    finally:
        cursor.close()


def _delete_batch_rows(conn: pymysql.connections.Connection, table_name: str, batch_id: int) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"DELETE FROM `{table_name}` WHERE ingest_batch_id = %s",
            (batch_id,),
        )
        conn.commit()
        return cursor.rowcount
    finally:
        cursor.close()
