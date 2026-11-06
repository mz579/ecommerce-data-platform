"""Repository for the ``etl_batch_log`` audit table.

Tracks one batch per loaded source file: file SHA-256, target table, committed
row count, timestamps and status. Drives the skip-if-already-loaded behaviour,
the failure/cleanup/resume flow and the `status` CLI view.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pymysql

_COLUMNS = (
    "batch_id",
    "source_file",
    "file_sha256",
    "table_name",
    "status",
    "row_count",
    "started_at",
    "finished_at",
    "error_summary",
)


@dataclass(frozen=True)
class BatchLogEntry:
    batch_id: int
    source_file: str
    file_sha256: str
    table_name: str
    status: str
    row_count: int
    started_at: datetime
    finished_at: datetime | None
    error_summary: str | None


def _entry_from_row(row: tuple) -> BatchLogEntry:
    return BatchLogEntry(
        batch_id=int(row[0]),
        source_file=row[1],
        file_sha256=row[2],
        table_name=row[3],
        status=row[4],
        row_count=int(row[5]),
        started_at=row[6],
        finished_at=row[7],
        error_summary=row[8],
    )


class BatchLogRepo:
    """Thin CRUD over ``etl_batch_log`` using a caller-supplied connection."""

    def __init__(self, conn: pymysql.connections.Connection) -> None:
        self._conn = conn
        self._cursor = conn.cursor()

    def register(
        self, source_file: str, file_sha256: str, table_name: str, started_at: datetime
    ) -> int:
        """Insert a PENDING batch row and return its batch_id."""
        self._cursor.execute(
            "INSERT INTO etl_batch_log "
            "(source_file, file_sha256, table_name, status, row_count, started_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (source_file, file_sha256, table_name, "PENDING", 0, started_at),
        )
        self._conn.commit()
        return int(self._cursor.lastrowid)

    def mark_loaded(self, batch_id: int, row_count: int, finished_at: datetime) -> None:
        self._update_status(batch_id, "LOADED", row_count=row_count, finished_at=finished_at)

    def mark_failed(
        self, batch_id: int, row_count: int, error_summary: str, finished_at: datetime
    ) -> None:
        self._update_status(
            batch_id,
            "FAILED",
            row_count=row_count,
            finished_at=finished_at,
            error_summary=error_summary,
        )

    def mark_cleaned(self, batch_id: int, finished_at: datetime) -> None:
        """Mark a batch CLEANED after its rows were deleted (audit trail)."""
        self._cursor.execute(
            "UPDATE etl_batch_log SET status = %s, finished_at = %s WHERE batch_id = %s",
            ("CLEANED", finished_at, batch_id),
        )
        self._conn.commit()

    def find_by_id(self, batch_id: int) -> list[BatchLogEntry]:
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM etl_batch_log WHERE batch_id = %s",
            (batch_id,),
        )
        row = self._cursor.fetchone()
        return [_entry_from_row(row)] if row else []

    def find_for_file(self, source_file: str) -> list[BatchLogEntry]:
        """All batches for a source file, newest first."""
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM etl_batch_log "
            "WHERE source_file = %s ORDER BY batch_id DESC",
            (source_file,),
        )
        return [_entry_from_row(row) for row in self._cursor.fetchall()]

    def list_recent(self, limit: int = 20) -> list[BatchLogEntry]:
        """Most recent batches, newest first."""
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM etl_batch_log ORDER BY batch_id DESC LIMIT %s",
            (limit,),
        )
        return [_entry_from_row(row) for row in self._cursor.fetchall()]

    def delete_for_file(self, source_file: str) -> None:
        """Remove log rows for a source file (test cleanup helper)."""
        self._cursor.execute("DELETE FROM etl_batch_log WHERE source_file = %s", (source_file,))
        self._conn.commit()

    def _update_status(
        self,
        batch_id: int,
        status: str,
        *,
        row_count: int | None = None,
        finished_at: datetime | None = None,
        error_summary: str | None = None,
    ) -> None:
        sets = ["status = %s", "finished_at = %s"]
        params: list = [status, finished_at]
        if row_count is not None:
            sets.append("row_count = %s")
            params.append(row_count)
        if error_summary is not None:
            sets.append("error_summary = %s")
            params.append(error_summary)
        params.append(batch_id)
        self._cursor.execute(
            f"UPDATE etl_batch_log SET {', '.join(sets)} WHERE batch_id = %s",
            params,
        )
        self._conn.commit()


def _cols_sql() -> str:
    return ", ".join(_COLUMNS)
