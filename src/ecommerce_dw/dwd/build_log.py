"""Repository for the ``dwd_build_log`` audit table (Phase 3).

Tracks one row per DWD build run, keyed by *scope* (``FULL`` or a set of UTC
dates) plus the transform version. Mirrors the shape of the ODS-side
``BatchLogRepo`` but keeps its own row-count semantics (processed / ok /
duplicate / reject) because a DWD build is a derived layer, not a file load.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pymysql

_COLUMNS = (
    "build_id",
    "scope_type",
    "dates_csv",
    "status",
    "transform_version",
    "processed_rows",
    "ok_rows",
    "duplicate_rows",
    "reject_rows",
    "started_at",
    "finished_at",
    "error_summary",
)


@dataclass(frozen=True)
class BuildLogEntry:
    build_id: int
    scope_type: str
    dates_csv: str | None
    status: str
    transform_version: str
    processed_rows: int
    ok_rows: int
    duplicate_rows: int
    reject_rows: int
    started_at: datetime
    finished_at: datetime | None
    error_summary: str | None


def _entry_from_row(row: tuple) -> BuildLogEntry:
    return BuildLogEntry(
        build_id=int(row[0]),
        scope_type=row[1],
        dates_csv=row[2],
        status=row[3],
        transform_version=row[4],
        processed_rows=int(row[5]),
        ok_rows=int(row[6]),
        duplicate_rows=int(row[7]),
        reject_rows=int(row[8]),
        started_at=row[9],
        finished_at=row[10],
        error_summary=row[11],
    )


class BuildLogRepo:
    """Thin CRUD over ``dwd_build_log`` using a caller-supplied connection."""

    def __init__(self, conn: pymysql.connections.Connection) -> None:
        self._conn = conn
        self._cursor = conn.cursor()

    def register(
        self,
        scope_type: str,
        dates_csv: str | None,
        transform_version: str,
        started_at: datetime,
    ) -> int:
        self._cursor.execute(
            "INSERT INTO dwd_build_log "
            "(scope_type, dates_csv, status, transform_version, processed_rows, "
            " ok_rows, duplicate_rows, reject_rows, started_at) "
            "VALUES (%s, %s, 'RUNNING', %s, 0, 0, 0, 0, %s)",
            (scope_type, dates_csv, transform_version, started_at),
        )
        self._conn.commit()
        return int(self._cursor.lastrowid)

    def mark_loaded(
        self,
        build_id: int,
        *,
        processed: int,
        ok_rows: int,
        duplicate_rows: int,
        reject_rows: int,
        finished_at: datetime,
    ) -> None:
        self._cursor.execute(
            "UPDATE dwd_build_log SET status = 'LOADED', processed_rows = %s, "
            "ok_rows = %s, duplicate_rows = %s, reject_rows = %s, finished_at = %s "
            "WHERE build_id = %s",
            (processed, ok_rows, duplicate_rows, reject_rows, finished_at, build_id),
        )
        self._conn.commit()

    def mark_failed(self, build_id: int, error_summary: str, finished_at: datetime) -> None:
        self._cursor.execute(
            "UPDATE dwd_build_log SET status = 'FAILED', error_summary = %s, "
            "finished_at = %s WHERE build_id = %s",
            (error_summary, finished_at, build_id),
        )
        self._conn.commit()

    def mark_cleaned(self, build_id: int, finished_at: datetime) -> None:
        self._cursor.execute(
            "UPDATE dwd_build_log SET status = 'CLEANED', finished_at = %s WHERE build_id = %s",
            (finished_at, build_id),
        )
        self._conn.commit()

    def find_by_id(self, build_id: int) -> list[BuildLogEntry]:
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM dwd_build_log WHERE build_id = %s",
            (build_id,),
        )
        row = self._cursor.fetchone()
        return [_entry_from_row(row)] if row else []

    def find_for_scope(self, scope_type: str, dates_csv: str | None) -> list[BuildLogEntry]:
        """All builds for a scope, newest first (confirmed builds stay for audit).

        ``<=>`` is MySQL's null-safe equality: it matches when both sides are
        NULL (the FULL-scope case) as well as plain value equality.
        """
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM dwd_build_log "
            "WHERE scope_type = %s AND dates_csv <=> %s "
            "ORDER BY build_id DESC",
            (scope_type, dates_csv),
        )
        return [_entry_from_row(row) for row in self._cursor.fetchall()]

    def list_recent(self, limit: int = 20) -> list[BuildLogEntry]:
        self._cursor.execute(
            f"SELECT {_cols_sql()} FROM dwd_build_log ORDER BY build_id DESC LIMIT %s",
            (limit,),
        )
        return [_entry_from_row(row) for row in self._cursor.fetchall()]


def _cols_sql() -> str:
    return ", ".join(_COLUMNS)
