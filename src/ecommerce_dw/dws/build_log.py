"""Repository for the ``dws_ads_build_log`` audit table (Phase 4).

One row per DWS/ADS aggregation build, keyed by *scope* (``FULL`` or a set of
UTC dates) plus the aggregation version. Mirrors the shape of the ODS/DWD logs
but keeps per-output-table row counts (``table_stats`` JSON) because an
aggregation run writes up to seven derived tables at once — a shape neither
``etl_batch_log`` (file hash key) nor ``dwd_build_log`` (ok/duplicate/reject
split) can express.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime

import pymysql

_COLUMNS = (
    "build_id",
    "scope_type",
    "dates_csv",
    "status",
    "agg_version",
    "table_stats",
    "rows_written",
    "started_at",
    "finished_at",
    "error_summary",
)


@dataclass(frozen=True)
class AggBuildLogEntry:
    build_id: int
    scope_type: str
    dates_csv: str | None
    status: str
    agg_version: str
    table_stats: dict[str, int] | None
    rows_written: int
    started_at: datetime
    finished_at: datetime | None
    error_summary: str | None


def _entry_from_row(row: tuple) -> AggBuildLogEntry:
    stats: dict[str, int] | None = None
    if row[5]:
        try:
            stats = {k: int(v) for k, v in json.loads(row[5]).items()}
        except (ValueError, TypeError):
            stats = None
    return AggBuildLogEntry(
        build_id=int(row[0]),
        scope_type=row[1],
        dates_csv=row[2],
        status=row[3],
        agg_version=row[4],
        table_stats=stats,
        rows_written=int(row[6]),
        started_at=row[7],
        finished_at=row[8],
        error_summary=row[9],
    )


class AggBuildLogRepo:
    """Thin CRUD over ``dws_ads_build_log`` using a caller-supplied connection."""

    def __init__(self, conn: pymysql.connections.Connection) -> None:
        self._conn = conn
        self._cur = conn.cursor()

    def register(
        self, scope_type: str, dates_csv: str | None, agg_version: str, started_at: datetime
    ) -> int:
        self._cur.execute(
            "INSERT INTO dws_ads_build_log "
            "(scope_type, dates_csv, status, agg_version, rows_written, started_at) "
            "VALUES (%s, %s, 'RUNNING', %s, 0, %s)",
            (scope_type, dates_csv, agg_version, started_at),
        )
        self._conn.commit()
        return int(self._cur.lastrowid)

    def mark_loaded(
        self,
        build_id: int,
        *,
        table_stats: dict[str, int],
        finished_at: datetime,
    ) -> None:
        rows = sum(table_stats.values())
        self._cur.execute(
            "UPDATE dws_ads_build_log SET status = 'LOADED', table_stats = %s, "
            "rows_written = %s, finished_at = %s WHERE build_id = %s",
            (json.dumps(table_stats), rows, finished_at, build_id),
        )
        self._conn.commit()

    def mark_failed(self, build_id: int, error_summary: str, finished_at: datetime) -> None:
        self._cur.execute(
            "UPDATE dws_ads_build_log SET status = 'FAILED', error_summary = %s, "
            "finished_at = %s WHERE build_id = %s",
            (error_summary, finished_at, build_id),
        )
        self._conn.commit()

    def mark_cleaned(self, build_id: int, finished_at: datetime) -> None:
        self._cur.execute(
            "UPDATE dws_ads_build_log SET status = 'CLEANED', finished_at = %s WHERE build_id = %s",
            (finished_at, build_id),
        )
        self._conn.commit()

    def find_by_id(self, build_id: int) -> list[AggBuildLogEntry]:
        self._cur.execute(
            f"SELECT {_cols_sql()} FROM dws_ads_build_log WHERE build_id = %s", (build_id,)
        )
        row = self._cur.fetchone()
        return [_entry_from_row(row)] if row else []

    def find_for_scope(self, scope_type: str, dates_csv: str | None) -> list[AggBuildLogEntry]:
        """All builds for a scope, newest first.

        ``<=>`` is MySQL's null-safe equality: matches both NULL (FULL) and value.
        """
        self._cur.execute(
            f"SELECT {_cols_sql()} FROM dws_ads_build_log "
            "WHERE scope_type = %s AND dates_csv <=> %s ORDER BY build_id DESC",
            (scope_type, dates_csv),
        )
        return [_entry_from_row(row) for row in self._cur.fetchall()]

    def list_recent(self, limit: int = 20) -> list[AggBuildLogEntry]:
        self._cur.execute(
            f"SELECT {_cols_sql()} FROM dws_ads_build_log ORDER BY build_id DESC LIMIT %s",
            (limit,),
        )
        return [_entry_from_row(row) for row in self._cur.fetchall()]


def _cols_sql() -> str:
    return ", ".join(_COLUMNS)
