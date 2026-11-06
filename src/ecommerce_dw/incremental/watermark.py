"""Watermark repository for incremental processing (Phase 5).

The ``etl_watermark`` table records, per pipeline and per business date, the last
ODS ingest batch whose events were already rolled through DWD -> DWS -> ADS. The
incremental planner compares that value with the batches currently present in
``ods_events_raw``: a business date whose current max batch exceeds its recorded
watermark either progressed (new tail) or received late / corrected data.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pymysql

# Pipeline key for the events DWD+DWS+ADS incremental chain. Distinct pipelines
# (e.g. a separate item-detail chain) would be recorded under their own key.
DEFAULT_PIPELINE = "incremental"


class WatermarkRepo:
    """Thin CRUD over ``etl_watermark`` using a caller-supplied connection."""

    def __init__(self, conn: pymysql.connections.Connection) -> None:
        self._conn = conn
        self._cur = conn.cursor()

    def load(self, pipeline: str = DEFAULT_PIPELINE) -> dict[date, int]:
        """Return ``{business_date: max_ingest_batch}`` for a pipeline."""
        self._cur.execute(
            "SELECT event_date_utc, max_ingest_batch FROM etl_watermark WHERE pipeline = %s",
            (pipeline,),
        )
        return {row[0]: int(row[1]) for row in self._cur.fetchall()}

    def upsert_many(
        self,
        pipeline: str,
        entries: dict[date, int],
        processed_at: str | None = None,
    ) -> int:
        """Record the consumed ingest batch per business date (idempotent)."""
        if not entries:
            return 0
        stamp = processed_at or datetime.now(UTC).replace(tzinfo=None)
        rows = [(pipeline, day, batch, stamp) for day, batch in sorted(entries.items())]
        self._cur.executemany(
            "INSERT INTO etl_watermark (pipeline, event_date_utc, max_ingest_batch,"
            " processed_at) VALUES (%s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE max_ingest_batch = VALUES(max_ingest_batch),"
            " processed_at = VALUES(processed_at)",
            rows,
        )
        self._conn.commit()
        return len(rows)
