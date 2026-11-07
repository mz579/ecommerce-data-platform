"""Operational status helpers for the Phase 9 realtime pipeline.

Collects the four things an operator needs during/after a replay batch:

1. Flink job state and job-level metrics;
2. checkpoint success rate, duration and state size;
3. Kafka consumer-group lag per partition;
4. RT table freshness (``rt_traffic_daily`` row/final counts, ``MAX(updated_at)``).

It also provides the immutable production 13-table baseline used before and
after Phase 9.4 acceptance runs.

Everything is exposed through ``scripts/streaming_status.py``; the parsing and
aggregation helpers are pure and unit tested.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from ecommerce_dw.streaming.batch_replay import (
    DEFAULT_BOOTSTRAP,
    DEFAULT_FLINK_URL,
    DEFAULT_GROUP,
    DEFAULT_TOPIC,
    FlinkRest,
    KafkaLagProbe,
    filter_operator_metrics,
    summarize_checkpoints,
)

PRODUCTION_BASELINE: dict[str, int] = {
    "ods_events_raw": 2_756_101,
    "dwd_events": 2_755_641,
    "dws_user_daily": 1_649_534,
    "dws_item_daily": 1_672_186,
    "dws_category_daily": 94_194,
    "ads_traffic_daily": 139,
    "ads_funnel_daily": 1_251,
    "ads_item_ranking_daily": 13_900,
    "ads_retention_cohort": 278,
    "etl_watermark": 139,
    "ads_funnel_interval": 1,
    "ads_item_behavior_daily": 139,
    "ads_publish_log": 12,
}

RT_TABLES = (
    "rt_traffic_daily",
    "rt_funnel_daily",
    "rt_item_behavior_daily",
    "rt_reconciliation_daily",
    "rt_late_events",
)


def select_job(
    jobs: Sequence[Mapping[str, Any]], job_id: str | None = None
) -> Mapping[str, Any] | None:
    """Pick an explicit job, else the running job, else the newest job."""
    if job_id is not None:
        for job in jobs:
            if job.get("jid") == job_id:
                return job
        return None
    running = [job for job in jobs if job.get("state") == "RUNNING"]
    if running:
        return max(running, key=lambda job: job.get("start-time", 0))
    if not jobs:
        return None
    return max(jobs, key=lambda job: job.get("start-time", 0))


def collect_status(
    *,
    flink_url: str = DEFAULT_FLINK_URL,
    job_id: str | None = None,
    group: str = DEFAULT_GROUP,
    topic: str = DEFAULT_TOPIC,
    bootstrap: str = DEFAULT_BOOTSTRAP,
) -> dict[str, Any]:
    """Collect Flink + Kafka status without touching MySQL."""
    rest = FlinkRest(flink_url)
    jobs = rest.jobs()
    selected = select_job(jobs, job_id)
    status: dict[str, Any] = {
        "collected_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "flink_url": flink_url,
        "jobs": jobs,
        "selected_job": selected,
    }
    if selected is None:
        return status
    selected_id = str(selected["jid"])
    status["job_id"] = selected_id
    try:
        status["job_detail"] = rest.job(selected_id)
    except Exception as exc:
        status["job_detail_error"] = f"{type(exc).__name__}: {exc}"
    try:
        status["checkpoints"] = summarize_checkpoints(rest.checkpoints(selected_id))
    except Exception as exc:
        status["checkpoints_error"] = f"{type(exc).__name__}: {exc}"
    try:
        metrics = rest.metrics(selected_id)
        status["operator_metrics"] = filter_operator_metrics(metrics)
        status["num_restarts"] = metrics.get("numRestarts")
    except Exception as exc:
        status["metrics_error"] = f"{type(exc).__name__}: {exc}"
    probe = KafkaLagProbe(bootstrap, group, topic)
    try:
        status["kafka_lag"] = probe.snapshot()
    except Exception as exc:
        status["kafka_lag_error"] = f"{type(exc).__name__}: {exc}"
    finally:
        probe.close()
    return status


def rt_freshness(connection: Any) -> dict[str, Any]:
    """Read RT row counts, final coverage and ``MAX(updated_at)``."""
    result: dict[str, Any] = {"tables": {}}
    with connection.cursor() as cursor:
        for table in RT_TABLES:
            cursor.execute(f"SELECT COUNT(*) AS n FROM {table}")
            result["tables"][table] = {"rows": int(cursor.fetchone()["n"])}
        cursor.execute(
            "SELECT COUNT(*) AS n, SUM(is_final = 1) AS final_rows, "
            "MAX(updated_at) AS updated_at, MAX(CASE WHEN is_final = 1 THEN event_date_utc END) "
            "AS last_final_date FROM rt_traffic_daily"
        )
        row = cursor.fetchone()
        result["traffic"] = {
            "rows": int(row["n"] or 0),
            "final_rows": int(row["final_rows"] or 0),
            "max_updated_at": None if row["updated_at"] is None else str(row["updated_at"]),
            "last_final_date": None
            if row["last_final_date"] is None
            else str(row["last_final_date"]),
        }
        cursor.execute(
            "SELECT event_date_utc, pv, uv, dau, is_final, updated_at "
            "FROM rt_traffic_daily ORDER BY event_date_utc DESC LIMIT 5"
        )
        result["latest_days"] = [
            {
                "event_date": str(item["event_date_utc"]),
                "pv": int(item["pv"]),
                "uv": int(item["uv"]),
                "dau": int(item["dau"]),
                "is_final": int(item["is_final"]),
                "updated_at": str(item["updated_at"]),
            }
            for item in cursor.fetchall()
        ]
    return result


def production_baseline(connection: Any) -> dict[str, Any]:
    """Count the 13 production tables and compare them with the fixed baseline."""
    observed: dict[str, int] = {}
    with connection.cursor() as cursor:
        for table in PRODUCTION_BASELINE:
            cursor.execute(f"SELECT COUNT(*) AS n FROM {table}")
            observed[table] = int(cursor.fetchone()["n"])
    mismatches = {
        table: {"expected": expected, "observed": observed[table]}
        for table, expected in PRODUCTION_BASELINE.items()
        if observed[table] != expected
    }
    return {
        "checked_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "expected": dict(PRODUCTION_BASELINE),
        "observed": observed,
        "mismatches": mismatches,
        "match": not mismatches,
    }


def discover_bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP") or DEFAULT_BOOTSTRAP
