"""Batched full-range replay orchestration for Phase 9.4.

The TaskManager only has ~160 MB of task heap, so the 2.76 M DWD events cannot be
replayed in one Flink job. This module plans contiguous batches of full days,
submits a fresh Flink job per batch with an independent consumer group and
``--starting-offsets latest``, replays ``N target days + 2 lookahead days``, waits
until every target day has ``rt_traffic_daily.is_final = 1``, then cancels the
job and runs the §6.3 checkpoint janitor.

The lookahead days are replayed again in the next batch; the new job only sees
messages produced after it started, so Kafka does not re-deliver them. Any
duplicate caused by a batch retry is absorbed by the job's ``event_id`` dedup
state, and the MySQL upsert overwrites the same daily rows.

The pure planning/summary helpers are unit tested; IO lives in the small
``FlinkRest`` / ``KafkaLagProbe`` / ``DockerCli`` wrappers and ``run_batch``.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any

import pymysql
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.streaming.events import (
    build_event_message,
    encode_event,
    kafka_key_for_visitor,
    validate_event,
)
from ecommerce_dw.streaming.reconcile_all import (
    RT_DATABASE,
    connect_mysql,
    rt_app_credentials,
)
from ecommerce_dw.streaming.replay import build_producer_config
from ecommerce_dw.streaming.verify_rt import rt_recon_credentials

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPLAY_SCRIPT = PROJECT_ROOT / "scripts" / "replay_to_kafka.py"

DEFAULT_GROUP = "ecommerce-rt-flink-v1-recon"
DEFAULT_TOPIC = "ecommerce.events.v1"
DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_FLINK_URL = "http://localhost:8081"
DEFAULT_BATCH_DAYS = 7
DEFAULT_LOOKAHEAD_DAYS = 2
DEFAULT_RATE = 5000.0
DEFAULT_FINAL_TIMEOUT = 300.0
DEFAULT_SAMPLE_INTERVAL = 5.0
DEFAULT_MAX_BATCH_RETRIES = 2
WATERMARK_PUSHER_EVENT_ID_PREFIX = "wm-pusher-"

JM_CONTAINER = "ecommerce-flink-jobmanager"
TM_CONTAINER = "ecommerce-flink-taskmanager"
KAFKA_CONTAINER = "ecommerce-kafka"
JOB_JAR = "/opt/flink/jobs/ecommerce-rt-job.jar"

_MYSQL_DEFAULTS = {"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"}
_METRIC_NAME = re.compile(r"^(\d+)\.(.+)\.([^.]+)$")
_INTERESTING_METRICS = (
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    # Cumulative counters, used to derive true interval throughput (the PerSecond
    # meters are 60 s EWMAs and under-report short batches).
    "numRecordsIn",
    "numRecordsOut",
    "backPressuredTimeMsPerSecond",
    "idleTimeMsPerSecond",
    "busyTimeMsPerSecond",
    "ingest_latency_p50_ms",
    "ingest_latency_p95_ms",
    "ingest_latency_ms",
    "numRestarts",
)

LOG = logging.getLogger("phase94.replay")


# ---------------------------------------------------------------------------
# Pure planning helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchPlan:
    """One replay job: target days plus the lookahead needed to finalize them."""

    index: int
    target_dates: tuple[date, ...]
    replay_from: date
    replay_to: date
    needs_pusher: bool
    pusher_event_time: datetime | None

    @property
    def last_target(self) -> date:
        return self.target_dates[-1]


def needs_watermark_pusher(last_target: date, data_end: date, lookahead_days: int) -> bool:
    """True when the source cannot naturally reach ``last_target + 2 days``.

    A target day ``D`` becomes final at event time ``D + 2 00:00`` (window end
    ``D + 1 00:00`` + 24 h allowed lateness). If ``D + lookahead_days`` is beyond
    the last available data date, replay alone cannot push the watermark there.
    """
    return last_target + timedelta(days=lookahead_days) > data_end


def build_watermark_pusher(
    last_target: date,
    lookahead_days: int,
    *,
    now: datetime | None = None,
    visitor_id: int = 1,
    item_id: int = 1,
) -> dict[str, Any]:
    """Build the single synthetic event that pushes the watermark past the last day.

    The event is a legal ``event_v1`` message dated ``last_target + lookahead_days``
    at 01:00 UTC. It lands in a window that is never reconciled and is only used to
    advance event time; it must not be counted as a business metric.
    """
    pusher_date = last_target + timedelta(days=lookahead_days)
    event_time = datetime.combine(pusher_date, dtime(1, 0), tzinfo=UTC)
    ingest = now or datetime.now(UTC)
    row = {
        "dedup_key": f"{WATERMARK_PUSHER_EVENT_ID_PREFIX}{pusher_date.strftime('%Y%m%d')}",
        "event_time_utc": event_time.replace(tzinfo=None),
        "visitor_id": visitor_id,
        "event_type": "view",
        "item_id": item_id,
        "transaction_id": None,
        "raw_event_type": "view",
        "quality_flags": "ok",
    }
    return build_event_message(row, ingest_time=ingest)


def plan_batches(
    full_days: Sequence[date],
    *,
    batch_days: int = DEFAULT_BATCH_DAYS,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    data_end: date,
    already_final: Iterable[date] = (),
    replay_head: date | None = None,
) -> list[BatchPlan]:
    """Split pending full days into contiguous batches with forwards lookahead.

    ``already_final`` days are skipped; a gap splits the current run so a resumed
    run never replays days that are already complete. The first batch may start
    at ``replay_head`` (the partial first day, 2015-05-03) so the RT tables cover
    the complete date range even though that day is reconciled as SKIP.
    """
    if batch_days <= 0:
        raise ValueError("batch_days must be positive")
    if lookahead_days < 0:
        raise ValueError("lookahead_days must be >= 0")

    ordered = sorted(full_days)
    if not ordered:
        return []
    final_set = set(already_final)
    pending = [day for day in ordered if day not in final_set]

    runs: list[list[date]] = []
    current: list[date] = []
    previous: date | None = None
    for day in pending:
        if previous is not None and day != previous + timedelta(days=1):
            runs.append(current)
            current = []
        current.append(day)
        previous = day
    if current:
        runs.append(current)

    plans: list[BatchPlan] = []
    index = 0
    first_full_day = ordered[0]
    for run in runs:
        for start in range(0, len(run), batch_days):
            target = tuple(run[start : start + batch_days])
            replay_from = target[0]
            if (
                replay_head is not None
                and index == 0
                and target[0] == first_full_day
                and replay_head <= target[0]
            ):
                replay_from = replay_head
            replay_to = min(target[-1] + timedelta(days=lookahead_days), data_end)
            pusher = needs_watermark_pusher(target[-1], data_end, lookahead_days)
            pusher_time = None
            if pusher:
                pusher_time = datetime.combine(
                    target[-1] + timedelta(days=lookahead_days),
                    dtime(1, 0),
                    tzinfo=UTC,
                )
            plans.append(
                BatchPlan(
                    index=index,
                    target_dates=target,
                    replay_from=replay_from,
                    replay_to=replay_to,
                    needs_pusher=pusher,
                    pusher_event_time=pusher_time,
                )
            )
            index += 1
    return plans


# ---------------------------------------------------------------------------
# Pure summary helpers
# ---------------------------------------------------------------------------


VERTEX_OPERATOR = "__vertex__"


def parse_metric_name(raw: str) -> tuple[str, str, str] | None:
    """Split ``<subtask>.<operator>.<metric>``; ``None`` for job-level metrics.

    Operator names may themselves contain dots (the Kafka source is named
    ``kafka-source-ecommerce.events.v1``), so the middle group is greedy.
    Vertex-level task metrics such as ``0.backPressuredTimeMsPerSecond`` have
    only one dot and are attributed to the pseudo-operator ``__vertex__``.
    """
    match = _METRIC_NAME.match(raw)
    if match is not None:
        return match.group(1), match.group(2), match.group(3)
    vertex_match = re.match(r"^(\d+)\.([^.]+)$", raw)
    if vertex_match is not None:
        return vertex_match.group(1), VERTEX_OPERATOR, vertex_match.group(2)
    return None


def filter_operator_metrics(metrics: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Keep only the throughput/backpressure/latency metrics needed for evidence."""
    selected: list[dict[str, Any]] = []
    for name, value in metrics.items():
        if not any(name.endswith(suffix) or suffix in name for suffix in _INTERESTING_METRICS):
            continue
        parsed = parse_metric_name(name)
        if parsed is None:
            selected.append(
                {"name": name, "subtask": None, "operator": None, "metric": name, "value": value}
            )
            continue
        subtask, operator, metric = parsed
        selected.append(
            {
                "name": name,
                "subtask": subtask,
                "operator": operator,
                "metric": metric,
                "value": value,
            }
        )
    return selected


def _as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(round(quantile * len(ordered) + 0.5))))
    return ordered[rank - 1]


def _sample_timestamp(sample: Mapping[str, Any]) -> datetime | None:
    raw = sample.get("ts")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


_SUMMED_METRICS = (
    "numRecordsInPerSecond",
    "numRecordsOutPerSecond",
    "numRecordsIn",
    "numRecordsOut",
)


def summarize_samples(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate sampler snapshots into max/median per operator and latency gauge.

    Record rates (``*PerSecond``) are 60 s EWMAs and under-report short replays, so
    the cumulative ``numRecordsIn``/``numRecordsOut`` counters are also turned into
    interval throughput: ``delta(counter) / delta(sample_time)`` per operator. A
    negative delta means the job restarted and the counter reset; that interval is
    dropped rather than reported as negative throughput.
    """
    per_metric: dict[str, dict[str, list[float]]] = {}
    latency: dict[str, list[float]] = {
        "ingest_latency_ms_median": [],
        "ingest_latency_ms_p95": [],
        "ingest_latency_p50_ms": [],
        "ingest_latency_p95_ms": [],
    }
    counter_series: dict[str, dict[str, list[tuple[datetime, float]]]] = {
        "numRecordsIn": {},
        "numRecordsOut": {},
    }

    for sample in samples:
        metrics = sample.get("operator_metrics", [])
        if not isinstance(metrics, list):
            continue
        timestamp = _sample_timestamp(sample)
        # Sum records-in/out across subtasks for one operator; max the time metrics.
        sample_values: dict[tuple[str, str], list[float]] = {}
        for item in metrics:
            operator = item.get("operator")
            metric = item.get("metric")
            value = _as_float(item.get("value"))
            if operator is None or metric is None or value is None:
                continue
            sample_values.setdefault((operator, metric), []).append(value)
        for (operator, metric), values in sample_values.items():
            if metric in _SUMMED_METRICS:
                aggregate = sum(values)
            else:
                aggregate = max(values)
            per_metric.setdefault(metric, {}).setdefault(operator, []).append(aggregate)
            if metric in latency:
                latency[metric].append(aggregate)
            if metric in counter_series and timestamp is not None:
                counter_series[metric].setdefault(operator, []).append((timestamp, aggregate))

    summary: dict[str, Any] = {}
    for metric, operators in sorted(per_metric.items()):
        summary[metric] = {
            operator: {
                "max": max(values),
                "median": statistics.median(values),
                "samples": len(values),
            }
            for operator, values in sorted(operators.items())
        }
    for metric, operators in counter_series.items():
        derived: dict[str, dict[str, float | int]] = {}
        for operator, points in sorted(operators.items()):
            ordered = sorted(points, key=lambda point: point[0])
            # Flink's REST metric endpoint can repeat the previous counter value on
            # one sample and return the accumulated value on the next; differencing
            # raw points would then emit a 0 delta followed by a double-sized delta.
            # Equal readings are therefore collapsed. A smaller reading means the job
            # restarted and the counter reset: the interval is dropped and the series
            # restarts from the reset point so later intervals stay measurable.
            rates: list[float] = []
            anchor: tuple[datetime, float] | None = None
            for timestamp, count in ordered:
                if anchor is None:
                    anchor = (timestamp, count)
                    continue
                if count == anchor[1]:
                    continue
                if count < anchor[1]:
                    anchor = (timestamp, count)
                    continue
                elapsed = (timestamp - anchor[0]).total_seconds()
                if elapsed > 0:
                    rates.append((count - anchor[1]) / elapsed)
                anchor = (timestamp, count)
            if rates:
                derived[operator] = {
                    "max": max(rates),
                    "median": statistics.median(rates),
                    "samples": len(rates),
                }
        summary[f"derived_{metric}PerSecond"] = derived
    summary["latency"] = {
        metric: {
            "max": max(values) if values else None,
            "median": statistics.median(values) if values else None,
            "samples": len(values),
        }
        for metric, values in latency.items()
    }
    return summary


def summarize_checkpoints(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Extract success rate, duration and state-size statistics from the REST payload."""
    counts = payload.get("counts", {}) or {}
    history = payload.get("history", []) or []
    completed = [item for item in history if item.get("status") == "COMPLETED"]
    durations = [
        float(item["end_to_end_duration"])
        for item in completed
        if item.get("end_to_end_duration") is not None
    ]
    sizes = [int(item["state_size"]) for item in completed if item.get("state_size") is not None]
    completed_count = int(counts.get("completed", len(completed)) or 0)
    failed_count = int(counts.get("failed", 0) or 0)
    total_finished = completed_count + failed_count
    latest = (payload.get("latest", {}) or {}).get("completed") or {}
    return {
        "counts": {
            "completed": completed_count,
            "failed": failed_count,
            "restored": int(counts.get("restored", 0) or 0),
            "total": int(counts.get("total", 0) or 0),
            "in_progress": int(counts.get("in_progress", 0) or 0),
        },
        "success_rate": (completed_count / total_finished) if total_finished else None,
        "duration_ms": {
            "max": max(durations) if durations else None,
            "p95": _percentile(durations, 0.95),
            "median": statistics.median(durations) if durations else None,
            "samples": len(durations),
        },
        "state_size_bytes": {
            "max": max(sizes) if sizes else None,
            "p95": _percentile([float(value) for value in sizes], 0.95),
            "median": statistics.median(sizes) if sizes else None,
            "samples": len(sizes),
        },
        "latest_completed": {
            "id": latest.get("id"),
            "end_to_end_duration": latest.get("end_to_end_duration"),
            "state_size": latest.get("state_size"),
            "completed_at": latest.get("completed_at"),
            "external_path": (latest.get("external_path") or None),
        },
    }


def parse_replay_summary(text: str) -> dict[str, Any]:
    """Parse the deterministic summary block printed by ``replay_to_kafka.py``.

    Progress lines also contain ``queried=/sent=/acked=``; only the final
    ``== replay summary ==`` block is authoritative, so parse the suffix.
    """
    summary_text = text.rsplit("== replay summary ==", 1)[-1]
    patterns = {
        "queried": r"queried=(\d+) sent=(\d+) acked=(\d+)",
        "failures": r"failed_attempts=(\d+) retried=(\d+) spooled=(\d+)",
        "invalid_schema": r"invalid_schema=(\d+)",
        "elapsed_throughput": r"elapsed=([\d.]+)s throughput=([\d.]+) msg/s",
    }
    result: dict[str, Any] = {}
    match = re.search(patterns["queried"], summary_text)
    if match:
        result.update(
            queried=int(match.group(1)),
            sent=int(match.group(2)),
            acked=int(match.group(3)),
        )
    match = re.search(patterns["failures"], summary_text)
    if match:
        result.update(
            failed_attempts=int(match.group(1)),
            retried=int(match.group(2)),
            spooled=int(match.group(3)),
        )
    match = re.search(patterns["invalid_schema"], summary_text)
    if match:
        result["invalid_schema"] = int(match.group(1))
    match = re.search(patterns["elapsed_throughput"], summary_text)
    if match:
        result.update(
            elapsed_s=float(match.group(1)),
            throughput_msg_s=float(match.group(2)),
        )
    return result


def janitor_selection(directories: Sequence[str], keep: int = 2) -> tuple[list[str], list[str]]:
    """Return ``(keep, remove)`` for a newest-first checkpoint directory listing."""
    if keep < 0:
        raise ValueError("keep must be >= 0")
    return list(directories[:keep]), list(directories[keep:])


# ---------------------------------------------------------------------------
# Flink REST / Kafka / Docker wrappers
# ---------------------------------------------------------------------------


class FlinkRest:
    """Minimal Flink 1.20 REST client using the standard library."""

    def __init__(self, base_url: str = DEFAULT_FLINK_URL, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, path: str, method: str = "GET") -> Any:
        request = urllib.request.Request(self.base_url + path, method=method)
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            body = response.read().decode("utf-8")
        return json.loads(body) if body else None

    def jobs(self) -> list[dict[str, Any]]:
        return list((self._request("/jobs/overview") or {}).get("jobs", []))

    def job(self, job_id: str) -> dict[str, Any]:
        return self._request(f"/jobs/{job_id}")

    def job_state(self, job_id: str) -> str | None:
        try:
            detail = self.job(job_id)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return None
            raise
        state = detail.get("state")
        return str(state) if state else None

    def checkpoints(self, job_id: str) -> dict[str, Any]:
        return self._request(f"/jobs/{job_id}/checkpoints")

    def _values(self, path: str, names: Sequence[str], chunk_size: int = 40) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for start in range(0, len(names), chunk_size):
            chunk = names[start : start + chunk_size]
            query = ",".join(urllib.parse.quote(name, safe="") for name in chunk)
            values = self._request(f"{path}?get={query}") or []
            for item in values:
                if "value" in item:
                    result[item["id"]] = item["value"]
        return result

    def metrics(self, job_id: str) -> dict[str, Any]:
        """Collect job-level and per-vertex metrics (Flink only returns values on ``get=``).

        The vertex listing is queried first so only the ~50 observability metrics are
        fetched; requesting all ~650 names would exceed reasonable URL sizes.
        """
        result: dict[str, Any] = {}
        job_names = [item["id"] for item in (self._request(f"/jobs/{job_id}/metrics") or [])]
        result.update(self._values(f"/jobs/{job_id}/metrics", job_names))
        detail = self.job(job_id)
        for vertex in detail.get("vertices", []) or []:
            vertex_id = vertex.get("id")
            if not vertex_id:
                continue
            names = [
                item["id"]
                for item in (self._request(f"/jobs/{job_id}/vertices/{vertex_id}/metrics") or [])
            ]
            selected = [
                name for name in names if any(marker in name for marker in _INTERESTING_METRICS)
            ]
            result.update(self._values(f"/jobs/{job_id}/vertices/{vertex_id}/metrics", selected))
        return result

    def cancel(self, job_id: str) -> Any:
        return self._request(f"/jobs/{job_id}?mode=cancel", method="PATCH")


class KafkaLagProbe:
    """Read committed group offsets and topic high watermarks via confluent-kafka."""

    def __init__(self, bootstrap: str, group: str, topic: str) -> None:
        self.bootstrap = bootstrap
        self.group = group
        self.topic = topic
        self.admin = AdminClient({"bootstrap.servers": bootstrap})
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": f"lag-probe-{os.getpid()}",
                "enable.auto.commit": False,
            }
        )

    def close(self) -> None:
        self.consumer.close()

    def _committed(self) -> dict[int, int | None]:
        request = ConsumerGroupTopicPartitions(self.group, None)
        futures = self.admin.list_consumer_group_offsets([request])
        result: dict[int, int | None] = {}
        try:
            resolved = list(futures.values())[0].result(timeout=10)
        except Exception as exc:  # group may not exist before the first checkpoint
            LOG.debug("consumer group %s offsets unavailable: %s", self.group, exc)
            return result
        for tp in resolved.topic_partitions:
            if tp.topic == self.topic:
                result[int(tp.partition)] = (
                    None if tp.offset is None or tp.offset < 0 else int(tp.offset)
                )
        return result

    def snapshot(self) -> dict[str, Any]:
        committed = self._committed()
        metadata = self.consumer.list_topics(self.topic, timeout=10)
        partitions = metadata.topics[self.topic].partitions
        details: list[dict[str, Any]] = []
        total_lag = 0
        lag_known = True
        for partition in sorted(partitions):
            low, high = self.consumer.get_watermark_offsets(
                TopicPartition(self.topic, partition), timeout=10, cached=False
            )
            offset = committed.get(partition)
            lag = None if offset is None else max(0, high - offset)
            if lag is None:
                lag_known = False
            else:
                total_lag += lag
            details.append(
                {
                    "partition": partition,
                    "low": low,
                    "end": high,
                    "committed": offset,
                    "lag": lag,
                }
            )
        return {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "group": self.group,
            "topic": self.topic,
            "partitions": details,
            "total_lag": total_lag if lag_known else None,
        }


class DockerCli:
    """Run docker commands inside WSL on Windows, or directly elsewhere."""

    def __init__(self, distro: str = "Ubuntu24.04") -> None:
        self.distro = distro

    def _base(self) -> list[str]:
        if os.name == "nt":
            return ["wsl", "-d", self.distro, "--", "docker"]
        return ["docker"]

    def run(self, args: Sequence[str], timeout: float = 120.0) -> subprocess.CompletedProcess:
        command = self._base() + list(args)
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )

    def ensure_containers(self, timeout: float = 180.0) -> None:
        required = {JM_CONTAINER, TM_CONTAINER, KAFKA_CONTAINER}
        deadline = time.monotonic() + timeout
        last_error = ""
        while time.monotonic() < deadline:
            try:
                result = self.run(["ps", "--format", "{{.Names}}"], timeout=30)
            except (OSError, subprocess.TimeoutExpired) as exc:
                last_error = str(exc)
                time.sleep(5)
                continue
            names = set(result.stdout.split())
            missing = required - names
            if not missing:
                return
            last_error = f"missing containers: {sorted(missing)}"
            time.sleep(5)
        raise RuntimeError(f"containers not ready after {timeout:.0f}s ({last_error})")

    def exec(self, container: str, args: Sequence[str], timeout: float = 120.0):
        return self.run(["exec", container, *args], timeout=timeout)

    def checkpoint_dirs(self) -> list[str]:
        listing = (
            "find /opt/flink/checkpoints -mindepth 1 -maxdepth 1 -type d "
            "-printf '%T@ %f\\n' | sort -nr | cut -d' ' -f2-"
        )
        result = self.exec(TM_CONTAINER, ["bash", "-lc", listing], timeout=60)
        if result.returncode != 0:
            raise RuntimeError(f"could not list checkpoints: {result.stderr.strip()}")
        return [line.strip() for line in result.stdout.splitlines() if line.strip()]

    def directory_bytes(self, directory: str) -> int:
        result = self.exec(
            TM_CONTAINER,
            ["bash", "-lc", f"du -sb /opt/flink/checkpoints/{directory}"],
            timeout=60,
        )
        if result.returncode != 0:
            return 0
        return int(result.stdout.split()[0])

    def checkpoints_size(self) -> int:
        result = self.exec(
            TM_CONTAINER,
            ["bash", "-lc", "du -sb /opt/flink/checkpoints"],
            timeout=60,
        )
        if result.returncode != 0:
            return 0
        return int(result.stdout.split()[0])

    def remove_checkpoint_dir(self, directory: str) -> None:
        result = self.exec(
            TM_CONTAINER,
            ["bash", "-lc", f"rm -rf /opt/flink/checkpoints/{directory}"],
            timeout=120,
        )
        if result.returncode != 0:
            raise RuntimeError(f"could not remove {directory}: {result.stderr.strip()}")

    def run_janitor(self, keep: int = 2) -> dict[str, Any]:
        before = self.checkpoints_size()
        directories = self.checkpoint_dirs()
        kept, removed = janitor_selection(directories, keep=keep)
        removed_detail = []
        for directory in removed:
            size = self.directory_bytes(directory)
            self.remove_checkpoint_dir(directory)
            removed_detail.append({"job_id": directory, "bytes": size})
        after = self.checkpoints_size()
        return {
            "kept": kept,
            "removed": removed_detail,
            "bytes_before": before,
            "bytes_after": after,
        }


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


class BatchFailure(RuntimeError):
    """Raised when one replay batch cannot reach the final state."""


@dataclass
class RunConfig:
    evidence_dir: Path
    bootstrap: str = DEFAULT_BOOTSTRAP
    flink_url: str = DEFAULT_FLINK_URL
    group: str = DEFAULT_GROUP
    topic: str = DEFAULT_TOPIC
    rate: float = DEFAULT_RATE
    high_rate: float = 0.0
    high_rate_batch: int = 0
    batch_days: int = DEFAULT_BATCH_DAYS
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS
    final_timeout: float = DEFAULT_FINAL_TIMEOUT
    drain_timeout: float = 60.0
    sample_interval: float = DEFAULT_SAMPLE_INTERVAL
    max_batch_retries: int = DEFAULT_MAX_BATCH_RETRIES
    parallelism: int = 2
    wsl_distro: str = "Ubuntu24.04"


class Orchestrator:
    def __init__(
        self,
        config: RunConfig,
        *,
        full_days: Sequence[date],
        date_from: date,
        date_to: date,
        data_end: date,
        replay_head: date | None,
        already_final: Iterable[date],
        reset_done: bool,
        force: bool = False,
    ) -> None:
        self.config = config
        self.full_days = list(full_days)
        self.date_from = date_from
        self.date_to = date_to
        self.data_end = data_end
        self.replay_head = replay_head
        self.already_final = set(already_final)
        self.reset_done = reset_done
        self.force = force
        self.rest = FlinkRest(config.flink_url)
        self.docker = DockerCli(config.wsl_distro)
        self.batch_records: list[dict[str, Any]] = []
        self.samples_path = config.evidence_dir / "samples.jsonl"
        self.samples_path.parent.mkdir(parents=True, exist_ok=True)

    # -- database helpers -------------------------------------------------
    def _dwd_connection(self):
        cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
        return connect_mysql(cfg, read_only=True, cursorclass=pymysql.cursors.DictCursor)

    def _rt_ro_connection(self):
        user, password = rt_recon_credentials(os.environ)
        cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
        cfg = cfg.__class__(
            engine="mysql",
            host=cfg.host,
            port=cfg.port,
            user=user,
            password=password,
            database=RT_DATABASE,
        )
        return connect_mysql(cfg, read_only=True, cursorclass=pymysql.cursors.DictCursor)

    def _rt_app_connection(self):
        user, password = rt_app_credentials(os.environ)
        cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
        cfg = cfg.__class__(
            engine="mysql",
            host=cfg.host,
            port=cfg.port,
            user=user,
            password=password,
            database=RT_DATABASE,
        )
        return connect_mysql(cfg, read_only=False, cursorclass=pymysql.cursors.DictCursor)

    def reset_rt(self) -> None:
        LOG.warning("clearing ecommerce_rt metric tables and rt_reconciliation_daily")
        connection = self._rt_app_connection()
        try:
            with connection.cursor() as cursor:
                for table in (
                    "rt_late_events",
                    "rt_traffic_daily",
                    "rt_funnel_daily",
                    "rt_item_behavior_daily",
                    "rt_reconciliation_daily",
                ):
                    cursor.execute(f"DELETE FROM {table}")
            connection.commit()
        finally:
            connection.close()

    def final_days(self) -> set[date]:
        connection = self._rt_ro_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT event_date_utc FROM rt_traffic_daily "
                    "WHERE is_final = 1 AND event_date_utc BETWEEN %s AND %s",
                    (self.date_from, self.date_to),
                )
                return {row["event_date_utc"] for row in cursor.fetchall()}
        finally:
            connection.close()

    # -- job lifecycle ----------------------------------------------------
    def submit_job(self) -> str:
        args = [
            "exec",
            JM_CONTAINER,
            "flink",
            "run",
            "-d",
            JOB_JAR,
            "--group-id",
            self.config.group,
            "--starting-offsets",
            "latest",
            "--parallelism",
            str(self.config.parallelism),
        ]
        result = self.docker.run(args, timeout=180)
        output = (result.stdout or "") + (result.stderr or "")
        if result.returncode != 0:
            raise BatchFailure(f"flink run failed (rc={result.returncode}): {output.strip()}")
        match = re.search(r"JobID ([0-9a-f]{32})", output)
        if match is None:
            raise BatchFailure(f"could not parse JobID from flink run output: {output.strip()}")
        return match.group(1)

    def wait_running(self, job_id: str, timeout: float = 120.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = self.rest.job_state(job_id)
            if state == "RUNNING":
                return
            if state in ("FAILED", "CANCELED"):
                raise BatchFailure(f"job {job_id} reached {state} before replay")
            time.sleep(1)
        raise BatchFailure(f"job {job_id} did not reach RUNNING within {timeout:.0f}s")

    def wait_final(self, job_id: str, target_dates: Sequence[date]) -> dict[str, Any]:
        wanted = list(target_dates)
        deadline = time.monotonic() + self.config.final_timeout
        last_report = 0.0
        while time.monotonic() < deadline:
            state = self.rest.job_state(job_id)
            if state in ("FAILED", "CANCELED"):
                raise BatchFailure(f"job {job_id} became {state} while waiting for is_final")
            connection = self._rt_ro_connection()
            try:
                placeholders = ",".join(["%s"] * len(wanted))
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT event_date_utc FROM rt_traffic_daily "
                        f"WHERE is_final = 1 AND event_date_utc IN ({placeholders})",
                        tuple(wanted),
                    )
                    done = {row["event_date_utc"] for row in cursor.fetchall()}
            finally:
                connection.close()
            if len(done) == len(wanted):
                return {
                    "target_dates": [day.isoformat() for day in wanted],
                    "final_dates": [day.isoformat() for day in sorted(done)],
                    "waited_s": round(self.config.final_timeout - (deadline - time.monotonic()), 3),
                }
            now = time.monotonic()
            if now - last_report >= 10:
                LOG.info(
                    "waiting for is_final: %d/%d dates done, missing=%s",
                    len(done),
                    len(wanted),
                    [day.isoformat() for day in wanted if day not in done][:5],
                )
                last_report = now
            time.sleep(2)
        raise BatchFailure(
            f"target dates did not become final within {self.config.final_timeout:.0f}s: "
            f"{[day.isoformat() for day in wanted]}"
        )

    def wait_drained(self, job_id: str, timeout: float | None = None) -> dict[str, Any]:
        """Wait until the consumer group lag reaches 0 after the replays finished.

        ``is_final`` can be reached before every lookahead event is consumed (the
        finalize timer only needs event time, not the tail of the last lookahead
        day). Draining makes the latency histogram cover the whole batch and
        leaves a clean committed-offset boundary. A timeout is logged, not fatal.
        """
        budget = self.config.drain_timeout if timeout is None else timeout
        started = time.monotonic()
        deadline = started + budget
        probe = KafkaLagProbe(self.config.bootstrap, self.config.group, self.config.topic)
        last: dict[str, Any] | None = None
        try:
            while time.monotonic() < deadline:
                state = self.rest.job_state(job_id)
                if state in ("FAILED", "CANCELED"):
                    break
                last = probe.snapshot()
                if last.get("total_lag") == 0:
                    return {
                        "drained": True,
                        "waited_s": round(time.monotonic() - started, 3),
                        "lag": last,
                    }
                time.sleep(2)
        finally:
            probe.close()
        LOG.warning(
            "consumer lag did not reach 0 within %.0fs (last total_lag=%s)",
            budget,
            None if last is None else last.get("total_lag"),
        )
        return {
            "drained": False,
            "waited_s": round(time.monotonic() - started, 3),
            "lag": last,
        }

    def cancel_and_wait(self, job_id: str, timeout: float = 90.0) -> str:
        try:
            self.rest.cancel(job_id)
        except (urllib.error.URLError, urllib.error.HTTPError, OSError) as exc:
            LOG.warning("REST cancel failed for %s: %s", job_id, exc)
        deadline = time.monotonic() + timeout
        state = None
        while time.monotonic() < deadline:
            state = self.rest.job_state(job_id)
            if state in ("CANCELED", "FINISHED", "FAILED", None):
                return str(state)
            time.sleep(1)
        return str(state)

    # -- replay -----------------------------------------------------------
    def run_replay(self, plan: BatchPlan, log_path: Path, rate: float) -> dict[str, Any]:
        command = [
            sys.executable,
            str(REPLAY_SCRIPT),
            "--from",
            plan.replay_from.isoformat(),
            "--to",
            plan.replay_to.isoformat(),
            "--rate",
            str(rate),
            "--progress-interval",
            "10",
            "--bootstrap",
            self.config.bootstrap,
        ]
        log_path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        with log_path.open("w", encoding="utf-8") as handle:
            handle.write("$ " + " ".join(command) + "\n")
            handle.flush()
            result = subprocess.run(
                command,
                cwd=PROJECT_ROOT,
                stdout=handle,
                stderr=subprocess.STDOUT,
                timeout=3600,
                check=False,
            )
        text = log_path.read_text(encoding="utf-8", errors="replace")
        summary = parse_replay_summary(text)
        summary.update(
            command=" ".join(command),
            returncode=result.returncode,
            wall_seconds=round(time.monotonic() - started, 3),
            log=str(log_path),
        )
        if result.returncode != 0:
            raise BatchFailure(f"replay failed (rc={result.returncode}), see {log_path}")
        if summary.get("invalid_schema", 0) or summary.get("spooled", 0):
            raise BatchFailure(f"replay reported schema/delivery failures: {summary}")
        return summary

    def send_watermark_pusher(self, plan: BatchPlan) -> dict[str, Any]:
        assert plan.pusher_event_time is not None
        message = build_watermark_pusher(plan.last_target, self.config.lookahead_days)
        report = validate_event(message)
        if not report.valid:
            raise BatchFailure(f"synthetic watermark pusher is invalid: {report.reason}")
        producer_config = build_producer_config(self.config.bootstrap, client_id="wm-pusher")
        from confluent_kafka import Producer

        producer = Producer(producer_config)
        producer.produce(
            self.config.topic,
            key=kafka_key_for_visitor(message["visitor_id"]).encode("utf-8"),
            value=encode_event(message),
        )
        remaining = producer.flush(30)
        if remaining:
            raise BatchFailure(f"watermark pusher not delivered ({remaining} queued)")
        LOG.warning(
            "injected synthetic watermark pusher event_id=%s event_time=%s "
            "(not a business metric; window is never reconciled)",
            message["event_id"],
            message["event_time"],
        )
        return {
            "event_id": message["event_id"],
            "event_time": message["event_time"],
            "ingest_time": message["ingest_time"],
            "note": "synthetic watermark pusher; not counted in any reconciliation day",
        }

    # -- sampling ---------------------------------------------------------
    def _sample_once(self, job_id: str, probe: KafkaLagProbe, rt_connection) -> dict[str, Any]:
        sample: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
            "job_state": self.rest.job_state(job_id),
        }
        try:
            metrics = self.rest.metrics(job_id)
            sample["operator_metrics"] = filter_operator_metrics(metrics)
            sample["num_restarts"] = metrics.get("numRestarts")
        except Exception as exc:  # sampling must never break the replay
            sample["metrics_error"] = f"{type(exc).__name__}: {exc}"
        try:
            sample["checkpoints"] = summarize_checkpoints(self.rest.checkpoints(job_id))
        except Exception as exc:
            sample["checkpoints_error"] = f"{type(exc).__name__}: {exc}"
        try:
            sample["lag"] = probe.snapshot()
        except Exception as exc:
            sample["lag_error"] = f"{type(exc).__name__}: {exc}"
        try:
            with rt_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) AS days, "
                    "SUM(is_final = 1) AS final_days, MAX(updated_at) AS updated_at "
                    "FROM rt_traffic_daily"
                )
                row = cursor.fetchone()
            sample["rt_traffic"] = {
                "rows": int(row["days"] or 0),
                "final_rows": int(row["final_days"] or 0),
                "updated_at": None if row["updated_at"] is None else str(row["updated_at"]),
            }
        except Exception as exc:
            sample["rt_error"] = f"{type(exc).__name__}: {exc}"
        return sample

    def _start_sampler(self, job_id: str, store: list[dict[str, Any]], stop: threading.Event):
        def loop() -> None:
            probe = KafkaLagProbe(self.config.bootstrap, self.config.group, self.config.topic)
            rt_connection = self._rt_ro_connection()
            try:
                while not stop.wait(self.config.sample_interval):
                    sample = self._sample_once(job_id, probe, rt_connection)
                    store.append(sample)
                    with self.samples_path.open("a", encoding="utf-8") as handle:
                        handle.write(json.dumps(sample, ensure_ascii=False) + "\n")
            finally:
                probe.close()
                rt_connection.close()

        thread = threading.Thread(target=loop, name=f"sampler-{job_id}", daemon=True)
        thread.start()
        return thread

    # -- batch ------------------------------------------------------------
    def run_batch(self, plan: BatchPlan) -> dict[str, Any]:
        rate = self.config.rate
        if self.config.high_rate > 0 and plan.index + 1 == self.config.high_rate_batch:
            rate = self.config.high_rate
        record: dict[str, Any] = {
            "batch": plan.index + 1,
            "target_dates": [day.isoformat() for day in plan.target_dates],
            "replay_from": plan.replay_from.isoformat(),
            "replay_to": plan.replay_to.isoformat(),
            "lookahead_days": self.config.lookahead_days,
            "rate": rate,
            "attempts": [],
            "started_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        }
        started = time.monotonic()
        attempts = self.config.max_batch_retries + 1
        last_error = ""
        for attempt in range(1, attempts + 1):
            job_id: str | None = None
            samples: list[dict[str, Any]] = []
            stop = threading.Event()
            sampler = None
            # A high-rate stress attempt retries at the normal rate so a capacity
            # failure cannot block the full reconciliation.
            attempt_rate = rate if attempt == 1 else min(rate, self.config.rate)
            attempt_record: dict[str, Any] = {"attempt": attempt, "rate": attempt_rate}
            try:
                LOG.info(
                    "batch %d attempt %d: submitting job for %s..%s (targets %s..%s, rate=%s)",
                    plan.index + 1,
                    attempt,
                    plan.replay_from,
                    plan.replay_to,
                    plan.target_dates[0],
                    plan.target_dates[-1],
                    attempt_rate,
                )
                self.docker.ensure_containers()
                job_id = self.submit_job()
                attempt_record["job_id"] = job_id
                self.wait_running(job_id)
                time.sleep(2)
                baseline_probe = KafkaLagProbe(
                    self.config.bootstrap, self.config.group, self.config.topic
                )
                try:
                    attempt_record["lag_baseline"] = baseline_probe.snapshot()
                finally:
                    baseline_probe.close()
                sampler = self._start_sampler(job_id, samples, stop)
                replay_log = (
                    self.config.evidence_dir
                    / "batches"
                    / (f"batch_{plan.index + 1:02d}_attempt_{attempt}_replay.log")
                )
                attempt_record["replay"] = self.run_replay(plan, replay_log, attempt_rate)
                if plan.needs_pusher:
                    attempt_record["pusher"] = self.send_watermark_pusher(plan)
                attempt_record["final_wait"] = self.wait_final(job_id, plan.target_dates)
                attempt_record["drain"] = self.wait_drained(job_id)
                stop.set()
                if sampler is not None:
                    sampler.join(timeout=20)
                final_probe = KafkaLagProbe(
                    self.config.bootstrap, self.config.group, self.config.topic
                )
                final_rt = self._rt_ro_connection()
                try:
                    final_sample = self._sample_once(job_id, final_probe, final_rt)
                finally:
                    final_probe.close()
                    final_rt.close()
                samples.append(final_sample)
                with self.samples_path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(final_sample, ensure_ascii=False) + "\n")
                attempt_record["lag_end"] = final_sample.get("lag")
                attempt_record["checkpoints"] = summarize_checkpoints(self.rest.checkpoints(job_id))
                attempt_record["job_state_before_cancel"] = self.rest.job_state(job_id)
                attempt_record["job_metrics"] = filter_operator_metrics(self.rest.metrics(job_id))
                attempt_record["samples"] = len(samples)
                attempt_record["sample_summary"] = summarize_samples(samples)
                attempt_record["cancel_state"] = self.cancel_and_wait(job_id)
                janitor = self.docker.run_janitor(keep=2)
                attempt_record["janitor"] = janitor
                LOG.info(
                    "batch %d attempt %d OK job=%s checkpoints=%s/%s state_size=%s lag_end=%s "
                    "janitor_removed=%s",
                    plan.index + 1,
                    attempt,
                    job_id,
                    attempt_record["checkpoints"]["counts"]["completed"],
                    attempt_record["checkpoints"]["counts"]["failed"],
                    attempt_record["checkpoints"]["latest_completed"]["state_size"],
                    _last_lag(samples),
                    [item["job_id"] for item in janitor["removed"]],
                )
                record["attempts"].append(attempt_record)
                record["job_id"] = job_id
                record["finished_at"] = datetime.now(UTC).isoformat(timespec="milliseconds")
                record["duration_s"] = round(time.monotonic() - started, 3)
                self._write_batch_record(record)
                self.batch_records.append(record)
                return record
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                LOG.error("batch %d attempt %d failed: %s", plan.index + 1, attempt, last_error)
                stop.set()
                if sampler is not None:
                    sampler.join(timeout=20)
                attempt_record["error"] = last_error
                if samples:
                    attempt_record["samples"] = len(samples)
                    attempt_record["sample_summary"] = summarize_samples(samples)
                record["attempts"].append(attempt_record)
                if job_id is not None:
                    attempt_record["cancel_state"] = self.cancel_and_wait(job_id, timeout=30)
                if attempt < attempts:
                    LOG.warning(
                        "retrying batch %d after full replay (duplicates are deduped)",
                        plan.index + 1,
                    )
                    time.sleep(5)
        record["finished_at"] = datetime.now(UTC).isoformat(timespec="milliseconds")
        record["duration_s"] = round(time.monotonic() - started, 3)
        record["error"] = last_error
        self._write_batch_record(record)
        raise BatchFailure(f"batch {plan.index + 1} failed after {attempts} attempts: {last_error}")

    def _write_batch_record(self, record: Mapping[str, Any]) -> None:
        directory = self.config.evidence_dir / "batches"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"batch_{int(record['batch']):02d}.json"
        path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def run(self) -> int:
        self.docker.ensure_containers()
        if self.reset_done:
            self.reset_rt()
        already = set() if (self.reset_done or self.force) else self.final_days()
        plans = plan_batches(
            self.full_days,
            batch_days=self.config.batch_days,
            lookahead_days=self.config.lookahead_days,
            data_end=self.data_end,
            already_final=already,
            replay_head=self.replay_head,
        )
        LOG.info(
            "plan: full_days=%d already_final=%d batches=%d data_end=%s",
            len(self.full_days),
            len(already),
            len(plans),
            self.data_end,
        )
        for plan in plans:
            LOG.info(
                "batch %d: targets=%s..%s replay=%s..%s pusher=%s",
                plan.index + 1,
                plan.target_dates[0],
                plan.target_dates[-1],
                plan.replay_from,
                plan.replay_to,
                plan.needs_pusher,
            )
        for plan in plans:
            self.run_batch(plan)
        summary = {
            "total_batches": len(plans),
            "target_days": sum(len(plan.target_dates) for plan in plans),
            "full_days_total": len(self.full_days),
            "already_final_before_run": sorted(day.isoformat() for day in already),
            "batches": [
                {
                    "batch": record["batch"],
                    "job_id": record.get("job_id"),
                    "target_dates": record["target_dates"],
                    "replay_from": record["replay_from"],
                    "replay_to": record["replay_to"],
                    "rate": record["rate"],
                    "duration_s": record.get("duration_s"),
                    "checkpoints": (record.get("attempts") or [{}])[-1].get("checkpoints"),
                    "janitor": (record.get("attempts") or [{}])[-1].get("janitor"),
                }
                for record in self.batch_records
            ],
            "finished_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        }
        path = self.config.evidence_dir / "run_summary.json"
        path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        LOG.info("run finished: %s", path)
        return 0


def _last_lag(samples: Sequence[Mapping[str, Any]]) -> Any:
    for sample in reversed(samples):
        lag = sample.get("lag")
        if isinstance(lag, Mapping):
            return lag.get("total_lag")
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--from", dest="date_from", type=date.fromisoformat, default=None)
    parser.add_argument("--to", dest="date_to", type=date.fromisoformat, default=None)
    parser.add_argument("--batch-days", type=int, default=DEFAULT_BATCH_DAYS)
    parser.add_argument("--lookahead-days", type=int, default=DEFAULT_LOOKAHEAD_DAYS)
    parser.add_argument("--rate", type=float, default=DEFAULT_RATE)
    parser.add_argument(
        "--high-rate",
        type=float,
        default=0.0,
        help="optional higher rate for one batch (e.g. 10000)",
    )
    parser.add_argument(
        "--high-rate-batch",
        type=int,
        default=0,
        help="1-based batch index that uses --high-rate",
    )
    parser.add_argument("--group-id", default=DEFAULT_GROUP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--flink-url", default=DEFAULT_FLINK_URL)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--reset-rt", action="store_true", help="clear RT tables before replay")
    parser.add_argument("--force", action="store_true", help="replay days already marked final")
    parser.add_argument("--max-batch-retries", type=int, default=DEFAULT_MAX_BATCH_RETRIES)
    parser.add_argument("--final-timeout", type=float, default=DEFAULT_FINAL_TIMEOUT)
    parser.add_argument("--drain-timeout", type=float, default=60.0)
    parser.add_argument("--sample-interval", type=float, default=DEFAULT_SAMPLE_INTERVAL)
    parser.add_argument("--list-batches", action="store_true", help="print the plan and exit")
    parser.add_argument(
        "--reconcile", action="store_true", help="run reconcile_all --write at the end"
    )
    return parser


def _setup_logging(evidence_dir: Path) -> None:
    evidence_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)
    file_handler = logging.FileHandler(evidence_dir / "orchestrator.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)


def _load_plan(
    *,
    date_from: date | None,
    date_to: date | None,
) -> tuple[list[date], date, date, date, date | None]:
    cfg = db_config_from_env("MYSQL", engine="mysql", defaults=_MYSQL_DEFAULTS)
    connection = connect_mysql(cfg, read_only=True, cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT MIN(event_date_utc) AS mn, MAX(event_date_utc) AS mx FROM ads_traffic_daily"
            )
            bounds = cursor.fetchone()
            data_start, data_end = bounds["mn"], bounds["mx"]
            cursor.execute(
                "SELECT event_date_utc, is_full_day FROM ads_traffic_daily "
                "WHERE event_date_utc BETWEEN %s AND %s ORDER BY event_date_utc",
                (
                    date_from or data_start,
                    date_to or data_end,
                ),
            )
            rows = cursor.fetchall()
    finally:
        connection.close()
    if not rows:
        raise ValueError("ads_traffic_daily has no rows in the requested range")
    full_days = [row["event_date_utc"] for row in rows if int(row["is_full_day"]) == 1]
    actual_from = date_from or rows[0]["event_date_utc"]
    actual_to = date_to or rows[-1]["event_date_utc"]
    replay_head = data_start if actual_from <= data_start else None
    return full_days, actual_from, actual_to, data_end, replay_head


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    _setup_logging(args.evidence_dir)

    bootstrap = args.bootstrap or os.environ.get("KAFKA_BOOTSTRAP") or DEFAULT_BOOTSTRAP
    config = RunConfig(
        evidence_dir=args.evidence_dir,
        bootstrap=bootstrap,
        flink_url=args.flink_url,
        group=args.group_id,
        topic=args.topic,
        rate=args.rate,
        high_rate=args.high_rate,
        high_rate_batch=args.high_rate_batch,
        batch_days=args.batch_days,
        lookahead_days=args.lookahead_days,
        final_timeout=args.final_timeout,
        drain_timeout=args.drain_timeout,
        sample_interval=args.sample_interval,
        max_batch_retries=args.max_batch_retries,
        parallelism=args.parallelism,
        wsl_distro=os.environ.get("WSL_DISTRO", "Ubuntu24.04"),
    )
    if args.high_rate > 0 and args.high_rate_batch <= 0:
        parser.error("--high-rate requires --high-rate-batch >= 1")

    try:
        full_days, date_from, date_to, data_end, replay_head = _load_plan(
            date_from=args.date_from, date_to=args.date_to
        )
        orchestrator = Orchestrator(
            config,
            full_days=full_days,
            date_from=date_from,
            date_to=date_to,
            data_end=data_end,
            replay_head=replay_head,
            already_final=(),
            reset_done=args.reset_rt,
            force=args.force,
        )
        if args.list_batches:
            already = set() if args.reset_rt or args.force else orchestrator.final_days()
            plans = plan_batches(
                full_days,
                batch_days=args.batch_days,
                lookahead_days=args.lookahead_days,
                data_end=data_end,
                already_final=already,
                replay_head=replay_head,
            )
            for plan in plans:
                print(
                    f"batch {plan.index + 1:02d} targets={plan.target_dates[0]}.."
                    f"{plan.target_dates[-1]} ({len(plan.target_dates)}d) "
                    f"replay={plan.replay_from}..{plan.replay_to} pusher={plan.needs_pusher}"
                )
            print(
                f"TOTAL_BATCHES={len(plans)} TARGET_DAYS={sum(len(p.target_dates) for p in plans)}"
            )
            return 0
        code = orchestrator.run()
        if args.reconcile:
            from ecommerce_dw.streaming import reconcile_all

            reconcile_args = [
                "--write",
                "--from",
                date_from.isoformat(),
                "--to",
                date_to.isoformat(),
                "--json-out",
                str(args.evidence_dir / "reconciliation.json"),
            ]
            code = reconcile_all.main(reconcile_args) or code
        return code
    except Exception as exc:
        LOG.error("orchestration failed: %s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
