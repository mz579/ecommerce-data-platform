"""Unit tests for the Phase 9.4 batch planning, summaries and monitoring helpers."""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from ecommerce_dw.streaming import monitoring
from ecommerce_dw.streaming.batch_replay import (
    BatchPlan,
    build_watermark_pusher,
    filter_operator_metrics,
    janitor_selection,
    needs_watermark_pusher,
    parse_metric_name,
    parse_replay_summary,
    plan_batches,
    summarize_checkpoints,
    summarize_samples,
)
from ecommerce_dw.streaming.events import validate_event


def _days(start: date, count: int) -> list[date]:
    return [start + timedelta(days=offset) for offset in range(count)]


def test_plan_batches_contiguous_with_lookahead_and_last_pusher():
    days = _days(date(2015, 5, 4), 10)
    plans = plan_batches(
        days,
        batch_days=4,
        lookahead_days=2,
        data_end=date(2015, 5, 15),
    )
    assert [len(plan.target_dates) for plan in plans] == [4, 4, 2]
    assert plans[0].replay_from == date(2015, 5, 4)
    assert plans[0].replay_to == date(2015, 5, 9)
    assert plans[1].replay_to == date(2015, 5, 13)
    assert plans[2].replay_to == date(2015, 5, 15)
    assert all(not plan.needs_pusher for plan in plans)


def test_plan_batches_marks_pusher_when_lookahead_exceeds_data_end():
    days = _days(date(2015, 9, 11), 7)
    plans = plan_batches(
        days,
        batch_days=7,
        lookahead_days=2,
        data_end=date(2015, 9, 18),
    )
    assert len(plans) == 1
    plan = plans[0]
    assert plan.target_dates[-1] == date(2015, 9, 17)
    assert plan.replay_to == date(2015, 9, 18)
    assert plan.needs_pusher
    assert plan.pusher_event_time is not None
    assert plan.pusher_event_time.date() == date(2015, 9, 19)
    assert plan.pusher_event_time.hour == 1


def test_plan_batches_resume_skips_final_days_and_splits_gaps():
    days = _days(date(2015, 5, 4), 10)
    final = {date(2015, 5, 6), date(2015, 5, 7)}
    plans = plan_batches(
        days,
        batch_days=3,
        lookahead_days=2,
        data_end=date(2015, 5, 15),
        already_final=final,
    )
    targets = [day for plan in plans for day in plan.target_dates]
    assert targets == [day for day in days if day not in final]
    # The gap caused by already-final days splits the runs, so no batch spans it.
    assert [len(plan.target_dates) for plan in plans] == [2, 3, 3]
    assert plans[1].target_dates[0] == date(2015, 5, 8)


def test_plan_batches_replay_head_only_applies_to_first_full_day():
    days = _days(date(2015, 5, 4), 5)
    plans = plan_batches(
        days,
        batch_days=2,
        lookahead_days=1,
        data_end=date(2015, 5, 10),
        replay_head=date(2015, 5, 3),
    )
    assert plans[0].replay_from == date(2015, 5, 3)
    assert all(plan.replay_from == plan.target_dates[0] for plan in plans[1:])


def test_plan_batches_validation():
    with pytest.raises(ValueError):
        plan_batches([date(2015, 5, 4)], batch_days=0, data_end=date(2015, 5, 5))
    with pytest.raises(ValueError):
        plan_batches([date(2015, 5, 4)], lookahead_days=-1, data_end=date(2015, 5, 5))
    assert plan_batches([], data_end=date(2015, 5, 5)) == []


def test_needs_watermark_pusher_boundary():
    assert not needs_watermark_pusher(date(2015, 9, 16), date(2015, 9, 18), 2)
    assert needs_watermark_pusher(date(2015, 9, 17), date(2015, 9, 18), 2)


def test_watermark_pusher_is_a_valid_event_v1_message():
    message = build_watermark_pusher(date(2015, 9, 17), 2)
    report = validate_event(message)
    assert report.valid, report.reason
    assert message["event_id"] == "wm-pusher-20150919"
    assert message["event_time"] == "2015-09-19T01:00:00.000Z"
    assert message["event_type"] == "view"
    assert message["transaction_id"] is None
    assert message["ingest_time"].endswith("Z")


def test_watermark_pusher_event_id_is_stable_and_unique_per_date():
    first = build_watermark_pusher(date(2015, 9, 17), 2)
    second = build_watermark_pusher(date(2015, 9, 17), 2)
    other = build_watermark_pusher(date(2015, 9, 16), 2)
    assert first["event_id"] == second["event_id"] == "wm-pusher-20150919"
    assert other["event_id"] == "wm-pusher-20150918"


def test_parse_replay_summary_reads_all_counters():
    text = """
[progress] queried=50 sent=50 acked=49 failed=0 invalid=0 rate=5000.0 msg/s
== replay summary ==
queried=100 sent=100 acked=100
failed_attempts=0 retried=0 spooled=0
invalid_schema=0
elapsed=2.50s throughput=40.0 msg/s
"""
    summary = parse_replay_summary(text)
    assert summary == {
        "queried": 100,
        "sent": 100,
        "acked": 100,
        "failed_attempts": 0,
        "retried": 0,
        "spooled": 0,
        "invalid_schema": 0,
        "elapsed_s": 2.5,
        "throughput_msg_s": 40.0,
    }


def test_summarize_checkpoints_success_duration_and_state_size():
    payload = {
        "counts": {"completed": 3, "failed": 1, "restored": 0, "total": 5, "in_progress": 1},
        "latest": {
            "completed": {
                "id": 3,
                "end_to_end_duration": 200,
                "state_size": 50,
                "completed_at": "2026-11-07T00:00:00Z",
                "external_path": "/opt/flink/checkpoints/x/chk-3",
            }
        },
        "history": [
            {"status": "COMPLETED", "end_to_end_duration": 100, "state_size": 10},
            {"status": "COMPLETED", "end_to_end_duration": 200, "state_size": 20},
            {"status": "COMPLETED", "end_to_end_duration": 300, "state_size": 30},
            {"status": "FAILED"},
        ],
    }
    summary = summarize_checkpoints(payload)
    assert summary["counts"]["completed"] == 3
    assert summary["counts"]["failed"] == 1
    assert summary["success_rate"] == pytest.approx(0.75)
    assert summary["duration_ms"]["max"] == 300
    assert summary["duration_ms"]["samples"] == 3
    assert summary["state_size_bytes"]["max"] == 30
    assert summary["latest_completed"]["id"] == 3


def test_parse_metric_name_handles_source_operator_dots():
    parsed = parse_metric_name("0.kafka-source-ecommerce.events.v1.numRecordsInPerSecond")
    assert parsed == (
        "0",
        "kafka-source-ecommerce.events.v1",
        "numRecordsInPerSecond",
    )
    assert parse_metric_name("0.backPressuredTimeMsPerSecond") == (
        "0",
        "__vertex__",
        "backPressuredTimeMsPerSecond",
    )
    assert parse_metric_name("numRestarts") is None


def test_filter_operator_metrics_keeps_only_observability_metrics():
    metrics = {
        "0.parse-validate-event-v1.valid_records": 10,
        "0.parse-validate-event-v1.ingest_latency_p95_ms": 12.5,
        "1.daily-window-aggregate.backPressuredTimeMsPerSecond": 0.0,
        "numRestarts": 0,
        "ignored_metric": 99,
    }
    selected = filter_operator_metrics(metrics)
    names = {item["name"] for item in selected}
    assert "0.parse-validate-event-v1.valid_records" not in names
    assert "ignored_metric" not in names
    assert "0.parse-validate-event-v1.ingest_latency_p95_ms" in names
    assert "1.daily-window-aggregate.backPressuredTimeMsPerSecond" in names
    assert "numRestarts" in names


def test_summarize_samples_derives_interval_throughput_from_counters():
    def sample(ts: str, counts: list[str]) -> dict:
        return {
            "ts": ts,
            "operator_metrics": [
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "numRecordsIn",
                    "value": value,
                }
                for value in counts
            ],
        }

    samples = [
        sample("2026-11-07T00:00:00.000+00:00", ["700", "300"]),
        sample("2026-11-07T00:00:05.000+00:00", ["1500", "1000"]),
        # Job restarted: the counter reset, so the interval is discarded.
        sample("2026-11-07T00:00:10.000+00:00", ["10"]),
        sample("2026-11-07T00:00:15.000+00:00", ["760"]),
    ]
    summary = summarize_samples(samples)
    # Subtasks are summed per sample (1000, 2500, 10, 760). The reset interval is
    # dropped, leaving 1500/5 s = 300 msg/s and 750/5 s = 150 msg/s.
    derived = summary["derived_numRecordsInPerSecond"]["parse-validate-event-v1"]
    assert derived == {"max": 300, "median": 225.0, "samples": 2}
    assert summary["derived_numRecordsOutPerSecond"] == {}


def test_summarize_samples_ignores_stale_repeated_counter_readings():
    # Flink REST sometimes repeats the previous counter value on one scrape; the
    # next scrape then carries two intervals' worth of records. Naive differencing
    # would report 0 then double the real rate.
    def sample(ts: str, count: str) -> dict:
        return {
            "ts": ts,
            "operator_metrics": [
                {
                    "operator": "Source__kafka-source-ecommerce_events_v1",
                    "metric": "numRecordsIn",
                    "value": count,
                }
            ],
        }

    samples = [
        sample("2026-11-07T00:00:00.000+00:00", "0"),
        sample("2026-11-07T00:00:05.000+00:00", "25000"),
        sample("2026-11-07T00:00:10.000+00:00", "25000"),
        sample("2026-11-07T00:00:15.000+00:00", "50000"),
    ]
    derived = summarize_samples(samples)["derived_numRecordsInPerSecond"][
        "Source__kafka-source-ecommerce_events_v1"
    ]
    # 25000/5 s and 25000/10 s (stale scrape collapsed into one interval).
    assert derived["max"] == 5000
    assert derived["median"] == 3750.0
    assert derived["samples"] == 2


def test_summarize_samples_sums_records_and_maxes_backpressure():
    samples = [
        {
            "operator_metrics": [
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "numRecordsInPerSecond",
                    "value": "100",
                },
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "numRecordsInPerSecond",
                    "value": "50",
                },
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "backPressuredTimeMsPerSecond",
                    "value": "10",
                },
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "ingest_latency_p95_ms",
                    "value": "20",
                },
            ]
        },
        {
            "operator_metrics": [
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "numRecordsInPerSecond",
                    "value": "200",
                },
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "backPressuredTimeMsPerSecond",
                    "value": "30",
                },
                {
                    "operator": "parse-validate-event-v1",
                    "metric": "ingest_latency_p95_ms",
                    "value": "40",
                },
            ]
        },
    ]
    summary = summarize_samples(samples)
    throughput = summary["numRecordsInPerSecond"]["parse-validate-event-v1"]
    # Subtasks are summed within a sample (150 then 200), then max/median across samples.
    assert throughput["max"] == 200
    assert throughput["median"] == 175
    backpressure = summary["backPressuredTimeMsPerSecond"]["parse-validate-event-v1"]
    assert backpressure["max"] == 30
    assert summary["latency"]["ingest_latency_p95_ms"]["max"] == 40
    assert summary["latency"]["ingest_latency_p95_ms"]["median"] == 30


def test_janitor_selection_keeps_newest_two():
    kept, removed = janitor_selection(["c", "b", "a"], keep=2)
    assert kept == ["c", "b"]
    assert removed == ["a"]
    assert janitor_selection([], keep=2) == ([], [])
    with pytest.raises(ValueError):
        janitor_selection(["a"], keep=-1)


def test_batch_plan_properties():
    plan = BatchPlan(
        index=0,
        target_dates=(date(2015, 9, 17),),
        replay_from=date(2015, 9, 17),
        replay_to=date(2015, 9, 18),
        needs_pusher=True,
        pusher_event_time=None,
    )
    assert plan.last_target == date(2015, 9, 17)


def test_select_job_prefers_running_then_newest():
    jobs = [
        {"jid": "old", "state": "CANCELED", "start-time": 1},
        {"jid": "new", "state": "CANCELED", "start-time": 3},
        {"jid": "running", "state": "RUNNING", "start-time": 2},
    ]
    assert monitoring.select_job(jobs)["jid"] == "running"
    assert monitoring.select_job([jobs[0], jobs[1]])["jid"] == "new"
    assert monitoring.select_job(jobs, "old")["jid"] == "old"
    assert monitoring.select_job([], "missing") is None


def test_production_baseline_covers_the_13_tables():
    assert len(monitoring.PRODUCTION_BASELINE) == 13
    assert monitoring.PRODUCTION_BASELINE["dwd_events"] == 2_755_641
    assert monitoring.PRODUCTION_BASELINE["ads_traffic_daily"] == 139
