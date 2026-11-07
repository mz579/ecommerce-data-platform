"""Integration tests for Phase 6 Airflow orchestration helpers (ops.control).

Runs end to end against the dedicated ``*_test`` schema (never production):

- ``plan_and_register`` FORCE plans the window and registers a RUNNING control
  batch (``airflow_pipeline_run``), scoring the same dates the Phase-5 planner
  would;
- re-registering the **same** ``dag_run_id`` while RUNNING reuses the same
  ``batch_id`` (DAG retry / re-run idempotency), and after ``finish_run`` a new
  run id is allocated;
- ``finish_run`` closes the batch with SUCCESS/FAILED and records quality run +
  publish ids;
- ``publish_after_gate`` refuses a non-PASS gate, skips (and closes SUCCESS)
  on an empty rebuild scope, and publishes + closes SUCCESS on a PASS gate.
"""

from __future__ import annotations

import csv
import uuid
from datetime import date
from pathlib import Path

import pytest

from ecommerce_dw.config import DbConfig
from ecommerce_dw.dwd import builder as dwd_builder
from ecommerce_dw.dwd.transform import date_ms_range
from ecommerce_dw.incremental import init_watermark
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ops.control import finish_run, plan_and_register, publish_after_gate
from ecommerce_dw.quality import QualityGateError
from ecommerce_dw.quality.runner import connect as q_connect

_EVENTS_HEADER = ["timestamp", "visitorid", "event", "itemid", "transactionid"]
_EVENTS_TABLE = "ods_events_raw"
_DWD_TABLES = (
    "dwd_events",
    "dwd_events_duplicate",
    "dwd_events_reject",
    "dwd_events_stage",
)
_AGG_TABLES = (
    "dws_user_daily",
    "dws_item_daily",
    "dws_category_daily",
    "ads_traffic_daily",
    "ads_funnel_daily",
    "ads_item_ranking_daily",
    "ads_retention_cohort",
)

D1 = date(2099, 2, 1)
D2 = date(2099, 2, 2)
D3 = date(2099, 2, 3)
D4 = date(2099, 2, 4)
_DAYS = (D1, D2, D3, D4)


def _clean_sample_rows() -> list[list[str]]:
    s1, _ = date_ms_range(D1)
    s2, _ = date_ms_range(D2)
    s3, _ = date_ms_range(D3)
    s4, _ = date_ms_range(D4)
    return [
        [str(s1 + 1000), "1", "view", "1001", ""],  # D1
        [str(s1 + 1001), "1", "view", "1001", ""],
        [str(s2 + 1000), "2", "view", "1002", ""],  # D2
        [str(s2 + 1001), "2", "view", "1002", ""],
        [str(s3 + 1000), "3", "view", "3003", ""],  # D3
        [str(s4 + 1000), "3", "view", "3003", ""],  # D4
    ]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def _stage_events(cfg, tmp_path: Path, rows: list[list[str]]) -> str:
    source_file = f"events_ph6_{uuid.uuid4().hex[:10]}.csv"
    src = _write_csv(tmp_path / source_file, _EVENTS_HEADER, rows)
    return loader.load_csv(cfg, src, _EVENTS_TABLE).source_file


def _scalar(cfg: DbConfig, sql: str, params: tuple = ()):
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _control_row(cfg: DbConfig, run_id: int) -> tuple | None:
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT pipeline, dag_run_id, mode, window_start, window_end, dwd_dates,"
            " agg_dates, status, published, publish_id, quality_run_id, error_summary"
            " FROM airflow_pipeline_run WHERE run_id = %s",
            (run_id,),
        )
        return cur.fetchone()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def cfg(mysql_testdb):
    if not mysql_testdb.database.endswith("_test"):
        raise RuntimeError(
            f"integration tests refuse to run against a non-test schema: {mysql_testdb.database!r}"
        )
    return mysql_testdb


@pytest.fixture
def tracked(cfg):
    events_files: list[str] = []
    dwd_builds: list[int] = []

    def _track(events: str | None = None, dwd_id: int | None = None):
        if events:
            events_files.append(events)
        if dwd_id is not None:
            dwd_builds.append(dwd_id)

    yield _track

    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        for src in events_files:
            cur.execute("DELETE FROM ods_events_raw WHERE source_file = %s", (src,))
            BatchLogRepo(conn).delete_for_file(src)
            for table in _DWD_TABLES:
                cur.execute(f"DELETE FROM `{table}` WHERE source_file = %s", (src,))
        for bid in dwd_builds:
            cur.execute("DELETE FROM dwd_build_log WHERE build_id = %s", (bid,))
        for table in _AGG_TABLES:
            cur.execute(f"DELETE FROM `{table}`")
        for sql in (
            "DELETE FROM airflow_pipeline_run",  # Phase-6 control table
            "DELETE FROM etl_watermark",
            "DELETE FROM quality_check_result",
            "DELETE FROM quality_check_run",
            "DELETE FROM ads_publish_log",
        ):
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def test_plan_register_finish_lifecycle(cfg, tmp_path, tracked):
    file1 = _stage_events(cfg, tmp_path, _clean_sample_rows())
    tracked(events=file1)
    b1 = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=file1)
    tracked(dwd_id=b1.build_id)
    assert b1.status == "LOADED"

    # ---- FORCE plan over D1..D2: both dates have ODS rows -> both planned ---- #
    res = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-force",
    )
    assert res["batch_id"] >= 1
    assert res["dwd_dates"] == "2099-02-01,2099-02-02"
    assert res["mode"] == "FORCE"
    # agg scope = dwd ∪ predecessors present in dwd_events (D1 has none)
    assert res["agg_dates"] == "2099-02-01,2099-02-02"

    row = _control_row(cfg, res["batch_id"])
    assert row is not None
    assert row[7] == "RUNNING"  # status
    assert str(row[3]) == "2099-02-01" and str(row[4]) == "2099-02-02"

    # ---- idempotent re-register while RUNNING reuses the same batch_id ---- #
    res_idem = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-force",
    )
    assert res_idem["batch_id"] == res["batch_id"]

    # ---- finish closes the batch and persists quality/publish ids ---- #
    finish_run(
        cfg,
        res["batch_id"],
        "SUCCESS",
        published="PUBLISHED",
        publish_id=7,
        quality_run_id=9,
    )
    row = _control_row(cfg, res["batch_id"])
    assert row[7] == "SUCCESS"
    assert row[8] == "PUBLISHED" and int(row[9]) == 7 and int(row[10]) == 9

    # ---- after finish, the same dag_run_id registers a NEW batch ---- #
    res_new = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-force",
    )
    assert res_new["batch_id"] != res["batch_id"]

    # ---- failed finish records the error summary ---- #
    finish_run(cfg, res_new["batch_id"], "FAILED", error_summary="dag run failed")
    row = _control_row(cfg, res_new["batch_id"])
    assert row[7] == "FAILED" and row[11] == "dag run failed"


def test_plan_tail_stable_state_is_noop(cfg, tmp_path, tracked):
    file1 = _stage_events(cfg, tmp_path, _clean_sample_rows())
    tracked(events=file1)
    b1 = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=file1)
    tracked(dwd_id=b1.build_id)

    # bootstrap watermark for ALL built dates -> TAIL over D1..D3 is a no-op
    init_watermark(cfg, pipeline=DEFAULT_PIPELINE, to_date=D4)
    res = plan_and_register(
        cfg,
        mode="TAIL",
        window="2099-02-01,2099-02-03",
        dag_run_id="itest-tail-stable",
    )
    assert res["dwd_dates"] == ""  # nothing new inside the window
    assert res["agg_dates"] == ""
    assert res["dwd_count"] == 0

    # FORCE over the same window still plans the rebuild (deep-correction path)
    res_f = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-03",
        dag_run_id="itest-force-stable",
    )
    assert res_f["dwd_dates"] == "2099-02-01,2099-02-02,2099-02-03"
    # agg predecessor for D2/D3 = D1/D2 already in dwd_events
    assert res_f["agg_dates"] == "2099-02-01,2099-02-02,2099-02-03"


def test_publish_after_gate_guards(cfg, tmp_path, tracked):
    file1 = _stage_events(cfg, tmp_path, _clean_sample_rows())
    tracked(events=file1)
    b1 = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=file1)
    tracked(dwd_id=b1.build_id)

    # ---- non-PASS gate: publish refused, batch stays RUNNING ---- #
    batch = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-gate-fail",
    )["batch_id"]
    with pytest.raises(QualityGateError):
        publish_after_gate(
            cfg,
            batch_id=batch,
            window="2099-02-01,2099-02-02",
            dwd_dates="2099-02-01,2099-02-02",
            gate_run_id=1,
            gate_status="FAIL",
        )
    assert _control_row(cfg, batch)[7] == "RUNNING"
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 0

    # ---- empty rebuild scope: publish skipped, batch closed SUCCESS ------- #
    # make the state stable first (watermark consumed) so TAIL plans no dates
    init_watermark(cfg, pipeline=DEFAULT_PIPELINE, to_date=D4)
    batch_noop = plan_and_register(
        cfg,
        mode="TAIL",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-gate-noop",
    )["batch_id"]
    assert batch_noop
    out = publish_after_gate(
        cfg,
        batch_id=batch_noop,
        window="2099-02-01,2099-02-02",
        dwd_dates="",  # stable state -> nothing rebuilt
        gate_run_id=None,
        gate_status="PASS",
    )
    assert out == {"published": False, "publish_id": None, "reason": "nothing-rebuilt"}
    row = _control_row(cfg, batch_noop)
    assert row[7] == "SUCCESS" and row[8] == "SKIPPED"
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 0

    # ---- PASS gate: publish approved, one ads_publish_log row, SUCCESS ----- #
    # fabricate a PASS quality run the publish guard accepts
    qconn = q_connect(cfg)
    try:
        cur = qconn.cursor()
        cur.execute(
            "INSERT INTO quality_check_run"
            " (scope_type, dates_csv, window_start, window_end, status, rules_total,"
            "  rules_passed, rules_failed, checks_total, started_at, finished_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                "DATES",
                "2099-02-01,2099-02-02",
                D1,
                D2,
                "PASS",
                60,
                60,
                0,
                0,
                "2099-02-05 00:00:00",
                "2099-02-05 00:00:01",
            ),
        )
        qconn.commit()
        gate_run_id = int(cur.lastrowid)
    finally:
        qconn.close()

    batch_ok = plan_and_register(
        cfg,
        mode="FORCE",
        window="2099-02-01,2099-02-02",
        dag_run_id="itest-gate-pass",
    )["batch_id"]
    out = publish_after_gate(
        cfg,
        batch_id=batch_ok,
        window="2099-02-01,2099-02-02",
        dwd_dates="2099-02-01,2099-02-02",
        gate_run_id=gate_run_id,
        gate_status="PASS",
    )
    assert out["published"] is True and out["publish_id"] >= 1
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 1
    assert _control_row(cfg, batch_ok)[7] == "SUCCESS"
    assert int(_control_row(cfg, batch_ok)[9]) == out["publish_id"]
