"""Integration tests for Phase 5: quality gate + watermark incremental processing.

Covers, end to end against the dedicated ``*_test`` schema (never production):

- a hard gate **passes** on a clean DWD+DWS+ADS state, persists run + per-rule
  results, and a publish is approved;
- injecting one bad ODS row makes the gate **FAIL** (completeness/validity/
  batch-coverage), the FAILed run is recorded, and ``publish_ads_scope`` refuses
  both with a stale-FAIL run id and with a fresh gate - ``ads_publish_log`` is
  untouched;
- after deleting the row the gate passes again and publishing is approved;
- the watermark-run is a **no-op** while the state is stable (idempotency);
- a late-days batch (a second ODS file for an already-built date) is detected
  inside the lookback window and recomputes D1's DWS/ADS, advances the watermark,
  and a second incremental run is again a no-op.
"""

from __future__ import annotations

import csv
import uuid
from datetime import date
from pathlib import Path

import pytest

from ecommerce_dw.dwd import builder as dwd_builder
from ecommerce_dw.dwd.transform import date_ms_range
from ecommerce_dw.dws import builder as agg_builder
from ecommerce_dw.incremental import init_watermark, run_incremental
from ecommerce_dw.incremental.watermark import DEFAULT_PIPELINE, WatermarkRepo
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.keys import make_row_key
from ecommerce_dw.quality import (
    QualityGateError,
    build_rule_catalog,
    publish_ads_scope,
    run_quality_gate,
)
from ecommerce_dw.quality.runner import connect as q_connect
from ecommerce_dw.quality.runner import list_run_results, list_runs

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

D1 = date(2099, 1, 1)
D2 = date(2099, 1, 2)
D3 = date(2099, 1, 3)
D4 = date(2099, 1, 4)
_DAYS = (D1, D2, D3, D4)
_WIN = (D1, D3)  # test-only full window: D4 stays partial
# Gate catalog with the synthetic window (production keeps the real 2015 window).
_CATALOG = build_rule_catalog(valid_window=(D1, D4))


def _clean_sample_rows() -> list[list[str]]:
    """Clean events only (no reject rows): dedup + sentinel + cross-day + funnel."""
    s1, _ = date_ms_range(D1)
    s2, _ = date_ms_range(D2)
    s3, _ = date_ms_range(D3)
    s4, _ = date_ms_range(D4)
    return [
        # D1: 4 viewers (6 views), 3 cart/buyer actors
        [str(s1 + 1000), "1", "view", "1001", ""],
        [str(s1 + 1001), "1", "view", "1001", ""],
        [str(s1 + 1002), "2", "view", "1002", ""],
        [str(s1 + 1003), "2", "view", "1002", ""],
        [str(s1 + 1004), "3", "view", "3003", ""],
        [str(s1 + 1005), "3", "addtocart", "3003", ""],
        [str(s1 + 1006), "3", "transaction", "3003", "70001"],
        [str(s1 + 1007), "3", "transaction", "3003", "70002"],
        [str(s1 + 1008), "5", "addtocart", "5050", ""],
        [str(s1 + 1009), "9", "view", "9090", ""],
        [str(s1 + 1010), "9", "transaction", "4004", "70003"],
        # exact duplicate - goes to dwd_events_duplicate, never aggregated
        [str(s1 + 1000), "1", "view", "1001", ""],
        # D2: cross-day return + sentinel (PV only) + cart-only visitor
        [str(s2 + 1000), "3", "view", "3003", ""],
        [str(s2 + 1001), "0", "view", "11111", ""],
        [str(s2 + 1002), "5", "addtocart", "5050", ""],
        # D3: zero-view day (NULL funnel rates)
        [str(s3 + 1000), "7", "addtocart", "5050", ""],
        [str(s3 + 1001), "11", "addtocart", "1010", ""],
        # D4: partial-day transactions past the window end
        [str(s4 + 1000), "7", "transaction", "5051", "70031"],
        [str(s4 + 1001), "12", "transaction", "5051", "70041"],
    ]


def _late_sample_rows() -> list[list[str]]:
    """Late second batch: v3's *second* D1 view of item 3003."""
    s1, _ = date_ms_range(D1)
    return [[str(s1 + 3000), "3", "view", "3003", ""]]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def _stage_events(cfg, tmp_path: Path, rows: list[list[str]]) -> str:
    source_file = f"events_ph5_{uuid.uuid4().hex[:10]}.csv"
    src = _write_csv(tmp_path / source_file, _EVENTS_HEADER, rows)
    return loader.load_csv(cfg, src, _EVENTS_TABLE).source_file


def _scalar(cfg, sql: str, params: tuple = ()):
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _traffic(cfg) -> dict[date, tuple]:
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT event_date_utc, pv, uv, dau FROM ads_traffic_daily ORDER BY event_date_utc"
        )
        return {d: (pv, uv, dau) for d, pv, uv, dau in cur.fetchall()}
    finally:
        conn.close()


def _inject_bad_row(cfg, source_file: str) -> None:
    """One orphan ODS row (unknown event type + batch never recorded), and the
    file's covered batch now under-counts: ODS 20 rows vs batch_log 19.
    """
    s1, _ = date_ms_range(D1)
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO ods_events_raw (row_key, `timestamp`, visitorid, event,"
            " itemid, transactionid, source_file, source_row_number, ingest_batch_id)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                make_row_key(source_file, 999_999),
                str(s1 + 4000),
                "4",
                "purchase",
                "9999",
                "",
                source_file,
                999_999,
                424_242,
            ),
        )
        # stale the recorded count of the already-LOADED batch so the
        # ods_batch_log_rowcount completeness rule fires too.
        cur.execute(
            "UPDATE etl_batch_log SET row_count = 0 WHERE source_file = %s AND status = 'LOADED'",
            (source_file,),
        )
        conn.commit()
    finally:
        conn.close()


def _delete_bad_row(cfg, source_file: str) -> None:
    conn = q_connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM ods_events_raw WHERE source_file = %s AND source_row_number = %s",
            (source_file, 999_999),
        )
        # re-sync each LOADED batch count to its actual ODS row count
        cur.execute(
            "UPDATE etl_batch_log b"
            " SET b.row_count = ("
            "   SELECT COUNT(*) FROM ods_events_raw o"
            "   WHERE o.source_file = b.source_file AND o.ingest_batch_id = b.batch_id)"
            " WHERE b.source_file = %s AND b.status = 'LOADED'",
            (source_file,),
        )
        conn.commit()
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
    """Remove everything this module writes (staged files, builds, control rows)."""
    events_files: list[str] = []
    dwd_builds: list[int] = []
    agg_builds: list[int] = []

    def _track(events: str | None = None, dwd_id: int | None = None, agg_id: int | None = None):
        if events:
            events_files.append(events)
        if dwd_id is not None:
            dwd_builds.append(dwd_id)
        if agg_id is not None:
            agg_builds.append(agg_id)

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
        for bid in agg_builds:
            cur.execute("DELETE FROM dws_ads_build_log WHERE build_id = %s", (bid,))
        for table in _AGG_TABLES:
            cur.execute(f"DELETE FROM `{table}`")
        for sql in (
            "DELETE FROM etl_watermark",
            "DELETE FROM quality_check_result",
            "DELETE FROM quality_check_run",
            "DELETE FROM ads_publish_log",
        ):
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def test_quality_gate_and_incremental_end_to_end(cfg, tmp_path, tracked):
    # ---------------- clean DWD+DWS+ADS state ------------------------------- #
    file1 = _stage_events(cfg, tmp_path, _clean_sample_rows())
    tracked(events=file1)
    b1 = dwd_builder.build_dwd(cfg, dates=list(_DAYS), source_file=file1)
    tracked(dwd_id=b1.build_id)
    assert (b1.ods_count, b1.ok_rows, b1.duplicate_rows, b1.reject_rows) == (19, 18, 1, 0)

    res = agg_builder.build_dws_ads(
        cfg, dates=list(_DAYS), window_start=D1, window_end=D3, top_n=100
    )
    tracked(agg_id=res.build_id)
    assert res.status == "LOADED"
    assert _traffic(cfg) == {
        D1: (6, 4, 5),
        D2: (2, 1, 2),
        D3: (0, 0, 2),
        D4: (0, 0, 2),
    }

    # ---------------- bootstrap watermark; stable state is a no-op ---------- #
    init_watermark(cfg, pipeline=DEFAULT_PIPELINE, to_date=D4, window_start=D1, window_end=D3)
    r_noop = run_incremental(
        cfg, pipeline=DEFAULT_PIPELINE, window_start=D1, window_end=D3, top_n=100
    )
    assert r_noop.dates_built == ()
    assert r_noop.watermark_upserted == 0

    # ---------------- gate PASS + persistence + publish --------------------- #
    report = run_quality_gate(cfg, catalog=_CATALOG)
    assert report.ok
    assert report.failed == 0
    assert report.rules_total == len(_CATALOG) == 60
    runs = list_runs(cfg)
    assert runs and runs[0].run_id == report.run_id and runs[0].status == "PASS"
    results = list_run_results(cfg, report.run_id)
    assert len(results) == len(_CATALOG)
    assert all(r[2] == "PASS" for r in results)

    # date-scoped gate also passes on the rebuilt day subset
    scoped = run_quality_gate(cfg, dates=(D2, D3), catalog=_CATALOG)
    assert scoped.ok

    publish_id = publish_ads_scope(cfg, quality_run_id=report.run_id)
    assert publish_id >= 1
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 1

    # ---------------- inject one bad row -> gate FAIL, publish blocked ------ #
    _inject_bad_row(cfg, file1)
    with pytest.raises(QualityGateError) as exc:
        run_quality_gate(cfg, catalog=_CATALOG)
    msg = str(exc.value)
    assert "FAIL" in msg and "rules FAILED" in msg
    for key in (
        "ods_batch_log_rowcount",
        "ods_batch_coverage",
        "ods_event_type_valid",
        "dwd_identity",
    ):
        assert key in msg

    fail_run_id = _scalar(cfg, "SELECT MAX(run_id) FROM quality_check_run WHERE status = 'FAIL'")
    assert fail_run_id is not None
    fail_results = list_run_results(cfg, int(fail_run_id))
    assert any(r[2] == "FAIL" for r in fail_results)

    # publish refused: stale-FAIL run id AND fresh gate over the bad data
    with pytest.raises(QualityGateError):
        publish_ads_scope(cfg, quality_run_id=int(fail_run_id))
    with pytest.raises(QualityGateError):
        publish_ads_scope(cfg, catalog=_CATALOG)
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 1  # untouched

    # ---------------- fix the row -> gate PASS, publish allowed ------------ #
    _delete_bad_row(cfg, file1)
    report2 = run_quality_gate(cfg, catalog=_CATALOG)
    assert report2.ok
    publish_ads_scope(cfg, quality_run_id=report2.run_id)
    assert _scalar(cfg, "SELECT COUNT(*) FROM ads_publish_log") == 2

    # ---------------- late batch: D1 rebuilt through the incremental path --- #
    assert _traffic(cfg)[D1] == (6, 4, 5)
    batch_before = _scalar(cfg, "SELECT MAX(ingest_batch_id) FROM ods_events_raw")
    file2 = _stage_events(cfg, tmp_path, _late_sample_rows())
    tracked(events=file2)
    batch_after = _scalar(cfg, "SELECT MAX(ingest_batch_id) FROM ods_events_raw")
    assert batch_after > batch_before

    r_late = run_incremental(
        cfg, pipeline=DEFAULT_PIPELINE, window_start=D1, window_end=D3, top_n=100
    )
    assert r_late.dates_built == (D1,)
    assert r_late.late_dates == (D1,)
    assert r_late.watermark_upserted == 1
    assert _traffic(cfg)[D1] == (7, 4, 5)  # D1 pv picked up the late view
    assert _traffic(cfg)[D2] == (2, 1, 2)  # untouched days unchanged

    # watermark advanced to the second batch for D1
    conn = q_connect(cfg)
    try:
        wm = WatermarkRepo(conn).load(DEFAULT_PIPELINE)
    finally:
        conn.close()
    assert wm[D1] == int(batch_after)
    assert wm[D4] == int(batch_before)

    # stable again -> another incremental is a no-op; gate still passes
    r_stable = run_incremental(
        cfg, pipeline=DEFAULT_PIPELINE, window_start=D1, window_end=D3, top_n=100
    )
    assert r_stable.dates_built == ()
    final = run_quality_gate(cfg, catalog=_CATALOG)
    assert final.ok
