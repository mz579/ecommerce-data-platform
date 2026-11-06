"""Integration tests for the DWD build against a real MySQL (Phase 3).

The tests stage a small synthetic events CSV into ``ods_events_raw`` via the
Phase-2 loader, run ``build_dwd`` for test-only UTC dates (2099), and verify:

- the identity ``ODS = dwd_events + dwd_events_duplicate + dwd_events_reject``;
- the event mapping (``addtocart`` -> ``add_to_cart`` with raw preserved);
- the duplicate rows land in the duplicate table joined by ``dedup_key``;
- reject rows are locatable per ``source_row_number`` with their reason;
- re-running the same build is idempotent.

Everything the tests create is removed afterwards (ODS rows + ODS batch log +
DWD rows + DWD build log), so a passing run leaves the business tables clean.
Tests are skipped when MySQL is unreachable or the DWD DDL is not applied.
"""

from __future__ import annotations

import csv
import uuid
from datetime import date
from pathlib import Path

import pymysql
import pytest
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.dwd import builder
from ecommerce_dw.dwd.build_log import BuildLogRepo
from ecommerce_dw.dwd.builder import BuildResult
from ecommerce_dw.dwd.transform import date_ms_range
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_EVENTS_TABLE = "ods_events_raw"
_DWD_TABLES = (
    "dwd_events",
    "dwd_events_duplicate",
    "dwd_events_reject",
    "dwd_events_stage",
)

TEST_DAY = date(2099, 1, 1)
_DAY_START, _DAY_END = date_ms_range(TEST_DAY)

_EVENTS_HEADER = ["timestamp", "visitorid", "event", "itemid", "transactionid"]


def _build_cfg():
    load_dotenv(PROJECT_ROOT / ".env")
    return db_config_from_env(
        "MYSQL",
        engine="mysql",
        defaults={"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"},
    )


def _write_events_csv(path: Path, rows) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(_EVENTS_HEADER)
        writer.writerows(rows)
    return path


def _unique_name() -> str:
    return f"events_dwd_it_{uuid.uuid4().hex[:10]}.csv"


def _sample_rows() -> list[list[str]]:
    """15 rows: 4 winners + 2 exact duplicates + 9 rejects (all on TEST_DAY)."""
    t0 = _DAY_START
    return [
        [str(t0 + 1000), "111", "view", "1001", ""],  # A view
        [str(t0 + 2000), "222", "addtocart", "1002", ""],  # B addtocart -> add_to_cart
        [str(t0 + 3000), "333", "transaction", "1003", "90001"],  # C transaction
        [str(t0 + 1000), "111", "view", "1001", ""],  # A2 exact duplicate of A
        [str(t0 + 3000), "333", "transaction", "1003", "90001"],  # C2 exact duplicate of C
        [str(t0 + 4000), "0", "view", "1004", ""],  # D sentinel visitor
        [str(t0 + 5000), "444", "purchase", "1005", ""],  # E unknown event type
        [str(t0 + 6000), "555", "view", "", ""],  # F missing item
        ["oops", "666", "view", "1006", ""],  # G invalid timestamp
        [str(t0 + 7000), "777", "transaction", "1007", ""],  # H txn w/o txnid
        [str(t0 + 8000), "888", "view", "1008", "99"],  # I non-txn with txnid
        [str(t0 + 9000), "999", "view", "", ""],  # J missing item
        [str(t0 + 10000), "", "view", "1010", ""],  # K missing visitor
        [str(t0 + 11000), "abc", "view", "1011", ""],  # L invalid visitor
        [str(t0 + 12000), "123", "view", "abc", ""],  # M invalid item
    ]


@pytest.fixture(scope="module")
def cfg():
    config = _build_cfg()
    probe = None
    try:
        probe = pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.database,
            charset="utf8mb4",
            connect_timeout=3,
        )
        with probe.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s "
                "AND table_name IN ('dwd_events','dwd_events_duplicate',"
                "'dwd_events_reject','dwd_events_stage','dwd_build_log')",
                (config.database,),
            )
            dwd_tables = int(cur.fetchone()[0])
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(
            f"MySQL unavailable ({type(exc).__name__}: {exc}); skipping DWD integration tests"
        )
        return None
    finally:
        if probe is not None:
            probe.close()
    if dwd_tables < 5:
        pytest.skip("DWD DDL not applied; run `python scripts/apply_dwd_ddl.py` first")
    return config


@pytest.fixture
def tracked(cfg):
    """Track test rows/builds and wipe everything after each test.

    The source file is registered *before* a build runs, so ODS/DWD rows are
    removed even when ``build_dwd`` fails mid-run. Any untracked build-log
    residue for the module's fixed future-date scopes is swept too, so a failing
    test never blocks the next run with a stale FAILED build.
    """
    created_files: list[str] = []
    created_builds: list[int] = []

    def _track(source_file: str, build_ids: list[int] | None = None) -> None:
        created_files.append(source_file)
        if build_ids:
            created_builds.extend(build_ids)

    yield _track
    if cfg is None:
        return
    conn = builder.connect(cfg)
    try:
        cur = conn.cursor()
        for source_file in created_files:
            cur.execute("DELETE FROM ods_events_raw WHERE source_file = %s", (source_file,))
            BatchLogRepo(conn).delete_for_file(source_file)
            for table in _DWD_TABLES:
                cur.execute(f"DELETE FROM `{table}` WHERE source_file = %s", (source_file,))
        for build_id in created_builds:
            cur.execute("DELETE FROM dwd_build_log WHERE build_id = %s", (build_id,))
        # A failed build is never registered by the test; sweep the module's
        # fixed future-date scopes (2099 dates are test-only by construction).
        for dates_csv in ("2099-01-01", "2099-06-01"):
            cur.execute(
                "DELETE FROM dwd_build_log WHERE scope_type = 'DATES' AND dates_csv = %s",
                (dates_csv,),
            )
        conn.commit()
    finally:
        conn.close()


def _count(cfg, table: str, source_file: str) -> int:
    conn = builder.connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE source_file = %s", (source_file,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _select_all(cfg, table: str, source_file: str) -> list[tuple]:
    conn = builder.connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT * FROM `{table}` WHERE source_file = %s ORDER BY source_row_number",
            (source_file,),
        )
        return list(cur.fetchall())
    finally:
        conn.close()


def _stable(row: tuple, drop: set[int]) -> tuple:
    """Strip run-specific columns so different builds compare equal."""
    return tuple(v for i, v in enumerate(row) if i not in drop)


# Per-table volatile indices: surrogate key, build_id, loaded_at differ per run.
_EVENTS_VOLATILE = frozenset({0, 11, 15})  # event_sk / build_id / loaded_at
_DUP_VOLATILE = frozenset({15, 19})  # build_id / loaded_at (TABLE column order)
_REJ_VOLATILE = frozenset({11, 15})  # build_id / loaded_at


def _stage_into_ods(cfg, tmp_path: Path) -> str:
    """Load the synthetic rows into ODS via the Phase-2 loader; return filename."""
    source_file = _unique_name()
    src = _write_events_csv(tmp_path / source_file, _sample_rows())
    result = loader.load_csv(cfg, src, _EVENTS_TABLE)
    assert result.rows_committed == 15
    return source_file


def test_dwd_build_maps_dedups_isolates_and_is_idempotent(cfg, tmp_path, tracked):
    source_file = _stage_into_ods(cfg, tmp_path)
    tracked(source_file)  # register before building so a failure still cleans up

    first: BuildResult = builder.build_dwd(
        cfg, dates=[TEST_DAY], source_file=source_file, chunk_size=5
    )
    tracked(source_file, [first.build_id])
    assert first.status == "LOADED"
    assert first.ods_count == 15
    assert first.ok_rows == 4
    assert first.duplicate_rows == 2
    assert first.reject_rows == 9
    # Core identity: ODS = wins + dups + rejects.
    assert first.ods_count == first.ok_rows + first.duplicate_rows + first.reject_rows
    assert _count(cfg, "dwd_events", source_file) == 4
    assert _count(cfg, "dwd_events_duplicate", source_file) == 2
    assert _count(cfg, "dwd_events_reject", source_file) == 9

    # ---- canonical detail rows (dwd_events: 0 event_sk 1 row_key 2 dedup_key
    # 3 visitor 4 item 5 raw_event 6 event_type 7 time 8 date 9 txn 10 flags
    # 11 build 12 ingest 13 src 14 row# 15 loaded) ----
    dwd = _select_all(cfg, "dwd_events", source_file)
    add_to_cart = [r for r in dwd if r[6] == "add_to_cart"]
    assert len(add_to_cart) == 1
    assert add_to_cart[0][5] == "addtocart"  # raw preserved
    txn_rows = [r for r in dwd if r[6] == "transaction"]
    assert len(txn_rows) == 1
    assert txn_rows[0][9] == 90001
    # sentinel visitor "0" -> NULL + flag, row kept
    zero_rows = [r for r in dwd if r[10] == "visitor_zero"]
    assert len(zero_rows) == 1
    assert zero_rows[0][3] is None
    # all rows are on the requested UTC day
    assert {r[8] for r in dwd} == {TEST_DAY}

    # ---- duplicates carry the original fields and the same dedup_key ----
    # (dwd_events_duplicate TABLE order, i.e. SELECT *: 0 row_key 1 dedup_key
    # 2 raw_ts 3 raw_visitorid 4 raw_event 5 raw_itemid 6 raw_txnid
    # 7 visitor_id 8 item_id 9 raw_event_type 10 event_type 11 event_time
    # 12 event_date 13 transaction_id 14 quality_flags 15 build_id
    # 16 ingest_batch_id 17 source_file 18 source_row_number 19 loaded_at)
    dups = _select_all(cfg, "dwd_events_duplicate", source_file)
    assert len(dups) == 2
    dup_keys = {r[1] for r in dups}  # dedup_key
    dwd_keys = {r[2] for r in dwd}
    assert dup_keys <= dwd_keys  # duplicate keys exist among the winners' keys
    assert {r[3] for r in dups} == {"111", "333"}  # raw_visitorid strings kept

    # ---- rejects are locatable by source_row_number with a reason ----
    # (dwd_events_reject: 0 row_key 1 raw_ts ... 6 event_type 9 reject_reason
    # 10 reject_message 14 source_row_number)
    rej = _select_all(cfg, "dwd_events_reject", source_file)
    reasons = {r[9]: r[14] for r in rej}  # reason -> source_row_number
    assert len(reasons) == 8  # 9 rows, 'missing_item' occurs twice
    # source rows: header=1, so data rows are 2..16.
    assert reasons["unknown_event_type"] == 8
    assert reasons["missing_item"] in (9, 13)
    assert reasons["invalid_timestamp"] == 10
    assert reasons["missing_txn_on_transaction"] == 11
    assert reasons["unexpected_txnid"] == 12
    assert reasons["missing_visitor"] == 14
    assert reasons["invalid_visitor_id"] == 15
    assert reasons["invalid_item_id"] == 16

    # ---- re-running the same scope reproduces identical content ----
    second: BuildResult = builder.build_dwd(
        cfg, dates=[TEST_DAY], source_file=source_file, chunk_size=5
    )
    tracked(source_file, [second.build_id])
    assert second.ods_count == first.ods_count
    assert second.ok_rows == first.ok_rows
    assert second.duplicate_rows == first.duplicate_rows
    assert second.reject_rows == first.reject_rows
    dwd2 = _select_all(cfg, "dwd_events", source_file)
    assert [_stable(r, _EVENTS_VOLATILE) for r in dwd2] == [
        _stable(r, _EVENTS_VOLATILE) for r in dwd
    ]
    dups2 = _select_all(cfg, "dwd_events_duplicate", source_file)
    assert [_stable(r, _DUP_VOLATILE) for r in dups2] == [_stable(r, _DUP_VOLATILE) for r in dups]
    rej2 = _select_all(cfg, "dwd_events_reject", source_file)
    assert [_stable(r, _REJ_VOLATILE) for r in rej2] == [_stable(r, _REJ_VOLATILE) for r in rej]


def test_verify_build_reports_identity_and_cleanup_removes_rows(cfg, tmp_path, tracked):
    source_file = _stage_into_ods(cfg, tmp_path)
    tracked(source_file)
    result = builder.build_dwd(cfg, dates=[TEST_DAY], source_file=source_file)
    tracked(source_file, [result.build_id])

    counts = builder.verify_build(cfg, dates=[TEST_DAY], source_file=source_file)
    assert counts["ods"] == 15
    assert counts["ok"] + counts["duplicate"] + counts["reject"] == 15
    assert counts["ok"] == result.ok_rows

    deleted = builder.cleanup_build(cfg, result.build_id)
    assert deleted == 30  # events(4) + duplicates(2) + rejects(9) + stage(15)
    conn = builder.connect(cfg)
    try:
        entry = BuildLogRepo(conn).find_by_id(result.build_id)[0]
        assert entry.status == "CLEANED"
    finally:
        conn.close()
    # row-level cleanup happens via the tracked fixture afterwards.


def test_build_over_disjoint_date_scope_yields_only_date_less_rows(cfg, tmp_path, tracked):
    """Rows dated outside a requested UTC day are out of scope — except the
    date-less invalid-timestamp row, which every scope must still reject."""
    source_file = _stage_into_ods(cfg, tmp_path)
    tracked(source_file)
    disjoint_day = date(2099, 6, 1)
    build = builder.build_dwd(cfg, dates=[disjoint_day], source_file=source_file)
    tracked(source_file, [build.build_id])
    # 14 Jan-dated rows are out of scope; the 'oops' row has no UTC day, so it
    # is picked up by the scope and rejected (never silently dropped).
    assert build.ods_count == 1
    assert build.ok_rows == 0 and build.duplicate_rows == 0 and build.reject_rows == 1
    assert build.status == "LOADED"
    rej = _select_all(cfg, "dwd_events_reject", source_file)
    assert len(rej) == 1
    assert rej[0][9] == "invalid_timestamp"
    assert rej[0][8] is None  # event_date_utc is NULL for a date-less row
