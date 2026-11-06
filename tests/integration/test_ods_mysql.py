"""Integration tests for ODS ingestion against a real MySQL (Phase 2).

These tests run the full load pipeline — batch registration, chunked inserts,
skip-on-reload, force reload, failure -> cleanup -> resume — against the real
``ecommerce_dw`` database and then remove every row and log entry they created.

They are skipped (not failed) when MySQL is unreachable or the ODS DDL has not
been applied, so a plain ``pytest`` also stays green on a machine without the
database (see ``docs/ods_design.md``).
"""

from __future__ import annotations

import csv
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pymysql
import pytest
from dotenv import load_dotenv

from ecommerce_dw.config import db_config_from_env
from ecommerce_dw.ingestion import loader
from ecommerce_dw.ingestion.batch_log import BatchLogRepo
from ecommerce_dw.ingestion.db import connect
from ecommerce_dw.ingestion.keys import make_row_key, sha256_file

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Tests only ever touch rows whose source_file matches these names.
_EVENTS_TABLE = "ods_events_raw"
_IP_TABLE = "ods_item_properties_raw"
_CT_TABLE = "ods_category_tree_raw"


def _build_cfg():
    load_dotenv(PROJECT_ROOT / ".env")
    return db_config_from_env(
        "MYSQL",
        engine="mysql",
        defaults={"PORT": "3306", "USER": "ecom_app", "DATABASE": "ecommerce_dw"},
    )


def _write_csv(path: Path, header, rows) -> Path:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)
    return path


def _unique_name(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:10]}.csv"


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
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name IN "
                "('etl_batch_log','ods_events_raw','ods_item_properties_raw','ods_category_tree_raw')",
                (config.database,),
            )
            tables = int(cur.fetchone()[0])
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(
            f"MySQL unavailable ({type(exc).__name__}: {exc}); skipping ODS integration tests"
        )
        return None
    finally:
        if probe is not None:
            probe.close()
    if tables < 4:
        pytest.skip("ODS DDL not applied; run `python scripts/apply_ods_ddl.py` first")
    return config


@pytest.fixture
def tracked(cfg):
    """Track (source_file, table) pairs and wipe them after each test."""
    created: set[tuple[str, str]] = set()

    def _track(source_file: str, table_name: str) -> None:
        created.add((source_file, table_name))

    yield _track
    if cfg is None:
        return
    conn = connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        cur = conn.cursor()
        for source_file, table_name in created:
            cur.execute(f"DELETE FROM `{table_name}` WHERE source_file = %s", (source_file,))
            repo.delete_for_file(source_file)
        conn.commit()
    finally:
        conn.close()


def _count(cfg, table: str, source_file: str) -> int:
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE source_file = %s", (source_file,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _count_by_batch(cfg, table: str, batch_id: int) -> int:
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM `{table}` WHERE ingest_batch_id = %s", (batch_id,))
        return int(cur.fetchone()[0])
    finally:
        conn.close()


def _find(cfg, source_file):
    conn = connect(cfg)
    try:
        return BatchLogRepo(conn).find_for_file(source_file)
    finally:
        conn.close()


def test_load_events_then_reload_is_skipped_and_count_unchanged(cfg, tmp_path, tracked):
    src = _write_csv(
        tmp_path / _unique_name("ods_it_events_"),
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [
            ["1433221332117", "257597", "view", "355908", ""],
            ["1433224214164", "992329", "view", "248676", ""],
            ["1433221999827", "0", "addtocart", "318965", ""],
            ["1433221955914", "483717", "transaction", "253185", "42"],
        ],
    )
    tracked(src.name, _EVENTS_TABLE)

    first = loader.load_csv(cfg, src, _EVENTS_TABLE)
    assert first.status == "LOADED" and not first.skipped
    assert first.rows_committed == 4
    assert _count(cfg, _EVENTS_TABLE, src.name) == 4

    # Re-importing the identical file must be skipped, not duplicated.
    second = loader.load_csv(cfg, src, _EVENTS_TABLE)
    assert second.skipped and second.status == "SKIPPED"
    assert _count(cfg, _EVENTS_TABLE, src.name) == 4

    entries = _find(cfg, src.name)
    assert [e.status for e in entries] == ["LOADED"]  # skips are not logged


def test_force_reload_keeps_same_row_count_and_marks_previous_batch_cleaned(cfg, tmp_path, tracked):
    src = _write_csv(
        tmp_path / _unique_name("ods_it_force_"),
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [
            ["1433221332117", "1", "view", "10", ""],
            ["1433221332118", "2", "view", "20", ""],
            ["1433221332119", "3", "transaction", "30", "9"],
        ],
    )
    tracked(src.name, _EVENTS_TABLE)

    loader.load_csv(cfg, src, _EVENTS_TABLE)
    reloaded = loader.load_csv(cfg, src, _EVENTS_TABLE, force=True)
    assert not reloaded.skipped
    assert reloaded.rows_committed == 3
    assert _count(cfg, _EVENTS_TABLE, src.name) == 3

    statuses = sorted(e.status for e in _find(cfg, src.name))
    assert statuses == ["CLEANED", "LOADED"]


def test_failed_batch_can_be_cleaned_and_rerun(cfg, tmp_path, tracked):
    name = _unique_name("ods_it_fail_")
    src = tmp_path / name
    _write_csv(
        src,
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [
            ["1433221332117", "1", "view", "10", ""],
            ["1433221332118", "2", "view", "20", ""],
            # third row is much too wide -> MalformedCsvRowError inside chunk 2
            ["1433221332119", "3", "view", "30", "", "extra", "fields"],
        ],
    )
    tracked(src.name, _EVENTS_TABLE)

    # chunk 1 (first two rows) commits; chunk 2 aborts -> batch FAILED.
    with pytest.raises(loader.LoadError, match="load of"):
        loader.load_csv(cfg, src, _EVENTS_TABLE, chunk_size=2)
    failed = [e for e in _find(cfg, src.name) if e.status == "FAILED"]
    assert len(failed) == 1
    assert failed[0].row_count == 2
    assert failed[0].error_summary  # diagnostic text recorded
    assert _count_by_batch(cfg, _EVENTS_TABLE, failed[0].batch_id) == 2

    # Re-loading while the FAILED batch exists is guarded.
    with pytest.raises(loader.BatchStateError, match="failed/changed batches"):
        loader.load_csv(cfg, src, _EVENTS_TABLE, chunk_size=2)

    # Cleaning the batch removes its partial rows.
    deleted = loader.cleanup_batch(cfg, failed[0].batch_id)
    assert deleted == 2
    assert _count_by_batch(cfg, _EVENTS_TABLE, failed[0].batch_id) == 0
    assert [e.status for e in _find(cfg, src.name)] == ["CLEANED"]

    # Fixing the file (same name, new sha256) loads cleanly.
    fixed = _write_csv(
        src,
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [
            ["1433221332117", "1", "view", "10", ""],
            ["1433221332118", "2", "transaction", "20", "7"],
        ],
    )
    rerun = loader.load_csv(cfg, fixed, _EVENTS_TABLE)
    assert not rerun.skipped
    assert rerun.rows_committed == 2
    assert _count(cfg, _EVENTS_TABLE, src.name) == 2


def test_load_item_properties_and_category_tree_roundtrip(cfg, tmp_path, tracked):
    ip_name = _unique_name("ods_it_ip_")
    ip = _write_csv(
        tmp_path / ip_name,
        ["timestamp", "itemid", "property", "value"],
        [
            ["1435460400000", "460429", "categoryid", "1338"],
            ["1441508400000", "206783", "888", "1116713 960601 n277.200"],
            ["1439089200000", "395014", "400", ""],
        ],
    )
    tracked(ip_name, _IP_TABLE)
    first = loader.load_csv(cfg, ip, _IP_TABLE)
    assert first.rows_committed == 3
    assert _count(cfg, _IP_TABLE, ip_name) == 3
    skipped = loader.load_csv(cfg, ip, _IP_TABLE)
    assert skipped.skipped

    ct_name = _unique_name("ods_it_ct_")
    ct = _write_csv(
        tmp_path / ct_name,
        ["categoryid", "parentid"],
        [["1016", "213"], ["809", ""], ["570", "9"]],
    )
    tracked(ct_name, _CT_TABLE)
    ok = loader.load_csv(cfg, ct, _CT_TABLE)
    assert ok.rows_committed == 3
    assert _count(cfg, _CT_TABLE, ct_name) == 3
    # An empty parentid is preserved as an empty string (string fidelity).
    # `809,` is the third CSV line => source_row_number = 3 (header is line 1).
    conn = connect(cfg)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT `parentid` FROM ods_category_tree_raw "
            "WHERE source_file = %s AND source_row_number = %s",
            (ct_name, 3),
        )
        assert cur.fetchone()[0] == ""
    finally:
        conn.close()


def test_pending_batch_is_resumed_in_place_after_interrupted_run(cfg, tmp_path, tracked):
    """A crashed run's PENDING batch continues on the next run (no data loss).

    Simulates a mid-load process kill: one chunk committed, then the process
    died before the batch could be marked LOADED. Re-running the loader must
    resume the same batch_id (counting the already-committed rows) and finish
    the file, rather than registering a fresh batch or duplicating rows.
    """
    ip_name = _unique_name("ods_it_resume_")
    src = _write_csv(
        tmp_path / ip_name,
        ["timestamp", "itemid", "property", "value"],
        [
            ["1435460400000", "1", "categoryid", "10"],
            ["1435460400001", "2", "categoryid", "20"],
            ["1435460400002", "3", "categoryid", "30"],
        ],
    )
    tracked(ip_name, _IP_TABLE)

    # Reproduce the DB state left behind by the killed run: a PENDING batch row
    # plus a committed chunk (here: source_row_numbers 2 only).
    conn = connect(cfg)
    try:
        repo = BatchLogRepo(conn)
        started = datetime.now(UTC).replace(tzinfo=None)
        batch_id = repo.register(ip_name, sha256_file(src), _IP_TABLE, started)
        cur = conn.cursor()
        cur.execute(
            "INSERT IGNORE INTO ods_item_properties_raw "
            "(row_key, `timestamp`, itemid, `property`, `value`, "
            " source_file, source_row_number, ingest_batch_id, loaded_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                make_row_key(ip_name, 2),
                "1435460400000",
                "1",
                "categoryid",
                "10",
                ip_name,
                2,
                batch_id,
                started,
            ),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    assert _count(cfg, _IP_TABLE, ip_name) == 1
    assert _find(cfg, ip_name)[0].status == "PENDING"

    # The next run continues the same batch from row 3.
    result = loader.load_csv(cfg, src, _IP_TABLE)
    assert not result.skipped
    assert result.batch_id == batch_id  # same batch, not a fresh registration
    assert result.rows_committed == 3
    assert _count(cfg, _IP_TABLE, ip_name) == 3

    loaded = [e for e in _find(cfg, ip_name) if e.status == "LOADED"]
    assert len(loaded) == 1  # no duplicate log row
    assert loaded[0].batch_id == batch_id
    assert loaded[0].row_count == 3
