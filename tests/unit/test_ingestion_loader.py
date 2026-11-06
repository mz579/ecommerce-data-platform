"""Unit tests for ODS loader helpers (insert SQL and resume-skip math)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from ecommerce_dw.ingestion.chunk import CsvChunkReader
from ecommerce_dw.ingestion.keys import make_row_key
from ecommerce_dw.ingestion.loader import (
    ColumnTooLongError,
    _params_for,
    _suffix,
    _validate_cell_lengths,
    build_insert_sql,
)
from ecommerce_dw.ingestion.specs import TABLE_SPECS

_AT = datetime(2026, 11, 6, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)
_EVENTS = TABLE_SPECS["ods_events_raw"]
_IP = TABLE_SPECS["ods_item_properties_raw"]


def test_build_insert_sql_has_metadata_columns():
    sql = build_insert_sql(_EVENTS)
    assert sql.startswith("INSERT IGNORE INTO `ods_events_raw`")
    for col in (
        "row_key",
        "`timestamp`",
        "visitorid",
        "event",
        "itemid",
        "transactionid",
        "source_file",
        "source_row_number",
        "ingest_batch_id",
        "loaded_at",
    ):
        assert col in sql


def test_build_insert_sql_placeholders_match_columns():
    sql = build_insert_sql(_IP)
    n_cols = 4 + 5  # 4 raw columns + row_key and the 4 metadata columns
    assert sql.count("%s") == n_cols


def test_suffix_handles_full_partial_and_surplus_skips():
    rows = (("a",), ("b",), ("c",))
    # Partial skip -> drops the leading rows and shifts the base row number.
    base, rest = _suffix(rows, 1, 10)
    assert base == 11
    assert rest == (("b",), ("c",))
    # Whole-chunk skip -> empty remainder.
    base, rest = _suffix(rows, 3, 10)
    assert base == 13
    assert rest == ()
    # Surplus skip (more skipped than the chunk holds) is capped at the chunk size.
    base, rest = _suffix(rows, 7, 10)
    assert base == 13
    assert rest == ()


def test_validate_cell_lengths_accepts_within_limit_rows_and_locates_overlong_cell():
    limits = {
        "timestamp": 64,
        "visitorid": 64,
        "event": 32,
        "itemid": 64,
        "transactionid": 64,
    }
    # Exact-width values are fine, and so is a value that just fits the limit.
    ok = (("1", "2", "v" * 32, "3", ""), ("4", "5", "x" * 31, "6", "7"))
    _validate_cell_lengths(ok, 2, _EVENTS, limits)  # must not raise

    # The 300-char `event` sits in the second row => source_row_number = 2 + 1.
    bad = (("1", "2", "view", "3", ""), ("9", "9", "v" * 300, "9", ""))
    with pytest.raises(ColumnTooLongError, match=r"row 3, column 'event'.*300 > .*32 characters"):
        _validate_cell_lengths(bad, 2, _EVENTS, limits)


def test_params_for_assigns_deterministic_row_keys_and_numbers():
    rows = (("1", "2", "view", "3", ""), ("4", "5", "addtocart", "6", ""))
    params = _params_for(rows, 10, _EVENTS, "events.csv", 42, _AT)
    assert len(params) == 2
    first, second = params
    # Layout: row_key, *5 raw columns, source_file, source_row_number, batch_id, loaded_at
    assert first[1] == "1"  # raw `timestamp` column
    assert first[-4] == "events.csv"
    assert first[-3] == 10
    assert first[-2] == 42
    assert first[-1] == _AT
    assert first[0] == make_row_key("events.csv", 10)
    assert second[-3] == 11
    assert second[0] == make_row_key("events.csv", 11)


def test_resume_rebuilds_params_from_committed_row_count(tmp_path):
    # 15 data rows of the 4-column item_properties shape => lines 2..16.
    csv_text = "timestamp,itemid,property,value\n" + "".join(
        f"{i},item{i},prop{i},value{i}\n" for i in range(15)
    )
    path = tmp_path / "ip.csv"
    path.write_text(csv_text, encoding="utf-8")

    inserted = 6  # source_row_number 2..7 already committed before the restart
    params = []
    reader = CsvChunkReader(path, chunk_size=8, expected_header=_IP.raw_columns)
    skip_rows = inserted
    for chunk in reader.iter_chunks():
        base, rows = _suffix(chunk.rows, skip_rows, chunk.start_row_number)
        skip_rows -= base - chunk.start_row_number
        params.extend(_params_for(rows, base, _IP, "ip.csv", 7, _AT))

    assert [p[-3] for p in params] == [8, 9, 10, 11, 12, 13, 14, 15, 16]
    assert len(params) == 9  # exactly the rows that were not yet committed
    assert params[0][0] == make_row_key("ip.csv", 8)
    assert params[-1][0] == make_row_key("ip.csv", 16)
