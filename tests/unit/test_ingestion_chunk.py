"""Unit tests for the chunked CSV reader used by ODS ingestion."""

from __future__ import annotations

import pytest

from ecommerce_dw.ingestion.chunk import CsvChunkReader, MalformedCsvRowError, read_csv_header

EVENTS_HEADER = ("timestamp", "visitorid", "event", "itemid", "transactionid")


def _write_csv(tmp_path, name, header, rows):
    p = tmp_path / name
    p.write_text(
        "\n".join([",".join(header), *[",".join(r) for r in rows]]) + "\n", encoding="utf-8"
    )
    return p


def test_chunk_start_positions_follow_physical_line_numbers(tmp_path):
    p = _write_csv(
        tmp_path,
        "events.csv",
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [
            ["1", "2", "view", "3", ""],
            ["4", "5", "view", "6", ""],
            ["7", "8", "view", "9", ""],
            ["10", "11", "view", "12", ""],
            ["13", "14", "view", "15", ""],
        ],
    )
    reader = CsvChunkReader(p, chunk_size=2)
    chunks = list(reader.iter_chunks())
    assert [c.start_row_number for c in chunks] == [2, 4, 6]
    assert [len(c.rows) for c in chunks] == [2, 2, 1]
    assert reader.header == EVENTS_HEADER
    assert reader.n_data_rows == 5
    assert reader.n_malformed_rows == 0
    # First data row is physical line 2 (header is line 1).
    assert chunks[0].rows[0] == ("1", "2", "view", "3", "")


def test_chunk_size_larger_than_file_yields_single_chunk(tmp_path):
    p = _write_csv(tmp_path, "ct.csv", ["categoryid", "parentid"], [["1016", "213"]])
    reader = CsvChunkReader(p, chunk_size=10)
    chunks = list(reader.iter_chunks())
    assert len(chunks) == 1
    assert chunks[0].start_row_number == 2
    assert reader.n_data_rows == 1


def test_short_rows_are_padded_and_counted_as_malformed(tmp_path):
    p = tmp_path / "wide_or_short.csv"
    p.write_text("a,b,c\n1,2\n3,\n4,5,6\n", encoding="utf-8")
    reader = CsvChunkReader(p, chunk_size=2)
    chunks = list(reader.iter_chunks())
    assert reader.n_data_rows == 3
    assert reader.n_malformed_rows == 2
    assert chunks[0].rows[0] == ("1", "2", "")
    assert chunks[0].rows[1] == ("3", "", "")


def test_wide_rows_raise_malformed_error(tmp_path):
    p = tmp_path / "wide.csv"
    p.write_text("a,b,c\n1,2\n1,2,3,4\n", encoding="utf-8")
    reader = CsvChunkReader(p, chunk_size=1)
    with pytest.raises(MalformedCsvRowError):
        list(reader.iter_chunks())


def test_expected_header_mismatch_raises_on_iteration(tmp_path):
    p = _write_csv(
        tmp_path,
        "events.csv",
        ["timestamp", "visitorid", "event", "itemid", "transactionid"],
        [["1", "2", "view", "3", ""]],
    )
    reader = CsvChunkReader(p, chunk_size=1, expected_header=("a", "b"))
    with pytest.raises(ValueError, match="header mismatch"):
        list(reader.iter_chunks())


def test_read_csv_header_validates_expected(tmp_path):
    p = _write_csv(tmp_path, "ct.csv", ["categoryid", "parentid"], [["1", ""]])
    assert read_csv_header(p) == ("categoryid", "parentid")
    assert read_csv_header(p, expected=("categoryid", "parentid")) == ("categoryid", "parentid")
    with pytest.raises(ValueError, match="header mismatch"):
        read_csv_header(p, expected=("timestamp", "itemid"))


def test_empty_file_raises(tmp_path):
    p = tmp_path / "empty.csv"
    p.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="empty"):
        read_csv_header(p)
    reader = CsvChunkReader(p, chunk_size=1)
    with pytest.raises(ValueError, match="empty"):
        list(reader.iter_chunks())


def test_chunk_size_must_be_positive(tmp_path):
    p = _write_csv(tmp_path, "e.csv", ["a"], [["1"]])
    with pytest.raises(ValueError, match="chunk_size"):
        CsvChunkReader(p, chunk_size=0)
