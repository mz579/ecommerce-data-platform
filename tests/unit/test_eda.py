"""Unit tests for the Phase 1 EDA / data-profiling helpers.

A small synthetic CSV is written into ``tmp_path`` for each test so nothing
here depends on the raw dataset (which is git-ignored and absent in CI).
"""

from __future__ import annotations

from pathlib import Path

from ecommerce_dw.eda._core import DigestSink, infer_column_type
from ecommerce_dw.eda.category_tree import summarize_category_tree
from ecommerce_dw.eda.events import summarize_events
from ecommerce_dw.eda.item_properties import summarize_item_properties
from ecommerce_dw.eda.scan import scan_csv

EVENTS_HEADER = "timestamp,visitorid,event,itemid,transactionid\n"


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# scan_csv
# ---------------------------------------------------------------------------


def test_scan_csv_counts_rows_missing_distinct_and_duplicates(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER
        + "1433221332117,257597,view,355908,\n"
        + "1433224214164,992329,view,248676,\n"
        + "1433224214164,992329,view,248676,\n"  # exact duplicate of row 2
        + "1433224214165,992329,addtocart,248676,\n"
        + "1433224214167,7,transaction,556,4001\n",
    )

    result = scan_csv(path)

    assert result.n_rows == 5
    assert result.n_malformed_rows == 0
    assert result.n_duplicate_rows == 1  # one extra occurrence
    assert result.n_duplicate_groups == 1  # one distinct row repeated

    columns = {c.name: c for c in result.columns}
    assert columns["visitorid"].n_missing == 0
    assert columns["visitorid"].n_nonempty == 5
    assert columns["visitorid"].distinct_nonempty == 3
    assert columns["transactionid"].n_missing == 4
    assert columns["transactionid"].distinct_nonempty == 1


def test_scan_csv_infers_types_and_handles_empty_column(tmp_path):
    path = _write(
        tmp_path,
        "typed.csv",
        "a,b,c,d\n1,1.5,true,\n2,2.5,false,x\n",
    )

    result = scan_csv(path)
    columns = {c.name: c for c in result.columns}

    assert columns["a"].inferred_type == "int"
    assert columns["b"].inferred_type == "float"
    assert columns["c"].inferred_type == "bool"
    assert columns["d"].inferred_type == "str"
    assert columns["d"].n_missing == 1
    assert result.n_duplicate_rows == 0


def test_scan_csv_counts_malformed_rows_and_whitespace_missing(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER
        + "1433221332117,257597,view,355908,\n"
        + "1433221332117,257597,view,355908,EXTRA\n"
        + "1433221332118,992329,view,\n",  # short row: missing itemid
    )

    result = scan_csv(path)

    assert result.n_rows == 3
    assert result.n_malformed_rows == 1  # only the short row is malformed
    columns = {c.name: c for c in result.columns}
    assert columns["itemid"].n_missing == 1  # itemid padded to "" on short row


def test_scan_csv_repeated_distinct_values_count_as_distinct_groups(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER + "1,1,view,1,\n1,1,view,1,\n1,1,view,1,\n2,2,view,2,\n",
    )

    result = scan_csv(path)

    assert result.n_duplicate_rows == 2  # two extra occurrences
    assert result.n_duplicate_groups == 1


def test_scan_csv_deterministic_across_chunk_boundaries(tmp_path):
    header = EVENTS_HEADER
    rows = [f"{i},7,view,{i},\n" for i in range(5)] + [f"{i},7,view,{i},\n" for i in range(5)]
    path = _write(tmp_path, "events.csv", header + "".join(rows))

    small = scan_csv(path, chunk_size=3)
    large = scan_csv(path, chunk_size=100)

    assert small.n_duplicate_rows == large.n_duplicate_rows == 5
    assert small.columns[3].distinct_nonempty == large.columns[3].distinct_nonempty == 5


# ---------------------------------------------------------------------------
# summarize_events
# ---------------------------------------------------------------------------


def test_summarize_events_counts_types_coverage_and_unknown(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER
        + "1433221332117,257597,view,355908,\n"
        + "1433224214164,992329,view,248676,\n"
        + "1433224214165,992329,addtocart,248676,\n"
        + "1433224214166,7,transaction,555,4000\n"
        + "1433224214167,7,transaction,556,4001\n"
        + "1433224214168,7,addtocart,557,4001\n"  # txnid on a non-transaction row
        + "1433224214169,8,mystery,557,\n"  # unknown event type
        + "bad-ts,9,view,558,\n",  # invalid timestamp
    )

    stats = summarize_events(path)

    assert stats.n_rows == 8
    assert stats.event_counts == {"view": 3, "addtocart": 2, "transaction": 2}
    assert stats.unknown_events == {"mystery": 1}
    assert stats.n_visitors == 5
    assert stats.n_items == 6
    assert stats.n_transactionids == 2
    assert stats.n_transaction_rows == 2
    assert stats.n_txnid_on_transaction_rows == 2
    assert stats.transaction_rows_missing_txnid == 0
    assert stats.txnid_non_transaction_rows == 1
    assert stats.n_invalid_ts == 1
    assert stats.ts_min_ms == 1_433_221_332_117
    assert stats.ts_max_ms == 1_433_224_214_169
    assert stats.n_missing_visitor == 0


def test_summarize_events_allows_custom_event_vocabulary(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER + "1433224214168,8,mystery,557,\n",
    )

    strict = summarize_events(path)
    custom = summarize_events(path, valid_events={"mystery"})

    assert strict.unknown_events == {"mystery": 1}
    assert custom.unknown_events == {}
    assert custom.event_counts == {"mystery": 1}


def test_summarize_events_tracks_zero_sentinel_and_txnid_reuse(tmp_path):
    path = _write(
        tmp_path,
        "events.csv",
        EVENTS_HEADER
        + "1433224214100,0,view,1,\n"  # visitorid "0" sentinel
        + "1433224214101,0,view,2,\n"
        + "1433224214102,5,transaction,3,900\n"
        + "1433224214103,5,transaction,4,900\n"  # same order id reused
        + "1433224214104,5,transaction,5,900\n",
    )

    stats = summarize_events(path)

    assert stats.n_visitor_zero == 2
    assert stats.n_txnid_reused == 1
    assert stats.max_txnid_events == 3
    assert stats.n_transactionids == 1
    assert stats.n_visitors == 2  # {0, 5}


# ---------------------------------------------------------------------------
# summarize_item_properties
# ---------------------------------------------------------------------------


def test_summarize_item_properties_merges_parts(tmp_path):
    part1 = _write(
        tmp_path,
        "item_properties_part1.csv",
        "timestamp,itemid,property,value\n"
        + "1435460400000,460429,categoryid,1338\n"
        + "1435460400000,460429,available,true\n"
        + "1435460400001,206783,888,featureA\n",
    )
    part2 = _write(
        tmp_path,
        "item_properties_part2.csv",
        "timestamp,itemid,property,value\n"
        + "1435460400002,460429,categoryid,1338\n"
        + "1435460400002,206783,available,false\n",
    )

    stats = summarize_item_properties([part1, part2])

    assert stats.n_files == 2
    assert stats.n_rows == 5
    assert stats.n_properties == 3
    assert stats.n_items == 2
    assert stats.n_distinct_values == 4  # true/false/featureA/1338
    assert stats.property_counts["categoryid"] == 2
    assert stats.n_missing == {}
    assert stats.items == frozenset({"460429", "206783"})
    assert stats.categoryid_values == frozenset({"1338"})
    assert stats.ts_min_ms == 1_435_460_400_000
    assert stats.ts_max_ms == 1_435_460_400_002


def test_summarize_item_properties_counts_duplicates_and_missing(tmp_path):
    part = _write(
        tmp_path,
        "item_properties_part1.csv",
        "timestamp,itemid,property,value\n"
        + "1,10,cat,5\n"
        + "1,10,cat,5\n"  # exact duplicate row
        + "1,,avail,\n",  # missing itemid and value
    )

    stats = summarize_item_properties([part])

    assert stats.n_rows == 3
    assert stats.n_duplicate_rows == 1
    assert stats.n_duplicate_groups == 1
    assert stats.n_missing == {"itemid": 1, "value": 1}


# ---------------------------------------------------------------------------
# category_tree
# ---------------------------------------------------------------------------


def test_category_tree_reports_roots_orphans_and_depth(tmp_path):
    path = _write(
        tmp_path,
        "category_tree.csv",
        "categoryid,parentid\n"
        + "100,\n"
        + "200,100\n"
        + "300,200\n"
        + "400,200\n"
        + "600,\n"
        + "700,999\n",  # orphan parent
    )

    stats = summarize_category_tree(path)

    assert stats.n_nodes == 6
    assert stats.root_count == 2
    assert stats.n_edges == 4
    assert stats.orphan_references == frozenset({"999"})
    assert stats.n_orphan_parent_nodes == 1
    assert stats.max_depth == 2  # 300 (or 400) -> 200 -> 100
    assert stats.n_cycle_nodes == 0
    assert stats.n_unreachable_nodes == 1


def test_category_tree_detects_cycle_and_duplicate_nodes(tmp_path):
    path = _write(
        tmp_path,
        "category_tree.csv",
        "categoryid,parentid\n"
        + "1,\n"
        + "2,3\n"
        + "3,2\n"  # 2 <-> 3 cycle
        + "3,2\n",  # duplicate node 3, same parent (keeps the cycle intact)
    )

    stats = summarize_category_tree(path)

    assert stats.n_rows == 4
    assert stats.n_nodes == 3
    assert stats.n_duplicate_nodes == 1
    assert stats.n_cycle_nodes == 2
    assert stats.n_unreachable_nodes == 2
    assert stats.max_depth == 0  # only the root is reachable


# ---------------------------------------------------------------------------
# DigestSink and type inference
# ---------------------------------------------------------------------------


def test_digest_sink_folds_distinct_and_duplicates(tmp_path):
    sink = DigestSink(tmp_path, chunk_size=2)  # tiny chunks force spilling
    for value in ["a", "b", "a", "c", "b", "b"]:
        sink.add(value)

    stats = sink.close()

    assert stats.distinct == 3
    assert stats.duplicate_extra == 3  # a +1, b +2
    assert stats.duplicate_groups == 2  # a and b repeat


def test_digest_sink_empty():
    sink = DigestSink(chunk_size=2)
    stats = sink.close()
    assert (stats.distinct, stats.duplicate_extra, stats.duplicate_groups) == (0, 0, 0)


def test_infer_column_type():
    assert infer_column_type([]) == "str"
    assert infer_column_type(["1", "42", "-7", "+8"]) == "int"
    assert infer_column_type(["1.5", "2", "-3.25"]) == "float"
    assert infer_column_type(["true", "FALSE", "True"]) == "bool"
    assert infer_column_type(["view", "1"]) == "str"
