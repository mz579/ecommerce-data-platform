"""Streaming statistics for the Retailrocket ``item_properties_*.csv`` files.

The two parts share one header and are streamed back-to-back in a single
pass (the combined table is ~900 MB / ~20M rows, far too big for memory).
Accumulated:

- property vocabulary: counts per property name, plus the distinct count
- coverage: distinct items, distinct property values, missing-field counts
- timestamp range (Unix milliseconds) and invalid-timestamp count
- exact full-row duplicate statistics via external-sort digests

No database is required; only local CSV files are read.
"""

from __future__ import annotations

import csv
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ecommerce_dw.eda._core import DigestSink

PROPERTY_HEADER = ("timestamp", "itemid", "property", "value")


@dataclass(frozen=True)
class ItemPropertyStats:
    n_rows: int
    n_files: int
    ts_min_ms: int
    ts_max_ms: int
    n_invalid_ts: int
    n_properties: int  # distinct property names
    property_counts: dict[str, int]  # sorted descending by count
    n_items: int
    n_distinct_values: int
    n_duplicate_rows: int  # extra occurrences of full rows that repeat
    n_duplicate_groups: int  # distinct full rows appearing at least twice
    n_missing: dict[str, int]  # missing cells per column
    items: frozenset[str]
    categoryid_values: frozenset[str]  # values of the "categoryid" property


def summarize_item_properties(
    paths: Iterable[str | Path],
    *,
    tmp_dir: str | Path | None = None,
    chunk_size: int = 200_000,
) -> ItemPropertyStats:
    """Stream all ``item_properties`` files and return their joint statistics."""
    source_paths = [Path(p) for p in paths]
    property_counts = Counter()
    items: set[str] = set()
    categoryid_values: set[str] = set()
    value_sink = DigestSink(tmp_dir, chunk_size=chunk_size)
    row_sink = DigestSink(tmp_dir, chunk_size=chunk_size)

    n_rows = 0
    ts_min: int | None = None
    ts_max: int | None = None
    n_invalid_ts = 0
    missing = Counter()

    for src in source_paths:
        with src.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            _ = next(reader)  # header
            for row in reader:
                if len(row) != 4:  # pragma: no cover - malformed rows are rare
                    continue
                ts, item_id, property_name, value = row
                n_rows += 1

                for col_name, cell in zip(PROPERTY_HEADER, row, strict=True):
                    if cell == "" or cell.isspace():
                        missing[col_name] += 1

                property_counts[property_name] += 1
                if item_id != "":
                    items.add(item_id)
                if value != "":
                    value_sink.add(value)
                if property_name == "categoryid" and value != "":
                    categoryid_values.add(value)
                row_sink.add("\x1f".join(row))

                if ts == "":
                    n_invalid_ts += 1
                else:
                    try:
                        ts_val = int(ts)
                    except ValueError:
                        n_invalid_ts += 1
                    else:
                        if ts_min is None or ts_val < ts_min:
                            ts_min = ts_val
                        if ts_max is None or ts_val > ts_max:
                            ts_max = ts_val

    value_stats = value_sink.close()
    dup_stats = row_sink.close()

    return ItemPropertyStats(
        n_rows=n_rows,
        n_files=len(source_paths),
        ts_min_ms=ts_min if ts_min is not None else 0,
        ts_max_ms=ts_max if ts_max is not None else 0,
        n_invalid_ts=n_invalid_ts,
        n_properties=len(property_counts),
        property_counts=dict(property_counts.most_common()),
        n_items=len(items),
        n_distinct_values=value_stats.distinct,
        n_duplicate_rows=dup_stats.duplicate_extra,
        n_duplicate_groups=dup_stats.duplicate_groups,
        n_missing=dict(missing),
        items=frozenset(items),
        categoryid_values=frozenset(categoryid_values),
    )
