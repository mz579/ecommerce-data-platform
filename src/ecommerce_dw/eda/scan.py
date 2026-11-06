"""Generic streaming scan of a CSV file.

``scan_csv`` reads a CSV row-by-row (``csv`` module, never loading the whole
table) and produces per-column profiles plus full-row duplicate statistics:

- **type inference**: a sample (first ``max_type_samples`` non-empty cells)
  per column drives ``infer_column_type``
- **missing counts**: empty or whitespace-only cells per column
- **distinct counts**: exact distinct non-empty values per column, computed
  with the external-sort ``DigestSink`` so arbitrarily large files stay safe
- **duplicate rows**: exact count of full-row duplicates (extra occurrences
  beyond the first, plus how many distinct rows are duplicated)

No database is required; the input is read from local CSV files only.
"""

from __future__ import annotations

import csv
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from ecommerce_dw.eda._core import DigestSink, _is_missing, infer_column_type

DEFAULT_CHUNK_SIZE = 200_000
DEFAULT_MAX_TYPE_SAMPLES = 1_000


@dataclass(frozen=True)
class ColumnProfile:
    name: str
    inferred_type: str
    n_missing: int
    n_nonempty: int
    distinct_nonempty: int

    @property
    def n_rows_repr(self) -> int:
        return self.n_missing + self.n_nonempty


@dataclass(frozen=True)
class CsvScanResult:
    path: Path
    header: tuple[str, ...]
    n_rows: int
    n_malformed_rows: int
    n_duplicate_rows: int  # extra occurrences of full rows that repeat
    n_duplicate_groups: int  # distinct full rows appearing at least twice
    columns: tuple[ColumnProfile, ...]


def scan_csv(
    path: str | Path,
    *,
    tmp_dir: str | Path | None = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_type_samples: int = DEFAULT_MAX_TYPE_SAMPLES,
) -> CsvScanResult:
    """Stream a CSV file and return schema-like profiles and row duplicates."""
    src = Path(path)
    header: Sequence[str]
    with src.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            header = tuple(next(reader))
        except StopIteration as exc:  # pragma: no cover - header required
            raise ValueError(f"{src} is empty, cannot scan") from exc

    ncols = len(header)
    sinks = [DigestSink(tmp_dir, chunk_size=chunk_size) for _ in range(ncols)]
    row_sink = DigestSink(tmp_dir, chunk_size=chunk_size)
    samples: list[list[str]] = [[] for _ in range(ncols)]
    missing: list[int] = [0] * ncols
    nonempty: list[int] = [0] * ncols
    distinct: list[int] = [0] * ncols
    n_rows = 0
    n_malformed = 0

    with src.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        _ = next(reader)  # skip header
        for raw in reader:
            n_rows += 1
            malformed = len(raw) != ncols
            if malformed:
                n_malformed += 1
            row = list(raw[:ncols]) + [""] * (ncols - len(raw))
            row_sink.add("\x1f".join(row))
            for idx, value in enumerate(row):
                if _is_missing(value):
                    missing[idx] += 1
                    continue
                nonempty[idx] += 1
                sinks[idx].add(value)
                if len(samples[idx]) < max_type_samples:
                    samples[idx].append(value)

    for idx, sink in enumerate(sinks):
        stats = sink.close()
        distinct[idx] = stats.distinct
    dup_stats = row_sink.close()

    columns = tuple(
        ColumnProfile(
            name=name,
            inferred_type=infer_column_type(samples[idx]),
            n_missing=missing[idx],
            n_nonempty=nonempty[idx],
            distinct_nonempty=distinct[idx],
        )
        for idx, name in enumerate(header)
    )

    return CsvScanResult(
        path=src,
        header=header,
        n_rows=n_rows,
        n_malformed_rows=n_malformed,
        n_duplicate_rows=dup_stats.duplicate_extra,
        n_duplicate_groups=dup_stats.duplicate_groups,
        columns=columns,
    )
