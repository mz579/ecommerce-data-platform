"""Memory-bounded chunked CSV reader that tracks physical row numbers.

The reader streams a CSV file with the stdlib ``csv`` module and yields rows in
chunks of at most ``chunk_size`` rows. It never loads more than one chunk into
memory and records, for every yielded chunk, the physical row number of its
first row so the loader can derive a deterministic per-row key.

Row numbering follows ``docs/data_contract.md``: the header row is line 1, the
first data row is line 2 (``grep -n``-compatible, i.e. physical line number).

Malformed-row policy for the ODS layer (no cleaning in this layer):
- a row with fewer fields than the header is padded with empty strings and
  counted in ``n_malformed_rows`` (kept, faithful to the raw file);
- a row with more fields than the header raises ``MalformedCsvRowError``
  (unexpected column count usually indicates a corrupt file; failing loudly is
  safer than silently dropping data).
"""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CsvChunk:
    """A batch of rows plus the physical row number of its first row."""

    start_row_number: int
    rows: tuple[tuple[str, ...], ...]


class MalformedCsvRowError(ValueError):
    """Raised when a CSV row has more fields than the header."""


def read_csv_header(path: str | Path, *, expected: tuple[str, ...] | None = None):
    """Return the header row, optionally validating it against ``expected``."""
    with Path(path).open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        try:
            header = tuple(next(reader))
        except StopIteration as exc:
            raise ValueError(f"{path} is empty (no header row)") from exc
    if expected is not None and header != tuple(expected):
        raise ValueError(
            f"header mismatch for {Path(path).name}: got {header!r}, expected {tuple(expected)!r}"
        )
    return header


class CsvChunkReader:
    """Stream a UTF-8 CSV file in chunks, tracking physical row numbers."""

    def __init__(
        self,
        path: str | Path,
        *,
        chunk_size: int,
        expected_header: tuple[str, ...] | None = None,
    ) -> None:
        if chunk_size < 1:
            raise ValueError(f"chunk_size must be >= 1, got {chunk_size}")
        self._path = Path(path)
        self._chunk_size = chunk_size
        self._expected_header = tuple(expected_header) if expected_header else None
        self.header: tuple[str, ...] | None = None
        self.n_data_rows = 0
        self.n_malformed_rows = 0

    def iter_chunks(self) -> Iterator[CsvChunk]:
        """Yield the file's data rows in chunks; totals are on this object."""
        with self._path.open(newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            try:
                header = tuple(next(reader))
            except StopIteration as exc:
                raise ValueError(f"{self._path} is empty (no header row)") from exc
            if self._expected_header is not None and header != self._expected_header:
                raise ValueError(
                    f"header mismatch for {self._path.name}: got {header!r}, "
                    f"expected {self._expected_header!r}"
                )
            self.header = header
            ncols = len(header)
            chunk: list[tuple[str, ...]] = []
            row_number = 2  # header is line 1, first data row is line 2
            start_row = row_number
            for raw in reader:
                self.n_data_rows += 1
                chunk.append(self._normalize(raw, ncols))
                row_number += 1
                if len(chunk) >= self._chunk_size:
                    yield CsvChunk(start_row_number=start_row, rows=tuple(chunk))
                    chunk = []
                    start_row = row_number
            if chunk:
                yield CsvChunk(start_row_number=start_row, rows=tuple(chunk))

    def _normalize(self, raw: list[str], ncols: int) -> tuple[str, ...]:
        if len(raw) == ncols:
            return tuple(raw)
        if len(raw) < ncols:
            self.n_malformed_rows += 1
            return tuple(raw) + ("",) * (ncols - len(raw))
        raise MalformedCsvRowError(
            f"row at line {self.n_data_rows + 1} has {len(raw)} fields, expected {ncols}"
        )
