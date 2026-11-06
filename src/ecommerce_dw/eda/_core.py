"""Streaming exact distinct / duplicate counters over CSV cell tokens.

CSV files in this project are too large to load into memory wholesale
(eventually ~20M rows for ``item_properties``), so all statistics in this
package are computed by streaming row-by-row. This module provides the
shared building blocks:

- ``DigestSink``: exact distinct / duplicate stats via chunked external sort
  of 128-bit md5 digests. Memory stays bounded by ``chunk_size`` regardless
  of how many rows are fed in; small inputs take the same code path.
- ``infer_column_type``: lightweight sample-based column type inference.
- ``_is_missing``: the default definition of a missing CSV cell.

md5 collisions (probability negligible for short, fixed-shape CSV cells) are
the only source of error; counts are otherwise exact.
"""

from __future__ import annotations

import hashlib
import heapq
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

_DIGEST_SIZE = 16


def _md5(value: str) -> bytes:
    return hashlib.md5(value.encode("utf-8")).digest()


@dataclass(frozen=True)
class DigestStats:
    """Result of folding a multi-set of values.

    ``distinct``        number of distinct values
    ``duplicate_extra`` occurrences beyond the first one per distinct value
    ``duplicate_groups``number of distinct values that occurred at least twice
    """

    distinct: int
    duplicate_extra: int
    duplicate_groups: int


def _fold_sorted(sorted_digests: Iterable[bytes]) -> tuple[int, int, int]:
    """Count runs of consecutive equal digests in an already-sorted stream."""
    it = iter(sorted_digests)
    distinct = duplicate_extra = duplicate_groups = 0
    try:
        prev = next(it)
    except StopIteration:
        return 0, 0, 0
    run = 1
    for cur in it:
        if cur == prev:
            run += 1
            continue
        distinct += 1
        if run > 1:
            duplicate_extra += run - 1
            duplicate_groups += 1
        prev = cur
        run = 1
    distinct += 1
    if run > 1:
        duplicate_extra += run - 1
        duplicate_groups += 1
    return distinct, duplicate_extra, duplicate_groups


class DigestSink:
    """Exact distinct / duplicate stats from a stream of strings.

    Values are hashed to 16-byte md5 digests, buffered in sorted chunks on
    disk, then merged with a k-way heap merge in :meth:`close`. Peak memory is
    a function of ``chunk_size`` (digest objects), not of the input size.
    """

    def __init__(self, tmp_dir: Path | None = None, *, chunk_size: int = 200_000) -> None:
        self._chunk_size = chunk_size
        self._buffer: list[bytes] = []
        self._chunk_paths: list[Path] = []
        self._owns_tmp = tmp_dir is None
        self._tmp_dir = tmp_dir or Path(tempfile.mkdtemp(prefix="eda-digest-"))

    def add(self, value: str) -> None:
        self._buffer.append(_md5(value))
        if len(self._buffer) >= self._chunk_size:
            self._flush_chunk()

    def _flush_chunk(self) -> None:
        if not self._buffer:
            return
        self._buffer.sort()
        path = self._tmp_dir / f"chunk-{len(self._chunk_paths):06d}.bin"
        with path.open("wb") as fh:
            for digest in self._buffer:
                fh.write(digest)
        self._chunk_paths.append(path)
        self._buffer.clear()

    def close(self) -> DigestStats:
        self._flush_chunk()
        stats = self._fold()
        if self._owns_tmp:
            for path in self._chunk_paths:
                path.unlink(missing_ok=True)
            self._tmp_dir.rmdir()
        self._chunk_paths.clear()
        return stats

    def _fold(self) -> DigestStats:
        if not self._chunk_paths:
            return DigestStats(distinct=0, duplicate_extra=0, duplicate_groups=0)
        streams = [self._read_records(path) for path in self._chunk_paths]
        if len(streams) == 1:
            distinct, extra, groups = _fold_sorted(streams[0])
        else:
            distinct, extra, groups = _fold_sorted(heapq.merge(*streams))
        return DigestStats(distinct=distinct, duplicate_extra=extra, duplicate_groups=groups)

    @staticmethod
    def _read_records(path: Path) -> Iterator[bytes]:
        with path.open("rb") as fh:
            while record := fh.read(_DIGEST_SIZE):
                yield record


# ---------------------------------------------------------------------------
# CSV cell helpers
# ---------------------------------------------------------------------------


def _is_missing(value: str) -> bool:
    """A CSV cell is missing when it is empty or whitespace-only."""
    return value == "" or value.isspace()


def infer_column_type(samples: Sequence[str]) -> str:
    """Infer a column type from non-empty samples.

    Returns one of ``"int"``, ``"float"``, ``"bool"`` or ``"str"``. Empty
    samples are expected to be filtered by the caller. With no samples at all
    the column is conservatively typed as ``"str"``.
    """
    if not samples:
        return "str"

    def _is_int(value: str) -> bool:
        text = value[1:] if value[:1] in "+-" else value
        return text.isdigit()

    if all(_is_int(value) for value in samples):
        return "int"
    try:
        if all(float(value) is not None for value in samples):
            return "float"
    except ValueError:
        pass
    if all(value.lower() in {"true", "false"} for value in samples):
        return "bool"
    return "str"
