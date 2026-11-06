"""Deterministic row keys and file checksums for ODS ingestion.

Row-key rule (see ``docs/ods_design.md`` for the full design):

    row_key = SHA-256( UTF-8(source_file + U+001F + source_row_number) )

stored as ``BINARY(32)``. The digest depends only on the byte-identity of a
source row (file name + physical line number including the header offset), so it
is deterministic across runs and across batches. It is the business idempotency
key of the ODS layer and must not be confused with MySQL auto-increment primary
keys, which are internal only.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

# Field separator inside the row-key payload; U+001F cannot appear in a line.
_SEPARATOR = "\x1f"
_BLOCK_SIZE = 1 << 20


def make_row_key(source_file: str, source_row_number: int) -> bytes:
    """Return the 32-byte deterministic surrogate key for a source row."""
    if source_row_number < 1:
        raise ValueError(f"source_row_number must be >= 1, got {source_row_number}")
    payload = f"{source_file}{_SEPARATOR}{source_row_number}".encode()
    return hashlib.sha256(payload).digest()


def sha256_file(path: str | Path) -> str:
    """Stream the file and return its SHA-256 hex digest (memory-bounded)."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for block in _iter_blocks(fh):
            digest.update(block)
    return digest.hexdigest()


def _iter_blocks(fh: BinaryIO) -> Iterator[bytes]:
    while True:
        block = fh.read(_BLOCK_SIZE)
        if not block:
            return
        yield block
