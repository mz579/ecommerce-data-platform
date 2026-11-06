"""Unit tests for deterministic row keys and file checksums (ODS ingestion)."""

from __future__ import annotations

import hashlib

import pytest

from ecommerce_dw.ingestion.keys import make_row_key, sha256_file


def test_make_row_key_is_deterministic_and_32_bytes():
    a = make_row_key("events.csv", 2)
    assert len(a) == 32
    assert a == make_row_key("events.csv", 2)


def test_make_row_key_changes_with_input():
    assert make_row_key("events.csv", 2) != make_row_key("events.csv", 3)
    assert make_row_key("events.csv", 2) != make_row_key("other.csv", 2)


def test_make_row_key_matches_documented_encoding():
    # Documented rule: SHA-256( UTF-8(source_file + U+001F + source_row_number) )
    expected = hashlib.sha256(b"events.csv\x1f2").digest()
    assert make_row_key("events.csv", 2) == expected


def test_make_row_key_handles_unicode_source_file():
    k1 = make_row_key("事件.csv", 1)
    k2 = make_row_key("事件.csv", 1)
    assert k1 == k2 and len(k1) == 32


def test_make_row_key_rejects_invalid_row_number():
    with pytest.raises(ValueError):
        make_row_key("a.csv", 0)
    with pytest.raises(ValueError):
        make_row_key("a.csv", -3)


def test_sha256_file_matches_known_content(tmp_path):
    p = tmp_path / "sample.csv"
    content = b"a,b\n1,2\n3,4\n"
    p.write_bytes(content)
    assert sha256_file(p) == hashlib.sha256(content).hexdigest()


def test_sha256_file_is_streaming_for_large_files(tmp_path):
    p = tmp_path / "big.bin"
    block = b"x" * (1 << 20)  # 1 MiB
    p.write_bytes(block * 6)  # 6 MiB total
    assert sha256_file(p) == hashlib.sha256(block * 6).hexdigest()
