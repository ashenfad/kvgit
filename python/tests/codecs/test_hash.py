"""Tests for the canonical buffer hash."""

from __future__ import annotations

from kvgit.codecs._hash import HASH_LEN, hash_bytes


def test_hash_length():
    assert len(hash_bytes(b"hello")) == HASH_LEN


def test_hash_is_deterministic():
    assert hash_bytes(b"abc") == hash_bytes(b"abc")


def test_distinct_bytes_distinct_hashes():
    assert hash_bytes(b"abc") != hash_bytes(b"abd")


def test_hash_accepts_memoryview():
    data = b"some bytes"
    assert hash_bytes(memoryview(data)) == hash_bytes(data)


def test_hash_memoryview_with_typed_format():
    """A non-byte-format memoryview must hash like its raw bytes view."""
    import array

    arr = array.array("i", [1, 2, 3, 4])
    mv = memoryview(arr)
    assert hash_bytes(mv) == hash_bytes(bytes(mv))
