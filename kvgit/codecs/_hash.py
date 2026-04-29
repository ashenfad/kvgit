"""Canonical buffer hashing for chunk references.

Centralizes the hash function so all codecs and chunk-sink
implementations agree on how a chunk is identified. The hash must
be stable across processes, Python versions, and platforms — only
the raw bytes are hashed; never object identity or implementation
metadata.
"""

from __future__ import annotations

import hashlib

# 40 hex chars (160 bits) — same length as kvgit's commit hashes,
# enough collision resistance for billions of chunks.
HASH_LEN = 40


def hash_bytes(data: bytes | memoryview) -> str:
    """Hash arbitrary bytes-like data to a hex digest."""
    h = hashlib.sha256()
    if isinstance(data, memoryview):
        # Cast to byte-level memoryview for a stable read regardless of
        # original element format. ``hashlib.update`` accepts memoryview
        # without copying.
        h.update(data.cast("B"))
    else:
        h.update(data)
    return h.hexdigest()[:HASH_LEN]
