"""JSON byte helpers used by the versioned storage layer.

Thin wrappers around ``json.dumps``/``json.loads`` that fix the byte
encoding to a deterministic compact form. Centralized here so that
the on-disk wire format is defined in exactly one place — anyone
who needs to construct or inspect kvgit storage bytes (including
tests and tooling) goes through these.
"""

import json


def dumps(obj) -> bytes:
    """Serialize a JSON-safe Python object to bytes (compact, deterministic)."""
    return json.dumps(obj, separators=(",", ":")).encode()


def loads(raw: bytes):
    """Deserialize JSON bytes to a Python object."""
    return json.loads(raw)


def safe_loads(raw: bytes):
    """Like ``loads`` but returns None on any decode/parse error.

    Useful when reading values from a store that may contain garbage
    (corruption, partial writes, version skew).
    """
    try:
        return json.loads(raw)
    except Exception:
        return None
