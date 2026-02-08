"""Content types: encode/decode + merge for typed values."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable

from .versioned import MergeFn


@dataclass
class ContentType:
    """A typed content handler with encode, decode, and merge logic.

    The merge function operates on decoded values:
        (old_value | None, our_value, their_value) -> merged_value

    Use ``as_merge_fn()`` to get a bytes-level MergeFn for registration
    with ``Versioned.set_merge_fn()``.
    """

    encode: Callable[[Any], bytes]
    decode: Callable[[bytes], Any]
    merge: Callable[[Any | None, Any, Any], Any]

    def as_merge_fn(self) -> MergeFn:
        """Convert to a bytes-level merge function."""

        def fn(
            old: bytes | None, ours: bytes | None, theirs: bytes | None
        ) -> bytes:
            old_val = self.decode(old) if old is not None else None
            ours_val = self.decode(ours) if ours is not None else None
            theirs_val = self.decode(theirs) if theirs is not None else None
            return self.encode(self.merge(old_val, ours_val, theirs_val))

        return fn


def counter(
    encoding: str = "big", byte_length: int = 8
) -> ContentType:
    """A counter content type. Merge = ours + theirs - old.

    Values are stored as big-endian (default) or little-endian integers.
    """

    def encode(val: int) -> bytes:
        return val.to_bytes(byte_length, byteorder=encoding, signed=True)

    def decode(raw: bytes) -> int:
        return int.from_bytes(raw, byteorder=encoding, signed=True)

    def merge(old: int | None, ours: int, theirs: int) -> int:
        base = old if old is not None else 0
        return ours + theirs - base

    return ContentType(encode=encode, decode=decode, merge=merge)


def last_writer_wins() -> ContentType:
    """Last-writer-wins: always returns theirs (no decode overhead)."""
    return ContentType(
        encode=lambda v: v,
        decode=lambda v: v,
        merge=lambda old, ours, theirs: theirs,
    )


def json_value(
    merge_fn: Callable[[Any | None, Any, Any], Any] | None = None,
) -> ContentType:
    """JSON-encoded content type with optional merge function.

    Args:
        merge_fn: Custom merge for decoded JSON values.
            Defaults to last-writer-wins on the decoded values.
    """

    def encode(val: Any) -> bytes:
        return json.dumps(val, sort_keys=True).encode("utf-8")

    def decode(raw: bytes) -> Any:
        return json.loads(raw.decode("utf-8"))

    if merge_fn is None:
        merge_fn = lambda old, ours, theirs: theirs

    return ContentType(encode=encode, decode=decode, merge=merge_fn)
