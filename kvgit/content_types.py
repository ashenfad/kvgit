"""Merge functions for typed values."""

from collections.abc import Callable
from typing import Any

from .merges import CantMark, make_text_merge

MergeFn = Callable[[Any | None, Any, Any], Any]
"""Merge function: (old_value | None, our_value, their_value) -> merged_value.

Any argument can be None (key absent or removed on that side).

Returning a :class:`~kvgit.versioned.protocol.MergeChoice` instead of a
value keeps that side's committed value as it stands: no value is
encoded and the merge writes no new blob for the key.
"""


def counter() -> MergeFn:
    """Counter merge: ours + theirs - old."""

    def merge(old: int | None, ours: int, theirs: int) -> int:
        base = old if old is not None else 0
        return ours + theirs - base

    return merge


def last_writer_wins() -> MergeFn:
    """Last-writer-wins: always returns theirs."""
    return lambda old, ours, theirs: theirs


def text_merge(*, ours_label: str = "ours", theirs_label: str = "theirs") -> MergeFn:
    """Marker merge for ``str`` (or ``bytes``) values, for use with ``Staged``.

    The value-level counterpart to :func:`kvgit.merges.text`. ``Staged``
    decodes both sides before calling a merge function, so a key holding
    ``str`` reaches a bytes-level function as ``str`` and fails there;
    this one encodes ``str`` sides as UTF-8, marker-merges, and decodes
    the result back to ``str`` when any side was ``str``. A key whose
    values are ``bytes`` merges as bytes and comes back as ``bytes``.

    Disjoint line changes merge cleanly and overlapping ones come back
    with git-style ``<<<<<<<`` markers (labelled by ``ours_label`` /
    ``theirs_label``) rather than raising. A value that is neither
    ``str`` nor ``bytes``, and anything the marker merge cannot handle —
    undecodable bytes, NUL bytes, inputs over the size cap — raises
    :class:`~kvgit.merges.CantMark`, which the merge machinery files as
    an ordinary conflict.
    """
    merge_bytes = make_text_merge(ours_label=ours_label, theirs_label=theirs_label)

    def encode(value: Any) -> bytes | None:
        if value is None or isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
        raise CantMark(f"not text: {type(value).__name__}")

    def merge(old: Any, ours: Any, theirs: Any) -> str | bytes:
        # One side holding str is enough to make the merged value str:
        # the sides are the same key's value at different commits, so a
        # bytes side is the same text in another spelling.
        as_str = any(isinstance(v, str) for v in (old, ours, theirs))
        merged = merge_bytes(encode(old), encode(ours), encode(theirs))
        return merged.decode("utf-8") if as_str else merged

    return merge
