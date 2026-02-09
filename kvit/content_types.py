"""Merge functions for typed values."""

from typing import Any, Callable

MergeFn = Callable[[Any | None, Any, Any], Any]
"""Merge function: (old_value | None, our_value, their_value) -> merged_value.

Any argument can be None (key absent or removed on that side).
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
