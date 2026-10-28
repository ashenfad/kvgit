"""Text merge with conflict markers.

A batteries-included bytes-level merge for line-oriented text: disjoint
changes merge cleanly, overlapping changes come back with git-style
``<<<<<<<`` markers instead of raising. Register it as a per-key fn or
as ``default_merge`` wherever values are ``bytes``; for ``str`` values,
use :func:`kvgit.text_merge`.

Anything that cannot be marked — undecodable bytes, NUL bytes, inputs
over the size cap — raises :class:`CantMark`, which the merge machinery
files as an ordinary conflict. Callers that need to tell "no encoding"
from "too big" catch it themselves; callers that don't let it flow.

Also here: :func:`ours` and :func:`theirs`, the pick-a-side merges, for
keys where one branch simply owns the value.
"""

from __future__ import annotations

from collections.abc import Callable

from . import _core
from ._types import BytesMergeFn, MergeChoice

#: Inputs whose combined size exceeds this are refused with
#: :class:`CantMark` rather than merged. Line-diffing is quadratic-ish
#: in the worst case (minified single-line megabytes); a merge this big
#: is a whole-file conflict by inspection anyway.
MAX_MARK_BYTES: int = _core.MAX_MARK_BYTES


class CantMark(Exception):
    """A value cannot be marker-merged: not text, too big, or a conflict
    under ``strict=True``.

    Raised instead of returning bytes when the inputs are undecodable as
    UTF-8, contain NUL bytes, or exceed ``MAX_MARK_BYTES`` combined — or,
    when built with ``strict=True``, when the merge would have written
    conflict markers. The merge machinery reports it as a conflict for
    that key.
    """


def text_merge_result(
    old: bytes | None,
    ours: bytes | None,
    theirs: bytes | None,
    *,
    ours_label: str = "ours",
    theirs_label: str = "theirs",
) -> tuple[bytes, bool]:
    """Marker-merge returning ``(merged_bytes, conflicted)``.

    Same merge as :func:`make_text_merge`, but the flag tells whether the
    merge introduced conflict hunks — reliably, where counting
    ``<<<<<<<`` lines cannot, because the inputs may legitimately contain
    marker-like lines that merge through untouched. Anything unmarkable
    still raises :class:`CantMark`.
    """
    return _core.text_merge(old, ours, theirs, ours_label, theirs_label, CantMark)


def make_text_merge(
    *,
    ours_label: str = "ours",
    theirs_label: str = "theirs",
    strict: bool = False,
) -> BytesMergeFn:
    """Build a marker-merge fn with custom conflict labels.

    Labels ride git's positions (``<<<<<<< <ours>`` /
    ``>>>>>>> <theirs>``); pass branch names so conflicts read
    attributably. Labels may not contain newlines (any ``str.splitlines``
    boundary: LF, lone CR, VT/FF, FS/GS/RS, NEL, U+2028/2029).

    With ``strict=True`` the fn raises :class:`CantMark` instead of
    writing markers when sides conflict — for branches where a true
    conflict must abort the commit rather than land hunks. The merge
    machinery files it as an ordinary conflict, same as unmarkable
    input.
    """
    _core.check_text_labels(ours_label, theirs_label)

    def merge(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
        merged, conflicted = text_merge_result(
            old, ours, theirs, ours_label=ours_label, theirs_label=theirs_label
        )
        if conflicted and strict:
            raise CantMark("strict text merge: conflicting changes cannot be marked")
        return merged

    return merge


def text(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
    """Marker-merge with default labels. Usable as ``default_merge`` directly."""
    return make_text_merge()(old, ours, theirs)


TextMergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
"""Type of what :func:`make_text_merge` builds (== ``BytesMergeFn``)."""


def ours(old: bytes | None, our: bytes | None, their: bytes | None) -> MergeChoice:
    """Take our side: the merged value is our committed value.

    Keeps our existing blob pointer rather than rewriting the value, so
    the merge writes nothing for this key. If we removed the key, the
    merge removes it. Usable as a per-key, per-prefix, or default merge.
    """
    return MergeChoice.OURS


def theirs(old: bytes | None, our: bytes | None, their: bytes | None) -> MergeChoice:
    """Take their side: the merged value is their committed value.

    Keeps their existing blob pointer rather than rewriting the value,
    so the merge writes nothing for this key. If they removed the key,
    the merge removes it. Usable as a per-key, per-prefix, or default
    merge.
    """
    return MergeChoice.THEIRS
