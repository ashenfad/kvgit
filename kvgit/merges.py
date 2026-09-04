"""Text merge with conflict markers.

A batteries-included ``BytesMergeFn`` for line-oriented text: disjoint
changes merge cleanly, overlapping changes come back with git-style
``<<<<<<<`` markers instead of raising. Register it as a per-key fn or
as ``default_merge`` wherever text blobs merge (see ``resolve_merge``).

Anything that cannot be marked — undecodable bytes, NUL bytes, inputs
over the size cap — raises :class:`CantMark`, which the merge machinery
files as an ordinary conflict. Callers that need to tell "no encoding"
from "too big" catch it themselves; callers that don't let it flow.
"""

from __future__ import annotations

import difflib
from collections.abc import Callable

from .versioned.protocol import BytesMergeFn

#: Inputs whose combined size exceeds this are refused with
#: :class:`CantMark` rather than merged. Line-diffing is quadratic-ish
#: in the worst case (minified single-line megabytes); a merge this big
#: is a whole-file conflict by inspection anyway.
MAX_MARK_BYTES = 1024 * 1024


class CantMark(Exception):
    """A value cannot be marker-merged: not text, or too big.

    Raised instead of returning bytes when the inputs are undecodable as
    UTF-8, contain NUL bytes, or exceed ``MAX_MARK_BYTES`` combined.
    The merge machinery reports it as a conflict for that key.
    """


def _decode(data: bytes | None) -> list[str]:
    """Split one side into lines; a removed side is the empty list.

    The markers then show one populated side against nothing, which
    reads correctly.
    """
    if data is None:
        return []
    if b"\x00" in data:
        raise CantMark("NUL byte: not text")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as e:
        raise CantMark(f"not UTF-8: {e}") from e
    return text.splitlines(keepends=True)


def _changes(base: list[str], side: list[str]) -> list[tuple[int, int, list[str]]]:
    """Non-equal opcode intervals of ``side`` against ``base``.

    Each tuple is ``(base_start, base_end, replacement_lines)``;
    pure insertions have ``base_start == base_end``.
    """
    matcher = difflib.SequenceMatcher(None, base, side, autojunk=False)
    return [
        (i1, i2, side[j1:j2])
        for tag, i1, i2, j1, j2 in matcher.get_opcodes()
        if tag != "equal"
    ]


def _merge_lines(
    base: list[str],
    ours: list[str],
    theirs: list[str],
    ours_label: str,
    theirs_label: str,
) -> tuple[list[str], bool]:
    """Three-way line merge. Returns ``(lines, conflicted)``."""
    if ours == theirs:
        return list(ours), False

    # Whole-file deletion on one side against any change on the other is
    # a modify/delete conflict with the survivor shown whole — the
    # interval machinery below only sees hunks and would drop the
    # surviving context. (Unchanged-vs-deleted resolves above through
    # the empty-changes fast paths.)
    if base and not ours and theirs != base:
        return (
            [
                f"<<<<<<< {ours_label}\n",
                "=======\n",
                *theirs,
                f">>>>>>> {theirs_label}\n",
            ],
            True,
        )
    if base and not theirs and ours != base:
        return (
            [
                f"<<<<<<< {ours_label}\n",
                *ours,
                "=======\n",
                f">>>>>>> {theirs_label}\n",
            ],
            True,
        )

    our_changes = _changes(base, ours)
    their_changes = _changes(base, theirs)
    if not our_changes:
        return list(theirs), False
    if not their_changes:
        return list(ours), False

    # Merge overlapping change intervals into conflict groups; disjoint
    # intervals resolve independently. Intervals are half-open over base
    # positions; pure insertions are points. Join on strict overlap, plus
    # same-point inserts colliding with each other — an insert merely
    # adjacent to a change interval resolves cleanly beside it, while two
    # different inserts at one point genuinely conflict.
    events = [("ours", s, e, lines) for s, e, lines in our_changes]
    events += [("theirs", s, e, lines) for s, e, lines in their_changes]
    events.sort(key=lambda ev: (ev[1], ev[2]))

    # Each group tracks its member events plus its max base end, since
    # overlap is against the group's full span, not the last event.
    groups: list[list[tuple]] = []
    group_ends: list[int] = []
    for ev in events:
        _, s, e, _ = ev
        if groups and (
            s < group_ends[-1]
            or (s == e and any(g[1] == g[2] == s for g in groups[-1]))
        ):
            groups[-1].append(ev)
            group_ends[-1] = max(group_ends[-1], e)
        else:
            groups.append([ev])
            group_ends.append(e)

    out: list[str] = []
    conflicted = False
    pos = 0
    for group in groups:
        start = min(ev[1] for ev in group)
        end = max(ev[2] for ev in group)
        out.extend(base[pos:start])
        sides = {ev[0] for ev in group}
        ours_lines = [
            ln for side, _, _, lines in group if side == "ours" for ln in lines
        ]
        theirs_lines = [
            ln for side, _, _, lines in group if side == "theirs" for ln in lines
        ]
        if sides == {"ours"}:
            out.extend(ours_lines)
        elif sides == {"theirs"}:
            out.extend(theirs_lines)
        elif ours_lines == theirs_lines:
            out.extend(ours_lines)
        else:
            conflicted = True
            out.append(f"<<<<<<< {ours_label}\n")
            out.extend(ours_lines)
            out.append("=======\n")
            out.extend(theirs_lines)
            out.append(f">>>>>>> {theirs_label}\n")
        pos = end
    out.extend(base[pos:])

    # A line without a trailing newline glued onto following output
    # would corrupt both; clean regions stay byte-exact (a final line
    # keeps its missing newline), markers pay the newline tax.
    fixed: list[str] = []
    for i, ln in enumerate(out):
        if not ln.endswith("\n") and i + 1 < len(out):
            ln += "\n"
        fixed.append(ln)
    return fixed, conflicted


def make_text_merge(
    *, ours_label: str = "ours", theirs_label: str = "theirs"
) -> BytesMergeFn:
    """Build a marker-merge fn with custom conflict labels.

    Labels ride git's positions (``<<<<<<< <ours>`` /
    ``>>>>>>> <theirs>``); pass branch names so conflicts read
    attributably. Labels may not contain newlines.
    """
    for name, label in (("ours_label", ours_label), ("theirs_label", theirs_label)):
        if "\n" in label:
            raise ValueError(f"{name} may not contain a newline: {label!r}")

    def merge(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
        total = sum(len(d) for d in (old, ours, theirs) if d is not None)
        if total > MAX_MARK_BYTES:
            raise CantMark(f"inputs total {total} bytes over cap {MAX_MARK_BYTES}")
        base = _decode(old)
        our_lines = _decode(ours)
        their_lines = _decode(theirs)
        merged, _ = _merge_lines(base, our_lines, their_lines, ours_label, theirs_label)
        return "".join(merged).encode("utf-8")

    return merge


def text(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
    """Marker-merge with default labels. Usable as ``default_merge`` directly."""
    return make_text_merge()(old, ours, theirs)


TextMergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
"""Type of what :func:`make_text_merge` builds (== ``BytesMergeFn``)."""
