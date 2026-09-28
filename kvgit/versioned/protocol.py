"""Types shared by the commit log and its callers."""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class MergeChoice(Enum):
    """One side of a merge, named as the answer for a key.

    Two ways to use it, with deliberately different reach:

    * **Returned by a merge function**, in place of bytes: the merged
      value for that contested key is that side's committed value,
      unchanged. The merge keeps that side's existing blob pointer, so
      no new blob is written; when that side removed the key, the merge
      removes it.
    * **Registered in place of a merge function** (``set_merge_prefix``,
      ``set_merge_fn``, or the per-call maps): a standing policy that
      hands the key to one side outright. Unlike a merge function, which
      is consulted only where both sides changed a key, a registered
      choice governs *every* key either side changed under it — so
      ``OURS`` also drops a key the other side added and keeps one the
      other side removed. Nothing is read or decoded for those keys.
    """

    OURS = "ours"
    THEIRS = "theirs"


BytesMergeFn = Callable[
    [bytes | None, bytes | None, bytes | None], "bytes | MergeChoice"
]
"""Merge function: (old_value, our_value, their_value) -> merged_value.

Any argument can be None (key absent or removed on that side). Returning
a :class:`MergeChoice` instead of bytes keeps that side's existing value
without writing a new blob.
"""

MergePolicy = BytesMergeFn | MergeChoice
"""What a merge registration holds.

Either a :data:`BytesMergeFn`, consulted for keys both sides changed, or
a :class:`MergeChoice`, which gives one side every key it covers.
"""

PostCheck = Callable[[str, bytes], bool]
"""Post-merge predicate: (key, merged_bytes) -> accept?

Runs over values a merge function produced. Returning False files the
key as conflicted, as if no merge function had resolved it. kvgit never
inspects the bytes itself — callers that know what their values mean
(marker scans, schema checks) decide.

A merge function that answers with a :class:`MergeChoice` produces no
new value, so there are no bytes to check and the predicate does not run
for that key.
"""


@dataclass(frozen=True)
class DiffResult:
    """Key-level differences between two commits."""

    added: frozenset[str]
    removed: frozenset[str]
    modified: frozenset[str]


@dataclass(frozen=True)
class TagInfo:
    """What a store records about one tag.

    ``time`` and ``info`` come from a record written just after the tag
    itself, so both are ``None`` for a tag whose record is missing —
    a crash between the two writes, or a store written by hand.

    ``dangling`` means the tagged commit is not in the store. A tag
    cannot be created for a commit that does not exist, so this is
    damage rather than an ordinary state, and a dangling tag keeps
    nothing alive: garbage collection marks nothing from it.
    """

    name: str
    commit: str
    time: float | None
    info: dict | None
    dangling: bool


@dataclass(frozen=True)
class MergeResult:
    """What a commit, merge, or applied change did. Truthy when it landed.

    Returned by ``Worktree.commit``, ``merge``, ``apply``,
    ``cherry_pick`` and ``revert``.
    """

    merged: bool
    """Whether the call landed; False when it abandoned
    (``on_conflict="abandon"``)."""

    commit: str | None
    """The commit the branch is at afterwards; None when abandoned."""

    strategy: str
    """How it landed:

    * ``"no_op"`` -- nothing to do: no changes to commit, or the branch
      already contains what was merged or applied.
    * ``"fast_forward"`` -- a commit on a branch that had not moved, or a
      merge that moved the branch to theirs without a merge commit.
    * ``"three_way"`` -- a merge commit, from a merge or from a commit
      that found the branch had moved.
    * ``"apply"`` -- a change applied as a single-parent commit
      (``apply``, ``cherry_pick``, ``revert``).
    """

    auto_merged_keys: tuple[str, ...]
    """Keys a merge rule decided: keys both sides changed that a merge
    function (or the ``MergeChoice`` it returned) resolved, and keys under
    a registered ``MergeChoice`` that either side changed."""

    carried_keys: tuple[str, ...]
    """Keys the other side changed that the merge took as they were: the
    other writer's changes on a lost race, the merged branch's on a merge
    or fast-forward, the picked change on ``apply``. Empty for a commit
    with no other side and for ``no_op``; a key both sides changed alike
    appears in neither list."""

    def __bool__(self) -> bool:
        return self.merged
