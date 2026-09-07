"""Versioned protocol and types."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, runtime_checkable


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
    """Result of a merge operation."""

    merged: bool
    commit: str | None
    strategy: str  # "no_op", "fast_forward", "three_way"
    auto_merged_keys: tuple[str, ...]
    carried_keys: tuple[str, ...]

    def __bool__(self) -> bool:
        return self.merged


@runtime_checkable
class Versioned(Protocol):
    """Protocol for versioned key-value stores.

    Defines the common interface implemented by ``VersionedKV``.
    """

    last_merge_result: MergeResult | None

    @property
    def current_commit(self) -> str: ...

    @property
    def base_commit(self) -> str: ...

    @property
    def current_branch(self) -> str: ...

    @property
    def latest_head(self) -> str | None: ...

    @property
    def initial_commit(self) -> str: ...

    # -- Read operations --

    def get(self, key: str) -> bytes | None: ...

    def get_many(self, *keys: str) -> dict[str, bytes]: ...

    def keys(self) -> Iterable[str]: ...

    def __contains__(self, key: str) -> bool: ...

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergePolicy) -> None: ...

    def set_merge_prefix(self, prefix: str, fn: MergePolicy) -> None: ...

    def set_default_merge(self, fn: MergePolicy) -> None: ...

    # -- Write operations --

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        info: dict | None = None,
        chunks: dict[str, bytes] | None = None,
        chunk_refs: dict[str, list[str]] | None = None,
    ) -> MergeResult: ...

    def merge_heads(
        self,
        their_head: str,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        post_check: PostCheck | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Merge another head (any commit in the store, usually another
        branch's HEAD) into this branch: LCA + three-way resolve + a
        two-parent merge commit, CAS-guarded on our own head.
        """
        ...

    def refresh(self) -> None: ...

    def checkout(
        self,
        commit_hash: str | None = None,
        *,
        branch: str | None = None,
        tag: str | None = None,
    ) -> "Versioned | None": ...

    def create_branch(self, name: str, *, at: str | None = None) -> "Versioned": ...

    def delete_branch(self, name: str) -> None: ...

    def switch_branch(self, name: str) -> None: ...

    def peek(
        self, key: str, *, branch: str | None = None, tag: str | None = None
    ) -> bytes | None: ...

    # -- Tags --

    def tag(
        self, name: str, *, at: str | None = None, info: dict | None = None
    ) -> str: ...

    def tags(self) -> dict[str, str]: ...

    def tag_info(self, name: str) -> TagInfo | None: ...

    def delete_tag(self, name: str) -> None: ...

    def reset_to(self, commit_hash: str) -> bool: ...

    def history(
        self, commit_hash: str | None = None, *, all_parents: bool = False
    ) -> Iterable[str]: ...

    def list_branches(self) -> list[str]: ...

    def commit_info(self, commit_hash: str | None = None) -> dict | None: ...

    def diff(self, commit_a: str, commit_b: str) -> DiffResult: ...

    def parents(self, commit_hash: str | None = None) -> tuple[str, ...]: ...
