"""Shared commit/merge orchestration for versioned stores."""

import functools
import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, KeysView
from typing import Any, TypeVar

from ..errors import (
    ConcurrencyError,
    MergeConflict,
    UnknownBranchError,
    UnknownCommitError,
)
from .helpers import changes_as_diff, walk_history
from .merge import Change, MergeResolution, resolve_merge
from .protocol import DiffResult, MergePolicy, MergeResult, PostCheck

_F = TypeVar("_F", bound=Callable[..., Any])


class _CommitKeys(KeysView):
    """The keys of one commit, as ``VersionedBase.keys`` hands them out.

    A commit changes the handle's key dict in place. While a view (or
    an iteration over it) is alive, the handle changes a copy instead,
    so the view keeps the keys it was taken at, as a view of a dict the
    handle replaced would.
    """

    __slots__ = ("__weakref__", "_keys")

    def __init__(self, keys: dict[str, str]) -> None:
        super().__init__(keys)
        self._keys = keys

    def __iter__(self) -> Iterator[str]:
        # A generator, so a running iteration keeps its view alive.
        yield from self._keys

    def __repr__(self) -> str:
        return f"{type(self).__name__}({list(self._keys)!r})"


def _viewing(ref: "weakref.ref[_CommitKeys]", keys: dict[str, str]) -> bool:
    """Whether the view ``ref`` names is alive and over ``keys``."""
    view = ref()
    return view is not None and view._keys is keys


def _rolls_back(method: _F) -> _F:
    """Wrap a state-changing call: one that raises partway rolls back
    the attempt it opened, leaving the handle as it found it."""

    @functools.wraps(method)
    def wrapper(self: "VersionedBase", *args: Any, **kwargs: Any) -> Any:
        try:
            return method(self, *args, **kwargs)
        except BaseException:
            self._roll_back_attempt()
            raise
        finally:
            self._end_attempt()

    return wrapper  # type: ignore[return-value]


class VersionedBase(ABC):
    """Base class providing commit and merge orchestration.

    Subclasses implement storage-specific operations (CAS, commit
    creation, blob reading, etc.).  The shared ``commit()`` and
    ``_three_way_merge()`` methods handle the orchestration logic
    (fast-forward vs. merge, CAS retry, state rollback) identically
    for all backends.
    """

    def __init__(self, *, branch: str, commit_hash: str) -> None:
        self._branch = branch
        self._current_commit: str = commit_hash
        self._base_commit: str = commit_hash
        self._commit_keys: dict[str, str] = {}
        # Views handed out by ``keys()``, by weak reference.
        self._key_views: list[weakref.ref[_CommitKeys]] = []
        self._merge_fns: dict[str, MergePolicy] = {}
        self._merge_prefixes: dict[str, MergePolicy] = {}
        self._default_merge: MergePolicy | None = None
        self.last_merge_result: MergeResult | None = None

    # -- Properties --

    @property
    def current_commit(self) -> str:
        return self._current_commit

    @property
    def base_commit(self) -> str:
        return self._base_commit

    @property
    def current_branch(self) -> str:
        return self._branch

    @property
    def initial_commit(self) -> str:
        """The root commit hash (cached after first access)."""
        if not hasattr(self, "_initial_commit"):
            last = self._current_commit
            for commit in self.history():
                last = commit
            self._initial_commit = last
        return self._initial_commit

    def __repr__(self) -> str:
        n_keys = len(self._commit_keys)
        short_hash = self._current_commit[:8]
        return (
            f"{type(self).__name__}"
            f"(branch={self._branch!r}, commit={short_hash}..., keys={n_keys})"
        )

    # -- Read operations --

    def keys(self) -> Iterable[str]:
        """All keys in the current commit.

        The view keeps the keys it was taken at: a later commit doesn't
        change it.
        """
        view = _CommitKeys(self._commit_keys)
        self._key_views = [r for r in self._key_views if r() is not None]
        self._key_views.append(weakref.ref(view))
        return view

    def _owned_keys(self) -> dict[str, str]:
        """``_commit_keys``, ready to change in place.

        A commit changes the dict rather than rebuilding it, so its cost
        follows the size of the change. While a view from ``keys()`` is
        alive, the change goes to a copy instead and the view keeps the
        dict it was taken over.
        """
        keys = self._commit_keys
        if self._key_views:
            if any(_viewing(r, keys) for r in self._key_views):
                keys = self._commit_keys = dict(keys)
            self._key_views = []
        return keys

    def __contains__(self, key: str) -> bool:
        return key in self._commit_keys

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergePolicy) -> None:
        """Register a merge function, or a ``MergeChoice``, for one key."""
        self._merge_fns[key] = fn

    def set_merge_prefix(self, prefix: str, fn: MergePolicy) -> None:
        """Register a merge function, or a ``MergeChoice``, under a prefix.

        Prefixes cover keys whose names are not known when the policy is
        set (``"runs/"`` for ``runs/<id>``). A key takes the most
        specific registration: its exact key, else the longest
        registered prefix it starts with, else the default.

        What the registration holds decides how far it reaches. A merge
        function is consulted only where both sides changed a key. A
        ``MergeChoice`` is a standing policy over the whole prefix: it
        gives that side every key either side changed under it, so
        ``OURS`` also drops a key the other side added and keeps one the
        other side removed.
        """
        self._merge_prefixes[prefix] = fn

    def set_default_merge(self, fn: MergePolicy) -> None:
        """Register a default merge function for unregistered keys."""
        self._default_merge = fn

    # -- History and diff --

    def diff(self, commit_a: str, commit_b: str) -> DiffResult:
        """Compute key-level differences between two commits."""
        return changes_as_diff(self._changes(commit_a, commit_b))

    def history(
        self,
        commit_hash: str | None = None,
        *,
        all_parents: bool = False,
    ) -> Iterable[str]:
        """Yield the commit chain from newest to oldest."""
        start = commit_hash or self._current_commit
        yield from walk_history(start, self._load_parents, all_parents=all_parents)

    def parents(self, commit_hash: str | None = None) -> tuple[str, ...]:
        """Get the direct parent commit(s) of a commit."""
        target = commit_hash or self._current_commit
        return self._load_parents(target)

    def merge_base(self, commit_a: str, commit_b: str) -> str | None:
        """Lowest common ancestor of two commits, or None if unrelated.

        When several commits tie for lowest (criss-cross histories),
        the smallest hash wins — deterministic, but arbitrary. This is
        exactly the base a merge of the two commits would use.
        """
        return self._find_lca(commit_a, commit_b)

    # -- Commit orchestration --

    def _three_way_merge(
        self,
        their_head: str,
        *,
        on_conflict: str,
        merge_fns: dict[str, MergePolicy] | None,
        default_merge: MergePolicy | None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        post_check: PostCheck | None = None,
        info: dict | None,
        saved_state: tuple | None = None,
        cas_from: str | None = None,
        parents: tuple[str, ...] | None = None,
        base: str | None = None,
        strategy: str = "three_way",
        ancestor: tuple[str | None] | None = None,
    ) -> MergeResult:
        """Perform a three-way merge between our branch and their HEAD.

        ``their_head`` is any commit in the store — the moved HEAD of our
        own branch (the concurrent-write path) or another branch's HEAD
        (cross-branch merge). ``cas_from`` names the commit the merge
        commits on top of (defaults to ``their_head``, the concurrent
        case); cross-branch callers pass their own head.

        ``base`` replaces the common ancestor, which is how a change is
        applied rather than a history merged: the change from ``base`` to
        ``their_head`` lands on ours as an ordinary commit (``parents``
        of one), under ``strategy``. A change that leaves our state as it
        is commits nothing.

        ``ancestor`` passes in a common ancestor the caller has already
        found, as a one-tuple, so that None (no common ancestor) can be
        passed too.
        """
        if base is not None:
            lca: str | None = base
        elif ancestor is not None:
            (lca,) = ancestor
        else:
            lca = self._find_lca(self._current_commit, their_head)
        if lca is None:
            if saved_state is not None:
                self._restore_state(saved_state)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy=strategy,
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError(
                "No common ancestor found between current commit and HEAD."
            )

        # Each side's changes since the ancestor, read as structural
        # diffs: subtrees the two commits share are never read, so the
        # cost follows the size of the change rather than the keyset.
        our_changes = self._changes(lca, self._current_commit)
        their_changes = self._changes(lca, their_head)

        # Build effective merge function lookup
        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_prefixes = dict(self._merge_prefixes)
        if merge_prefixes:
            effective_prefixes.update(merge_prefixes)
        effective_default = default_merge or self._default_merge

        # Resolve the merge
        try:
            resolution = resolve_merge(
                our_changes,
                their_changes,
                blob_reader=self._read_blob,
                merge_fns=effective_fns,
                default_merge=effective_default,
                merge_prefixes=effective_prefixes,
            )
            if post_check is not None:
                refused = {
                    key
                    for key, value in resolution.merged_values.items()
                    if not post_check(key, value)
                }
                if refused:
                    raise MergeConflict(refused)
        except MergeConflict:
            if saved_state is not None:
                self._restore_state(saved_state)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy=strategy,
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise

        if base is not None and not resolution:
            # The change is already in our state, or changes nothing: an
            # answer that holds only while our state is the branch's.
            return self._live_no_op(self._current_commit, on_conflict)

        if parents is None:
            # Concurrent-write default: keep following the moved HEAD, as
            # before. Cross-branch callers pass (our_head, their_head) so
            # linear history stays on the merging branch (git convention).
            parents = (their_head, self._current_commit)

        self._create_merge_commit(resolution, parents, info)
        merge_hash = self._current_commit

        # CAS HEAD onto the merge commit: from their_head in the
        # concurrent-write path, from our own head cross-branch.
        if self._cas_head(cas_from or their_head, merge_hash):
            self._base_commit = merge_hash
            result = MergeResult(
                merged=True,
                commit=merge_hash,
                strategy=strategy,
                auto_merged_keys=resolution.auto_merged_keys,
                carried_keys=resolution.carried_keys,
            )
            self.last_merge_result = result
            return result

        if saved_state is not None:
            self._restore_state(saved_state)
        if on_conflict == "abandon":
            result = MergeResult(
                merged=False,
                commit=None,
                strategy=strategy,
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result
        raise ConcurrencyError(
            "HEAD changed during three-way merge. Refresh and retry."
        )

    @_rolls_back
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
        fast_forward: bool = True,
    ) -> MergeResult:
        """Merge another head into this branch.

        ``their_head`` is any commit in the store — usually another
        branch's HEAD. Finds the lowest common ancestor with our head,
        three-way resolves, and creates a two-parent merge commit,
        CAS-guarded on our own head (a race raises ``ConcurrencyError``
        and changes nothing). No common ancestor raises
        ``ConcurrencyError`` likewise without changing anything; retrying
        cannot help that case, unlike a race. A ``their_head`` not in the
        store raises ``UnknownCommitError`` before anything else is read.

        A head our history already contains merges as a no-op, provided
        the branch is still at our head. When ours
        is an ancestor of theirs, the branch fast-forwards — HEAD moves
        to ``their_head`` and no commit is written, so ``info`` is not
        recorded — unless ``fast_forward=False``, which writes the merge
        commit anyway.

        ``post_check`` runs over each merge-function-produced value;
        a False files that key as conflicted (handled per
        ``on_conflict`` like any other conflict).
        """
        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )
        # Read as empty, an unknown head would surface as a misleading
        # "no common ancestor".
        self._require_commits(their_head)
        our_head = self._current_commit
        lca = self._find_lca(our_head, their_head)
        if lca == their_head:
            return self._live_no_op(our_head, on_conflict)
        if lca == our_head and fast_forward:
            return self._fast_forward(their_head, on_conflict=on_conflict)
        return self._three_way_merge(
            their_head,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            merge_prefixes=merge_prefixes,
            default_merge=default_merge,
            post_check=post_check,
            info=info,
            saved_state=self._snapshot_state(),
            cas_from=our_head,
            parents=(our_head, their_head),
            ancestor=(lca,),
        )

    @_rolls_back
    def apply_change(
        self,
        base: str,
        target: str,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        post_check: PostCheck | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Apply the change from ``base`` to ``target`` as one commit.

        A three-way merge with ``base`` in place of the common ancestor:
        what ``target`` changed relative to ``base`` lands on our head,
        and our own changes since then are kept, conflicts resolved (or
        raised) as in a merge. The result is an ordinary single-parent
        commit, CAS-guarded on our head — a cherry-pick is
        ``apply_change(parent, commit)`` and a revert
        ``apply_change(commit, parent)``.

        A change already present commits nothing (``no_op``), provided the
        branch is still at our head: if it moved, the answer may no longer
        hold, and this raises ``ConcurrencyError`` (or abandons, with
        ``on_conflict='abandon'``) as a lost publish would.

        Raises:
            UnknownCommitError: if ``base`` or ``target`` is not in the
                store, checked before anything else is read.
            UnknownBranchError: if the branch was deleted.
            ConcurrencyError: if the branch moved during the apply, or
                away from our head when the change is already present.
            MergeConflict: if keys conflict and no merge function
                resolves them.
        """
        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )
        # A missing commit would read as an empty tree, and applying the
        # change to or from empty removes or adds every key.
        self._require_commits(base, target)
        our_head = self._current_commit
        return self._three_way_merge(
            target,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            merge_prefixes=merge_prefixes,
            default_merge=default_merge,
            post_check=post_check,
            info=info,
            saved_state=self._snapshot_state(),
            cas_from=our_head,
            parents=(our_head,),
            base=base,
            strategy="apply",
        )

    def _live_no_op(self, our_head: str, on_conflict: str) -> MergeResult:
        """A merge or apply that changes nothing, at ``our_head``.

        Nothing is written, so no publish guards this answer the way the
        other outcomes' CAS on our head does: it holds only if the branch
        is still where this handle left it. A branch that moved raises
        ``ConcurrencyError``, or abandons under ``on_conflict='abandon'``.
        """
        live = self.latest_head
        if live is None:
            raise UnknownBranchError(f"Branch '{self._branch}' has no HEAD")
        if live != our_head:
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="no_op",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError("HEAD changed during merge. Refresh and retry.")
        result = MergeResult(
            merged=True,
            commit=our_head,
            strategy="no_op",
            auto_merged_keys=(),
            carried_keys=(),
        )
        self.last_merge_result = result
        return result

    def _require_commits(self, *commits: str) -> None:
        """Raise ``UnknownCommitError`` for the first of ``commits`` that is
        not in the store."""
        missing = self._missing_commits(commits)
        for commit in commits:
            if commit in missing:
                raise UnknownCommitError(f"Commit '{commit}' does not exist")

    # -- Abstract methods (implemented by subclasses) --

    @abstractmethod
    def _fast_forward(self, their_head: str, *, on_conflict: str) -> MergeResult:
        """Move this branch's HEAD from our head to ``their_head``, a
        descendant of it, writing no commit."""

    @property
    @abstractmethod
    def latest_head(self) -> str | None:
        """Read HEAD directly from storage (reflects other writers)."""

    @abstractmethod
    def _snapshot_state(self) -> tuple:
        """Capture in-memory state before a commit attempt."""

    @abstractmethod
    def _restore_state(self, saved: tuple) -> None:
        """Restore in-memory state after a failed commit attempt."""

    def _roll_back_attempt(self) -> None:  # noqa: B027 - optional hook
        """Restore the state an attempt opened by ``_snapshot_state``
        started from, if one is still open: the call running it raised."""

    def _end_attempt(self) -> None:  # noqa: B027 - optional hook
        """Forget the open attempt, if any: the call running it is over."""

    @abstractmethod
    def _create_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
        chunks: dict[str, bytes] | None = None,
        chunk_refs: dict[str, list[str]] | None = None,
    ) -> str:
        """Create a single-parent commit with the given changes.

        Must update ``self._commit_keys`` and ``self._current_commit``.
        ``chunks`` / ``chunk_refs`` are the optional content-addressed
        chunks referenced by encoded blobs; backends that don't
        support them should ignore.
        """

    @abstractmethod
    def _create_merge_commit(
        self,
        resolution: MergeResolution,
        parents: tuple[str, ...],
        info: dict | None,
    ) -> str:
        """Create a commit from a resolved merge, on ``parents``: our
        state with the resolution's changes applied.

        Must update ``self._commit_keys`` and ``self._current_commit``.
        """

    @abstractmethod
    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD from expected to new_head."""

    @abstractmethod
    def _changes(self, commit_a: str | None, commit_b: str) -> dict[str, Change]:
        """Each key whose value differs from commit ``a`` to ``b``; a
        ``None`` or missing ``a`` reads as empty."""

    @abstractmethod
    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""

    @abstractmethod
    def _missing_commits(self, commits: Iterable[str]) -> set[str]:
        """Which of ``commits`` are not in the store."""

    @abstractmethod
    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits."""

    @abstractmethod
    def _read_blob(self, content_id: str) -> bytes | None:
        """Read a blob by its content identifier."""
