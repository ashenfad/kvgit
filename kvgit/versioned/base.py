"""Shared commit/merge orchestration for versioned stores."""

from abc import ABC, abstractmethod
from typing import Iterable

from ..errors import ConcurrencyError, MergeConflict
from .helpers import diff_keysets, walk_history
from .merge import MergeResolution, resolve_merge
from .protocol import BytesMergeFn, DiffResult, MergeResult


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
        self._merge_fns: dict[str, BytesMergeFn] = {}
        self._default_merge: BytesMergeFn | None = None
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
        """All keys in the current commit."""
        return self._commit_keys.keys()

    def __contains__(self, key: str) -> bool:
        return key in self._commit_keys

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: BytesMergeFn) -> None:
        """Register a merge function for a specific key."""
        self._merge_fns[key] = fn

    def set_default_merge(self, fn: BytesMergeFn) -> None:
        """Register a default merge function for unregistered keys."""
        self._default_merge = fn

    # -- History and diff --

    def diff(self, commit_a: str, commit_b: str) -> DiffResult:
        """Compute key-level differences between two commits."""
        return diff_keysets(self._load_keyset(commit_a), self._load_keyset(commit_b))

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

    # -- Commit orchestration --

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, BytesMergeFn] | None = None,
        default_merge: BytesMergeFn | None = None,
        info: dict | None = None,
        chunks: dict[str, bytes] | None = None,
        chunk_refs: dict[str, list[str]] | None = None,
    ) -> MergeResult:
        """Commit changes and atomically advance HEAD.

        Creates a new commit with the given changes and advances the
        branch HEAD.  If HEAD has diverged, performs a three-way merge.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.
            on_conflict: ``'raise'`` (default) or ``'abandon'`` for CAS failures.
            merge_fns: Per-key merge functions (override instance-level).
            default_merge: Default merge function (override instance-level).
            info: Optional metadata dict for the commit.
            chunks: Optional content-addressed chunks to write under
                ``kvgit:chunk:<hash>``. Keyed by chunk hash. Backends
                that don't understand chunks ignore this argument.
            chunk_refs: Optional per-key list of chunk hashes referenced
                by that key's encoded blob. Stored on the keyset
                ``MetaEntry.chunks`` so GC can trace reachability.

        Returns:
            A ``MergeResult`` (truthy when committed, falsy if abandoned).

        Raises:
            ConcurrencyError: If ``on_conflict='raise'`` and CAS fails.
            MergeConflict: If keys conflict and no merge function
                resolves them.
        """
        # No-op if no changes
        if not updates and not removals and info is None:
            result = MergeResult(
                merged=True,
                commit=self._current_commit,
                strategy="no_op",
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result

        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )

        current_head = self.latest_head

        if current_head == self._base_commit:
            # Fast-forward path
            saved = self._snapshot_state()
            self._create_commit(
                updates,
                removals,
                info=info,
                chunks=chunks,
                chunk_refs=chunk_refs,
            )

            if self._cas_head(self._base_commit, self._current_commit):
                self._base_commit = self._current_commit
                result = MergeResult(
                    merged=True,
                    commit=self._current_commit,
                    strategy="fast_forward",
                    auto_merged_keys=(),
                    carried_keys=tuple(self._commit_keys.keys()),
                )
                self.last_merge_result = result
                return result
            self._restore_state(saved)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="fast_forward",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError(
                f"HEAD changed from {self._base_commit}. Refresh and retry."
            )

        # Three-way merge path
        if current_head is None:
            raise ValueError(f"Branch '{self._branch}' has no HEAD")
        saved = self._snapshot_state()
        self._create_commit(
            updates,
            removals,
            chunks=chunks,
            chunk_refs=chunk_refs,
        )
        return self._three_way_merge(
            current_head,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            default_merge=default_merge,
            info=info,
            saved_state=saved,
        )

    def _three_way_merge(
        self,
        their_head: str,
        *,
        on_conflict: str,
        merge_fns: dict[str, BytesMergeFn] | None,
        default_merge: BytesMergeFn | None,
        info: dict | None,
        saved_state: tuple | None = None,
    ) -> MergeResult:
        """Perform a three-way merge between our branch and their HEAD."""
        lca = self._find_lca(self._current_commit, their_head)
        if lca is None:
            if saved_state is not None:
                self._restore_state(saved_state)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="three_way",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError(
                "No common ancestor found between current commit and HEAD."
            )

        # Load each unique commit's keyset exactly once. The naive
        # form (calling self.diff() then self._load_keyset() three
        # more times in resolve_merge) loads each commit twice or
        # three times. For backends with non-trivial per-call
        # latency, deduping cuts merge round-trips by ~60%.
        lca_keyset = self._load_keyset(lca)
        our_keyset = self._load_keyset(self._current_commit)
        their_keyset = self._load_keyset(their_head)

        our_diff = diff_keysets(lca_keyset, our_keyset)
        their_diff = diff_keysets(lca_keyset, their_keyset)

        # Build effective merge function lookup
        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_default = default_merge or self._default_merge

        # Resolve the merge
        try:
            resolution = resolve_merge(
                lca_keyset=lca_keyset,
                our_keyset=our_keyset,
                their_keyset=their_keyset,
                our_diff=our_diff,
                their_diff=their_diff,
                blob_reader=self._read_blob,
                merge_fns=effective_fns,
                default_merge=effective_default,
            )
        except MergeConflict:
            if saved_state is not None:
                self._restore_state(saved_state)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="three_way",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise

        auto_merged = resolution.auto_merged_keys
        parents = (their_head, self._current_commit)

        self._create_merge_commit(resolution, parents, info)
        merge_hash = self._current_commit
        merged_keyset = self._commit_keys

        # CAS HEAD from their_head to merge_hash
        if self._cas_head(their_head, merge_hash):
            self._base_commit = merge_hash
            result = MergeResult(
                merged=True,
                commit=merge_hash,
                strategy="three_way",
                auto_merged_keys=tuple(auto_merged),
                carried_keys=tuple(
                    k
                    for k in merged_keyset
                    if k not in auto_merged and k not in resolution.merged_values
                ),
            )
            self.last_merge_result = result
            return result

        if saved_state is not None:
            self._restore_state(saved_state)
        if on_conflict == "abandon":
            result = MergeResult(
                merged=False,
                commit=None,
                strategy="three_way",
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result
        raise ConcurrencyError(
            "HEAD changed during three-way merge. Refresh and retry."
        )

    # -- Abstract methods (implemented by subclasses) --

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
        """Create a multi-parent merge commit from a resolved merge.

        Must update ``self._commit_keys`` and ``self._current_commit``.
        """

    @abstractmethod
    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD from expected to new_head."""

    @abstractmethod
    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        """Load the keyset for a commit (key -> content identifier)."""

    @abstractmethod
    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""

    @abstractmethod
    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits."""

    @abstractmethod
    def _read_blob(self, content_id: str) -> bytes | None:
        """Read a blob by its content identifier."""
