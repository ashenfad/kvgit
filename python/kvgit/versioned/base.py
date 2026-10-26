"""Shared commit/merge orchestration for versioned stores."""

import weakref
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, KeysView
from typing import Any, TypeVar

from .helpers import changes_as_diff, walk_history
from .merge import Change
from .protocol import DiffResult, MergePolicy, MergeResult

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

    # -- Abstract methods (implemented by subclasses) --

    @property
    @abstractmethod
    def latest_head(self) -> str | None:
        """Read HEAD directly from storage (reflects other writers)."""

    @abstractmethod
    def _changes(self, commit_a: str | None, commit_b: str) -> dict[str, Change]:
        """Each key whose value differs from commit ``a`` to ``b``; a
        ``None`` or missing ``a`` reads as empty."""

    @abstractmethod
    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""

    @abstractmethod
    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits."""
