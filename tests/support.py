"""Shared test helpers."""

from kvgit import Repo, Worktree
from kvgit.kv.base import KVStore
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import _load_root


def worktree(
    store: KVStore | None = None, branch: str = "main", **repo_options
) -> Worktree:
    """A worktree on ``branch`` of a repository over ``store``, created if new.

    Repository options (``codec``, ``merge_fns``, ...) pass through.
    """
    repo = Repo(store if store is not None else Memory(), **repo_options)
    return repo.worktree(branch, create=True)


def fork(wt: Worktree, name: str, at: str | None = None) -> Worktree:
    """A new branch at ``at`` (default: ``wt``'s head), checked out."""
    wt.repo.branches.create(name, at=at or wt.head)
    return wt.repo.worktree(name)


def pointers(store, commit: str) -> dict[str, str]:
    """A commit's keyset as key -> blob pointer."""
    root = _load_root(store, commit)
    if root is None:
        return {}
    return {k: e.blob for k, e in Keyset(store, root=root).materialize().items()}
