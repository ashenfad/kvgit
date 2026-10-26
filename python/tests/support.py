"""Shared test helpers."""

import time

from kvgit import Repo, Worktree
from kvgit.encoding import dumps
from kvgit.kv.base import KVStore
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import BRANCH_HEAD, GC_LEASE_KEY, _load_root


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


def race_publish(store: KVStore, branch: str, advance, times: int | None = 1) -> None:
    """Run ``advance`` just before each of the next ``times`` writes that
    move ``branch``'s head (every one, for ``None``), so each publish
    finds the branch moved. Writes ``advance`` makes itself go through
    untouched.
    """
    real = store.cas_many
    head = BRANCH_HEAD % branch
    left = times
    racing = False

    def cas_many(expected, writes, removes=()):
        nonlocal left, racing
        if head in writes and not racing and (left is None or left > 0):
            if left is not None:
                left -= 1
            racing = True
            try:
                advance()
            finally:
                racing = False
        return real(expected, writes, removes)

    store.cas_many = cas_many  # type: ignore[method-assign]


def fail_reads(store: KVStore, when, error: Exception) -> None:
    """Raise ``error`` from the first ``get_many`` whose keys ``when``
    holds for, then read normally."""
    real = store.get_many
    armed = True

    def get_many(*args):
        nonlocal armed
        keys = (
            list(args[0])
            if len(args) == 1 and not isinstance(args[0], str)
            else list(args)
        )
        if armed and when(keys):
            armed = False
            raise error
        return real(keys)

    store.get_many = get_many  # type: ignore[method-assign]


def hold_lease(store: KVStore, ttl: float) -> bytes:
    """Take the GC lease as a sweep would, for ``ttl`` seconds, and
    return the record written: a sweep held open by the test."""
    raw = store.get(GC_LEASE_KEY)
    ours = dumps({"owner": "a test's sweep", "expires": time.time() + ttl})
    assert store.cas(GC_LEASE_KEY, ours, expected=raw), "the lease was taken"
    return ours


def release_lease(store: KVStore, ours: bytes) -> None:
    """Give up a lease :func:`hold_lease` took, as a sweep does."""
    expired = dumps({"owner": "a test's sweep", "expires": 0})
    store.cas(GC_LEASE_KEY, expired, expected=ours)
