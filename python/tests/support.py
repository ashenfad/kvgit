"""Shared test helpers."""

import time

from kvgit import Repo, Worktree, _core
from kvgit._store import BRANCH_HEAD, COMMIT_ROOT, EMPTY_HASH, GC_LEASE_KEY, gc
from kvgit.encoding import dumps, safe_loads
from kvgit.kv.base import KVStore
from kvgit.kv.memory import Memory


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


def root_of(store, commit: str) -> str | None:
    """A commit's keyset root as stored, or None."""
    raw = store.get(COMMIT_ROOT % commit)
    root = safe_loads(raw) if raw is not None else None
    return root if isinstance(root, str) else None


def entries(store, commit: str) -> dict:
    """A commit's keyset as key -> its stored entry."""
    root = root_of(store, commit)
    if root is None or root == EMPTY_HASH:
        return {}
    op = _core.TreeOp.walk("kvgit:keyset:", 8, root, set(), True)
    kind, payload = op.resume()
    while kind == "get":
        kind, payload = op.resume(dict(store.get_many(payload)))
    return payload[0]


def pointers(store, commit: str) -> dict[str, str]:
    """A commit's keyset as key -> blob pointer."""
    return {k: e.blob for k, e in entries(store, commit).items()}


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


def clean_orphans(store: KVStore, min_age: float = 3600) -> int:
    """A sweep without the namespace scan, waiting out a busy lease."""
    return gc(store, min_age)


def deep_clean(store: KVStore, min_age: float = 3600) -> int:
    """A sweep with the namespace scan, raising on a busy lease."""
    return gc(store, min_age, deep=True, wait=False)
