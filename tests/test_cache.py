"""The repository's cache of what its store never rewrites."""

import threading

import pytest

from kvgit import Repo
from kvgit.cache import CachedStore, ContentCache, cacheable
from kvgit.errors import UnknownCommitError
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    COMMIT_ROOT,
    GC_LEASE_KEY,
    PARENT_COMMIT,
)


class Counting(Memory):
    """A backend that counts the reads that reach it."""

    def __init__(self):
        super().__init__()
        self.reads = 0
        self.keys_read: list[str] = []

    def get(self, key):
        self.reads += 1
        self.keys_read.append(key)
        return super().get(key)

    def get_many(self, *args):
        keys = list(self._normalize_keys(args))
        self.reads += 1
        self.keys_read.extend(keys)
        return super().get_many(keys)

    def reset(self):
        self.reads = 0
        self.keys_read = []


def _filled(store=None, *, n=500, **kwargs):
    store = store if store is not None else Counting()
    repo = Repo(store, **kwargs)
    wt = repo.worktree("main", create=True)
    for i in range(n):
        wt[f"k{i}"] = i
    wt.commit()
    return store, repo, wt


# -- what is cached ------------------------------------------------------------


def test_only_write_once_keys_are_cacheable():
    assert cacheable("kvgit:keyset:abc")
    assert cacheable(PARENT_COMMIT % "abc")
    # The commit root answers whether a commit exists, so it is always read.
    assert not cacheable(COMMIT_ROOT % "abc")
    assert not cacheable(BRANCH_HEAD % "main")
    assert not cacheable(GC_LEASE_KEY)
    assert not cacheable("kvgit:blob:abc")
    assert not cacheable("__tag_info__v1")


def test_back_to_back_commits_read_no_tree_from_the_store():
    store, _, wt = _filled()
    for n in range(3):
        store.reset()
        wt["k1"] = n
        wt.commit()
        assert not [k for k in store.keys_read if k.startswith("kvgit:keyset:")]


def test_a_snapshot_reads_its_root_and_its_blob_and_nothing_else():
    store, repo, wt = _filled()
    repo.snapshot(commit=wt.head)["k7"]  # warm
    store.reset()
    assert repo.snapshot(commit=wt.head)["k7"] == 7
    assert COMMIT_ROOT % wt.head in store.keys_read
    assert not [k for k in store.keys_read if k.startswith("kvgit:keyset:")]
    assert store.reads == 2


def test_hits_and_misses_are_counted():
    _, repo, wt = _filled()
    before = repo.cache.hits
    repo.snapshot(commit=wt.head)["k3"]
    assert repo.cache.hits > before
    assert repo.cache.misses > 0


def test_absence_is_not_remembered():
    store = Memory()
    cache = ContentCache()
    cached = CachedStore(store, cache)
    key = "kvgit:keyset:later"
    assert cached.get(key) is None
    store.set(key, b"node")
    assert cached.get(key) == b"node"


# -- what empties it -----------------------------------------------------------


def test_another_processs_sweep_empties_the_cache_before_the_next_commit():
    """Two repositories on one backend stand in for two processes. The
    other one's sweep rewrites the lease record; this one's next commit
    reads it with HEAD and starts from an empty cache."""
    backend = Memory()
    _, ours, wt = _filled(backend)
    theirs = Repo(backend)
    assert len(ours.cache) > 0

    theirs.gc(min_age=0)
    wt["k1"] = "after the sweep"
    wt.commit()

    assert ours.cache.clears == 1
    assert ours.snapshot(branch="main")["k1"] == "after the sweep"


def test_this_processs_own_sweep_empties_it_too():
    _, repo, wt = _filled()
    wt["k1"] = "x"
    wt.commit()
    repo.gc(min_age=0)
    assert repo.cache.clears >= 1


def test_a_commit_another_process_swept_still_reads_as_gone():
    """Its tree may still be in memory; its root, which is never cached,
    is what says whether it exists."""
    backend = Memory()
    _, ours, wt = _filled(backend)
    abandoned = wt.head
    repo_b = Repo(backend)
    repo_b.branches.create("side", at=abandoned)
    side = repo_b.worktree("side")
    side["only-here"] = 1
    side.commit()
    orphan = side.head
    ours.snapshot(commit=orphan)["only-here"]  # now cached here
    repo_b.branches.delete("side")
    repo_b.gc(min_age=0)

    with pytest.raises(UnknownCommitError):
        ours.snapshot(commit=orphan)


def test_a_tag_on_a_swept_commit_reads_as_dangling():
    backend = Memory()
    _, ours, wt = _filled(backend)
    other = Repo(backend)
    other.branches.create("side", at=wt.head)
    side = other.worktree("side")
    side["x"] = 1
    side.commit()
    ours.tags.create("t", side.head)
    ours.snapshot(tag="t")["x"]
    # Deleting what the tag points at, under the tag, the way a store
    # damaged by hand would: the tag's info must say so.
    backend.remove(COMMIT_ROOT % side.head)
    assert ours.tags.info("t").dangling


# -- the budget ----------------------------------------------------------------


def test_the_budget_bounds_what_is_held():
    cache = ContentCache(max_bytes=4_000)
    for i in range(100):
        cache.remember({f"kvgit:keyset:{i}": b"x" * 200})
    assert cache.size_bytes <= 4_000
    assert 0 < len(cache) < 100


def test_the_least_recently_used_goes_first():
    # Room for two of these three entries, not all of them.
    cache = ContentCache(max_bytes=800)
    cache.remember({"kvgit:keyset:a": b"a" * 250})
    cache.remember({"kvgit:keyset:b": b"b" * 250})
    cache.lookup(["kvgit:keyset:a"])  # a is now the more recent
    cache.remember({"kvgit:keyset:c": b"c" * 250})
    held = cache.lookup(["kvgit:keyset:a", "kvgit:keyset:b", "kvgit:keyset:c"])
    assert set(held) == {"kvgit:keyset:a", "kvgit:keyset:c"}


def test_an_entry_larger_than_the_budget_is_not_held():
    cache = ContentCache(max_bytes=500)
    cache.remember({"kvgit:keyset:big": b"x" * 1_000})
    assert len(cache) == 0


def test_a_repo_with_no_budget_has_no_cache():
    backend = Memory()
    repo = Repo(backend, cache_bytes=0)
    assert repo.cache is None
    assert repo._store is backend


def test_a_budget_must_be_positive():
    with pytest.raises(ValueError):
        ContentCache(max_bytes=0)


# -- the backend stays the backend ---------------------------------------------


def test_store_is_still_the_backend():
    backend = Memory()
    repo = Repo(backend)
    assert repo.store is backend


def test_what_the_backend_offers_is_reached_through_the_cache():
    class WithExtras(Memory):
        def drop(self):
            return "dropped"

    cached = CachedStore(WithExtras(), ContentCache())
    assert cached.drop() == "dropped"


def test_concurrent_readers_share_one_cache():
    _, repo, wt = _filled(n=2_000)
    head = wt.head
    errors = []

    def read(offset):
        try:
            snap = repo.snapshot(commit=head)
            for i in range(offset, 2_000, 7):
                assert snap[f"k{i}"] == i
        except Exception as e:  # noqa: BLE001 - reported below
            errors.append(e)

    threads = [threading.Thread(target=read, args=(n,)) for n in range(7)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert repo.cache.size_bytes <= repo.cache.max_bytes
