"""Concurrency coverage for the orphan sweep.

``clean_orphans`` used to reach its delete list through several
independent ``store.keys()`` scans. Anything a concurrent writer
committed *between* two of those scans was invisible to the mark phase
but visible to the sweep phase, so its HAMT nodes and chunks were
deleted while its ``__commit_root__`` (fixed at the earlier scan)
survived — a live branch HEAD pointing at missing nodes.

Every sweep now runs under the GC lease, and every commit batch lands
only while no lease is held, so a writer that starts mid-sweep waits for
the sweep to finish. The seam here is deterministic: ``ScanHookStore``
counts ``keys()`` calls and runs a callback after the Nth one, and the
tests start a writer thread from inside the sweep at the worst moment
the old code had, then check that its commit comes through whole.
"""

from __future__ import annotations

import threading
import time

from support import fork, worktree

from kvgit.encoding import dumps
from kvgit.hamt import EMPTY_HASH
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    CHUNK_PREFIX,
    COMMIT_ROOT,
    COMMIT_TIME,
    GC_LEASE_KEY,
    _load_root,
    _resolve_head,
    clean_orphans,
    deep_clean,
)

NODE_PREFIX = Keyset.DEFAULT_PREFIX

# Scan order inside a sweep: (1) in-flight markers, (2) branch heads,
# (3) __commit_root__, then on the deep path (4) kvgit:keyset:,
# (5) kvgit:chunk:, (6) kvgit:blob:. A writer that landed after #3 would
# be invisible to the mark phase and visible to everything after it.
AFTER_COMMIT_ROOT_SCAN = 3


class ScanHookStore(Memory):
    """Memory store that runs a callback after the Nth ``keys()`` call.

    The callback fires once the snapshot for that scan has been taken
    but before the caller has consumed it, which is exactly the moment
    a concurrent writer would have to hit to expose the race.
    """

    def __init__(self) -> None:
        super().__init__()
        self.keys_calls = 0
        self.hooks: dict[int, object] = {}

    def arm(self, nth: int, fn) -> None:
        """Register ``fn`` to run after the Nth ``keys()`` call from now.

        Resets the counter, so setup helpers that scan the store don't
        shift the seam.
        """
        self.keys_calls = 0
        self.hooks[nth] = fn

    def keys(self, prefix: str = ""):
        snapshot = super().keys(prefix)
        self.keys_calls += 1
        hook = self.hooks.pop(self.keys_calls, None)
        if hook is not None:
            hook()  # type: ignore[operator]
        return snapshot


def in_a_thread(fn):
    """A seam callback that starts ``fn`` on its own thread.

    A writer run on the sweep's own thread would wait on the lease the
    sweep holds, forever. Started from the seam instead, it begins at
    exactly the moment the seam marks and runs as a real concurrent
    writer; ``started.join()`` collects it once the sweep has returned.
    """
    started = threading.Thread(target=fn)
    return started.start, started


def node_hashes(store, commit_hash: str) -> set[str]:
    """Every HAMT node hash in a commit's keyset."""
    root = _load_root(store, commit_hash)
    if root is None or root == EMPTY_HASH:
        return set()
    _, nodes = Keyset(store, root=root).walk()
    return nodes


def missing_nodes(store, commit_hash: str, expected: set[str]) -> list[str]:
    """Which of ``expected`` are no longer in the store."""
    return sorted(n for n in expected if store.get(NODE_PREFIX + n) is None)


def age_commits(store, seconds: float) -> None:
    """Backdate every commit timestamp so min_age guards let go."""
    stale = dumps(time.time() - seconds)
    prefix = COMMIT_TIME.replace("%s", "")
    store.set_many({k: stale for k in store.keys() if k.startswith(prefix)})


def chunk_keys(store) -> list[str]:
    return sorted(k for k in store.keys() if k.startswith(CHUNK_PREFIX))


# A chunk-aware encoder/decoder pair with no numpy dependency: the
# whole value goes out as one content-addressed chunk and the blob is
# just the ref. Enough to exercise chunk reachability.
def chunky_encoder(value, sink) -> bytes:
    return sink.put(repr(value).encode()).encode()


def chunky_decoder(raw: bytes, reader):
    import ast

    return ast.literal_eval(reader.get(raw.decode()).decode())


class TestNodeRace:
    def test_commit_landing_mid_sweep_keeps_its_nodes(self):
        """A commit started between the root scan and the delete survives.

        Unguarded, the writer's HAMT nodes would land after the mark
        phase, count as unreachable and be deleted, while its
        ``__commit_root__`` stayed — a branch HEAD naming a keyset that
        cannot be loaded. Under the lease the writer waits for the sweep
        and lands after it.
        """
        store = ScanHookStore()
        s = worktree(store)
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        landed: dict[str, object] = {}

        def concurrent_writer():
            other = worktree(store)
            other["late"] = "written mid-sweep"
            landed["commit"] = other.commit().commit
            landed["nodes"] = node_hashes(store, landed["commit"])

        start, writer = in_a_thread(concurrent_writer)
        store.arm(AFTER_COMMIT_ROOT_SCAN, start)
        clean_orphans(store, min_age=3600)
        writer.join(timeout=10)

        head = _resolve_head(store, "main")
        assert head == landed["commit"], "the concurrent commit should be HEAD"
        assert store.get(COMMIT_ROOT % head) is not None

        gone = missing_nodes(store, head, landed["nodes"])  # type: ignore[arg-type]
        assert not gone, (
            f"live HEAD {head} on branch 'main' lost keyset nodes {gone} "
            f"(root {_load_root(store, head)}) — committed state is corrupt"
        )

        reader = worktree(store)
        assert reader["late"] == "written mid-sweep"
        assert reader["key0"] == 0

    def test_commit_landing_mid_sweep_keeps_its_chunks(self):
        """Same race, chunk namespace."""
        store = ScanHookStore()
        s = worktree(store, codec=(chunky_encoder, chunky_decoder))
        s["base"] = "base value"
        s.commit()
        age_commits(store, 10_000)
        before = set(chunk_keys(store))

        landed: dict[str, object] = {}

        def concurrent_writer():
            other = worktree(store, codec=(chunky_encoder, chunky_decoder))
            other["late"] = "chunked mid-sweep"
            landed["commit"] = other.commit().commit
            landed["chunks"] = set(chunk_keys(store)) - before

        start, writer = in_a_thread(concurrent_writer)
        store.arm(AFTER_COMMIT_ROOT_SCAN, start)
        clean_orphans(store, min_age=3600)
        writer.join(timeout=10)

        head = _resolve_head(store, "main")
        assert head == landed["commit"]
        lost = sorted(k for k in landed["chunks"] if store.get(k) is None)  # type: ignore[union-attr]
        assert not lost, (
            f"live HEAD {head} on branch 'main' lost chunks {lost} — "
            f"its blob payloads are unreadable"
        )

        reader = worktree(store, codec=(chunky_encoder, chunky_decoder))
        assert reader["late"] == "chunked mid-sweep"

    def test_writer_landing_before_the_sweep_is_untouched(self):
        """Control: the same writer, run before GC starts, is fine.

        Keeps the two tests above honest. If they went red for some
        reason other than the scan-ordering window — say, GC deleting
        any commit it did not itself observe being created — this one
        would go red too.
        """
        store = ScanHookStore()
        s = worktree(store)
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        other = worktree(store)
        other["late"] = "written before the sweep"
        late_commit = other.commit().commit
        late_nodes = node_hashes(store, late_commit)

        clean_orphans(store, min_age=3600)

        assert not missing_nodes(store, late_commit, late_nodes)
        assert worktree(store)["late"] == "written before the sweep"


class TestSharedStructure:
    def test_subtree_shared_with_a_live_branch_survives(self):
        """Deleting an orphan must not take shared HAMT nodes with it."""
        store = Memory()
        s = worktree(store)
        for i in range(60):  # >> bucket_max, so the HAMT actually branches
            s[f"key{i:03d}"] = i
        s.commit()

        dev = fork(s, "dev")
        dev["dev_only"] = "orphan payload"
        dev_commit = dev.commit().commit
        dev_nodes = node_hashes(store, dev_commit)

        main_commit = s.head
        main_nodes = node_hashes(store, main_commit)
        shared = main_nodes & dev_nodes
        assert shared, "test needs the two commits to actually share structure"

        s.repo.branches.delete("dev")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1

        assert not missing_nodes(store, main_commit, main_nodes), (
            "live branch 'main' lost nodes it shared with the deleted orphan"
        )
        reader = worktree(store)
        assert [reader[f"key{i:03d}"] for i in range(60)] == list(range(60))

        # The orphan's own, unshared nodes are gone.
        unshared = dev_nodes - main_nodes
        assert unshared
        assert missing_nodes(store, dev_commit, unshared) == sorted(unshared)

    def test_two_orphans_sharing_a_subtree_collect_cleanly(self):
        """Overlapping orphans may name the same hash twice; that's fine."""
        store = Memory()
        s = worktree(store)
        for i in range(60):
            s[f"key{i:03d}"] = i
        base = s.commit().commit

        one = fork(s, "one", at=base)
        one["a"] = "a"
        one.commit()
        two = fork(s, "two", at=base)
        two["b"] = "b"
        two.commit()

        s.repo.branches.delete("one")
        s.repo.branches.delete("two")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 2

        main_commit = s.head
        assert not missing_nodes(store, main_commit, node_hashes(store, main_commit))
        assert worktree(store)["key000"] == 0


class TestDamagedOrphans:
    def test_orphan_with_missing_nodes_does_not_crash(self):
        """Historical damage must not stall the sweep."""
        store = Memory()
        s = worktree(store)
        s["live"] = "keep me"
        s.commit()

        dev = fork(s, "dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        s.repo.branches.delete("dev")

        # Blow a hole in the orphan's keyset before the sweep sees it.
        store.remove(NODE_PREFIX + str(dev_root))

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert worktree(store)["live"] == "keep me"

    def test_orphan_with_corrupt_node_bytes_does_not_crash(self):
        store = Memory()
        s = worktree(store)
        s["live"] = "keep me"
        s.commit()

        dev = fork(s, "dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        s.repo.branches.delete("dev")

        store.set(NODE_PREFIX + str(dev_root), b"not json at all")

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert worktree(store)["live"] == "keep me"


class TestOrdinaryGarbage:
    def test_orphan_payload_is_still_collected(self):
        """The fix must not turn GC into a no-op.

        Everything an orphan alone holds — commit metadata, blob, HAMT
        nodes, chunks — is reclaimed by the routine sweep, and nothing
        the live branch shares with it is.
        """
        store = Memory()
        s = worktree(store, codec=(chunky_encoder, chunky_decoder))
        s["live"] = "keep me"
        s.commit()
        live_chunks = set(chunk_keys(store))

        dev = fork(s, "dev")  # inherits the chunked codec
        dev["dev_only"] = "throw me away"
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        dev_nodes = node_hashes(store, dev_commit) - node_hashes(store, s.head)
        dev_chunks = set(chunk_keys(store)) - live_chunks
        dev_blob = dev._engine._commit_keys["dev_only"]
        assert dev_chunks
        assert store.get(dev_blob) is not None

        s.repo.branches.delete("dev")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1

        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert store.get(COMMIT_TIME % dev_commit) is None
        assert store.get(dev_blob) is None, "orphan blob not collected"
        assert store.get(NODE_PREFIX + str(dev_root)) is None, (
            "orphan HAMT root not collected"
        )
        assert missing_nodes(store, dev_commit, dev_nodes) == sorted(dev_nodes)
        assert [k for k in dev_chunks if store.get(k) is not None] == []
        assert [k for k in live_chunks if store.get(k) is None] == [], (
            "the sweep took a chunk the live branch still references"
        )

        reader = worktree(store, codec=(chunky_encoder, chunky_decoder))
        assert reader["live"] == "keep me"


class TestDeepClean:
    def test_deep_clean_reclaims_what_the_safe_sweep_leaves(self):
        """Nodes and chunks no commit points at need the deep sweep."""
        store = Memory()
        s = worktree(store)
        s["live"] = "keep me"
        s.commit()
        live_commit = s.head
        live_nodes = node_hashes(store, live_commit)

        # Leftovers with no owning commit: exactly what an interrupted
        # write, or a store swept by an older kvgit, leaves behind.
        stray_node = NODE_PREFIX + "0" * 64
        stray_chunk = CHUNK_PREFIX + "1" * 40
        store.set_many({stray_node: b'{"kind":"leaf","items":{}}', stray_chunk: b"x"})

        assert clean_orphans(store, min_age=0) == 0
        assert store.get(stray_node) is not None, (
            "incremental sweep should leave commit-less nodes alone"
        )
        assert store.get(stray_chunk) is not None

        assert deep_clean(store, min_age=0) == 0
        assert store.get(stray_node) is None
        assert store.get(stray_chunk) is None
        assert not missing_nodes(store, live_commit, live_nodes)
        assert worktree(store)["live"] == "keep me"

    def test_deep_clean_reclaims_a_damaged_orphans_stranded_nodes(self):
        """An orphan with a missing root strands its children."""
        store = Memory()
        s = worktree(store)
        s["live"] = "keep me"
        s.commit()
        live_nodes = node_hashes(store, s.head)

        dev = fork(s, "dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_nodes = node_hashes(store, dev_commit) - live_nodes
        dev_root = str(_load_root(store, dev_commit))
        s.repo.branches.delete("dev")
        store.remove(NODE_PREFIX + dev_root)

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        stranded = sorted(
            n for n in dev_nodes if n != dev_root and store.get(NODE_PREFIX + n)
        )
        assert stranded, "test needs the orphan to have children below its root"

        assert deep_clean(store, min_age=0) == 0
        assert [n for n in stranded if store.get(NODE_PREFIX + n)] == []
        assert worktree(store)["live"] == "keep me"

    def test_no_batch_can_land_under_a_sweep(self):
        """The lease is load-bearing: a batch expecting the record read
        before the sweep began cannot land while the sweep runs, nor
        after it, and one expecting the current record lands once it is
        over."""
        store = ScanHookStore()
        worktree(store).commit()
        before = store.get(GC_LEASE_KEY)
        attempts: list[bool] = []

        def a_batch_mid_sweep():
            attempts.append(store.cas_many({GC_LEASE_KEY: before}, {"stray": b"x"}))

        store.arm(AFTER_COMMIT_ROOT_SCAN, a_batch_mid_sweep)
        deep_clean(store, min_age=3600)

        assert attempts == [False]
        assert store.get("stray") is None
        assert not store.cas_many({GC_LEASE_KEY: before}, {"stray": b"x"})
        after = store.get(GC_LEASE_KEY)
        assert store.cas_many({GC_LEASE_KEY: after}, {"stray": b"x"})


class TestChunkDedupRace:
    """Content is shared by key, so "the orphan owns it" is not enough.

    Chunks — like blobs and HAMT nodes — are keyed by their bytes, so an
    orphan's chunk and a brand-new commit's chunk are literally the same
    key. What keeps the sweep off a chunk a new commit uses is that the
    commit's batch cannot land while the sweep runs, so the mark phase
    has always seen every commit that could point at it.
    """

    def test_chunk_deduped_by_a_mid_sweep_commit_survives(self):
        """A live HEAD must not lose a chunk an orphan happened to own."""
        store = ScanHookStore()
        s = worktree(store, codec=(chunky_encoder, chunky_decoder))
        s["live"] = "keep me"
        s.commit()
        live_chunks = set(chunk_keys(store))

        # The orphan owns a chunk nothing live references (yet).
        shared_value = "a payload two unrelated commits both store"
        dev = fork(s, "dev")
        dev["dev_only"] = shared_value
        dev.commit()
        orphan_chunks = set(chunk_keys(store)) - live_chunks
        assert len(orphan_chunks) == 1, "test needs exactly one orphan-owned chunk"
        (shared_chunk,) = orphan_chunks
        s.repo.branches.delete("dev")
        age_commits(store, 10_000)

        landed: dict[str, object] = {}

        def concurrent_writer():
            """Commit content that dedups to the orphan's chunk."""
            other = worktree(store, codec=(chunky_encoder, chunky_decoder))
            other["late"] = shared_value
            landed["commit"] = other.commit().commit

        start, writer = in_a_thread(concurrent_writer)
        store.arm(AFTER_COMMIT_ROOT_SCAN, start)
        clean_orphans(store, min_age=3600)
        writer.join(timeout=10)

        head = _resolve_head(store, "main")
        assert head == landed["commit"], "the concurrent commit should be HEAD"
        assert store.get(shared_chunk) is not None, (
            f"live HEAD {head} on branch 'main' references chunk "
            f"{shared_chunk}, which the sweep deleted as an orphan's — "
            f"its value for 'late' is unreadable"
        )

        reader = worktree(store, codec=(chunky_encoder, chunky_decoder))
        assert reader["late"] == shared_value

    def test_the_orphan_chunk_is_reclaimed(self):
        """The other half of the contract: the space comes back.

        With nothing live sharing it, an orphan's chunk goes with the
        orphan on the routine sweep.
        """
        store = Memory()
        s = worktree(store, codec=(chunky_encoder, chunky_decoder))
        s["live"] = "keep me"
        s.commit()
        live_chunks = set(chunk_keys(store))

        dev = fork(s, "dev")
        dev["dev_only"] = "orphan payload"
        dev_commit = dev.commit().commit
        orphan_chunks = set(chunk_keys(store)) - live_chunks
        assert orphan_chunks

        s.repo.branches.delete("dev")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1

        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert [k for k in orphan_chunks if store.get(k) is not None] == []
        assert [k for k in live_chunks if store.get(k) is None] == []

        reader = worktree(store, codec=(chunky_encoder, chunky_decoder))
        assert reader["live"] == "keep me"
