"""Concurrency coverage for the orphan sweep.

``clean_orphans`` used to reach its delete list through several
independent ``store.keys()`` scans. Anything a concurrent writer
committed *between* two of those scans was invisible to the mark phase
but visible to the sweep phase, so its HAMT nodes and chunks were
deleted while its ``__commit_root__`` (fixed at the earlier scan)
survived — a live branch HEAD pointing at missing nodes.

The seam here is deterministic: ``ScanHookStore`` counts ``keys()``
calls and runs a callback after the Nth one. ``clean_orphans`` makes
its scans in a fixed order, so "commit between the commit-root scan
and the node scan" is expressible as "run the writer after keys()
call #2" with no sleeps and no real threads.
"""

from __future__ import annotations

import time

import pytest

from kvgit import Staged, VersionedKV
from kvgit.encoding import dumps
from kvgit.hamt import EMPTY_HASH
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    CHUNK_PREFIX,
    COMMIT_ROOT,
    COMMIT_TIME,
    _load_root,
    _resolve_head,
    clean_orphans,
    deep_clean,
)
from kvgit.versioned.keyset import Keyset

NODE_PREFIX = Keyset.DEFAULT_PREFIX

# Scan order inside clean_orphans: (1) branch heads, (2) __commit_root__,
# (3) kvgit:keyset:, (4) kvgit:chunk:. A writer that lands after #2 is
# invisible to the mark phase but visible to #3 and #4.
AFTER_COMMIT_ROOT_SCAN = 2


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

    def keys(self):
        snapshot = super().keys()
        self.keys_calls += 1
        hook = self.hooks.pop(self.keys_calls, None)
        if hook is not None:
            hook()  # type: ignore[operator]
        return snapshot


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
        """A commit made between the root scan and the node scan survives.

        Without the fix the node scan sees the new commit's HAMT nodes,
        finds them absent from the mark phase's reachable set, and
        deletes them — while the commit's ``__commit_root__``, chosen
        from the earlier snapshot, is left in place. The branch HEAD
        then names a commit whose keyset cannot be loaded.
        """
        store = ScanHookStore()
        s = Staged(VersionedKV(store))
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        landed: dict[str, object] = {}

        def concurrent_writer():
            other = Staged(VersionedKV(store))
            other["late"] = "written mid-sweep"
            landed["commit"] = other.commit().commit
            landed["nodes"] = node_hashes(store, landed["commit"])

        store.arm(AFTER_COMMIT_ROOT_SCAN, concurrent_writer)
        clean_orphans(store, min_age=3600)

        head = _resolve_head(store, "main")
        assert head == landed["commit"], "the concurrent commit should be HEAD"
        assert store.get(COMMIT_ROOT % head) is not None

        gone = missing_nodes(store, head, landed["nodes"])  # type: ignore[arg-type]
        assert not gone, (
            f"live HEAD {head} on branch 'main' lost keyset nodes {gone} "
            f"(root {_load_root(store, head)}) — committed state is corrupt"
        )

        reader = Staged(VersionedKV(store))
        assert reader["late"] == "written mid-sweep"
        assert reader["key0"] == 0

    def test_commit_landing_mid_sweep_keeps_its_chunks(self):
        """Same race, chunk namespace."""
        store = ScanHookStore()
        s = Staged(VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder)
        s["base"] = "base value"
        s.commit()
        age_commits(store, 10_000)
        before = set(chunk_keys(store))

        landed: dict[str, object] = {}

        def concurrent_writer():
            other = Staged(
                VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
            )
            other["late"] = "chunked mid-sweep"
            landed["commit"] = other.commit().commit
            landed["chunks"] = set(chunk_keys(store)) - before

        store.arm(AFTER_COMMIT_ROOT_SCAN, concurrent_writer)
        clean_orphans(store, min_age=3600)

        head = _resolve_head(store, "main")
        assert head == landed["commit"]
        lost = sorted(k for k in landed["chunks"] if store.get(k) is None)  # type: ignore[union-attr]
        assert not lost, (
            f"live HEAD {head} on branch 'main' lost chunks {lost} — "
            f"its blob payloads are unreadable"
        )

        reader = Staged(
            VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
        )
        assert reader["late"] == "chunked mid-sweep"

    def test_writer_landing_before_the_sweep_is_untouched(self):
        """Control: the same writer, run before GC starts, is fine.

        Keeps the two tests above honest. If they went red for some
        reason other than the scan-ordering window — say, GC deleting
        any commit it did not itself observe being created — this one
        would go red too.
        """
        store = ScanHookStore()
        s = Staged(VersionedKV(store))
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        other = Staged(VersionedKV(store))
        other["late"] = "written before the sweep"
        late_commit = other.commit().commit
        late_nodes = node_hashes(store, late_commit)

        clean_orphans(store, min_age=3600)

        assert not missing_nodes(store, late_commit, late_nodes)
        assert Staged(VersionedKV(store))["late"] == "written before the sweep"


class TestSharedStructure:
    def test_subtree_shared_with_a_live_branch_survives(self):
        """Deleting an orphan must not take shared HAMT nodes with it."""
        store = Memory()
        s = Staged(VersionedKV(store))
        for i in range(60):  # >> bucket_max, so the HAMT actually branches
            s[f"key{i:03d}"] = i
        s.commit()

        dev = s.create_branch("dev")
        dev["dev_only"] = "orphan payload"
        dev_commit = dev.commit().commit
        dev_nodes = node_hashes(store, dev_commit)

        main_commit = s.current_commit
        main_nodes = node_hashes(store, main_commit)
        shared = main_nodes & dev_nodes
        assert shared, "test needs the two commits to actually share structure"

        s.delete_branch("dev")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1

        assert not missing_nodes(store, main_commit, main_nodes), (
            "live branch 'main' lost nodes it shared with the deleted orphan"
        )
        reader = Staged(VersionedKV(store))
        assert [reader[f"key{i:03d}"] for i in range(60)] == list(range(60))

        # The orphan's own, unshared nodes are gone.
        unshared = dev_nodes - main_nodes
        assert unshared
        assert missing_nodes(store, dev_commit, unshared) == sorted(unshared)

    def test_two_orphans_sharing_a_subtree_collect_cleanly(self):
        """Overlapping orphans may name the same hash twice; that's fine."""
        store = Memory()
        s = Staged(VersionedKV(store))
        for i in range(60):
            s[f"key{i:03d}"] = i
        base = s.commit().commit

        one = s.create_branch("one", at=base)
        one["a"] = "a"
        one.commit()
        two = s.create_branch("two", at=base)
        two["b"] = "b"
        two.commit()

        s.delete_branch("one")
        s.delete_branch("two")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 2

        main_commit = s.current_commit
        assert not missing_nodes(store, main_commit, node_hashes(store, main_commit))
        assert Staged(VersionedKV(store))["key000"] == 0


class TestDamagedOrphans:
    def test_orphan_with_missing_nodes_does_not_crash(self):
        """Historical damage must not stall the sweep."""
        store = Memory()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()

        dev = s.create_branch("dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        s.delete_branch("dev")

        # Blow a hole in the orphan's keyset before the sweep sees it.
        store.remove(NODE_PREFIX + str(dev_root))

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert Staged(VersionedKV(store))["live"] == "keep me"

    def test_orphan_with_corrupt_node_bytes_does_not_crash(self):
        store = Memory()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()

        dev = s.create_branch("dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        s.delete_branch("dev")

        store.set(NODE_PREFIX + str(dev_root), b"not json at all")

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert Staged(VersionedKV(store))["live"] == "keep me"


class TestOrdinaryGarbage:
    def test_orphan_payload_is_still_collected(self):
        """The fix must not turn GC into a no-op."""
        store = Memory()
        s = Staged(VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder)
        s["live"] = "keep me"
        s.commit()
        live_chunks = set(chunk_keys(store))

        dev = s.create_branch("dev")  # inherits the chunked codec
        dev["dev_only"] = "throw me away"
        dev_commit = dev.commit().commit
        dev_root = _load_root(store, dev_commit)
        dev_nodes = node_hashes(store, dev_commit)
        dev_chunks = set(chunk_keys(store)) - live_chunks
        dev_blob = f"{dev_commit}:dev_only"
        assert dev_chunks
        assert store.get(dev_blob) is not None

        s.delete_branch("dev")
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1

        assert store.get(COMMIT_ROOT % dev_commit) is None
        assert store.get(COMMIT_TIME % dev_commit) is None
        assert store.get(dev_blob) is None, "orphan blob not collected"
        assert store.get(NODE_PREFIX + str(dev_root)) is None, (
            "orphan HAMT root not collected"
        )
        assert missing_nodes(store, dev_commit, dev_nodes) == sorted(dev_nodes)
        assert [k for k in dev_chunks if store.get(k) is not None] == [], (
            "orphan chunk not collected"
        )

        reader = Staged(
            VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
        )
        assert reader["live"] == "keep me"


class TestDeepClean:
    def test_deep_clean_reclaims_what_the_safe_sweep_leaves(self):
        """Nodes and chunks no commit points at need the deep sweep."""
        store = Memory()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()
        live_commit = s.current_commit
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
        assert Staged(VersionedKV(store))["live"] == "keep me"

    def test_deep_clean_reclaims_a_damaged_orphans_stranded_nodes(self):
        """An orphan with a missing root strands its children."""
        store = Memory()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()
        live_nodes = node_hashes(store, s.current_commit)

        dev = s.create_branch("dev")
        for i in range(40):
            dev[f"dev{i:03d}"] = i
        dev_commit = dev.commit().commit
        dev_nodes = node_hashes(store, dev_commit) - live_nodes
        dev_root = str(_load_root(store, dev_commit))
        s.delete_branch("dev")
        store.remove(NODE_PREFIX + dev_root)

        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        stranded = sorted(
            n for n in dev_nodes if n != dev_root and store.get(NODE_PREFIX + n)
        )
        assert stranded, "test needs the orphan to have children below its root"

        assert deep_clean(store, min_age=0) == 0
        assert [n for n in stranded if store.get(NODE_PREFIX + n)] == []
        assert Staged(VersionedKV(store))["live"] == "keep me"

    def test_deep_clean_is_the_unsafe_one(self):
        """Documented hazard, pinned: deep_clean loses a mid-sweep commit.

        Not a bug report — this is the reason deep_clean is opt-in and
        the reason clean_orphans no longer does this. If this ever
        starts passing cleanly, deep_clean has become safe and the
        docs should say so.
        """
        store = ScanHookStore()
        s = Staged(VersionedKV(store))
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        landed: dict[str, object] = {}

        def concurrent_writer():
            other = Staged(VersionedKV(store))
            other["late"] = "written mid-sweep"
            landed["commit"] = other.commit().commit
            landed["nodes"] = node_hashes(store, landed["commit"])

        store.arm(AFTER_COMMIT_ROOT_SCAN, concurrent_writer)
        deep_clean(store, min_age=3600)

        head = _resolve_head(store, "main")
        assert head == landed["commit"]
        assert missing_nodes(store, head, landed["nodes"]), (  # type: ignore[arg-type]
            "deep_clean is documented as unsafe under concurrent writers"
        )
        with pytest.raises(KeyError):
            Staged(VersionedKV(store))["late"]
