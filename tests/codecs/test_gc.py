"""Tests for chunk reachability and orphan cleanup."""

from __future__ import annotations

import time

import pytest

np = pytest.importorskip("numpy")

from kvgit import Staged, VersionedKV
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec
from kvgit.encoding import dumps
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    CHUNK_PREFIX,
    COMMIT_TIME,
)


def make_staged(store=None):
    encoder, decoder = compose(NumpyCodec(min_bytes=64))
    store = store or Memory()
    return (
        Staged(VersionedKV(store), encoder=encoder, decoder=decoder),
        store,
    )


def chunk_keys(store):
    return [k for k in store.keys() if k.startswith(CHUNK_PREFIX)]


def fresh_reader(store):
    """A cache-free view of ``store``, so reads really hit the chunks."""
    return make_staged(store)[0]


class TestChunkSweepOnDeleteBranch:
    def test_unreferenced_chunk_needs_a_deep_clean_after_branch_delete(self):
        """Deleting a branch does not reclaim its chunks; deep_clean does.

        Chunk keys are ``kvgit:chunk:<content_hash>`` with nothing
        commit-derived in them, so a chunk the orphan owns may be the
        very key a concurrent writer's new commit just deduped onto.
        The incremental sweep therefore leaves chunks alone and
        ``deep_clean``, which requires a quiescent store, reclaims
        them.
        """
        s, store = make_staged()
        s["base"] = np.arange(2048, dtype="float64")
        s.commit()

        # New branch with a unique chunk.
        dev = s.create_branch("dev")
        dev["only_on_dev"] = np.arange(2048, dtype="float64") + 100
        dev.commit()
        assert len(chunk_keys(store)) == 2

        # Delete dev. The dev-only chunk becomes unreferenced.
        # delete_branch calls clean_orphans internally.
        s.delete_branch("dev")
        # Force min_age to 0 by directly invoking clean_orphans —
        # delete_branch uses the default 3600 which won't sweep our
        # just-created commits in the test window.
        s.versioned.clean_orphans(min_age=0)

        assert len(chunk_keys(store)) == 2, (
            "the incremental sweep must not delete chunks"
        )

        s.versioned.deep_clean(min_age=0)
        assert len(chunk_keys(store)) == 1
        assert np.array_equal(
            fresh_reader(store)["base"], np.arange(2048, dtype="float64")
        )

    def test_shared_chunk_survives_branch_delete(self):
        """A chunk referenced by another live branch must survive delete."""
        s, store = make_staged()
        big = np.arange(2048, dtype="float64")
        s["x"] = big
        s.commit()

        dev = s.create_branch("dev")
        dev["y"] = big  # shares the same chunk via dedup
        dev.commit()
        assert len(chunk_keys(store)) == 1

        s.delete_branch("dev")
        s.versioned.clean_orphans(min_age=0)
        # main still references the chunk.
        assert len(chunk_keys(store)) == 1


class TestCommitlessChunks:
    def test_commitless_chunk_needs_a_deep_clean(self):
        """A chunk no commit references is only reclaimed by deep_clean.

        ``clean_orphans`` reaches chunks by walking the keysets of the
        commits it is deleting. A chunk that belongs to no commit at
        all — an interrupted write, a store swept by an older kvgit —
        has no keyset to be found through, so the safe sweep leaves it
        and the deep sweep takes it.
        """
        s, store = make_staged()
        s["x"] = np.arange(2048, dtype="float64")
        s.commit()

        # A chunk written directly under the chunk namespace, as an
        # in-flight writer would have staged it.
        rogue_hash = "deadbeef" * 5  # 40 chars
        store.set(CHUNK_PREFIX + rogue_hash, b"unreferenced bytes")

        s.versioned.clean_orphans(min_age=0)
        assert (CHUNK_PREFIX + rogue_hash) in store.keys()

        s.versioned.deep_clean(min_age=0)
        assert (CHUNK_PREFIX + rogue_hash) not in store.keys()
        # The referenced chunk survives both sweeps.
        assert len(chunk_keys(store)) == 1


class TestOrphanCommitChunks:
    def test_orphan_commit_chunks_protected_within_window(self):
        """Chunks from a young orphan commit must survive the GC pass.

        Young = commit timestamp inside the min_age cutoff. This is
        the in-flight-writer protection: a commit may have been
        written but not yet linked to a branch HEAD, and its chunks
        must not be swept out from under it.
        """
        s, store = make_staged()
        big = np.arange(2048, dtype="float64")

        # Stage an orphan commit by creating the commit but not
        # advancing any branch head. Easiest path: create-and-delete a
        # branch, leaving the commit metadata behind in the store.
        dev = s.create_branch("dev")
        dev["only"] = big
        dev.commit()
        # Capture the dev commit hash before deleting the branch.
        dev_commit = dev.current_commit
        # Detach the branch — the commit becomes unreachable.
        s.delete_branch("dev")

        # Make sure the commit is in the store but is now an orphan,
        # and is "young" (timestamp recent).
        assert store.get(COMMIT_TIME % dev_commit) is not None

        # Default GC with a generous min_age — the commit's chunks
        # should be retained because the commit is still young.
        before = chunk_keys(store)
        assert before
        # The dev chunk is unique to the dev branch (different content),
        # so without protection it would be swept.
        s.versioned.clean_orphans(min_age=3600)
        assert set(chunk_keys(store)) == set(before), (
            "young orphan commit's chunks were swept; this defeats the "
            "in-flight writer protection"
        )

        # The protection that actually does the work now lives in
        # deep_clean: its namespace scan would otherwise take any chunk
        # not reachable from a live head, including one an in-flight
        # writer has staged but not yet linked to a branch.
        s.versioned.deep_clean(min_age=3600)
        assert set(chunk_keys(store)) == set(before), (
            "deep_clean swept a young orphan commit's chunks"
        )

        # Once the commit ages out, the deep sweep is free to take them.
        store.set(COMMIT_TIME % dev_commit, dumps(time.time() - 7200))
        s.versioned.deep_clean(min_age=3600)
        assert chunk_keys(store) == []

    def test_old_orphan_commit_chunks_wait_for_deep_clean(self):
        """Even an aged-out orphan does not get its chunks swept.

        Age is what makes the *commit* collectable. It says nothing
        about the chunk, whose key a commit made one microsecond ago
        may share.
        """
        s, store = make_staged()

        dev = s.create_branch("dev")
        dev["only"] = np.arange(2048, dtype="float64") + 1
        dev.commit()
        dev_commit = dev.current_commit
        s.delete_branch("dev")

        # Backdate the commit so it's outside the cutoff.
        store.set(COMMIT_TIME % dev_commit, dumps(time.time() - 7200))

        s.versioned.clean_orphans(min_age=3600)
        assert store.get(COMMIT_TIME % dev_commit) is None, "commit not collected"
        assert len(chunk_keys(store)) == 1, (
            "the incremental sweep must not delete chunks"
        )

        s.versioned.deep_clean(min_age=0)
        assert chunk_keys(store) == []


class TestCleanOrphansHandlesPureV2Stores:
    def test_no_chunks_no_chunk_pass(self):
        """A store with no chunked codec ever used should still GC cleanly."""
        from kvgit import VersionedKV

        store = Memory()
        s = Staged(VersionedKV(store))
        s["a"] = "hello"
        s.commit()

        dev = s.create_branch("dev")
        dev["b"] = "goodbye"
        dev.commit()

        s.delete_branch("dev")
        # Should run without error, sweep nothing chunk-related.
        s.versioned.clean_orphans(min_age=0)
        assert chunk_keys(store) == []
