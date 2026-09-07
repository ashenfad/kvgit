"""The GC lease: what lets deep_clean sweep beside a live writer.

``deep_clean``'s namespace scan deletes every keyset node and chunk its
mark phase did not see, so it is only correct while nothing else is
writing. The lease makes that condition hold instead of asking the
caller to promise it: the sweep takes ``__gc_lease__`` by CAS, pauses
for ``grace`` so batches already in flight can land, sweeps, and
releases; every write path reads the lease immediately before its batch
and waits while a live one is held.

The tests here pin the two halves separately — a writer that starts
inside the grace window, and one that wakes while the sweep is already
running — plus the lease's own arithmetic: refusal, expiry, release.
``TestDeepClean`` in ``test_gc_concurrency`` holds the negative case,
a writer that cannot see the lease at all.
"""

from __future__ import annotations

import threading
import time

import pytest

from kvgit import GcBusy, Staged, VersionedKV
from kvgit.encoding import dumps
from kvgit.kv.memory import Memory
from kvgit.versioned import kv as kv_module
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    CHUNK_PREFIX,
    COMMIT_ROOT,
    GC_LEASE_KEY,
    STORAGE_VERSION_KEY,
    _acquire_gc_lease,
    _lease_expiry,
    _load_root,
    _release_gc_lease,
    _resolve_head,
    _wait_for_gc,
    deep_clean,
)

from .test_gc_concurrency import (
    AFTER_COMMIT_ROOT_SCAN,
    ScanHookStore,
    age_commits,
    chunk_keys,
    chunky_decoder,
    chunky_encoder,
    missing_nodes,
    node_hashes,
)


def lease_is_live(store) -> bool:
    return _lease_expiry(store.get(GC_LEASE_KEY)) > time.time()


class LeaseHookStore(Memory):
    """Memory store that runs a callback the instant the lease is taken.

    Turns "a writer that starts inside the grace window" into something
    with no sleep in it on the test's side: the callback fires from
    inside the acquiring CAS, so the writer begins at the exact
    moment the grace window opens.
    """

    def __init__(self) -> None:
        super().__init__()
        self._on_acquire = None

    def on_acquire(self, fn) -> None:
        self._on_acquire = fn

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        won = super().cas(key, value, expected)
        if won and key == GC_LEASE_KEY and self._on_acquire is not None:
            fn, self._on_acquire = self._on_acquire, None
            fn()
        return won


class CountingMemory(Memory):
    """Memory store that counts reads, to pin the uncontended cost."""

    def __init__(self) -> None:
        super().__init__()
        self.gets = 0

    def get(self, key: str) -> bytes | None:
        self.gets += 1
        return super().get(key)


class TestWritersUnderTheLease:
    def test_a_writer_in_the_grace_window_keeps_its_commit(self):
        """The hazard the lease exists for, in its original shape.

        A writer commits new keys and a chunk while a deep clean is in
        its grace window. Everything it wrote must still be there when
        the sweep finishes, and its commit must load.
        """
        store = LeaseHookStore()
        s = Staged(VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder)
        s["base"] = "base value"
        s.commit()
        age_commits(store, 10_000)
        before = set(chunk_keys(store))

        # Real garbage, so a sweep that quietly did nothing would show up
        # as a failure here rather than as a passing test.
        stray = CHUNK_PREFIX + "f" * 40
        store.set(stray, b"no commit references this")

        landed: dict[str, object] = {}

        def writer():
            other = Staged(
                VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
            )
            other["late"] = "written in the grace window"
            landed["commit"] = other.commit().commit
            landed["chunks"] = set(chunk_keys(store)) - before - {stray}
            landed["nodes"] = node_hashes(store, landed["commit"])

        writer_thread = threading.Thread(target=writer)
        store.on_acquire(writer_thread.start)
        deep_clean(store, min_age=3600, grace=0.3)
        writer_thread.join(timeout=10)
        assert not writer_thread.is_alive(), "the writer never finished"

        assert store.get(stray) is None, "the sweep did not actually run"

        head = _resolve_head(store, "main")
        assert head == landed["commit"]
        assert not missing_nodes(store, head, landed["nodes"])  # type: ignore[arg-type]
        assert [k for k in landed["chunks"] if store.get(k) is None] == [], (  # type: ignore[union-attr]
            "the sweep took a chunk the new commit references"
        )

        reader = Staged(
            VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
        )
        assert reader["late"] == "written in the grace window"
        assert reader["base"] == "base value"

    def test_a_writer_that_wakes_mid_sweep_waits_for_the_lease(self):
        """Past the grace window, waiting is what protects the writer.

        The writer starts after the mark phase has already chosen what
        is reachable, so nothing it writes could be marked. It blocks on
        the lease instead, and its batch lands after the sweep.
        """
        store = ScanHookStore()
        s = Staged(VersionedKV(store))
        for i in range(20):
            s[f"key{i}"] = i
        s.commit()
        age_commits(store, 10_000)

        landed: dict[str, object] = {}

        def writer():
            other = Staged(VersionedKV(store))
            other["late"] = "queued behind the sweep"
            landed["commit"] = other.commit().commit
            landed["nodes"] = node_hashes(store, landed["commit"])

        writer_thread = threading.Thread(target=writer)
        store.arm(AFTER_COMMIT_ROOT_SCAN, writer_thread.start)
        deep_clean(store, min_age=3600, grace=0)
        writer_thread.join(timeout=10)
        assert not writer_thread.is_alive(), "the writer never finished"

        head = _resolve_head(store, "main")
        assert head == landed["commit"]
        assert not missing_nodes(store, head, landed["nodes"])  # type: ignore[arg-type]
        assert Staged(VersionedKV(store))["late"] == "queued behind the sweep"

    def test_a_writer_proceeds_once_the_lease_is_released(self):
        store = Memory()
        s = Staged(VersionedKV(store))
        s["seed"] = 1
        s.commit()

        ours, _, _ = _acquire_gc_lease(store, 60.0)
        wrote = threading.Event()

        def writer():
            other = Staged(VersionedKV(store))
            other["late"] = "after the lease"
            other.commit()
            wrote.set()

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        try:
            assert not wrote.wait(0.3), "a writer wrote while the lease was live"
            _release_gc_lease(store, ours)
            assert wrote.wait(10), "a writer did not proceed after the release"
        finally:
            writer_thread.join(timeout=10)

        assert Staged(VersionedKV(store))["late"] == "after the lease"

    def test_a_writer_does_not_wait_past_the_leases_expiry(self):
        """A holder that dies mid-sweep blocks writers for its term only."""
        store = Memory()
        _acquire_gc_lease(store, 0.2)  # never released
        started = time.monotonic()
        _wait_for_gc(store)
        waited = time.monotonic() - started
        assert 0.1 < waited < 5.0, f"waited {waited:.2f}s for a 0.2s lease"

    def test_no_lease_costs_one_read(self):
        store = CountingMemory()
        _wait_for_gc(store)
        assert store.gets == 1


class TestLeaseArithmetic:
    def test_a_second_deep_clean_is_refused(self):
        store = Memory()
        ours, _, _ = _acquire_gc_lease(store, 60.0)
        with pytest.raises(GcBusy):
            deep_clean(store, min_age=0, grace=0)
        assert store.get(GC_LEASE_KEY) == ours, (
            "a refused call must leave the holder's lease alone"
        )

    def test_an_expired_lease_is_reclaimable(self):
        store = Memory()
        store.set(
            GC_LEASE_KEY,
            dumps({"owner": "a holder that died", "expires": time.time() - 1}),
        )
        assert deep_clean(store, min_age=0, grace=0) == 0
        assert not lease_is_live(store)

    def test_unreadable_lease_bytes_are_not_a_lease(self):
        """Garbage under the key must not wedge maintenance forever."""
        store = Memory()
        store.set(GC_LEASE_KEY, b"not json at all")
        assert deep_clean(store, min_age=0, grace=0) == 0
        assert not lease_is_live(store)

    def test_the_lease_is_released_after_a_sweep(self):
        store = Memory()
        deep_clean(store, min_age=0, grace=0)
        assert not lease_is_live(store)
        # The real proof it was released: the next call can take it.
        deep_clean(store, min_age=0, grace=0)

    def test_the_lease_is_released_when_the_sweep_raises(self, monkeypatch):
        store = Memory()

        def boom(*args, **kwargs):
            raise RuntimeError("sweep exploded")

        monkeypatch.setattr(kv_module, "_sweep", boom)
        with pytest.raises(RuntimeError, match="sweep exploded"):
            deep_clean(store, min_age=0, grace=0)
        assert not lease_is_live(store)

    def test_an_overrun_lease_is_reported_not_extended(self, caplog):
        store = Memory()
        with caplog.at_level("WARNING", logger="kvgit.orphans"):
            deep_clean(store, min_age=0, grace=0.1, lease_ttl=0.01)
        assert any("past its" in r.message for r in caplog.records), (
            "a sweep that outlives its lease must say so"
        )
        assert not lease_is_live(store)

    def test_the_incremental_sweep_takes_no_lease(self):
        """``clean_orphans`` needs none, so it must not leave one behind."""
        store = Memory()
        s = Staged(VersionedKV(store))
        s["k"] = 1
        s.commit()
        s.versioned.clean_orphans(min_age=0)
        assert store.get(GC_LEASE_KEY) is None


class PausedCommitKV(VersionedKV):
    """VersionedKV that stops between its write batch and publishing it.

    A commit lands in two steps: the ``set_many`` that writes its
    nodes, blobs, chunks and metadata, and — later — the CAS that makes
    it a branch HEAD, or the three-way merge that folds it into one.
    In between it is fully written and completely unreachable, which is
    indistinguishable from garbage. Holding a writer there turns "a
    sweep landed in that gap" into an event a test can schedule.
    """

    def __init__(self, *args, reached, release, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._reached = reached
        self._release = release

    def _create_commit(self, *args, **kwargs):
        commit_hash = super()._create_commit(*args, **kwargs)
        self._reached.set()
        assert self._release.wait(timeout=10), "test never released the writer"
        return commit_hash


class TestCommitsBetweenWriteAndPublish:
    """The window between a write batch and the HEAD advance that
    publishes it. ``grace`` bounds it on the clock, so the sweep keeps
    every unreachable commit stamped within ``grace`` of the moment it
    took the lease — whatever ``min_age`` says, since ``min_age`` is
    about abandoned work and this is about work still in progress.
    """

    def test_an_unpublished_commit_survives_a_min_age_zero_sweep(self):
        store = Memory()
        s = Staged(VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder)
        s["base"] = "base value"
        s.commit()
        before = set(chunk_keys(store))

        stray = CHUNK_PREFIX + "f" * 40
        store.set(stray, b"no commit references this")

        reached, release = threading.Event(), threading.Event()
        landed: dict[str, object] = {}

        def writer():
            other = Staged(
                PausedCommitKV(store, reached=reached, release=release),
                encoder=chunky_encoder,
                decoder=chunky_decoder,
            )
            other["late"] = "written before the lease, published after"
            landed["commit"] = other.commit().commit
            landed["chunks"] = set(chunk_keys(store)) - before - {stray}
            landed["nodes"] = node_hashes(store, landed["commit"])

        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        assert reached.wait(5), "the writer never reached its HEAD advance"

        # Its batch is on disk and nothing points at it. min_age=0 says
        # every unreachable commit is fair game; the lease's own time
        # bound says this one is not.
        deep_clean(store, min_age=0, grace=0.5)
        release.set()
        writer_thread.join(timeout=10)
        assert not writer_thread.is_alive(), "the writer never finished"

        assert store.get(stray) is None, "the sweep did not actually run"

        commit = landed["commit"]
        assert _resolve_head(store, "main") == commit
        assert store.get(COMMIT_ROOT % commit) is not None, (
            "the sweep deleted a commit whose writer had not published it yet"
        )
        assert not missing_nodes(store, commit, landed["nodes"])  # type: ignore[arg-type]
        Keyset(store, root=str(_load_root(store, commit))).walk()
        assert [k for k in landed["chunks"] if store.get(k) is None] == []  # type: ignore[union-attr]

        reader = Staged(
            VersionedKV(store), encoder=chunky_encoder, decoder=chunky_decoder
        )
        assert reader["late"] == "written before the lease, published after"
        assert reader["base"] == "base value"

    def test_the_merge_paths_first_commit_survives_too(self):
        """A lost race writes a commit the three-way merge reads back.

        On the merge path the write batch is published by folding it
        into a merge commit rather than by a CAS, so the same gap opens
        — and the merge cannot be built at all if the sweep took the
        commit it is supposed to fold.
        """
        store = Memory()
        s = Staged(VersionedKV(store))
        s["base"] = "base"
        s.commit()

        reached, release = threading.Event(), threading.Event()
        # Opened before the other writer moves HEAD, so this handle's
        # base commit is stale by the time it commits — the merge path.
        mine = Staged(PausedCommitKV(store, reached=reached, release=release))
        mine["ours"] = "merged in"

        theirs = Staged(VersionedKV(store))
        theirs["theirs"] = "landed first"
        theirs.commit()

        result: dict[str, object] = {}
        writer_thread = threading.Thread(
            target=lambda: result.update(merge=mine.commit())
        )
        writer_thread.start()
        assert reached.wait(5), "the writer never reached its merge"

        deep_clean(store, min_age=0, grace=0.5)
        release.set()
        writer_thread.join(timeout=10)
        assert not writer_thread.is_alive(), "the writer never finished"

        assert result["merge"].merged, result["merge"]  # type: ignore[union-attr]

        reader = Staged(VersionedKV(store))
        assert reader["ours"] == "merged in"
        assert reader["theirs"] == "landed first"
        assert reader["base"] == "base"


class SlowRemovalStore(LeaseHookStore):
    """Lease-hook store that dawdles just before it deletes.

    Opens a guaranteed window after the mark phase has decided what is
    garbage and before any of it is gone — the window in which a
    branch-root write that did not wait for the lease would install a
    head on a commit already condemned.
    """

    def remove_many(self, *args) -> None:
        time.sleep(0.1)
        super().remove_many(*args)


class TestBranchRootWrites:
    """Every write that makes a commit reachable waits on the lease.

    A head is what turns a commit into a GC root, so writing one under
    a running sweep either resurrects something already condemned or
    contradicts a mark phase that has already run. These paths wait the
    lease out *before* checking the target commit exists, so the check
    and the write see the same store: the outcome is a head on a commit
    that loads, or a refusal, never a head naming nothing.
    """

    def test_creating_a_branch_on_an_old_orphan_never_dangles(self):
        store = SlowRemovalStore()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()

        dev = s.create_branch("dev")
        dev["work"] = "abandoned"
        orphan = dev.commit().commit
        s.delete_branch("dev")
        age_commits(store, 10_000)

        outcome: dict[str, object] = {}

        def creator():
            try:
                outcome["branch"] = s.versioned.create_branch("revive", at=orphan)
            except ValueError as exc:
                outcome["error"] = str(exc)

        creator_thread = threading.Thread(target=creator)
        store.on_acquire(creator_thread.start)
        deep_clean(store, min_age=0, grace=0)
        creator_thread.join(timeout=10)
        assert not creator_thread.is_alive(), "the branch creation never finished"

        # Two outcomes are legal and which one lands depends on
        # scheduling: the creation waits out the lease and then reports
        # the commit gone, or it beat the lease and the head it wrote
        # made the commit reachable for the mark phase. The illegal
        # third outcome is a head naming a commit the sweep collected,
        # which is what this asserts against.
        head_raw = store.get(BRANCH_HEAD % "revive")
        if head_raw is None:
            assert "error" in outcome, outcome
        else:
            resolved = _resolve_head(store, "revive")
            assert resolved == orphan, "branch 'revive' does not resolve"
            assert store.get(COMMIT_ROOT % orphan) is not None
            Keyset(store, root=str(_load_root(store, orphan))).walk()
        assert Staged(VersionedKV(store))["live"] == "keep me"

    def test_reset_to_a_swept_commit_reports_it_rather_than_dangling(self):
        store = SlowRemovalStore()
        s = Staged(VersionedKV(store))
        s["live"] = "keep me"
        s.commit()

        dev = s.create_branch("dev")
        dev["work"] = "abandoned"
        orphan = dev.commit().commit
        s.delete_branch("dev")
        age_commits(store, 10_000)

        outcome: dict[str, object] = {}
        resetter = threading.Thread(
            target=lambda: outcome.update(ok=s.versioned.reset_to(orphan))
        )
        store.on_acquire(resetter.start)
        deep_clean(store, min_age=0, grace=0)
        resetter.join(timeout=10)
        assert not resetter.is_alive()

        assert outcome["ok"] is False, "HEAD was reset onto a collected commit"
        assert _resolve_head(store, "main") is not None
        assert Staged(VersionedKV(store))["live"] == "keep me"


class TestVersionCheckBeforeTheLease:
    def test_a_store_stamped_too_high_is_left_untouched(self):
        """A store this code must not sweep must not be written to.

        Checking after acquisition would leave an expired lease record
        in a store kvgit had no business writing to at all.
        """
        store = Memory()
        store.set(STORAGE_VERSION_KEY, dumps(99))
        with pytest.raises(ValueError, match="storage version"):
            deep_clean(store, min_age=0, grace=0)
        assert store.get(GC_LEASE_KEY) is None
        assert sorted(store.keys()) == [STORAGE_VERSION_KEY]
