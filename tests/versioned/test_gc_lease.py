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
from kvgit.versioned.kv import (
    CHUNK_PREFIX,
    GC_LEASE_KEY,
    _acquire_gc_lease,
    _lease_expiry,
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

        ours, _ = _acquire_gc_lease(store, 60.0)
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
        ours, _ = _acquire_gc_lease(store, 60.0)
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
