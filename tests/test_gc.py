"""Tests for GCVersioned garbage collection."""

import pytest

from vkv import ConcurrencyError, GCVersioned
from vkv.kv.memory import Memory
from vkv.versioned import BRANCH_HEAD, _to_bytes


class TestGCNoOp:
    def test_no_rebase_below_high_water(self):
        v = GCVersioned(high_water_bytes=1000)
        v.commit({"k": b"small"})
        result = v.maybe_rebase()
        assert not result.performed

    def test_commit_returns_merge_result(self):
        v = GCVersioned(high_water_bytes=1000)
        result = v.commit({"k": b"data"})
        assert result.merged
        assert result.commit is not None
        assert len(result.commit) == 16


class TestGCRebase:
    def test_drops_oldest_until_low_water(self):
        v = GCVersioned(high_water_bytes=100, low_water_bytes=50)
        # Each value is 40 bytes, total after 3 = 120 > high_water
        v.commit({"a": b"x" * 40})
        v.commit({"b": b"y" * 40})
        v.commit({"c": b"z" * 40})

        # commit auto-triggers GC
        result = v.last_rebase_result
        assert result is not None
        assert result.performed
        # Should have dropped "a" (oldest touch) to get under 50
        assert "a" in result.dropped_keys
        assert v.get("a") is None
        # Most recent keys retained
        assert v.get("c") == b"z" * 40

    def test_retains_system_keys(self):
        v = GCVersioned(high_water_bytes=100, low_water_bytes=50)
        v.commit({"__system__": b"x" * 200, "user": b"y" * 40})
        v.commit({"more": b"z" * 80})

        # System key should survive even though it's large
        assert v.get("__system__") == b"x" * 200

    def test_rebase_creates_fresh_root(self):
        v = GCVersioned(high_water_bytes=50, low_water_bytes=20)
        v.commit({"a": b"x" * 30})
        v.commit({"b": b"y" * 30})

        # After rebase, history should be short (fresh root)
        history = list(v.history())
        assert len(history) == 1  # just the rebase commit

    def test_explicit_keep_keys(self):
        v = GCVersioned(high_water_bytes=10000)
        v.commit({"a": b"1", "b": b"2", "c": b"3"})

        result = v.rebase(keep_keys={"a", "c"})
        assert result.performed
        assert v.get("a") == b"1"
        assert v.get("b") is None
        assert v.get("c") == b"3"


class TestGCDropOrder:
    def test_drops_oldest_touch_first(self):
        v = GCVersioned(high_water_bytes=150, low_water_bytes=80)
        # Create three keys (total=120, under high_water=150)
        v.commit({"a": b"x" * 40, "b": b"y" * 40, "c": b"z" * 40})
        # Touch "a" and "c" to make them recent; "b" stays coldest
        v.get("a")
        v.get("c")
        # Adding "d" pushes total to 160 > 150, triggering GC
        v.commit({"d": b"w" * 40})

        result = v.last_rebase_result
        assert result is not None
        assert result.performed
        # "b" had oldest touch, should be dropped first
        assert "b" in result.dropped_keys

    def test_drops_largest_among_same_touch(self):
        v = GCVersioned(high_water_bytes=60, low_water_bytes=30)
        # Same touch order but different sizes
        v.commit({"small": b"x" * 10, "big": b"y" * 50})
        v.commit({"extra": b"z" * 20})

        result = v.last_rebase_result
        assert result is not None
        assert result.performed
        # "big" should be dropped first (same touch, larger size)
        assert "big" in result.dropped_keys


class TestGCNamespaced:
    def test_system_keys_in_namespaces_retained(self):
        v = GCVersioned(high_water_bytes=100, low_water_bytes=50)
        v.commit({
            "ns/__system__": b"x" * 60,
            "ns/user_var": b"y" * 60,
        })
        v.commit({"trigger": b"z" * 20})

        # Namespaced system key should survive
        assert v.get("ns/__system__") == b"x" * 60


class TestGCOrphanCleanup:
    def test_clean_orphans_removes_unreachable(self):
        store = Memory()
        v = GCVersioned(store, high_water_bytes=10000)
        v.commit({"a": b"1"})

        # Create a branch that will become orphaned
        v2 = GCVersioned(store, high_water_bytes=10000)
        v2._create_commit({"orphan": b"data"})
        # Don't commit v2 — its commit is orphaned

        # Force orphan cleanup with min_age=0 to skip age check
        cleaned = v.clean_orphans(min_age=0)
        assert cleaned >= 1


class TestGCValidation:
    def test_high_water_must_be_positive(self):
        with pytest.raises(ValueError, match="high_water_bytes must be > 0"):
            GCVersioned(high_water_bytes=0)

    def test_low_water_defaults_to_80_percent(self):
        v = GCVersioned(high_water_bytes=1000)
        assert v.low_water == 800

    def test_invalid_low_water_falls_back(self):
        v = GCVersioned(high_water_bytes=1000, low_water_bytes=2000)
        assert v.low_water == 800  # falls back to 80%

    def test_rebase_cas_failure(self):
        """Rebase raises ConcurrencyError if HEAD was changed concurrently."""
        store = Memory()
        v = GCVersioned(store, high_water_bytes=10000)
        v.commit({"a": b"data"})

        # Advance HEAD behind v's back
        store.set(BRANCH_HEAD % "main", _to_bytes("bogus_hash"))

        with pytest.raises(ConcurrencyError, match="HEAD changed during rebase"):
            v.rebase()
