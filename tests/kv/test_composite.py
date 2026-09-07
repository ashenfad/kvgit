"""Tests for the Composite KV store."""

import logging

import pytest

from kvgit import Staged, VersionedKV
from kvgit.kv.composite import Composite
from kvgit.kv.memory import Memory


class _FlakyStore(Memory):
    """Memory tier that raises a configured exception on every operation."""

    def __init__(self, exc: Exception) -> None:
        super().__init__()
        self._exc = exc

    def get(self, key: str):
        raise self._exc

    def get_many(self, *args):
        raise self._exc

    def set(self, key: str, value: bytes) -> None:
        raise self._exc

    def set_many(self, items=None, /, **kwargs) -> None:
        raise self._exc

    def remove(self, key: str) -> None:
        raise self._exc

    def remove_many(self, *args) -> None:
        raise self._exc

    def clear(self) -> None:
        raise self._exc

    def __contains__(self, key: str) -> bool:
        raise self._exc


class TestCompositeBasic:
    def test_single_tier(self):
        m = Memory()
        c = Composite([m])
        c.set("k", b"v")
        assert c.get("k") == b"v"

    def test_two_tier_read_through(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        # Write goes to both tiers
        c.set("k", b"v")
        assert l2.get("k") == b"v"
        assert l1.get("k") == b"v"

    def test_l2_hit_populates_l1(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        # Put directly into l2 only
        l2.set("k", b"from-l2")
        assert l1.get("k") is None
        # Read through composite should populate l1
        assert c.get("k") == b"from-l2"
        assert l1.get("k") == b"from-l2"

    def test_three_tier_cascading(self):
        l1, l2, l3 = Memory(), Memory(), Memory()
        c = Composite([l1, l2, l3])
        l3.set("k", b"deep")
        assert c.get("k") == b"deep"
        assert l1.get("k") == b"deep"
        assert l2.get("k") == b"deep"

    def test_contains(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l2.set("k", b"v")
        assert "k" in c
        assert "nope" not in c

    def test_keys_from_authoritative(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l1.set("l1only", b"1")
        l2.set("l2only", b"2")
        c.set("both", b"3")
        # keys() comes from last tier only
        keys = set(c.keys())
        assert "l2only" in keys
        assert "both" in keys

    def test_empty_stores_raises(self):
        with pytest.raises(ValueError):
            Composite([])

    def test_get_many_partial_hits_across_tiers(self):
        # "a" only in L2, "b" only in L1, "c" missing entirely.
        # Composite must collect a + b and skip c, populating L1 with a.
        l1, l2 = Memory(), Memory()
        l1.set("b", b"from-l1")
        l2.set("a", b"from-l2")
        c = Composite([l1, l2])
        result = c.get_many("a", "b", "c")
        assert dict(result) == {"a": b"from-l2", "b": b"from-l1"}
        # L2 hit on "a" should have populated L1
        assert l1.get("a") == b"from-l2"

    def test_get_many_short_circuits_when_satisfied(self):
        # With all keys served from L1, L2 must never be consulted.
        l1 = Memory()
        l1.set_many(a=b"1", b=b"2")
        sentinel = _FlakyStore(AssertionError("L2 should not be touched"))
        c = Composite([l1, sentinel])
        result = c.get_many("a", "b")
        assert dict(result) == {"a": b"1", "b": b"2"}


class TestCompositeRemove:
    def test_remove_all_tiers(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        c.set("k", b"v")
        c.remove("k")
        assert l1.get("k") is None
        assert l2.get("k") is None

    def test_remove_many(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        c.set_many(a=b"1", b=b"2")
        c.remove_many("a", "b")
        assert l1.get("a") is None
        assert l2.get("a") is None


class TestCompositeCAS:
    def test_cas_on_authoritative(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        c.set("k", b"old")
        assert c.cas("k", b"new", expected=b"old")
        assert l2.get("k") == b"new"
        assert l1.get("k") == b"new"

    def test_cas_failure(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        c.set("k", b"old")
        assert not c.cas("k", b"new", expected=b"wrong")
        assert l2.get("k") == b"old"

    def test_clear(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        c.set_many(a=b"1", b=b"2")
        c.clear()
        assert l1.get("a") is None
        assert l2.get("a") is None


class TestCompositeFailureModes:
    """Operational tier failures fall through with a warning;
    programming-bug exceptions propagate."""

    def test_get_falls_through_on_operational_failure(self, caplog):
        flaky = _FlakyStore(OSError("disk gone"))
        l2 = Memory()
        l2.set("k", b"v")
        c = Composite([flaky, l2])
        with caplog.at_level(logging.WARNING, logger="kvgit.kv.composite"):
            assert c.get("k") == b"v"
        assert any("tier 0" in r.message for r in caplog.records)

    def test_get_propagates_bug_exception(self):
        flaky = _FlakyStore(TypeError("misconfigured tier"))
        l2 = Memory()
        l2.set("k", b"v")
        c = Composite([flaky, l2])
        with pytest.raises(TypeError, match="misconfigured tier"):
            c.get("k")

    def test_set_propagates_authoritative_failure(self):
        # A failure in the authoritative (last) tier always propagates
        # regardless of exception type — durability is the contract of set().
        l1 = Memory()
        flaky = _FlakyStore(OSError("disk full"))
        c = Composite([l1, flaky])
        with pytest.raises(OSError, match="disk full"):
            c.set("k", b"v")

    def test_set_logs_cache_tier_failure(self, caplog):
        # Authoritative tier succeeds; cache tier fails operationally.
        # User-visible behavior: success, with a warning.
        flaky = _FlakyStore(OSError("cache stale"))
        l2 = Memory()
        c = Composite([flaky, l2])
        with caplog.at_level(logging.WARNING, logger="kvgit.kv.composite"):
            c.set("k", b"v")
        assert l2.get("k") == b"v"
        assert any("tier 0" in r.message for r in caplog.records)

    def test_set_propagates_cache_tier_bug(self):
        flaky = _FlakyStore(AttributeError("typo on tier impl"))
        l2 = Memory()
        c = Composite([flaky, l2])
        with pytest.raises(AttributeError, match="typo on tier impl"):
            c.set("k", b"v")

    def test_contains_falls_through_on_operational_failure(self, caplog):
        flaky = _FlakyStore(OSError("network down"))
        l2 = Memory()
        l2.set("k", b"v")
        c = Composite([flaky, l2])
        with caplog.at_level(logging.WARNING, logger="kvgit.kv.composite"):
            assert "k" in c
        assert any("tier 0" in r.message for r in caplog.records)


class TestMutableKeysReadThrough:
    """``__``-prefixed keys are served by the authoritative tier alone.

    Branch heads, their prev-HEAD backups, the storage version stamp and
    the GC lease all change under a fixed key. Cached, they let a
    process keep serving state another process has already replaced —
    the failure being that a handle takes a ``ConcurrencyError`` on
    commit, refreshes, and reads the same stale head back out of L1.
    Content-derived keys are unaffected: the same key always holds the
    same bytes, so any tier's copy is the right answer.
    """

    def test_get_ignores_a_stale_cached_branch_head(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l1.set("__branch_head__main", b'"stale"')
        l2.set("__branch_head__main", b'"fresh"')
        assert c.get("__branch_head__main") == b'"fresh"'

    def test_a_mutable_key_missing_from_the_authoritative_tier_is_missing(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l1.set("__kvgit_version__", b"3")
        assert c.get("__kvgit_version__") is None
        assert "__kvgit_version__" not in c
        assert c.get_many(["__kvgit_version__"]) == {}

    def test_get_many_splits_mutable_from_content_keys(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l1.set("__kvgit_version__", b"2")
        l2.set("__kvgit_version__", b"3")
        l2.set("kvgit:chunk:x", b"chunk bytes")

        got = c.get_many(["__kvgit_version__", "kvgit:chunk:x"])
        assert got == {"__kvgit_version__": b"3", "kvgit:chunk:x": b"chunk bytes"}
        assert l1.get("__kvgit_version__") == b"2", (
            "a mutable key must not be written into a cache tier"
        )
        assert l1.get("kvgit:chunk:x") == b"chunk bytes"

    def test_cas_on_a_mutable_key_does_not_populate_the_cache(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        assert c.cas("__branch_head__main", b'"a"', expected=None)
        assert l2.get("__branch_head__main") == b'"a"'
        assert l1.get("__branch_head__main") is None

    def test_content_addressed_keys_are_still_cached(self):
        l1, l2 = Memory(), Memory()
        c = Composite([l1, l2])
        l2.set("kvgit:chunk:abc", b"payload")

        assert c.get("kvgit:chunk:abc") == b"payload"
        assert l1.get("kvgit:chunk:abc") == b"payload", "a miss must populate L1"

        # Only L1 can answer now, which is the point of caching them.
        l2.remove("kvgit:chunk:abc")
        assert c.get("kvgit:chunk:abc") == b"payload"


class TestCompositeBackedHandle:
    """A cache tier must not hide another process's commits."""

    def test_refresh_sees_a_commit_made_through_the_shared_tier(self):
        shared = Memory()
        cached = Composite([Memory(), shared])

        mine = Staged(VersionedKV(cached))
        mine["k"] = "first"
        mine.commit()

        # A second handle on the authoritative store, standing in for
        # another process sharing the same backend.
        theirs = Staged(VersionedKV(shared))
        theirs["k"] = "second"
        theirs.commit()

        mine.refresh()
        assert mine.current_commit == theirs.current_commit, (
            "the L1 tier served a branch head another writer had moved"
        )
        assert mine["k"] == "second"
