"""Tests for the Composite KV store."""

import pytest

from kvit.kv.composite import Composite
from kvit.kv.memory import Memory


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
