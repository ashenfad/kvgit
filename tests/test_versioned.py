"""Tests for the Versioned commit log."""

from vkv import Versioned
from vkv.kv.memory import Memory


class TestVersionedBasic:
    def test_empty_init(self):
        v = Versioned()
        assert v.current_commit is not None
        assert v.base_commit == v.current_commit
        assert list(v.keys()) == []

    def test_snapshot_and_get(self):
        v = Versioned()
        v.snapshot({"greeting": b"hello"})
        assert v.get("greeting") == b"hello"

    def test_get_missing(self):
        v = Versioned()
        assert v.get("nope") is None

    def test_get_many(self):
        v = Versioned()
        v.snapshot({"a": b"1", "b": b"2", "c": b"3"})
        result = v.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_keys(self):
        v = Versioned()
        v.snapshot({"a": b"1", "b": b"2"})
        assert set(v.keys()) == {"a", "b"}

    def test_contains(self):
        v = Versioned()
        v.snapshot({"k": b"v"})
        assert "k" in v
        assert "nope" not in v

    def test_snapshot_returns_hash(self):
        v = Versioned()
        h1 = v.snapshot({"k": b"v"})
        assert isinstance(h1, str)
        assert len(h1) == 16
        assert v.current_commit == h1

    def test_no_op_snapshot(self):
        v = Versioned()
        initial = v.current_commit
        result = v.snapshot()
        assert result == initial

    def test_content_addressable(self):
        """Same changes on same parent produce same hash."""
        store = Memory()
        v1 = Versioned(store)
        h1 = v1.snapshot({"k": b"v"})

        v2 = Versioned(Memory())
        h2 = v2.snapshot({"k": b"v"})
        assert h1 == h2


class TestVersionedUpdatesAndRemovals:
    def test_update_existing_key(self):
        v = Versioned()
        v.snapshot({"k": b"old"})
        v.snapshot({"k": b"new"})
        assert v.get("k") == b"new"

    def test_remove_key(self):
        v = Versioned()
        v.snapshot({"a": b"1", "b": b"2"})
        v.snapshot(removals={"a"})
        assert v.get("a") is None
        assert v.get("b") == b"2"

    def test_update_and_remove(self):
        v = Versioned()
        v.snapshot({"a": b"1", "b": b"2", "c": b"3"})
        v.snapshot(updates={"a": b"updated"}, removals={"c"})
        assert v.get("a") == b"updated"
        assert v.get("b") == b"2"
        assert v.get("c") is None

    def test_multiple_snapshots(self):
        v = Versioned()
        v.snapshot({"a": b"1"})
        v.snapshot({"b": b"2"})
        v.snapshot({"c": b"3"})
        assert v.get("a") == b"1"
        assert v.get("b") == b"2"
        assert v.get("c") == b"3"


class TestVersionedHistory:
    def test_history_chain(self):
        v = Versioned()
        h0 = v.current_commit
        h1 = v.snapshot({"a": b"1"})
        h2 = v.snapshot({"b": b"2"})
        history = list(v.history())
        assert history == [h2, h1, h0]

    def test_initial_commit(self):
        v = Versioned()
        h0 = v.current_commit
        v.snapshot({"a": b"1"})
        v.snapshot({"b": b"2"})
        assert v.initial_commit == h0

    def test_history_from_specific_commit(self):
        v = Versioned()
        h0 = v.current_commit
        h1 = v.snapshot({"a": b"1"})
        v.snapshot({"b": b"2"})
        history = list(v.history(commit_hash=h1))
        assert history == [h1, h0]


class TestVersionedCheckout:
    def test_checkout_old_commit(self):
        store = Memory()
        v = Versioned(store)
        v.snapshot({"a": b"1"})
        h1 = v.current_commit
        v.snapshot({"b": b"2"})

        old = v.checkout(h1)
        assert old is not None
        assert old.get("a") == b"1"
        assert old.get("b") is None

    def test_checkout_invalid(self):
        v = Versioned()
        assert v.checkout("nonexistent") is None

    def test_reset_to(self):
        store = Memory()
        v = Versioned(store)
        v.snapshot({"a": b"1"})
        h1 = v.current_commit
        v.snapshot({"b": b"2"})

        assert v.reset_to(h1)
        assert v.get("a") == b"1"
        assert v.get("b") is None
        assert v.current_commit == h1

    def test_reset_to_invalid(self):
        v = Versioned()
        assert not v.reset_to("nonexistent")


class TestVersionedMerge:
    def test_merge_fast_forward(self):
        store = Memory()
        v = Versioned(store)
        v.snapshot({"a": b"1"})
        assert v.merge()
        assert v.base_commit == v.current_commit

    def test_merge_no_changes(self):
        v = Versioned()
        assert v.merge()

    def test_merge_conflict_raises(self):
        import pytest

        from vkv import ConcurrencyError

        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        # v2 branches from same base
        v2 = Versioned(store)
        v2.snapshot({"b": b"2"})

        # v1 advances HEAD
        v1.snapshot({"c": b"3"})
        v1.merge()

        # v2's merge should fail — HEAD diverged
        with pytest.raises(ConcurrencyError):
            v2.merge()

    def test_merge_conflict_abandon(self):
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store)
        v2.snapshot({"b": b"2"})

        v1.snapshot({"c": b"3"})
        v1.merge()

        assert not v2.merge(on_conflict="abandon")

    def test_reset_after_conflict(self):
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store)
        v2.snapshot({"b": b"2"})

        v1.snapshot({"c": b"3"})
        v1.merge()

        v2.reset()
        assert v2.get("c") == b"3"
        assert v2.get("b") is None


class TestVersionedSharedStore:
    def test_two_writers_same_store(self):
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store)
        assert v2.get("a") == b"1"

    def test_latest_head(self):
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store, commit_hash=v1.initial_commit)
        assert v2.latest_head == v1.current_commit
