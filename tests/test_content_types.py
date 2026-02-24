"""Tests for merge functions."""


from kvgit import Staged, Versioned, counter, last_writer_wins
from kvgit.kv.memory import Memory


class TestCounter:
    def test_counter_merge(self):
        fn = counter()
        # old=5, ours=8, theirs=7 -> 8 + 7 - 5 = 10
        assert fn(5, 8, 7) == 10

    def test_counter_no_old(self):
        fn = counter()
        # old=None, ours=3, theirs=5 -> 3 + 5 - 0 = 8
        assert fn(None, 3, 5) == 8


class TestLastWriterWins:
    def test_always_returns_theirs(self):
        fn = last_writer_wins()
        assert fn("old", "ours", "theirs") == "theirs"


class TestMergeFnIntegration:
    def test_counter_end_to_end(self):
        """Full cycle: two branches increment counter, merge produces sum."""
        store = Memory()

        s1 = Staged(Versioned(store))
        s1["hits"] = 10
        s1.commit()

        s2 = Staged(Versioned(store))
        s2.set_merge_fn("hits", counter())

        # s1 increments to 15
        s1["hits"] = 15
        s1.commit()

        # s2 increments to 20
        # Three-way merge: 15 + 20 - 10 = 25
        s2["hits"] = 20
        assert s2.commit()
        assert s2.get("hits") == 25

    def test_set_merge_fn_resolves_conflict(self):
        """set_merge_fn registers the merge function."""
        store = Memory()

        s1 = Staged(Versioned(store))
        s1["x"] = 0
        s1.commit()

        s2 = Staged(Versioned(store))
        s2.set_merge_fn("x", counter())

        s1["x"] = 5
        s1.commit()

        # Without merge fn this would be a MergeConflict
        s2["x"] = 3
        assert s2.commit()
        assert s2.get("x") == 8  # 5 + 3 - 0

    def test_custom_merge_fn(self):
        """Custom merge function on decoded values."""

        def merge_lists(old, ours, theirs):
            base = set(old or [])
            return sorted(base | set(ours or []) | set(theirs or []))

        store = Memory()

        s1 = Staged(Versioned(store))
        s1["tags"] = ["a", "b"]
        s1.commit()

        s2 = Staged(Versioned(store))
        s2.set_merge_fn("tags", merge_lists)

        s1["tags"] = ["a", "b", "c"]
        s1.commit()

        s2["tags"] = ["a", "b", "d"]
        assert s2.commit()
        assert s2.get("tags") == ["a", "b", "c", "d"]
