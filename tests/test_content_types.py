"""Tests for merge functions."""

import pytest

from kvgit import (
    MergeConflict,
    Staged,
    VersionedKV as Versioned,
    counter,
    last_writer_wins,
    text_merge,
)
from kvgit.kv.memory import Memory
from kvgit.merges import CantMark


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


def _diverged(value_main, value_dev, base):
    """Main + dev Staged pair, each with its own change to "doc"."""
    from kvgit.store import store

    main = store(kind="memory", branch="main")
    main["doc"] = base
    main.commit()
    dev = main.create_branch("dev")
    dev["doc"] = value_dev
    dev.commit()
    main["doc"] = value_main
    main.commit()
    return main, dev


class TestTextMerge:
    """The value-level text merge, as registered on a Staged."""

    def test_str_values_merge_to_str_with_markers(self):
        main, dev = _diverged("ours\n", "theirs\n", "base\n")

        result = main.merge(dev.current_commit, default_merge=text_merge())
        assert result.merged
        merged = main["doc"]
        assert isinstance(merged, str)
        assert merged == "<<<<<<< ours\nours\n=======\ntheirs\n>>>>>>> theirs\n"

    def test_str_values_merge_disjoint_lines_cleanly(self):
        main, dev = _diverged("ALPHA\nbeta\n", "alpha\nBETA\n", "alpha\nbeta\n")

        result = main.merge(dev.current_commit, default_merge=text_merge())
        assert result.merged
        assert main["doc"] == "ALPHA\nBETA\n"

    def test_custom_labels(self):
        main, dev = _diverged("ours\n", "theirs\n", "base\n")

        main.merge(
            dev.current_commit,
            default_merge=text_merge(ours_label="main", theirs_label="dev"),
        )
        assert main["doc"].startswith("<<<<<<< main\n")
        assert main["doc"].endswith(">>>>>>> dev\n")

    def test_bytes_values_merge_to_bytes(self):
        main, dev = _diverged(b"ALPHA\nbeta\n", b"alpha\nBETA\n", b"alpha\nbeta\n")

        result = main.merge(dev.current_commit, default_merge=text_merge())
        assert result.merged
        assert main["doc"] == b"ALPHA\nBETA\n"

    def test_removed_side_yields_the_other_sides_text(self):
        main, dev = _diverged("ours\nkept\n", "base\n", "base\n")
        del dev["doc"]
        dev.commit()

        result = main.merge(dev.current_commit, default_merge=text_merge())
        assert result.merged
        assert main["doc"] == ("<<<<<<< ours\nours\nkept\n=======\n>>>>>>> theirs\n")

    def test_unchanged_side_removed_takes_the_removal(self):
        main, dev = _diverged("base\n", "base\n", "base\n")
        del dev["doc"]
        dev.commit()

        result = main.merge(dev.current_commit, default_merge=text_merge())
        assert result.merged
        assert "doc" not in main

    def test_binary_values_conflict_through_cant_mark(self):
        main, dev = _diverged(b"\x00ours", b"\x00theirs", b"\x00base")

        with pytest.raises(MergeConflict) as exc_info:
            main.merge(dev.current_commit, default_merge=text_merge())
        assert exc_info.value.conflicting_keys == {"doc"}
        assert isinstance(exc_info.value.merge_errors["doc"], CantMark)

    def test_non_text_values_conflict_through_cant_mark(self):
        main, dev = _diverged(1, 2, 0)

        with pytest.raises(MergeConflict) as exc_info:
            main.merge(dev.current_commit, default_merge=text_merge())
        assert isinstance(exc_info.value.merge_errors["doc"], CantMark)

    def test_mixed_str_and_bytes_sides_come_back_as_str(self):
        fn = text_merge()
        assert fn(None, "ours\n", b"ours\n") == "ours\n"

    def test_cant_mark_propagates_from_the_fn_itself(self):
        fn = text_merge()
        with pytest.raises(CantMark):
            fn(None, "text\n", b"\x00binary")
