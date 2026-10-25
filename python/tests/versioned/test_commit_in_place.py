"""A commit changes the handle's key maps in place, and a failed attempt
puts back exactly what it changed.

These pin the rollback on every failure path, and the stable view that
``keys()`` keeps across a commit.
"""

import gc

import pytest
from support import fail_reads, race_publish

from kvgit import ConcurrencyError, MergeConflict
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import IN_FLIGHT_KEY, VersionedKV as Versioned


class FailingMemory(Memory):
    """A memory store whose Nth commit batch (counting from 1 after
    ``arm``) fails to land."""

    def __init__(self) -> None:
        super().__init__()
        self._batches_left: int | None = None

    def arm(self, nth: int) -> None:
        self._batches_left = nth

    def cas_many(self, expected, writes, removes=()):
        is_batch = any(k.startswith(IN_FLIGHT_KEY % "") for k in writes)
        if is_batch and self._batches_left is not None:
            self._batches_left -= 1
            if self._batches_left == 0:
                self._batches_left = None
                raise RuntimeError("landing write failed")
        return super().cas_many(expected, writes, removes)


def _state(v: Versioned) -> dict:
    """Everything a failed attempt must restore: the commit, the base,
    and both key maps entry for entry."""
    return {
        "commit": v.current_commit,
        "base": v.base_commit,
        "keys": dict(v._commit_keys),
        "meta": dict(v._meta),
    }


def _race_once(loser: Versioned, advance) -> None:
    """Make ``loser``'s next HEAD swap lose: run ``advance`` first."""
    race_publish(loser.store, loser.current_branch, advance)


def _setup():
    """A branch with keys to update, keep and remove, and a second
    handle on it."""
    store = FailingMemory()
    writer = Versioned(store)
    writer.commit({"keep": b"k0", "change": b"c0", "drop": b"d0"})
    return store, writer, Versioned(store)


def _change(v: Versioned):
    return v.commit({"change": b"c1", "add": b"a1"}, {"drop"})


class TestRollback:
    def test_fast_forward_batch_fails_to_land(self):
        store, _, v = _setup()
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            _change(v)
        assert _state(v) == before

    def test_merge_batch_after_lost_race_fails_to_land(self):
        store, writer, v = _setup()
        before = _state(v)

        def advance():
            writer.commit({"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="landing write failed"):
            _change(v)
        assert _state(v) == before

    def test_merge_publish_loses_a_second_race(self):
        store, writer, v = _setup()
        before = _state(v)
        n = 0

        def always_race():
            nonlocal n
            writer.commit({f"other{n}": b"o"})
            n += 1

        race_publish(store, "main", always_race, times=None)
        with pytest.raises(ConcurrencyError):
            _change(v)
        assert _state(v) == before

    def test_merge_after_lost_race_conflicts(self):
        _, writer, v = _setup()
        before = _state(v)
        _race_once(v, lambda: writer.commit({"change": b"theirs"}))
        with pytest.raises(MergeConflict):
            _change(v)
        assert _state(v) == before

    def test_read_for_merge_after_lost_race_fails(self):
        store, writer, v = _setup()
        before = _state(v)

        def advance():
            writer.commit({"other": b"o1"})
            # The merge's first read of history: the two heads' parents.
            fail_reads(
                store,
                lambda keys: any(k.startswith("__parent_commit__") for k in keys),
                RuntimeError("store read failed"),
            )

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="store read failed"):
            _change(v)
        assert _state(v) == before

    def test_merge_of_another_head_fails_to_land(self):
        store, _, v = _setup()
        side = v.create_branch("side")
        side.commit({"side": b"s1"})
        v.commit({"main": b"m1"})
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.merge_heads(side.current_commit)
        assert _state(v) == before

    def test_apply_fails_to_land(self):
        store, _, v = _setup()
        parent = v.current_commit
        v.commit({"picked": b"p1"})
        target = v.current_commit
        v.commit(removals={"picked"})
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.apply_change(parent, target)
        assert _state(v) == before


class TestRetry:
    def test_retry_lands_on_the_moved_head(self):
        store, writer, v = _setup()

        def advance():
            writer.commit({"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="landing write failed"):
            _change(v)

        result = _change(v)
        assert result.merged
        assert result.strategy == "three_way"
        assert v.current_commit == v.latest_head
        assert _state(v) == _state(Versioned(store))
        assert {k: v.get(k) for k in v.keys()} == {
            "keep": b"k0",
            "change": b"c1",
            "add": b"a1",
            "other": b"o1",
        }

    def test_lost_race_keeps_state_in_step_with_head(self):
        store, writer, v = _setup()
        _race_once(v, lambda: writer.commit({"other": b"o1"}))
        assert _change(v).strategy == "three_way"
        assert _state(v) == _state(Versioned(store))


class TestKeysView:
    def test_commit_without_live_view_keeps_the_same_dict(self):
        _, _, v = _setup()
        keys = v._commit_keys
        list(v.keys())  # a view that is dropped at once pins nothing
        _change(v)
        assert v._commit_keys is keys

    def test_view_keeps_its_keys_across_a_commit(self):
        _, _, v = _setup()
        view = v.keys()
        _change(v)
        assert sorted(view) == ["change", "drop", "keep"]
        assert "drop" in view and "add" not in view
        assert len(view) == 3
        assert sorted(v.keys()) == ["add", "change", "keep"]

    def test_committing_while_iterating(self):
        """Committing inside a loop over ``keys()`` neither raises
        "dictionary changed size during iteration" nor changes what the
        loop sees."""
        _, _, v = _setup()
        seen = []
        for key in v.keys():
            seen.append(key)
            v.commit({f"{key}-copy": b"x"}, {key})
        assert sorted(seen) == ["change", "drop", "keep"]
        assert sorted(v.keys()) == ["change-copy", "drop-copy", "keep-copy"]

    def test_view_keeps_its_keys_across_a_rollback(self):
        store, writer, v = _setup()
        view = v.keys()

        def advance():
            writer.commit({"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError):
            _change(v)
        assert sorted(view) == ["change", "drop", "keep"]

    def test_a_dropped_view_stops_pinning(self):
        _, _, v = _setup()
        view = v.keys()
        del view
        gc.collect()
        keys = v._commit_keys
        _change(v)
        assert v._commit_keys is keys

    def test_view_compares_as_a_set(self):
        _, _, v = _setup()
        assert v.keys() == {"keep", "change", "drop"}
        assert v.keys() - {"keep"} == {"change", "drop"}
