"""A commit, merge or apply that fails leaves the worktree as it found
it, on every failure path: its head, its pending changes and every
value it reads. ``keys()`` keeps what it returned across a commit.
"""

import pytest
from support import fail_reads, fork, race_publish

from kvgit import ConcurrencyError, MergeConflict, Repo, Worktree
from kvgit._store import IN_FLIGHT_KEY
from kvgit.kv.memory import Memory


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


def _main(store) -> Worktree:
    """A worktree on ``main`` storing values as the bytes given."""
    return Repo(store, codec="bytes").worktree("main", create=True)


def _commit(wt: Worktree, updates: dict, removals=(), **options):
    wt.update(updates)
    for key in removals:
        del wt[key]
    return wt.commit(**options)


def _state(wt: Worktree) -> dict:
    """Everything a failed attempt must leave as it was: the head, the
    pending changes, and every key's value."""
    return {
        "commit": wt.head,
        "pending": wt.status(),
        "values": wt.get_many(*wt.keys()),
    }


def _race_once(loser: Worktree, advance) -> None:
    """Make ``loser``'s next HEAD swap lose: run ``advance`` first."""
    race_publish(loser.repo.store, loser.branch, advance)


def _setup():
    """A branch with keys to update, keep and remove, and a second
    worktree on it."""
    store = FailingMemory()
    writer = _main(store)
    _commit(writer, {"keep": b"k0", "change": b"c0", "drop": b"d0"})
    return store, writer, _main(store)


def _stage(wt: Worktree) -> None:
    wt.update({"change": b"c1", "add": b"a1"})
    if "drop" in wt:
        del wt["drop"]


def _change(wt: Worktree):
    _stage(wt)
    return wt.commit()


class TestRollback:
    def test_fast_forward_batch_fails_to_land(self):
        store, _, v = _setup()
        _stage(v)
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.commit()
        assert _state(v) == before

    def test_merge_batch_after_lost_race_fails_to_land(self):
        store, writer, v = _setup()
        _stage(v)
        before = _state(v)

        def advance():
            _commit(writer, {"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.commit()
        assert _state(v) == before

    def test_merge_publish_loses_a_second_race(self):
        store, writer, v = _setup()
        _stage(v)
        before = _state(v)
        n = 0

        def always_race():
            nonlocal n
            _commit(writer, {f"other{n}": b"o"})
            n += 1

        race_publish(store, "main", always_race, times=None)
        with pytest.raises(ConcurrencyError):
            v.commit()
        assert _state(v) == before

    def test_merge_after_lost_race_conflicts(self):
        _, writer, v = _setup()
        _stage(v)
        before = _state(v)
        _race_once(v, lambda: _commit(writer, {"change": b"theirs"}))
        with pytest.raises(MergeConflict):
            v.commit()
        assert _state(v) == before

    def test_read_for_merge_after_lost_race_fails(self):
        store, writer, v = _setup()
        _stage(v)
        before = _state(v)

        def advance():
            _commit(writer, {"other": b"o1"})
            # The merge's first read of history: the two heads' parents.
            fail_reads(
                store,
                lambda keys: any(k.startswith("__parent_commit__") for k in keys),
                RuntimeError("store read failed"),
            )

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="store read failed"):
            v.commit()
        assert _state(v) == before

    def test_merge_of_another_head_fails_to_land(self):
        store, _, v = _setup()
        side = fork(v, "side")
        _commit(side, {"side": b"s1"})
        _commit(v, {"main": b"m1"})
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.merge(commit=side.head)
        assert _state(v) == before

    def test_apply_fails_to_land(self):
        store, _, v = _setup()
        parent = v.head
        _commit(v, {"picked": b"p1"})
        target = v.head
        _commit(v, {}, {"picked"})
        before = _state(v)
        store.arm(1)
        with pytest.raises(RuntimeError, match="landing write failed"):
            v.apply(parent, target)
        assert _state(v) == before


class TestRetry:
    def test_retry_lands_on_the_moved_head(self):
        store, writer, v = _setup()

        def advance():
            _commit(writer, {"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError, match="landing write failed"):
            _change(v)

        result = _change(v)
        assert result.merged
        assert result.strategy == "three_way"
        assert v.head == v.repo.branches["main"]
        assert _state(v) == _state(_main(store))
        assert {k: v.get(k) for k in v.keys()} == {
            "keep": b"k0",
            "change": b"c1",
            "add": b"a1",
            "other": b"o1",
        }

    def test_lost_race_keeps_state_in_step_with_head(self):
        store, writer, v = _setup()
        _race_once(v, lambda: _commit(writer, {"other": b"o1"}))
        assert _change(v).strategy == "three_way"
        assert _state(v) == _state(_main(store))


class TestKeysView:
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
            _commit(v, {f"{key}-copy": b"x"}, {key})
        assert sorted(seen) == ["change", "drop", "keep"]
        assert sorted(v.keys()) == ["change-copy", "drop-copy", "keep-copy"]

    def test_view_keeps_its_keys_across_a_rollback(self):
        store, writer, v = _setup()
        view = v.keys()

        def advance():
            _commit(writer, {"other": b"o1"})
            store.arm(1)

        _race_once(v, advance)
        with pytest.raises(RuntimeError):
            _change(v)
        assert sorted(view) == ["change", "drop", "keep"]

    def test_view_compares_as_a_set(self):
        _, _, v = _setup()
        assert v.keys() == {"keep", "change", "drop"}
        assert v.keys() - {"keep"} == {"change", "drop"}
