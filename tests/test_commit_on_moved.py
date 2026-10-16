"""Tests for ``Worktree.commit(on_moved="raise")``: refuse to merge onto
a branch that moved since the worktree's head."""

import pytest
from support import worktree

from kvgit import BranchMovedError, ConcurrencyError
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import BRANCH_HEAD, IN_FLIGHT_KEY

IN_FLIGHT_PREFIX = IN_FLIGHT_KEY % ""


class RaceMemory(Memory):
    """A Memory that runs a one-shot hook just before the next
    ``cas_many`` that writes ``key``: the window between a commit's read
    of HEAD and the compare-and-swap that publishes it."""

    def __init__(self) -> None:
        super().__init__()
        self._hooks: dict = {}

    def before_writing(self, key: str, hook) -> None:
        self._hooks[key] = hook

    def cas_many(self, expected, writes, removes=()):
        for key in list(writes):
            hook = self._hooks.pop(key, None)
            if hook is not None:
                hook()
        return super().cas_many(expected, writes, removes)


def two_writers(store=None):
    """Two worktrees on ``main``, both based on a seed commit."""
    store = store if store is not None else Memory()
    seed = worktree(store)
    seed["seed"] = 0
    base = seed.commit().commit
    a = seed.repo.worktree("main")
    b = seed.repo.worktree("main")
    a["a"] = "from a"
    b["b"] = "from b"
    return seed.repo, store, base, a, b


def test_refuses_to_merge_disjoint_keys_onto_a_moved_branch():
    repo, _, base, a, b = two_writers()
    first = a.commit().commit

    with pytest.raises(BranchMovedError) as caught:
        b.commit(on_moved="raise")

    err = caught.value
    assert isinstance(err, ConcurrencyError)
    assert (err.branch, err.base, err.head) == ("main", base, first)
    # The branch holds only the first commit.
    assert repo.branches["main"] == first
    assert repo.get_commit(first).parents == (base,)
    assert set(repo.snapshot(branch="main").keys()) == {"seed", "a"}


def test_keeps_the_pending_changes_and_the_head():
    _, _, base, a, b = two_writers()
    a.commit()
    del b["seed"]

    with pytest.raises(BranchMovedError):
        b.commit(on_moved="raise")

    assert b.head == base
    assert b.status().updated == {"b"}
    assert b.status().removed == {"seed"}
    assert b["b"] == "from b"
    assert "seed" not in b


def test_refuses_whatever_on_conflict_says():
    _, _, _, a, b = two_writers()
    a.commit()
    with pytest.raises(BranchMovedError):
        b.commit(on_moved="raise", on_conflict="abandon")


def test_still_merges_by_default_and_commits_cleanly_after_a_refresh():
    repo, _, _, a, b = two_writers()
    first = a.commit().commit

    merged = b.commit()
    assert merged.strategy == "three_way"
    assert repo.get_commit(merged.commit).parents[0] == first

    # Retry pattern: refresh to the tip, redo the work, commit.
    c = repo.worktree("main")
    a["a"] = "again"
    tip = a.commit().commit
    c["c"] = 1
    with pytest.raises(BranchMovedError):
        c.commit(on_moved="raise")
    c.refresh()
    assert c.head == tip
    c["c"] = 1
    landed = c.commit(on_moved="raise")
    assert landed.strategy == "fast_forward"
    assert repo.get_commit(landed.commit).parents == (tip,)


def test_fast_forwards_when_the_branch_has_not_moved():
    repo, _, base, a, _ = two_writers()
    result = a.commit(on_moved="raise")
    assert result.strategy == "fast_forward"
    assert repo.get_commit(result.commit).parents == (base,)


def test_writes_nothing_when_the_branch_had_already_moved():
    _, store, _, a, b = two_writers()
    a.commit()
    before = set(store.keys())
    with pytest.raises(BranchMovedError):
        b.commit(on_moved="raise")
    assert set(store.keys()) == before


def test_refuses_a_move_inside_the_publishing_cas_window():
    store = RaceMemory()
    repo, _, base, a, b = two_writers(store)
    won = []
    store.before_writing(BRANCH_HEAD % "main", lambda: won.append(a.commit().commit))

    with pytest.raises(BranchMovedError) as caught:
        b.commit(on_moved="raise")

    [first] = won
    assert (caught.value.base, caught.value.head) == (base, first)
    assert repo.branches["main"] == first
    assert repo.get_commit(first).parents == (base,)
    assert b.head == base
    assert b.status().updated == {"b"}


def test_leaves_nothing_reachable_after_losing_the_cas():
    store = RaceMemory()
    repo, _, _, a, b = two_writers(store)
    before = set(store.keys())
    seen = {}

    def advance():
        # b's batch has landed; a's commit wins the head.
        seen["with_b"] = set(store.keys())
        a.commit()
        seen["with_a"] = set(store.keys())

    store.before_writing(BRANCH_HEAD % "main", advance)
    with pytest.raises(BranchMovedError):
        b.commit(on_moved="raise")

    b_batch = seen["with_b"] - before
    assert b_batch  # the attempt did write its commit before losing
    # Its in-flight marker is withdrawn, so its commit is an ordinary
    # orphan: the only one a sweep takes, and every key it wrote goes.
    assert not [k for k in store.keys() if k.startswith(IN_FLIGHT_PREFIX)]
    assert repo.gc(min_age=0) == 1
    assert not b_batch & set(store.keys())
    assert set(repo.snapshot(branch="main").keys()) == {"seed", "a"}


def test_rejects_an_unknown_disposition():
    _, _, _, a, _ = two_writers()
    with pytest.raises(ValueError):
        a.commit(on_moved="fail")
    assert a.status().updated == {"a"}
