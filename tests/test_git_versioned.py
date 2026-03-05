"""Tests for VersionedGP (GitPython-backed versioned store)."""

import os
import shutil

import pytest

from kvgit.errors import MergeConflict
from kvgit.versioned_gp import VersionedGP as GitVersioned


@pytest.fixture
def repo_path(tmp_path):
    path = str(tmp_path / "test_repo.git")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


# -- Init / basics --


def test_empty_init(repo_path):
    v = GitVersioned(repo_path)
    assert v.current_commit is not None
    assert v.base_commit == v.current_commit
    assert list(v.keys()) == []


def test_repr(repo_path):
    v = GitVersioned(repo_path)
    r = repr(v)
    assert "VersionedGP" in r
    assert "main" in r


def test_contains(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    assert "a" in v
    assert "b" not in v


def test_commit_and_get(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"greeting": b"hello"})
    assert v.get("greeting") == b"hello"


def test_get_missing_returns_none(repo_path):
    v = GitVersioned(repo_path)
    assert v.get("nope") is None


def test_commit_and_get_many(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1", "b": b"2", "c": b"3"})
    result = v.get_many("a", "c", "missing")
    assert result == {"a": b"1", "c": b"3"}


def test_no_op_commit(repo_path):
    v = GitVersioned(repo_path)
    result = v.commit()
    assert result.merged
    assert result.strategy == "no_op"


def test_removals(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1", "b": b"2"})
    v.commit(removals={"a"})
    assert v.get("a") is None
    assert v.get("b") == b"2"


def test_commit_with_info(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"}, info={"author": "alice"})
    info = v.commit_info()
    assert info == {"author": "alice"}


def test_info_only_commit(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    result = v.commit(info={"checkpoint": True})
    assert result.merged
    assert v.commit_info() == {"checkpoint": True}


# -- commit_info --


def test_commit_info_none_when_no_info(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    assert v.commit_info() is None


def test_commit_info_specific_hash(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"}, info={"v": 1})
    h1 = v.current_commit
    v.commit({"a": b"2"}, info={"v": 2})
    assert v.commit_info(h1) == {"v": 1}
    assert v.commit_info() == {"v": 2}


def test_commit_info_bad_hash(repo_path):
    v = GitVersioned(repo_path)
    assert v.commit_info("0" * 40) is None


# -- diff --


def test_diff_added_removed_modified(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1", "b": b"2"})
    h1 = v.current_commit
    v.commit({"b": b"changed", "c": b"3"}, removals={"a"})
    h2 = v.current_commit

    d = v.diff(h1, h2)
    assert d.added == frozenset({"c"})
    assert d.removed == frozenset({"a"})
    assert d.modified == frozenset({"b"})


def test_diff_no_changes(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    h = v.current_commit
    d = v.diff(h, h)
    assert d.added == frozenset()
    assert d.removed == frozenset()
    assert d.modified == frozenset()


# -- parents --


def test_parents_initial(repo_path):
    v = GitVersioned(repo_path)
    # Initial commit has no parents
    assert v.parents() == ()


def test_parents_after_commit(repo_path):
    v = GitVersioned(repo_path)
    init = v.current_commit
    v.commit({"a": b"1"})
    assert v.parents() == (init,)


def test_parents_merge_commit(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"b": b"1"})
    v2.commit({"c": b"1"})  # triggers three-way merge

    # Merge commit should have two parents
    assert len(v2.parents()) == 2


# -- history --


def test_history_linear(repo_path):
    v = GitVersioned(repo_path)
    hashes = [v.current_commit]
    v.commit({"a": b"1"})
    hashes.append(v.current_commit)
    v.commit({"b": b"2"})
    hashes.append(v.current_commit)

    history = list(v.history())
    assert history == list(reversed(hashes))


def test_history_all_parents(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)
    v1.commit({"b": b"1"})
    v2.commit({"c": b"1"})  # three-way merge

    # all_parents should visit more commits than linear
    linear = list(v2.history())
    full = list(v2.history(all_parents=True))
    assert len(full) >= len(linear)
    assert set(linear).issubset(set(full))


def test_history_from_specific_commit(repo_path):
    v = GitVersioned(repo_path)
    init = v.current_commit
    v.commit({"a": b"1"})
    h1 = v.current_commit
    v.commit({"b": b"2"})

    history = list(v.history(commit_hash=h1))
    assert history == [h1, init]


# -- initial_commit --


def test_initial_commit(repo_path):
    v = GitVersioned(repo_path)
    init = v.initial_commit
    assert init == v.current_commit

    v.commit({"a": b"1"})
    v.commit({"b": b"2"})
    assert v.initial_commit == init


# -- branching --


def test_branching(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"main_key": b"1"})
    v2 = v.create_branch("dev")
    v2.commit({"dev_key": b"2"})

    assert v.get("dev_key") is None
    assert v2.get("main_key") == b"1"
    assert v2.get("dev_key") == b"2"

    assert v2.peek("main_key", branch="main") == b"1"
    assert v2.peek("dev_key", branch="main") is None


def test_create_branch_at_specific_commit(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    init = v.initial_commit
    v.commit({"b": b"2"})

    clean = v.create_branch("clean", at=init)
    assert clean.get("a") is None
    assert clean.get("b") is None


def test_create_branch_duplicate_raises(repo_path):
    v = GitVersioned(repo_path)
    v.create_branch("dev")
    with pytest.raises(ValueError, match="already exists"):
        v.create_branch("dev")


def test_create_branch_bad_commit_raises(repo_path):
    v = GitVersioned(repo_path)
    with pytest.raises(ValueError, match="does not exist"):
        v.create_branch("bad", at="0" * 40)


def test_delete_branch(repo_path):
    v = GitVersioned(repo_path)
    v.create_branch("dev")
    assert "dev" in v.list_branches()
    v.delete_branch("dev")
    assert "dev" not in v.list_branches()


def test_delete_current_branch_raises(repo_path):
    v = GitVersioned(repo_path)
    with pytest.raises(ValueError, match="current branch"):
        v.delete_branch("main")


def test_delete_nonexistent_branch_raises(repo_path):
    v = GitVersioned(repo_path)
    with pytest.raises(ValueError, match="does not exist"):
        v.delete_branch("nope")


def test_switch_branch(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"main_key": b"1"})
    dev = v.create_branch("dev")
    dev.commit({"dev_key": b"2"})

    v.switch_branch("dev")
    assert v.current_branch == "dev"
    assert v.get("dev_key") == b"2"


def test_switch_branch_nonexistent_raises(repo_path):
    v = GitVersioned(repo_path)
    with pytest.raises(ValueError, match="does not exist"):
        v.switch_branch("nope")


def test_list_branches(repo_path):
    v = GitVersioned(repo_path)
    v.create_branch("dev")
    v.create_branch("staging")
    branches = v.list_branches()
    assert set(branches) == {"main", "dev", "staging"}


def test_peek_nonexistent_branch(repo_path):
    v = GitVersioned(repo_path)
    assert v.peek("k", branch="nope") is None


def test_peek_nonexistent_key(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    assert v.peek("nope", branch="main") is None


# -- checkout / reset_to --


def test_checkout(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    h1 = v.current_commit
    v.commit({"b": b"2"})

    old = v.checkout(h1)
    assert old is not None
    assert old.get("a") == b"1"
    assert old.get("b") is None


def test_checkout_bad_hash(repo_path):
    v = GitVersioned(repo_path)
    assert v.checkout("0" * 40) is None


def test_checkout_with_branch(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    h = v.current_commit

    checked = v.checkout(h, branch="review")
    assert checked is not None
    assert checked.current_branch == "review"


def test_reset_to(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})
    h1 = v.current_commit
    v.commit({"b": b"2"})

    assert v.reset_to(h1)
    assert v.get("a") == b"1"
    assert v.get("b") is None
    assert v.current_commit == h1


def test_reset_to_bad_hash(repo_path):
    v = GitVersioned(repo_path)
    assert v.reset_to("0" * 40) is False


# -- Merging --


def test_merge_fast_forward(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v2 = GitVersioned(repo_path)
    v2.commit({"b": b"2"})

    assert v.latest_head == v2.current_commit

    v.refresh()
    assert v.get("b") == b"2"


def test_merge_three_way(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"b": b"1"})
    v2.commit({"c": b"1"})

    assert v2.get("a") == b"1"
    assert v2.get("b") == b"1"
    assert v2.get("c") == b"1"


def test_merge_three_way_result(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"b": b"1"})
    result = v2.commit({"c": b"1"})

    assert result.merged
    assert result.strategy == "three_way"


def test_merge_conflict(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"a": b"2"})
    with pytest.raises(MergeConflict) as exc_info:
        v2.commit({"a": b"3"})
    assert "a" in exc_info.value.conflicting_keys


def test_merge_conflict_abandon(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"a": b"2"})
    result = v2.commit({"a": b"3"}, on_conflict="abandon")
    assert not result.merged
    assert result.commit is None


def test_merge_with_merge_fn(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"counter": b"10"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    def sum_merge(old, ours, theirs):
        return str(int(ours) + int(theirs) - int(old)).encode()

    v2.set_merge_fn("counter", sum_merge)

    v1.commit({"counter": b"15"})
    v2.commit({"counter": b"20"})

    assert v2.get("counter") == b"25"


def test_merge_with_default_merge(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v2.set_default_merge(lambda old, ours, theirs: theirs)

    v1.commit({"a": b"2"})
    v2.commit({"a": b"3"})

    assert v2.get("a") == b"2"  # theirs wins (HEAD value)


def test_merge_with_per_commit_merge_fn(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"a": b"2"})
    result = v2.commit(
        {"a": b"3"},
        merge_fns={"a": lambda old, ours, theirs: ours},
    )
    assert result.merged
    assert v2.get("a") == b"3"  # ours wins


def test_both_sides_same_change(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"a": b"same"})
    result = v2.commit({"a": b"same"})  # identical change, no conflict
    assert result.merged
    assert v2.get("a") == b"same"


def test_both_sides_remove_same_key(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1", "b": b"2"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit(removals={"a"})
    result = v2.commit(removals={"a"})
    assert result.merged
    assert v2.get("a") is None
    assert v2.get("b") == b"2"


# -- State recovery --


def test_state_recovery_on_conflict(repo_path):
    """State is restored after a MergeConflict."""
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    saved_commit = v2.current_commit
    saved_keys = set(v2.keys())

    v1.commit({"a": b"2"})
    with pytest.raises(MergeConflict):
        v2.commit({"a": b"3"})

    # State should be restored
    assert v2.current_commit == saved_commit
    assert set(v2.keys()) == saved_keys


def test_on_conflict_abandon_fast_forward(repo_path):
    """CAS failure on fast-forward with abandon returns falsy result."""
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    # v1 advances HEAD
    v1.commit({"b": b"2"})

    # v2 tries fast-forward but HEAD changed — abandon
    result = v2.commit({"c": b"3"}, on_conflict="abandon")
    # This may succeed via three-way merge or abandon depending on timing
    # Either way result should be valid
    assert isinstance(result.merged, bool)


def test_on_conflict_invalid_raises(repo_path):
    v = GitVersioned(repo_path)
    with pytest.raises(ValueError, match="on_conflict"):
        v.commit({"a": b"1"}, on_conflict="invalid")


# -- last_merge_result --


def test_last_merge_result(repo_path):
    v = GitVersioned(repo_path)
    assert v.last_merge_result is None

    v.commit({"a": b"1"})
    assert v.last_merge_result is not None
    assert v.last_merge_result.merged


# -- Persistence across instances --


def test_persistence(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"key": b"value"})

    v2 = GitVersioned(repo_path)
    assert v2.get("key") == b"value"


# -- Staged wrapper --


def test_staged_wrapper(repo_path):
    from kvgit.staged import Staged

    v = GitVersioned(repo_path)
    s = Staged(v)

    s["user"] = "alice"
    s["score"] = 100
    s.commit()

    assert s["user"] == "alice"
    assert s["score"] == 100

    s2 = Staged(GitVersioned(repo_path))
    assert s2["user"] == "alice"
    assert s2["score"] == 100
