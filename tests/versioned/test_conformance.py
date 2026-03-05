"""Conformance tests: verify VersionedKV and VersionedGP behave identically."""

import pytest

from kvgit.versioned.kv import VersionedKV
from kvgit.versioned.protocol import Versioned

try:
    from kvgit.versioned.gp import VersionedGP

    HAS_GP = True
except ImportError:
    HAS_GP = False


@pytest.fixture(params=["kv", "gp"])
def versioned(request, tmp_path) -> Versioned:
    if request.param == "kv":
        return VersionedKV()
    if not HAS_GP:
        pytest.skip("GitPython not installed")
    return VersionedGP(str(tmp_path / "repo.git"))


@pytest.fixture(params=["kv", "gp"])
def make_versioned(request, tmp_path):
    """Factory that creates versioned instances sharing the same backend."""
    _counter = [0]

    def _make(*, branch="main"):
        if request.param == "kv":
            if not hasattr(_make, "_store"):
                from kvgit.kv.memory import Memory

                _make._store = Memory()
            return VersionedKV(_make._store, branch=branch)
        if not HAS_GP:
            pytest.skip("GitPython not installed")
        if not hasattr(_make, "_path"):
            _make._path = str(tmp_path / "repo.git")
        return VersionedGP(_make._path, branch=branch)

    return _make


# -- Basic operations --


class TestBasic:
    def test_init(self, versioned):
        assert versioned.current_commit is not None
        assert len(versioned.current_commit) == 40
        assert versioned.base_commit == versioned.current_commit
        assert list(versioned.keys()) == []

    def test_commit_and_get(self, versioned):
        versioned.commit({"key": b"value"})
        assert versioned.get("key") == b"value"

    def test_get_missing(self, versioned):
        assert versioned.get("nope") is None

    def test_get_many(self, versioned):
        versioned.commit({"a": b"1", "b": b"2", "c": b"3"})
        result = versioned.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_keys(self, versioned):
        versioned.commit({"x": b"1", "y": b"2"})
        assert set(versioned.keys()) == {"x", "y"}

    def test_contains(self, versioned):
        versioned.commit({"present": b"yes"})
        assert "present" in versioned
        assert "absent" not in versioned

    def test_removal(self, versioned):
        versioned.commit({"a": b"1", "b": b"2"})
        versioned.commit(removals={"a"})
        assert versioned.get("a") is None
        assert versioned.get("b") == b"2"

    def test_update_value(self, versioned):
        versioned.commit({"k": b"v1"})
        versioned.commit({"k": b"v2"})
        assert versioned.get("k") == b"v2"

    def test_commit_returns_merge_result(self, versioned):
        result = versioned.commit({"k": b"v"})
        assert result.merged
        assert result.commit is not None
        assert len(result.commit) == 40


# -- History --


class TestHistory:
    def test_history_chain(self, versioned):
        versioned.commit({"a": b"1"})
        versioned.commit({"b": b"2"})
        history = list(versioned.history())
        assert len(history) == 3  # init + 2 commits
        assert history[0] == versioned.current_commit

    def test_parents(self, versioned):
        versioned.commit({"a": b"1"})
        first = versioned.current_commit
        versioned.commit({"b": b"2"})
        parents = versioned.parents()
        assert first in parents

    def test_initial_commit(self, versioned):
        init = versioned.current_commit
        versioned.commit({"a": b"1"})
        assert versioned.initial_commit == init

    def test_commit_info(self, versioned):
        versioned.commit({"a": b"1"}, info={"author": "test"})
        info = versioned.commit_info()
        assert info == {"author": "test"}

    def test_commit_info_none(self, versioned):
        versioned.commit({"a": b"1"})
        assert versioned.commit_info() is None


# -- Branching --


class TestBranching:
    def test_create_and_list_branches(self, make_versioned):
        v = make_versioned()
        v.commit({"a": b"1"})
        v.create_branch("feature")
        assert "feature" in v.list_branches()
        assert "main" in v.list_branches()

    def test_delete_branch(self, make_versioned):
        v = make_versioned()
        v.commit({"a": b"1"})
        v.create_branch("temp")
        v.delete_branch("temp")
        assert "temp" not in v.list_branches()

    def test_switch_branch(self, make_versioned):
        v = make_versioned()
        v.commit({"a": b"1"})
        v.create_branch("other")
        v.switch_branch("other")
        assert v.current_branch == "other"
        assert v.get("a") == b"1"

    def test_peek(self, make_versioned):
        v = make_versioned()
        v.commit({"shared": b"base"})
        v.create_branch("side")
        side = make_versioned(branch="side")
        side.commit({"only_side": b"yes"})
        assert v.peek("only_side", branch="side") == b"yes"
        assert v.peek("only_side", branch="main") is None


# -- Diff --


class TestDiff:
    def test_diff_added(self, versioned):
        c0 = versioned.current_commit
        versioned.commit({"a": b"1", "b": b"2"})
        c1 = versioned.current_commit
        diff = versioned.diff(c0, c1)
        assert diff.added == frozenset({"a", "b"})
        assert diff.removed == frozenset()
        assert diff.modified == frozenset()

    def test_diff_removed(self, versioned):
        versioned.commit({"a": b"1", "b": b"2"})
        c1 = versioned.current_commit
        versioned.commit(removals={"a"})
        c2 = versioned.current_commit
        diff = versioned.diff(c1, c2)
        assert "a" in diff.removed

    def test_diff_modified(self, versioned):
        versioned.commit({"a": b"1"})
        c1 = versioned.current_commit
        versioned.commit({"a": b"2"})
        c2 = versioned.current_commit
        diff = versioned.diff(c1, c2)
        assert "a" in diff.modified


# -- Merge --


class TestMerge:
    def test_fast_forward(self, make_versioned):
        v1 = make_versioned()
        v1.commit({"a": b"1"})
        v2 = make_versioned()
        v2.commit({"b": b"2"})
        # v1 is behind, committing triggers fast-forward merge
        result = v1.commit({"c": b"3"})
        assert result.merged
        assert result.strategy == "three_way"

    def test_non_conflicting_three_way(self, make_versioned):
        v1 = make_versioned()
        v1.commit({"base": b"shared"})
        v2 = make_versioned()
        # Diverge: v1 adds "a", v2 adds "b"
        v1.commit({"a": b"from_v1"})
        v2.commit({"b": b"from_v2"})
        # v2's commit triggers three-way merge
        assert v2.get("a") == b"from_v1"
        assert v2.get("b") == b"from_v2"
        assert v2.get("base") == b"shared"

    def test_conflict_with_merge_fn(self, make_versioned):
        v1 = make_versioned()
        v1.commit({"counter": b"100"})
        v2 = make_versioned()

        def add_merge(old, ours, theirs):
            o = int(old) if old else 0
            a = int(ours) if ours else 0
            b = int(theirs) if theirs else 0
            return str(a + b - o).encode()

        # Diverge: both update counter
        v1.commit({"counter": b"115"})
        v2.commit({"counter": b"120"}, merge_fns={"counter": add_merge})
        assert v2.get("counter") == b"135"

    def test_conflict_raises(self, make_versioned):
        from kvgit.errors import MergeConflict

        v1 = make_versioned()
        v1.commit({"key": b"base"})
        v2 = make_versioned()
        v1.commit({"key": b"v1_change"})
        with pytest.raises(MergeConflict):
            v2.commit({"key": b"v2_change"})

    def test_abandon_on_conflict(self, make_versioned):
        v1 = make_versioned()
        v1.commit({"key": b"base"})
        v2 = make_versioned()
        v1.commit({"key": b"v1_change"})
        result = v2.commit({"key": b"v2_change"}, on_conflict="abandon")
        assert not result.merged


# -- Checkout / Reset / Refresh --


class TestNavigation:
    def test_checkout(self, versioned):
        versioned.commit({"a": b"1"})
        c1 = versioned.current_commit
        versioned.commit({"a": b"2"})
        old = versioned.checkout(c1)
        assert old is not None
        assert old.get("a") == b"1"

    def test_checkout_invalid(self, versioned):
        assert versioned.checkout("0" * 40) is None

    def test_reset_to(self, versioned):
        versioned.commit({"a": b"1"})
        c1 = versioned.current_commit
        versioned.commit({"a": b"2"})
        assert versioned.reset_to(c1)
        assert versioned.get("a") == b"1"

    def test_refresh(self, make_versioned):
        v1 = make_versioned()
        v1.commit({"a": b"1"})
        v2 = make_versioned()
        v2.commit({"b": b"new"})
        v1.refresh()
        assert v1.get("b") == b"new"
