"""Tests for the Versioned commit log."""

import pytest

from vkv import MergeConflict, MergeResult, Versioned
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
        """Overlapping changes without merge fn raise MergeConflict."""
        import pytest

        from vkv import MergeConflict

        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"shared": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v2.snapshot({"shared": b"v2_value"})

        v1.snapshot({"shared": b"v1_value"})
        v1.merge()

        with pytest.raises(MergeConflict) as exc_info:
            v2.merge()
        assert "shared" in exc_info.value.conflicting_keys

    def test_merge_conflict_abandon(self):
        """Non-overlapping diverged changes auto-merge via three-way."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store)
        v2.snapshot({"b": b"2"})

        v1.snapshot({"c": b"3"})
        v1.merge()

        # Non-overlapping: auto-merges successfully
        assert v2.merge()
        assert v2.get("b") == b"2"
        assert v2.get("c") == b"3"

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


class TestParentFormat:
    def test_root_has_empty_parents(self):
        v = Versioned()
        parents = v._load_parents(v.initial_commit)
        assert parents == ()

    def test_normal_commit_single_parent(self):
        v = Versioned()
        h0 = v.current_commit
        h1 = v.snapshot({"a": b"1"})
        parents = v._load_parents(h1)
        assert parents == (h0,)

    def test_history_all_parents_linear(self):
        """all_parents=True yields same commits for a linear chain."""
        v = Versioned()
        h0 = v.current_commit
        h1 = v.snapshot({"a": b"1"})
        h2 = v.snapshot({"b": b"2"})
        linear = list(v.history())
        full = list(v.history(all_parents=True))
        assert linear == [h2, h1, h0]
        assert set(full) == {h0, h1, h2}


class TestCommitInfo:
    def test_snapshot_with_info(self):
        v = Versioned()
        v.snapshot({"a": b"1"}, info={"author": "agent-1", "message": "init"})
        info = v.commit_info()
        assert info == {"author": "agent-1", "message": "init"}

    def test_snapshot_without_info(self):
        v = Versioned()
        v.snapshot({"a": b"1"})
        assert v.commit_info() is None

    def test_info_affects_hash(self):
        """Same changes with different info produce different hashes."""
        v1 = Versioned()
        h1 = v1.snapshot({"a": b"1"}, info={"author": "agent-1"})

        v2 = Versioned()
        h2 = v2.snapshot({"a": b"1"}, info={"author": "agent-2"})

        v3 = Versioned()
        h3 = v3.snapshot({"a": b"1"})

        assert h1 != h2
        assert h1 != h3
        assert h2 != h3

    def test_commit_info_specific_commit(self):
        v = Versioned()
        h1 = v.snapshot({"a": b"1"}, info={"step": 1})
        h2 = v.snapshot({"b": b"2"}, info={"step": 2})
        assert v.commit_info(h1) == {"step": 1}
        assert v.commit_info(h2) == {"step": 2}


class TestDiff:
    def test_diff_additions(self):
        v = Versioned()
        h0 = v.current_commit
        h1 = v.snapshot({"a": b"1", "b": b"2"})
        d = v.diff(h0, h1)
        assert d.added == {"a", "b"}
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_removals(self):
        v = Versioned()
        h1 = v.snapshot({"a": b"1", "b": b"2"})
        h2 = v.snapshot(removals={"a"})
        d = v.diff(h1, h2)
        assert d.removed == {"a"}
        assert d.added == frozenset()
        assert d.modified == frozenset()

    def test_diff_modifications(self):
        v = Versioned()
        h1 = v.snapshot({"a": b"1", "b": b"2"})
        h2 = v.snapshot({"a": b"updated"})
        d = v.diff(h1, h2)
        assert d.modified == {"a"}
        assert d.added == frozenset()
        assert d.removed == frozenset()

    def test_diff_mixed(self):
        v = Versioned()
        h1 = v.snapshot({"a": b"1", "b": b"2", "c": b"3"})
        h2 = v.snapshot(updates={"a": b"new", "d": b"4"}, removals={"c"})
        d = v.diff(h1, h2)
        assert d.added == {"d"}
        assert d.removed == {"c"}
        assert d.modified == {"a"}

    def test_diff_identical(self):
        v = Versioned()
        h = v.snapshot({"a": b"1"})
        d = v.diff(h, h)
        assert d.added == frozenset()
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_carried_forward_not_modified(self):
        """Keys carried forward unchanged should not appear as modified."""
        v = Versioned()
        h1 = v.snapshot({"a": b"1", "b": b"2"})
        h2 = v.snapshot({"c": b"3"})  # a and b carried forward
        d = v.diff(h1, h2)
        assert d.added == {"c"}
        assert d.modified == frozenset()
        assert d.removed == frozenset()


class TestBranches:
    def test_default_branch_is_main(self):
        v = Versioned()
        assert v._branch == "main"

    def test_two_branches_coexist(self):
        store = Memory()
        v1 = Versioned(store, branch="main")
        v1.snapshot({"a": b"1"})
        v1.merge()

        v2 = Versioned(store, branch="dev")
        v2.snapshot({"b": b"2"})
        v2.merge()

        # Each branch has its own data
        main = Versioned(store, branch="main")
        dev = Versioned(store, branch="dev")
        assert main.get("a") == b"1"
        assert main.get("b") is None
        assert dev.get("b") == b"2"
        assert dev.get("a") is None

    def test_merge_targets_correct_branch(self):
        store = Memory()
        main = Versioned(store, branch="main")
        main.snapshot({"a": b"1"})
        main.merge()

        dev = Versioned(store, branch="dev")
        dev.snapshot({"b": b"2"})
        dev.merge()

        # Main HEAD should not be affected by dev merge
        main2 = Versioned(store, branch="main")
        assert main2.get("a") == b"1"
        assert main2.get("b") is None

    def test_branches_enumerate(self):
        store = Memory()
        Versioned(store, branch="main")
        Versioned(store, branch="dev")
        Versioned(store, branch="feature")
        assert Versioned.branches(store) == ["dev", "feature", "main"]

    def test_checkout_preserves_branch(self):
        store = Memory()
        v = Versioned(store, branch="dev")
        h = v.snapshot({"a": b"1"})
        old = v.checkout(h)
        assert old._branch == "dev"

    def test_create_branch_forks_current_commit(self):
        store = Memory()
        v = Versioned(store)
        v.snapshot({"a": b"1"})
        v.merge()

        dev = v.create_branch("dev")
        assert dev._branch == "dev"
        assert dev.current_commit == v.current_commit
        assert dev.get("a") == b"1"

    def test_create_branch_diverge_and_merge(self):
        store = Memory()
        v = Versioned(store)
        v.snapshot({"base": b"0"})
        v.merge()

        dev = v.create_branch("dev")
        dev.snapshot({"feature": b"1"})
        dev.merge()

        # Main doesn't see dev's data
        main = Versioned(store, branch="main")
        assert main.get("feature") is None
        assert main.get("base") == b"0"

    def test_create_branch_already_exists(self):
        import pytest

        store = Memory()
        v = Versioned(store)
        with pytest.raises(ValueError, match="already exists"):
            v.create_branch("main")

    def test_create_branch_appears_in_branches_list(self):
        store = Memory()
        v = Versioned(store)
        v.create_branch("dev")
        v.create_branch("staging")
        branches = Versioned.branches(store)
        assert "dev" in branches
        assert "staging" in branches
        assert "main" in branches


class TestThreeWayMerge:
    def test_auto_merge_non_overlapping(self):
        """Two branches with different keys auto-merge."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()
        v2.snapshot({"b": b"2"})

        assert v2.merge()
        assert v2.get("a") == b"1"
        assert v2.get("b") == b"2"
        assert v2.get("base") == b"0"

    def test_conflict_no_fn(self):
        """Both modify same key, no merge function -> MergeConflict."""
        import pytest

        from vkv import MergeConflict

        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"key": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"key": b"v1"})
        v1.merge()
        v2.snapshot({"key": b"v2"})

        with pytest.raises(MergeConflict) as exc_info:
            v2.merge()
        assert "key" in exc_info.value.conflicting_keys

    def test_conflict_resolved_by_fn(self):
        """Per-key merge function resolves conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"counter": b"10"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"counter": b"15"})
        v1.merge()
        v2.snapshot({"counter": b"20"})

        def add_merge(old, ours, theirs):
            o = int(old) if old else 0
            a = int(ours)
            b = int(theirs)
            return str(a + b - o).encode()

        assert v2.merge(merge_fns={"counter": add_merge})
        assert v2.get("counter") == b"25"  # 20 + 15 - 10

    def test_conflict_resolved_by_default(self):
        """Default merge function resolves conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"x": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"x": b"v1"})
        v1.merge()
        v2.snapshot({"x": b"v2"})

        lww = lambda old, ours, theirs: theirs
        assert v2.merge(default_merge=lww)
        assert v2.get("x") == b"v1"  # theirs = HEAD value

    def test_instance_level_merge_fn(self):
        """set_merge_fn on instance works for three-way merge."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"x": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v2.set_merge_fn("x", lambda old, ours, theirs: ours)  # ours wins

        v1.snapshot({"x": b"v1"})
        v1.merge()
        v2.snapshot({"x": b"v2"})

        assert v2.merge()
        assert v2.get("x") == b"v2"  # ours wins

    def test_remove_modify_conflict(self):
        """One side removes, other modifies -> conflict without fn."""
        import pytest

        from vkv import MergeConflict

        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"key": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot(removals={"key"})
        v1.merge()
        v2.snapshot({"key": b"modified"})

        with pytest.raises(MergeConflict):
            v2.merge()

    def test_both_remove_same_key(self):
        """Both remove same key -> no conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"key": b"base", "keep": b"yes"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot(removals={"key"})
        v1.merge()
        v2.snapshot(removals={"key"})

        assert v2.merge()
        assert v2.get("key") is None
        assert v2.get("keep") == b"yes"

    def test_both_identical_change(self):
        """Both sides make identical change -> no conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"key": b"base"})
        v1.merge()

        # Both branch from same point and make same change
        v2 = Versioned(store)
        v1.snapshot({"key": b"same"})
        v1.merge()
        v2.snapshot({"key": b"same"})

        assert v2.merge()
        assert v2.get("key") == b"same"

    def test_merge_commit_two_parents(self):
        """After three-way merge, commit has two parents."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()
        v1_head = v1.current_commit

        v2.snapshot({"b": b"2"})
        v2_commit = v2.current_commit

        v2.merge()
        parents = v2._load_parents(v2.current_commit)
        assert len(parents) == 2
        assert parents == (v1_head, v2_commit)

    def test_merge_result_populated(self):
        """last_merge_result is populated after merge."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()
        v2.snapshot({"b": b"2"})

        v2.merge()
        result = v2.last_merge_result
        assert result is not None
        assert result.merged is True
        assert result.strategy == "three_way"
        assert "b" in result.auto_merged_keys

    def test_history_all_parents_after_merge(self):
        """After merge, history(all_parents=True) traverses both branches."""
        store = Memory()
        v1 = Versioned(store)
        base = v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        h_v1 = v1.snapshot({"a": b"1"})
        v1.merge()
        h_v2 = v2.snapshot({"b": b"2"})

        v2.merge()
        merge_commit = v2.current_commit

        all_commits = set(v2.history(all_parents=True))
        assert merge_commit in all_commits
        assert h_v1 in all_commits
        assert h_v2 in all_commits
        assert base in all_commits

    def test_fast_forward_still_works(self):
        """Fast-forward merge still works when HEAD hasn't moved."""
        store = Memory()
        v = Versioned(store)
        v.snapshot({"a": b"1"})
        assert v.merge()
        assert v.last_merge_result.strategy == "fast_forward"

    def test_no_op_still_works(self):
        """No-op merge when no local changes."""
        v = Versioned()
        assert v.merge()
        assert v.last_merge_result.strategy == "no_op"

    def test_lca_diverged(self):
        """LCA finding works for diverged branches."""
        store = Memory()
        v1 = Versioned(store)
        base = v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        h1 = v1.snapshot({"a": b"1"})
        h2 = v2.snapshot({"b": b"2"})

        lca = v1._find_lca(h1, h2)
        assert lca == base

    def test_merge_with_info(self):
        """Merge commit carries info."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()
        v2.snapshot({"b": b"2"})

        v2.merge(info={"merged_by": "test"})
        assert v2.commit_info() == {"merged_by": "test"}

    def test_one_removes_other_adds(self):
        """One branch removes a key, other adds a new key."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"existing": b"val", "keep": b"yes"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot(removals={"existing"})
        v1.merge()
        v2.snapshot({"new_key": b"new"})

        assert v2.merge()
        assert v2.get("existing") is None
        assert v2.get("new_key") == b"new"
        assert v2.get("keep") == b"yes"


class TestMergeResultReturn:
    def test_merge_result_truthy(self):
        r = MergeResult(
            merged=True, commit="abc", strategy="no_op",
            auto_merged_keys=(), carried_keys=(),
        )
        assert r
        assert bool(r) is True

    def test_merge_result_falsy(self):
        r = MergeResult(
            merged=False, commit=None, strategy="fast_forward",
            auto_merged_keys=(), carried_keys=(),
        )
        assert not r
        assert bool(r) is False

    def test_merge_returns_result_object(self):
        """merge() returns a MergeResult, not just a bool."""
        store = Memory()
        v = Versioned(store)
        v.snapshot({"x": b"1"})
        result = v.merge()
        assert isinstance(result, MergeResult)
        assert result.merged is True
        assert result.strategy == "fast_forward"
        assert result.commit == v.current_commit

    def test_no_op_returns_result(self):
        """No local changes returns a no_op MergeResult."""
        v = Versioned()
        result = v.merge()
        assert isinstance(result, MergeResult)
        assert result.strategy == "no_op"

    def test_three_way_returns_result(self):
        """Three-way merge returns a MergeResult with details."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"base": b"0"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"a": b"1"})
        v1.merge()
        v2.snapshot({"b": b"2"})

        result = v2.merge()
        assert isinstance(result, MergeResult)
        assert result.strategy == "three_way"
        assert result.merged is True
        assert result.commit is not None

    def test_abandon_returns_falsy_result(self):
        """on_conflict='abandon' returns MergeResult with merged=False."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"x": b"1"})
        v1.merge()

        # v2 branches from v1's first commit (before the snapshot)
        # but v1 has already advanced HEAD via merge
        # Manually tamper with HEAD so CAS fails on fast-forward
        from vkv.versioned import BRANCH_HEAD
        import pickle

        v2 = Versioned(store)
        v2.snapshot({"y": b"2"})

        # Overwrite HEAD to something v2 doesn't expect
        store.set(BRANCH_HEAD % "main", pickle.dumps("bogus_hash"))

        result = v2.merge(on_conflict="abandon")
        assert isinstance(result, MergeResult)
        assert not result
        assert result.merged is False


class TestBugFixes:
    def test_snapshot_info_only_creates_commit(self):
        """snapshot(info=...) with no data changes still creates a commit."""
        v = Versioned()
        old_hash = v.current_commit
        new_hash = v.snapshot(info={"msg": "hi"})
        assert new_hash != old_hash
        assert v.commit_info() == {"msg": "hi"}

    def test_snapshot_no_changes_no_info_is_noop(self):
        """snapshot() with nothing at all is still a no-op."""
        v = Versioned()
        old_hash = v.current_commit
        assert v.snapshot() == old_hash

    def test_merge_fn_exception_surfaces_in_conflict(self):
        """Merge fn exceptions are attached to MergeConflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.snapshot({"k": b"base"})
        v1.merge()

        v2 = Versioned(store)
        v1.snapshot({"k": b"v1"})
        v1.merge()
        v2.snapshot({"k": b"v2"})

        def bad_fn(old, ours, theirs):
            raise ValueError("intentional error")

        with pytest.raises(MergeConflict) as exc_info:
            v2.merge(merge_fns={"k": bad_fn})
        assert "k" in exc_info.value.conflicting_keys
        assert "k" in exc_info.value.merge_errors
        assert isinstance(exc_info.value.merge_errors["k"], ValueError)

    def test_merge_invalid_on_conflict(self):
        """Invalid on_conflict value raises ValueError."""
        v = Versioned()
        v.snapshot({"x": b"1"})
        with pytest.raises(ValueError, match="on_conflict"):
            v.merge(on_conflict="bogus")
