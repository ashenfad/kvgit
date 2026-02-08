"""Tests for the Versioned commit log."""

import pytest

from vkv import MergeConflict, MergeResult, Versioned, counter
from vkv.kv.memory import Memory


class TestVersionedBasic:
    def test_empty_init(self):
        v = Versioned()
        assert v.current_commit is not None
        assert v.base_commit == v.current_commit
        assert list(v.keys()) == []

    def test_commit_and_get(self):
        v = Versioned()
        v.commit({"greeting": b"hello"})
        assert v.get("greeting") == b"hello"

    def test_get_missing(self):
        v = Versioned()
        assert v.get("nope") is None

    def test_get_many(self):
        v = Versioned()
        v.commit({"a": b"1", "b": b"2", "c": b"3"})
        result = v.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_keys(self):
        v = Versioned()
        v.commit({"a": b"1", "b": b"2"})
        assert set(v.keys()) == {"a", "b"}

    def test_contains(self):
        v = Versioned()
        v.commit({"k": b"v"})
        assert "k" in v
        assert "nope" not in v

    def test_commit_returns_merge_result(self):
        v = Versioned()
        result = v.commit({"k": b"v"})
        assert isinstance(result, MergeResult)
        assert result.merged is True
        assert result.commit is not None
        assert len(result.commit) == 16
        assert v.current_commit == result.commit

    def test_no_op_commit(self):
        v = Versioned()
        initial = v.current_commit
        result = v.commit()
        assert result.strategy == "no_op"
        assert v.current_commit == initial

    def test_content_addressable(self):
        """Same changes on same parent produce same hash."""
        store = Memory()
        v1 = Versioned(store)
        r1 = v1.commit({"k": b"v"})

        v2 = Versioned(Memory())
        r2 = v2.commit({"k": b"v"})
        assert r1.commit == r2.commit


class TestVersionedUpdatesAndRemovals:
    def test_update_existing_key(self):
        v = Versioned()
        v.commit({"k": b"old"})
        v.commit({"k": b"new"})
        assert v.get("k") == b"new"

    def test_remove_key(self):
        v = Versioned()
        v.commit({"a": b"1", "b": b"2"})
        v.commit(removals={"a"})
        assert v.get("a") is None
        assert v.get("b") == b"2"

    def test_update_and_remove(self):
        v = Versioned()
        v.commit({"a": b"1", "b": b"2", "c": b"3"})
        v.commit(updates={"a": b"updated"}, removals={"c"})
        assert v.get("a") == b"updated"
        assert v.get("b") == b"2"
        assert v.get("c") is None

    def test_multiple_commits(self):
        v = Versioned()
        v.commit({"a": b"1"})
        v.commit({"b": b"2"})
        v.commit({"c": b"3"})
        assert v.get("a") == b"1"
        assert v.get("b") == b"2"
        assert v.get("c") == b"3"


class TestVersionedHistory:
    def test_history_chain(self):
        v = Versioned()
        h0 = v.current_commit
        r1 = v.commit({"a": b"1"})
        r2 = v.commit({"b": b"2"})
        history = list(v.history())
        assert history == [r2.commit, r1.commit, h0]

    def test_initial_commit(self):
        v = Versioned()
        h0 = v.current_commit
        v.commit({"a": b"1"})
        v.commit({"b": b"2"})
        assert v.initial_commit == h0

    def test_history_from_specific_commit(self):
        v = Versioned()
        h0 = v.current_commit
        r1 = v.commit({"a": b"1"})
        v.commit({"b": b"2"})
        history = list(v.history(commit_hash=r1.commit))
        assert history == [r1.commit, h0]


class TestVersionedCheckout:
    def test_checkout_old_commit(self):
        store = Memory()
        v = Versioned(store)
        v.commit({"a": b"1"})
        h1 = v.current_commit
        v.commit({"b": b"2"})

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
        v.commit({"a": b"1"})
        h1 = v.current_commit
        v.commit({"b": b"2"})

        assert v.reset_to(h1)
        assert v.get("a") == b"1"
        assert v.get("b") is None
        assert v.current_commit == h1

    def test_reset_to_invalid(self):
        v = Versioned()
        assert not v.reset_to("nonexistent")


class TestVersionedCommit:
    def test_commit_fast_forward(self):
        store = Memory()
        v = Versioned(store)
        result = v.commit({"a": b"1"})
        assert result
        assert result.strategy == "fast_forward"
        assert v.base_commit == v.current_commit

    def test_commit_no_changes(self):
        v = Versioned()
        result = v.commit()
        assert result
        assert result.strategy == "no_op"

    def test_commit_conflict_raises(self):
        """Overlapping changes without merge fn raise MergeConflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"shared": b"base"})

        v2 = Versioned(store)

        v1.commit({"shared": b"v1_value"})

        with pytest.raises(MergeConflict) as exc_info:
            v2.commit({"shared": b"v2_value"})
        assert "shared" in exc_info.value.conflicting_keys

    def test_commit_auto_merge_non_overlapping(self):
        """Non-overlapping diverged changes auto-merge via three-way."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"a": b"1"})

        v2 = Versioned(store)

        v1.commit({"c": b"3"})

        # v2 has non-overlapping change
        result = v2.commit({"b": b"2"})
        assert result
        assert v2.get("b") == b"2"
        assert v2.get("c") == b"3"

    def test_refresh_after_other_writer(self):
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"a": b"1"})

        v2 = Versioned(store)

        v1.commit({"c": b"3"})

        v2.refresh()
        assert v2.get("c") == b"3"
        assert v2.get("a") == b"1"


class TestVersionedSharedStore:
    def test_two_writers_same_store(self):
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"a": b"1"})

        v2 = Versioned(store)
        assert v2.get("a") == b"1"

    def test_latest_head(self):
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"a": b"1"})

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
        r1 = v.commit({"a": b"1"})
        parents = v._load_parents(r1.commit)
        assert parents == (h0,)

    def test_history_all_parents_linear(self):
        """all_parents=True yields same commits for a linear chain."""
        v = Versioned()
        h0 = v.current_commit
        r1 = v.commit({"a": b"1"})
        r2 = v.commit({"b": b"2"})
        linear = list(v.history())
        full = list(v.history(all_parents=True))
        assert linear == [r2.commit, r1.commit, h0]
        assert set(full) == {h0, r1.commit, r2.commit}


class TestCommitInfo:
    def test_commit_with_info(self):
        v = Versioned()
        v.commit({"a": b"1"}, info={"author": "agent-1", "message": "init"})
        info = v.commit_info()
        assert info == {"author": "agent-1", "message": "init"}

    def test_commit_without_info(self):
        v = Versioned()
        v.commit({"a": b"1"})
        assert v.commit_info() is None

    def test_info_affects_hash(self):
        """Same changes with different info produce different hashes."""
        v1 = Versioned()
        r1 = v1.commit({"a": b"1"}, info={"author": "agent-1"})

        v2 = Versioned()
        r2 = v2.commit({"a": b"1"}, info={"author": "agent-2"})

        v3 = Versioned()
        r3 = v3.commit({"a": b"1"})

        assert r1.commit != r2.commit
        assert r1.commit != r3.commit
        assert r2.commit != r3.commit

    def test_commit_info_specific_commit(self):
        v = Versioned()
        r1 = v.commit({"a": b"1"}, info={"step": 1})
        r2 = v.commit({"b": b"2"}, info={"step": 2})
        assert v.commit_info(r1.commit) == {"step": 1}
        assert v.commit_info(r2.commit) == {"step": 2}


class TestDiff:
    def test_diff_additions(self):
        v = Versioned()
        h0 = v.current_commit
        r1 = v.commit({"a": b"1", "b": b"2"})
        d = v.diff(h0, r1.commit)
        assert d.added == {"a", "b"}
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_removals(self):
        v = Versioned()
        r1 = v.commit({"a": b"1", "b": b"2"})
        r2 = v.commit(removals={"a"})
        d = v.diff(r1.commit, r2.commit)
        assert d.removed == {"a"}
        assert d.added == frozenset()
        assert d.modified == frozenset()

    def test_diff_modifications(self):
        v = Versioned()
        r1 = v.commit({"a": b"1", "b": b"2"})
        r2 = v.commit({"a": b"updated"})
        d = v.diff(r1.commit, r2.commit)
        assert d.modified == {"a"}
        assert d.added == frozenset()
        assert d.removed == frozenset()

    def test_diff_mixed(self):
        v = Versioned()
        r1 = v.commit({"a": b"1", "b": b"2", "c": b"3"})
        r2 = v.commit(updates={"a": b"new", "d": b"4"}, removals={"c"})
        d = v.diff(r1.commit, r2.commit)
        assert d.added == {"d"}
        assert d.removed == {"c"}
        assert d.modified == {"a"}

    def test_diff_identical(self):
        v = Versioned()
        r = v.commit({"a": b"1"})
        d = v.diff(r.commit, r.commit)
        assert d.added == frozenset()
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_carried_forward_not_modified(self):
        """Keys carried forward unchanged should not appear as modified."""
        v = Versioned()
        r1 = v.commit({"a": b"1", "b": b"2"})
        r2 = v.commit({"c": b"3"})  # a and b carried forward
        d = v.diff(r1.commit, r2.commit)
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
        v1.commit({"a": b"1"})

        v2 = Versioned(store, branch="dev")
        v2.commit({"b": b"2"})

        # Each branch has its own data
        main = Versioned(store, branch="main")
        dev = Versioned(store, branch="dev")
        assert main.get("a") == b"1"
        assert main.get("b") is None
        assert dev.get("b") == b"2"
        assert dev.get("a") is None

    def test_commit_targets_correct_branch(self):
        store = Memory()
        main = Versioned(store, branch="main")
        main.commit({"a": b"1"})

        dev = Versioned(store, branch="dev")
        dev.commit({"b": b"2"})

        # Main HEAD should not be affected by dev commit
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
        r = v.commit({"a": b"1"})
        old = v.checkout(r.commit)
        assert old._branch == "dev"

    def test_create_branch_forks_current_commit(self):
        store = Memory()
        v = Versioned(store)
        v.commit({"a": b"1"})

        dev = v.create_branch("dev")
        assert dev._branch == "dev"
        assert dev.current_commit == v.current_commit
        assert dev.get("a") == b"1"

    def test_create_branch_diverge_and_commit(self):
        store = Memory()
        v = Versioned(store)
        v.commit({"base": b"0"})

        dev = v.create_branch("dev")
        dev.commit({"feature": b"1"})

        # Main doesn't see dev's data
        main = Versioned(store, branch="main")
        assert main.get("feature") is None
        assert main.get("base") == b"0"

    def test_create_branch_already_exists(self):
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
        v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1.commit({"a": b"1"})
        v2.commit({"b": b"2"})

        assert v2.get("a") == b"1"
        assert v2.get("b") == b"2"
        assert v2.get("base") == b"0"

    def test_conflict_no_fn(self):
        """Both modify same key, no merge function -> MergeConflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"key": b"base"})

        v2 = Versioned(store)
        v1.commit({"key": b"v1"})

        with pytest.raises(MergeConflict) as exc_info:
            v2.commit({"key": b"v2"})
        assert "key" in exc_info.value.conflicting_keys

    def test_conflict_resolved_by_fn(self):
        """Per-key merge function resolves conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"counter": b"10"})

        v2 = Versioned(store)
        v1.commit({"counter": b"15"})

        def add_merge(old, ours, theirs):
            o = int(old) if old else 0
            a = int(ours)
            b = int(theirs)
            return str(a + b - o).encode()

        result = v2.commit(
            {"counter": b"20"},
            merge_fns={"counter": add_merge},
        )
        assert result
        assert v2.get("counter") == b"25"  # 20 + 15 - 10

    def test_conflict_resolved_by_default(self):
        """Default merge function resolves conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"x": b"base"})

        v2 = Versioned(store)
        v1.commit({"x": b"v1"})

        lww = lambda old, ours, theirs: theirs
        result = v2.commit({"x": b"v2"}, default_merge=lww)
        assert result
        assert v2.get("x") == b"v1"  # theirs = HEAD value

    def test_instance_level_merge_fn(self):
        """set_merge_fn on instance works for three-way merge."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"x": b"base"})

        v2 = Versioned(store)
        v2.set_merge_fn("x", lambda old, ours, theirs: ours)  # ours wins

        v1.commit({"x": b"v1"})

        result = v2.commit({"x": b"v2"})
        assert result
        assert v2.get("x") == b"v2"  # ours wins

    def test_remove_modify_conflict(self):
        """One side removes, other modifies -> conflict without fn."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"key": b"base"})

        v2 = Versioned(store)
        v1.commit(removals={"key"})

        with pytest.raises(MergeConflict):
            v2.commit({"key": b"modified"})

    def test_both_remove_same_key(self):
        """Both remove same key -> no conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"key": b"base", "keep": b"yes"})

        v2 = Versioned(store)
        v1.commit(removals={"key"})

        result = v2.commit(removals={"key"})
        assert result
        assert v2.get("key") is None
        assert v2.get("keep") == b"yes"

    def test_both_identical_change(self):
        """Both sides make identical change -> no conflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"key": b"base"})

        # Both branch from same point and make same change
        v2 = Versioned(store)
        v1.commit({"key": b"same"})

        result = v2.commit({"key": b"same"})
        assert result
        assert v2.get("key") == b"same"

    def test_merge_commit_two_parents(self):
        """After three-way merge, commit has two parents."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1.commit({"a": b"1"})
        v1_head = v1.current_commit

        # v2 creates a local commit, then three-way merge
        v2._create_commit({"b": b"2"})
        v2_commit = v2.current_commit

        result = v2.commit({"b": b"2"})
        # The merge commit (on HEAD) has two parents
        parents = v2._load_parents(v2.current_commit)
        assert len(parents) == 2

    def test_merge_result_populated(self):
        """last_merge_result is populated after commit."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1.commit({"a": b"1"})

        result = v2.commit({"b": b"2"})
        assert result is not None
        assert result.merged is True
        assert result.strategy == "three_way"
        assert "b" in result.auto_merged_keys

    def test_history_all_parents_after_merge(self):
        """After merge, history(all_parents=True) traverses both branches."""
        store = Memory()
        v1 = Versioned(store)
        r_base = v1.commit({"base": b"0"})

        v2 = Versioned(store)
        r_v1 = v1.commit({"a": b"1"})

        r_v2 = v2.commit({"b": b"2"})
        merge_commit = v2.current_commit

        all_commits = set(v2.history(all_parents=True))
        assert merge_commit in all_commits

    def test_fast_forward_still_works(self):
        """Fast-forward merge still works when HEAD hasn't moved."""
        store = Memory()
        v = Versioned(store)
        result = v.commit({"a": b"1"})
        assert result
        assert result.strategy == "fast_forward"

    def test_no_op_still_works(self):
        """No-op commit when no changes."""
        v = Versioned()
        result = v.commit()
        assert result
        assert result.strategy == "no_op"

    def test_lca_diverged(self):
        """LCA finding works for diverged branches."""
        store = Memory()
        v1 = Versioned(store)
        r_base = v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1._create_commit({"a": b"1"})
        h1 = v1.current_commit
        v2._create_commit({"b": b"2"})
        h2 = v2.current_commit

        lca = v1._find_lca(h1, h2)
        assert lca == r_base.commit

    def test_commit_with_info(self):
        """Commit carries info."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1.commit({"a": b"1"})

        v2.commit({"b": b"2"}, info={"merged_by": "test"})
        # The info is on the three-way merge commit
        assert v2.commit_info() == {"merged_by": "test"}

    def test_one_removes_other_adds(self):
        """One branch removes a key, other adds a new key."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"existing": b"val", "keep": b"yes"})

        v2 = Versioned(store)
        v1.commit(removals={"existing"})

        result = v2.commit({"new_key": b"new"})
        assert result
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

    def test_commit_returns_result_object(self):
        """commit() returns a MergeResult."""
        store = Memory()
        v = Versioned(store)
        result = v.commit({"x": b"1"})
        assert isinstance(result, MergeResult)
        assert result.merged is True
        assert result.strategy == "fast_forward"
        assert result.commit == v.current_commit

    def test_no_op_returns_result(self):
        """No changes returns a no_op MergeResult."""
        v = Versioned()
        result = v.commit()
        assert isinstance(result, MergeResult)
        assert result.strategy == "no_op"

    def test_three_way_returns_result(self):
        """Three-way merge returns a MergeResult with details."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"base": b"0"})

        v2 = Versioned(store)
        v1.commit({"a": b"1"})

        result = v2.commit({"b": b"2"})
        assert isinstance(result, MergeResult)
        assert result.strategy == "three_way"
        assert result.merged is True
        assert result.commit is not None

    def test_abandon_returns_falsy_result(self):
        """on_conflict='abandon' returns MergeResult with merged=False."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"x": b"1"})

        from vkv.versioned import BRANCH_HEAD, _to_bytes

        v2 = Versioned(store)

        # Overwrite HEAD to something v2 doesn't expect
        store.set(BRANCH_HEAD % "main", _to_bytes("bogus_hash"))

        result = v2.commit({"y": b"2"}, on_conflict="abandon")
        assert isinstance(result, MergeResult)
        assert not result
        assert result.merged is False


class TestBugFixes:
    def test_commit_info_only_creates_commit(self):
        """commit(info=...) with no data changes still creates a commit."""
        v = Versioned()
        old_hash = v.current_commit
        result = v.commit(info={"msg": "hi"})
        assert result.commit != old_hash
        assert v.commit_info() == {"msg": "hi"}

    def test_commit_no_changes_no_info_is_noop(self):
        """commit() with nothing at all is a no-op."""
        v = Versioned()
        old_hash = v.current_commit
        result = v.commit()
        assert result.strategy == "no_op"
        assert v.current_commit == old_hash

    def test_merge_fn_exception_surfaces_in_conflict(self):
        """Merge fn exceptions are attached to MergeConflict."""
        store = Memory()
        v1 = Versioned(store)
        v1.commit({"k": b"base"})

        v2 = Versioned(store)
        v1.commit({"k": b"v1"})

        def bad_fn(old, ours, theirs):
            raise ValueError("intentional error")

        with pytest.raises(MergeConflict) as exc_info:
            v2.commit({"k": b"v2"}, merge_fns={"k": bad_fn})
        assert "k" in exc_info.value.conflicting_keys
        assert "k" in exc_info.value.merge_errors
        assert isinstance(exc_info.value.merge_errors["k"], ValueError)

    def test_commit_invalid_on_conflict(self):
        """Invalid on_conflict value raises ValueError."""
        v = Versioned()
        with pytest.raises(ValueError, match="on_conflict"):
            v.commit({"x": b"1"}, on_conflict="bogus")


class TestErgonomics:
    def test_list_branches_instance_method(self):
        store = Memory()
        v = Versioned(store)
        v.create_branch("dev")
        v.create_branch("staging")
        assert v.list_branches() == Versioned.branches(store)
        assert "dev" in v.list_branches()
        assert "staging" in v.list_branches()

    def test_checkout_with_branch(self):
        store = Memory()
        v = Versioned(store, branch="main")
        r = v.commit({"a": b"1"})
        other = v.checkout(r.commit, branch="other")
        assert other._branch == "other"
        assert other.get("a") == b"1"

    def test_checkout_default_branch_unchanged(self):
        store = Memory()
        v = Versioned(store, branch="dev")
        r = v.commit({"a": b"1"})
        same = v.checkout(r.commit)
        assert same._branch == "dev"

    def test_repr(self):
        v = Versioned()
        v.commit({"a": b"1", "b": b"2"})
        r = repr(v)
        assert "main" in r
        assert "keys=2" in r
        assert v.current_commit[:8] in r

    def test_repr_different_branch(self):
        store = Memory()
        v = Versioned(store, branch="dev")
        r = repr(v)
        assert "dev" in r

    def test_get_content_type(self):
        v = Versioned()
        ct = counter()
        v.set_content_type("hits", ct)
        assert v.get_content_type("hits") is ct

    def test_get_content_type_missing(self):
        v = Versioned()
        assert v.get_content_type("nope") is None
