"""Tests for the commit log through Repo, Worktree and Snapshot: commits,
reads, history, branches, three-way merges on commit, races between
worktrees on one branch, sweeps and HEAD recovery."""

import pytest
from support import entries, fail_reads, fork, race_publish, root_of, worktree

import kvgit
from kvgit import (
    ConcurrencyError,
    MergeChoice,
    MergeConflict,
    MergeResult,
    Repo,
    Worktree,
)
from kvgit._store import BRANCH_HEAD, GC_LEASE_KEY, blob_key
from kvgit.codecs._hash import hash_bytes
from kvgit.encoding import dumps
from kvgit.errors import (
    BranchExistsError,
    CorruptHeadError,
    KvgitError,
    UnknownBranchError,
    UnknownCommitError,
)
from kvgit.kv.memory import Memory


def _repo(store=None, **options) -> Repo:
    """A repository over ``store`` whose values are bytes, stored as given."""
    return Repo(store if store is not None else Memory(), codec="bytes", **options)


def _wt(store=None, branch: str = "main", **options) -> Worktree:
    """A worktree on ``branch`` (created if new) from a repository of its
    own over ``store``: one independent handle on the store."""
    return worktree(store, branch, codec="bytes", **options)


def _stage(wt: Worktree, updates=None, removals=()) -> None:
    """Write ``updates`` and delete ``removals`` as pending changes."""
    wt.update(updates or {})
    for key in removals:
        del wt[key]


def commit_to(wt: Worktree, updates=None, removals=(), **options) -> MergeResult:
    """Stage ``updates`` and ``removals`` on ``wt`` and commit them."""
    _stage(wt, updates, removals)
    return wt.commit(**options)


def _committed(wt: Worktree, key: str):
    """``key``'s value at ``wt``'s head, pending changes aside."""
    return wt.repo.snapshot(commit=wt.head).get(key)


def _history(wt: Worktree, commit: str | None = None, all_parents=False):
    """Commit hashes reachable from ``commit`` (default: ``wt``'s head),
    newest first; first parents only unless ``all_parents``."""
    return [
        c.hash
        for c in wt.repo.log(commit=commit or wt.head, first_parent=not all_parents)
    ]


class TestVersionedBasic:
    def test_empty_init(self):
        v = _wt()
        assert v.head is not None
        assert v.repo.branches["main"] == v.head
        assert list(v.keys()) == []

    def test_commit_and_get(self):
        v = _wt()
        commit_to(v, {"greeting": b"hello"})
        assert v.get("greeting") == b"hello"

    def test_get_missing(self):
        v = _wt()
        assert v.get("nope") is None

    def test_get_many(self):
        v = _wt()
        commit_to(v, {"a": b"1", "b": b"2", "c": b"3"})
        result = v.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_keys(self):
        v = _wt()
        commit_to(v, {"a": b"1", "b": b"2"})
        assert set(v.keys()) == {"a", "b"}

    def test_contains(self):
        v = _wt()
        commit_to(v, {"k": b"v"})
        assert "k" in v
        assert "nope" not in v

    def test_commit_returns_merge_result(self):
        v = _wt()
        result = commit_to(v, {"k": b"v"})
        assert isinstance(result, MergeResult)
        assert result.merged is True
        assert result.commit is not None
        assert len(result.commit) == 40
        assert v.head == result.commit

    def test_no_op_commit(self):
        v = _wt()
        initial = v.head
        result = v.commit()
        assert result.strategy == "no_op"
        assert v.head == initial

    def test_content_addressable(self):
        """Same changes on the same parent produce the same keyset root
        and the same blob; the commit hash covers the time as well."""
        store1, store2 = Memory(), Memory()
        r1 = commit_to(_wt(store1), {"k": b"v"})
        r2 = commit_to(_wt(store2), {"k": b"v"})
        assert r1.commit != r2.commit
        assert root_of(store1, r1.commit) == root_of(store2, r2.commit)
        assert blob_key(b"v") in store1.keys() and blob_key(b"v") in store2.keys()


class TestVersionedUpdatesAndRemovals:
    def test_update_existing_key(self):
        v = _wt()
        commit_to(v, {"k": b"old"})
        commit_to(v, {"k": b"new"})
        assert v.get("k") == b"new"

    def test_remove_key(self):
        v = _wt()
        commit_to(v, {"a": b"1", "b": b"2"})
        commit_to(v, removals={"a"})
        assert v.get("a") is None
        assert v.get("b") == b"2"

    def test_update_and_remove(self):
        v = _wt()
        commit_to(v, {"a": b"1", "b": b"2", "c": b"3"})
        commit_to(v, updates={"a": b"updated"}, removals={"c"})
        assert v.get("a") == b"updated"
        assert v.get("b") == b"2"
        assert v.get("c") is None

    def test_multiple_commits(self):
        v = _wt()
        commit_to(v, {"a": b"1"})
        commit_to(v, {"b": b"2"})
        commit_to(v, {"c": b"3"})
        assert v.get("a") == b"1"
        assert v.get("b") == b"2"
        assert v.get("c") == b"3"


class TestVersionedHistory:
    def test_history_chain(self):
        v = _wt()
        h0 = v.head
        r1 = commit_to(v, {"a": b"1"})
        r2 = commit_to(v, {"b": b"2"})
        assert _history(v) == [r2.commit, r1.commit, h0]

    def test_initial_commit(self):
        v = _wt()
        h0 = v.head
        commit_to(v, {"a": b"1"})
        commit_to(v, {"b": b"2"})
        assert _history(v)[-1] == h0

    def test_history_from_specific_commit(self):
        v = _wt()
        h0 = v.head
        r1 = commit_to(v, {"a": b"1"})
        commit_to(v, {"b": b"2"})
        assert _history(v, commit=r1.commit) == [r1.commit, h0]


class TestVersionedCheckout:
    def test_snapshot_old_commit(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"a": b"1"})
        h1 = v.head
        commit_to(v, {"b": b"2"})

        old = v.repo.snapshot(commit=h1)
        assert old.get("a") == b"1"
        assert old.get("b") is None

    def test_snapshot_invalid(self):
        v = _wt()
        with pytest.raises(UnknownCommitError):
            v.repo.snapshot(commit="nonexistent")

    def test_reset(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"a": b"1"})
        h1 = v.head
        commit_to(v, {"b": b"2"})

        v.reset(h1)
        assert v.get("a") == b"1"
        assert v.get("b") is None
        assert v.head == h1

    def test_reset_invalid(self):
        v = _wt()
        with pytest.raises(UnknownCommitError):
            v.reset("nonexistent")


class TestVersionedCommit:
    def test_commit_fast_forward(self):
        store = Memory()
        v = _wt(store)
        result = commit_to(v, {"a": b"1"})
        assert result
        assert result.strategy == "fast_forward"
        assert v.repo.branches["main"] == v.head

    def test_commit_no_changes(self):
        v = _wt()
        result = v.commit()
        assert result
        assert result.strategy == "no_op"

    def test_commit_conflict_raises(self):
        """Overlapping changes without merge fn raise MergeConflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"shared": b"base"})

        v2 = _wt(store)

        commit_to(v1, {"shared": b"v1_value"})

        with pytest.raises(MergeConflict) as exc_info:
            commit_to(v2, {"shared": b"v2_value"})
        assert "shared" in exc_info.value.conflicting_keys

    def test_commit_auto_merge_non_overlapping(self):
        """Non-overlapping diverged changes auto-merge via three-way."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"a": b"1"})

        v2 = _wt(store)

        commit_to(v1, {"c": b"3"})

        # v2 has non-overlapping change
        result = commit_to(v2, {"b": b"2"})
        assert result
        assert v2.get("b") == b"2"
        assert v2.get("c") == b"3"

    def test_refresh_after_other_writer(self):
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"a": b"1"})

        v2 = _wt(store)

        commit_to(v1, {"c": b"3"})

        v2.refresh()
        assert v2.get("c") == b"3"
        assert v2.get("a") == b"1"


def _race_once(loser: Worktree, advance) -> None:
    """Make ``loser``'s next publish lose: advance HEAD first, then run it."""
    race_publish(loser.repo.store, loser.branch, advance)


class TestFastForwardRace:
    """Issue #39: a HEAD move inside the fast-forward CAS window must
    re-read HEAD and merge internally — not raise an error whose named
    recovery (``refresh()``) discards the caller's staged work."""

    def test_lost_race_merges_instead_of_raising(self):
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})
        v2 = _wt(store)
        _race_once(v2, lambda: commit_to(v1, {"other": b"1"}))

        result = commit_to(v2, {"mine": b"2"})

        assert result.merged
        assert result.strategy == "three_way"
        assert v2.get("mine") == b"2"
        assert v2.get("other") == b"1"

    def test_lost_race_with_abandon_merges_clean_change(self):
        """A lost race is not a conflict: like the base-behind-head case,
        ``abandon`` still attempts the merge and succeeds when clean."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})
        v2 = _wt(store)
        _race_once(v2, lambda: commit_to(v1, {"other": b"1"}))

        result = commit_to(v2, {"mine": b"2"}, on_conflict="abandon")

        assert result.merged
        assert result.strategy == "three_way"

    def test_lost_race_with_abandon_bails_on_true_conflict(self):
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"x": b"1"})
        v2 = _wt(store)
        _race_once(v2, lambda: commit_to(v1, {"x": b"v1"}))

        result = commit_to(v2, {"x": b"v2"}, on_conflict="abandon")

        assert not result
        assert result.strategy == "three_way"
        assert v2.repo.branches["main"] == v1.head  # branch untouched
        assert _committed(v2, "x") == b"1"  # loser stays at its pre-commit head

    def test_repeated_race_still_raises(self):
        """The internal retry is bounded: a race on the merge CAS too
        still surfaces ConcurrencyError instead of looping."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})
        v2 = _wt(store)
        race_publish(store, "main", lambda: commit_to(v1, {"other": b"1"}), times=None)
        with pytest.raises(ConcurrencyError):
            commit_to(v2, {"mine": b"2"})

    def test_retry_finding_branch_gone_restores_state(self):
        """A retry whose HEAD re-read finds the branch deleted must
        leave the worktree at its pre-commit head, not at the
        uncommitted side commit."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})
        base_head = v1.head
        v2 = _wt(store)
        _race_once(v2, lambda: store.remove(BRANCH_HEAD % "main"))

        with pytest.raises(UnknownBranchError, match="has no HEAD"):
            commit_to(v2, {"mine": b"2"})

        assert v2.head == base_head
        assert _committed(v2, "mine") is None
        assert _committed(v2, "base") == b"0"

    def test_failing_head_reread_restores_state(self):
        """A retry whose HEAD re-read rejects leaves the worktree at its
        pre-commit head too, and the worktree stays usable afterwards."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})
        base_head = v1.head
        v2 = _wt(store)

        def advance():
            commit_to(v1, {"other": b"1"})
            # A head read reads the head, the lease and the base's root
            # together; the heal check before it reads them one by one.
            fail_reads(
                store,
                lambda keys: BRANCH_HEAD % "main" in keys and GC_LEASE_KEY in keys,
                RuntimeError("transient storage boom"),
            )

        _race_once(v2, advance)
        with pytest.raises(RuntimeError, match="transient storage boom"):
            commit_to(v2, {"mine": b"2"})

        assert v2.head == base_head
        assert _committed(v2, "mine") is None
        # The worktree stays usable.
        result = commit_to(v2, {"mine": b"2"})
        assert result.merged
        assert v2.get("mine") == b"2"


class TestBranchOpen:
    """Issue #43: opening a branch by name must not mint it.

    Reads (including opens) after a delete must not resurrect the
    branch and block a later create.
    """

    def test_open_missing_with_create_mints(self):
        store = Memory()
        repo = _repo(store)
        v = repo.worktree("dev", create=True)
        assert "dev" in repo.branches
        assert v.branch == "dev"

    def test_open_missing_raises_named_error(self):
        store = Memory()
        with pytest.raises(UnknownBranchError):
            _repo(store).worktree("dev")

    def test_named_error_is_a_kvgit_error(self):
        assert issubclass(UnknownBranchError, KvgitError)

    def test_open_missing_mints_no_branch(self):
        store = Memory()
        repo = _repo(store)
        with pytest.raises(UnknownBranchError):
            repo.worktree("dev")
        # No branch head installed: nothing listed, nothing to block a
        # later create. (A store version stamp names no branch.)
        assert list(repo.branches) == []
        assert store.get(BRANCH_HEAD % "dev") is None

    def test_open_existing(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"a": b"1"})
        v2 = _repo(store).worktree("main")
        assert v2.get("a") == b"1"

    def test_exists(self):
        store = Memory()
        repo = _repo(store)
        assert "dev" not in repo.branches
        repo.worktree("dev", create=True)
        assert "dev" in repo.branches
        assert "nope" not in repo.branches

    def test_branch_exists_from_worktree(self):
        v = _wt(Memory())
        assert "main" in v.repo.branches
        assert "dev" not in v.repo.branches

    def test_delete_then_open_does_not_resurrect(self):
        store = Memory()
        v = _wt(store)
        repo = v.repo
        repo.branches.create("dev", at=v.head)
        repo.branches.delete("dev")
        assert "dev" not in repo.branches
        with pytest.raises(UnknownBranchError):
            _repo(store).worktree("dev")
        assert "dev" not in repo.branches
        repo.branches.create("dev", at=v.head)  # recreate unblocked
        assert "dev" in repo.branches


class TestVersionedSharedStore:
    def test_two_writers_same_store(self):
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"a": b"1"})

        v2 = _wt(store)
        assert v2.get("a") == b"1"

    def test_branch_tip_past_worktree_head(self):
        store = Memory()
        v1 = _wt(store)
        h0 = v1.head
        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})

        assert v2.head == h0
        assert v2.repo.branches[v2.branch] == v1.head


class TestParentFormat:
    def test_root_has_empty_parents(self):
        v = _wt()
        assert v.repo.get_commit(v.head).parents == ()

    def test_normal_commit_single_parent(self):
        v = _wt()
        h0 = v.head
        r1 = commit_to(v, {"a": b"1"})
        assert v.repo.get_commit(r1.commit).parents == (h0,)

    def test_history_all_parents_linear(self):
        """Following every parent yields the same commits for a linear chain."""
        v = _wt()
        h0 = v.head
        r1 = commit_to(v, {"a": b"1"})
        r2 = commit_to(v, {"b": b"2"})
        linear = _history(v)
        full = _history(v, all_parents=True)
        assert linear == [r2.commit, r1.commit, h0]
        assert set(full) == {h0, r1.commit, r2.commit}


class TestCommitInfo:
    def test_commit_with_info(self):
        v = _wt()
        commit_to(v, {"a": b"1"}, info={"author": "agent-1", "message": "init"})
        info = v.repo.get_commit(v.head).info
        assert info == {"author": "agent-1", "message": "init"}

    def test_commit_without_info(self):
        v = _wt()
        commit_to(v, {"a": b"1"})
        assert v.repo.get_commit(v.head).info is None

    def test_info_affects_hash(self):
        """Same changes with different info produce different hashes."""
        r1 = commit_to(_wt(), {"a": b"1"}, info={"author": "agent-1"})
        r2 = commit_to(_wt(), {"a": b"1"}, info={"author": "agent-2"})
        r3 = commit_to(_wt(), {"a": b"1"})

        assert r1.commit != r2.commit
        assert r1.commit != r3.commit
        assert r2.commit != r3.commit

    def test_commit_info_specific_commit(self):
        v = _wt()
        r1 = commit_to(v, {"a": b"1"}, info={"step": 1})
        r2 = commit_to(v, {"b": b"2"}, info={"step": 2})
        assert v.repo.get_commit(r1.commit).info == {"step": 1}
        assert v.repo.get_commit(r2.commit).info == {"step": 2}


class TestDiff:
    def test_diff_additions(self):
        v = _wt()
        h0 = v.head
        r1 = commit_to(v, {"a": b"1", "b": b"2"})
        d = v.repo.diff(h0, r1.commit)
        assert d.added == {"a", "b"}
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_removals(self):
        v = _wt()
        r1 = commit_to(v, {"a": b"1", "b": b"2"})
        r2 = commit_to(v, removals={"a"})
        d = v.repo.diff(r1.commit, r2.commit)
        assert d.removed == {"a"}
        assert d.added == frozenset()
        assert d.modified == frozenset()

    def test_diff_modifications(self):
        v = _wt()
        r1 = commit_to(v, {"a": b"1", "b": b"2"})
        r2 = commit_to(v, {"a": b"updated"})
        d = v.repo.diff(r1.commit, r2.commit)
        assert d.modified == {"a"}
        assert d.added == frozenset()
        assert d.removed == frozenset()

    def test_diff_mixed(self):
        v = _wt()
        r1 = commit_to(v, {"a": b"1", "b": b"2", "c": b"3"})
        r2 = commit_to(v, updates={"a": b"new", "d": b"4"}, removals={"c"})
        d = v.repo.diff(r1.commit, r2.commit)
        assert d.added == {"d"}
        assert d.removed == {"c"}
        assert d.modified == {"a"}

    def test_diff_identical(self):
        v = _wt()
        r = commit_to(v, {"a": b"1"})
        d = v.repo.diff(r.commit, r.commit)
        assert d.added == frozenset()
        assert d.removed == frozenset()
        assert d.modified == frozenset()

    def test_diff_carried_forward_not_modified(self):
        """Keys carried forward unchanged should not appear as modified."""
        v = _wt()
        r1 = commit_to(v, {"a": b"1", "b": b"2"})
        r2 = commit_to(v, {"c": b"3"})  # a and b carried forward
        d = v.repo.diff(r1.commit, r2.commit)
        assert d.added == {"c"}
        assert d.modified == frozenset()
        assert d.removed == frozenset()


class TestBranches:
    def test_default_branch_is_main(self):
        v = kvgit.open(codec="bytes")
        assert v.branch == "main"

    def test_two_branches_coexist(self):
        store = Memory()
        v1 = _wt(store, branch="main")
        commit_to(v1, {"a": b"1"})

        v2 = _wt(store, branch="dev")
        commit_to(v2, {"b": b"2"})

        # Each branch has its own data
        main = _wt(store, branch="main")
        dev = _wt(store, branch="dev")
        assert main.get("a") == b"1"
        assert main.get("b") is None
        assert dev.get("b") == b"2"
        assert dev.get("a") is None

    def test_commit_targets_correct_branch(self):
        store = Memory()
        main = _wt(store, branch="main")
        commit_to(main, {"a": b"1"})

        dev = _wt(store, branch="dev")
        commit_to(dev, {"b": b"2"})

        # Main HEAD should not be affected by dev commit
        main2 = _wt(store, branch="main")
        assert main2.get("a") == b"1"
        assert main2.get("b") is None

    def test_branches_enumerate(self):
        store = Memory()
        repo = _repo(store)
        repo.worktree("main", create=True)
        repo.worktree("dev", create=True)
        repo.worktree("feature", create=True)
        assert list(repo.branches) == ["dev", "feature", "main"]

    def test_create_branch_forks_current_commit(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"a": b"1"})

        dev = fork(v, "dev")
        assert dev.branch == "dev"
        assert dev.head == v.head
        assert dev.get("a") == b"1"

    def test_create_branch_diverge_and_commit(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"base": b"0"})

        dev = fork(v, "dev")
        commit_to(dev, {"feature": b"1"})

        # Main doesn't see dev's data
        main = _wt(store, branch="main")
        assert main.get("feature") is None
        assert main.get("base") == b"0"

    def test_create_branch_already_exists(self):
        store = Memory()
        v = _wt(store)
        with pytest.raises(BranchExistsError, match="already exists"):
            v.repo.branches.create("main", at=v.head)

    def test_create_branch_appears_in_branches_list(self):
        store = Memory()
        v = _wt(store)
        v.repo.branches.create("dev", at=v.head)
        v.repo.branches.create("staging", at=v.head)
        branches = list(v.repo.branches)
        assert "dev" in branches
        assert "staging" in branches
        assert "main" in branches

    def test_delete_branch(self):
        store = Memory()
        v = _wt(store)
        v.repo.branches.create("dev", at=v.head)
        assert "dev" in list(v.repo.branches)
        v.repo.branches.delete("dev")
        assert "dev" not in list(v.repo.branches)

    def test_deleted_branch_name_reused_starts_empty(self):
        """Recreating a deleted branch's name must not resurrect the
        old state: deleting a branch removes its prev-HEAD recovery
        backup too, so _resolve_head's fallback cannot 'recover' the
        deleted branch from it."""
        store = Memory()
        v = _wt(store)
        dev = fork(v, "dev")
        commit_to(dev, {"secret": b"deleted data"})
        commit_to(dev, {"more": b"commits move prev-HEAD off genesis"})
        v.repo.branches.delete("dev")

        from kvgit._store import BRANCH_HEAD_PREV, _resolve_head

        assert store.get(BRANCH_HEAD_PREV % "dev") is None
        assert _resolve_head(store, "dev") is None  # gone, not recoverable

        reborn = _wt(store, branch="dev")
        assert reborn.get("secret") is None
        assert reborn.get("more") is None

    def test_delete_nonexistent_branch_raises(self):
        v = _wt()
        with pytest.raises(UnknownBranchError, match="does not exist"):
            v.repo.branches.delete("nope")

    def test_branch_property(self):
        v = _wt()
        assert v.branch == "main"
        v2 = _wt(v.repo.store, branch="dev")
        assert v2.branch == "dev"

    def test_create_branch_at_commit(self):
        store = Memory()
        v = _wt(store)
        initial = v.head
        commit_to(v, {"x": b"v1"})
        commit_to(v, {"x": b"v2"})

        # Create branch at initial (empty) commit
        v2 = fork(v, "fresh", at=initial)
        assert v2.get("x") is None
        assert v2.branch == "fresh"

    def test_create_branch_at_nonexistent_raises(self):
        v = _wt()
        with pytest.raises(UnknownCommitError, match="does not exist"):
            v.repo.branches.create("bad", at="nonexistent_hash")

    def test_snapshot_reads_other_branch(self):
        store = Memory()
        v = _wt(store)
        commit_to(v, {"shared": b"yes"})
        v.repo.branches.create("dev", at=v.head)
        dev = _wt(store, branch="dev")
        commit_to(dev, {"dev_key": b"dev_val"})

        # Read dev's state from main's repository
        snap = v.repo.snapshot(branch="dev")
        assert snap.get("dev_key") == b"dev_val"
        # A shared key
        assert snap.get("shared") == b"yes"
        # A nonexistent key
        assert snap.get("nope") is None
        # A nonexistent branch
        with pytest.raises(UnknownBranchError):
            v.repo.snapshot(branch="nope")


class TestThreeWayMerge:
    def test_auto_merge_non_overlapping(self):
        """Two branches with different keys auto-merge."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})
        commit_to(v2, {"b": b"2"})

        assert v2.get("a") == b"1"
        assert v2.get("b") == b"2"
        assert v2.get("base") == b"0"

    def test_conflict_no_fn(self):
        """Both modify same key, no merge function -> MergeConflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"key": b"base"})

        v2 = _wt(store)
        commit_to(v1, {"key": b"v1"})

        with pytest.raises(MergeConflict) as exc_info:
            commit_to(v2, {"key": b"v2"})
        assert "key" in exc_info.value.conflicting_keys

    def test_conflict_resolved_by_fn(self):
        """Per-key merge function resolves conflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"counter": b"10"})

        v2 = _wt(store)
        commit_to(v1, {"counter": b"15"})

        def add_merge(old, ours, theirs):
            o = int(old) if old else 0
            a = int(ours)
            b = int(theirs)
            return str(a + b - o).encode()

        result = commit_to(
            v2,
            {"counter": b"20"},
            merge_fns={"counter": add_merge},
        )
        assert result
        assert v2.get("counter") == b"25"  # 20 + 15 - 10

    def test_conflict_resolved_by_default(self):
        """Default merge function resolves conflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"x": b"base"})

        v2 = _wt(store)
        commit_to(v1, {"x": b"v1"})

        def lww(old, ours, theirs):
            return theirs

        result = commit_to(v2, {"x": b"v2"}, default_merge=lww)
        assert result
        assert v2.get("x") == b"v1"  # theirs = HEAD value

    def test_worktree_level_merge_fn(self):
        """set_merge_fn on a worktree works for three-way merge."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"x": b"base"})

        v2 = _wt(store)
        v2.set_merge_fn("x", lambda old, ours, theirs: ours)  # ours wins

        commit_to(v1, {"x": b"v1"})

        result = commit_to(v2, {"x": b"v2"})
        assert result
        assert v2.get("x") == b"v2"  # ours wins

    def test_remove_modify_conflict(self):
        """One side removes, other modifies -> conflict without fn."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"key": b"base"})

        v2 = _wt(store)
        commit_to(v1, removals={"key"})

        with pytest.raises(MergeConflict):
            commit_to(v2, {"key": b"modified"})

    def test_both_remove_same_key(self):
        """Both remove same key -> no conflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"key": b"base", "keep": b"yes"})

        v2 = _wt(store)
        commit_to(v1, removals={"key"})

        result = commit_to(v2, removals={"key"})
        assert result
        assert v2.get("key") is None
        assert v2.get("keep") == b"yes"

    def test_both_identical_change(self):
        """Both sides make identical change -> no conflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"key": b"base"})

        # Both branch from same point and make same change
        v2 = _wt(store)
        commit_to(v1, {"key": b"same"})

        result = commit_to(v2, {"key": b"same"})
        assert result
        assert v2.get("key") == b"same"

    def test_merge_commit_two_parents(self):
        """After three-way merge, commit has two parents."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})
        # v2's head is stale: its commit three-way merges onto v1's.
        commit_to(v2, {"b": b"2"})
        # The merge commit (on HEAD) has two parents
        parents = v2.repo.get_commit(v2.head).parents
        assert len(parents) == 2

    def test_merge_result_populated(self):
        """The commit's MergeResult describes the three-way merge."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})

        result = commit_to(v2, {"b": b"2"})
        assert result is not None
        assert result.merged is True
        assert result.strategy == "three_way"
        # The other writer's change, taken as it was; nothing contested.
        assert result.carried_keys == ("a",)
        assert result.auto_merged_keys == ()

    def test_history_all_parents_after_merge(self):
        """After merge, a log over all parents traverses both branches."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})

        commit_to(v2, {"b": b"2"})
        merge_commit = v2.head

        all_commits = set(_history(v2, all_parents=True))
        assert merge_commit in all_commits

    def test_fast_forward_still_works(self):
        """Fast-forward merge still works when HEAD hasn't moved."""
        store = Memory()
        v = _wt(store)
        result = commit_to(v, {"a": b"1"})
        assert result
        assert result.strategy == "fast_forward"

    def test_no_op_still_works(self):
        """No-op commit when no changes."""
        v = _wt()
        result = v.commit()
        assert result
        assert result.strategy == "no_op"

    def test_lca_diverged(self):
        """LCA finding works for diverged branches."""
        store = Memory()
        v1 = _wt(store)
        r_base = commit_to(v1, {"base": b"0"})

        v2 = fork(v1, "dev")
        h1 = commit_to(v1, {"a": b"1"}).commit
        h2 = commit_to(v2, {"b": b"2"}).commit

        lca = v1.repo.merge_base(h1, h2)
        assert lca == r_base.commit

    def test_merge_root_into_branches_keeps_base(self):
        """Issue #47: merging the root into each branch must not demote
        the merge base from X back to R."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {})
        root = v.head
        commit_to(v, {"x": b"1"})
        fork_point = v.head
        a = fork(v, "a")
        b = fork(v, "b")
        commit_to(a, {"a": b"1"})
        commit_to(b, {"b": b"2"})
        a.merge(commit=root)
        b.merge(commit=root)

        assert v.repo.merge_base(a.head, b.head) == fork_point
        assert v.repo.merge_base(b.head, a.head) == fork_point

    def test_criss_cross_base_is_deterministic(self):
        """Two best ancestors: the smallest hash wins, whichever order
        the commits are given in."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {})
        fork_a = fork(v, "pa")
        fork_b = fork(v, "pb")
        commit_to(fork_a, {"k": b"v1"})
        commit_to(fork_b, {"k": b"v2"})
        p1, p2 = fork_a.head, fork_b.head
        m = fork(v, "m", at=p1)
        m.merge(commit=p2, merge_fns={"k": MergeChoice.OURS})
        q = fork(v, "q", at=p2)
        q.merge(commit=p1, merge_fns={"k": MergeChoice.OURS})

        assert v.repo.merge_base(m.head, q.head) == min(p1, p2)
        assert v.repo.merge_base(q.head, m.head) == min(p1, p2)

    def test_criss_cross_merge_is_direction_independent(self):
        """Both merge directions use the same base, so they resolve to
        the same value instead of each side winning its own merge."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {})
        fork_a = fork(v, "pa")
        fork_b = fork(v, "pb")
        commit_to(fork_a, {"k": b"v1"})
        commit_to(fork_b, {"k": b"v2"})
        p1, p2 = fork_a.head, fork_b.head
        m = fork(v, "m", at=p1)
        m.merge(commit=p2, merge_fns={"k": MergeChoice.OURS})
        q = fork(v, "q", at=p2)
        q.merge(commit=p1, merge_fns={"k": MergeChoice.OURS})

        fwd = fork(v, "fwd", at=m.head)
        fwd.merge(commit=q.head)
        back = fork(v, "back", at=q.head)
        back.merge(commit=m.head)
        assert fwd.get("k") == back.get("k")

    def test_commit_with_info(self):
        """Commit carries info."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})

        commit_to(v2, {"b": b"2"}, info={"merged_by": "test"})
        # The info is on the three-way merge commit
        assert v2.repo.get_commit(v2.head).info == {"merged_by": "test"}

    def test_one_removes_other_adds(self):
        """One branch removes a key, other adds a new key."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"existing": b"val", "keep": b"yes"})

        v2 = _wt(store)
        commit_to(v1, removals={"existing"})

        result = commit_to(v2, {"new_key": b"new"})
        assert result
        assert v2.get("existing") is None
        assert v2.get("new_key") == b"new"
        assert v2.get("keep") == b"yes"

    def test_a_commit_reads_the_same_whatever_it_writes(self):
        """A commit's HAMT update reads its paths level by level, so
        writing or deleting many keys costs no more round trips than
        writing a few."""

        class Counting(Memory):
            reads = 0

            def get(self, key):
                Counting.reads += 1
                return super().get(key)

            def get_many(self, *args):
                Counting.reads += 1
                return super().get_many(*args)

        store = Counting()
        # No content cache: every read is a round trip to the store.
        v = _wt(store, cache_bytes=0)
        commit_to(v, {f"k{i}": b"%d" % i for i in range(5000)})

        def reads(updates, removals=()):
            _stage(v, updates, removals)
            Counting.reads = 0
            v.commit()
            return Counting.reads

        few = reads({f"a{i}": b"1" for i in range(3)})
        many = reads({f"b{i}": b"1" for i in range(200)})
        deletes = reads({}, {f"k{i}" for i in range(0, 5000, 25)})
        assert many <= few + 1
        assert deletes <= few + 2
        assert few < 15

    def test_a_merge_reads_the_change_not_the_keyset(self):
        """Each side's changes are read as structural diffs, so a merge
        of one-key changes over a large keyset reads a few HAMT nodes,
        not all of them."""

        class Counting(Memory):
            nodes_read = 0

            def get_many(self, *args):
                keys = self._normalize_keys(args)
                Counting.nodes_read += sum(k.startswith("kvgit:keyset:") for k in keys)
                return super().get_many(*args)

            def get(self, key):
                Counting.nodes_read += key.startswith("kvgit:keyset:")
                return super().get(key)

        store = Counting()
        # No content cache: every node read reaches the store.
        v1 = _wt(store, cache_bytes=0)
        commit_to(v1, {f"k{i}": b"%d" % i for i in range(2000)})
        total_nodes = sum(1 for k in store.keys("kvgit:keyset:"))

        v2 = _wt(store, cache_bytes=0)
        commit_to(v1, {"a": b"1"})
        _stage(v2, {"b": b"2"})
        Counting.nodes_read = 0
        result = v2.commit()  # loses the race: a three-way merge
        assert result.strategy == "three_way"
        assert v2.get("a") == b"1" and v2.get("b") == b"2"
        assert Counting.nodes_read < 40 < total_nodes


class TestMergeResultReturn:
    def test_merge_result_truthy(self):
        r = MergeResult(
            merged=True,
            commit="abc",
            strategy="no_op",
            auto_merged_keys=(),
            carried_keys=(),
        )
        assert r
        assert bool(r) is True

    def test_merge_result_falsy(self):
        r = MergeResult(
            merged=False,
            commit=None,
            strategy="fast_forward",
            auto_merged_keys=(),
            carried_keys=(),
        )
        assert not r
        assert bool(r) is False

    def test_commit_returns_result_object(self):
        """commit() returns a MergeResult."""
        store = Memory()
        v = _wt(store)
        result = commit_to(v, {"x": b"1"})
        assert isinstance(result, MergeResult)
        assert result.merged is True
        assert result.strategy == "fast_forward"
        assert result.commit == v.head

    def test_no_op_returns_result(self):
        """No changes returns a no_op MergeResult."""
        v = _wt()
        result = v.commit()
        assert isinstance(result, MergeResult)
        assert result.strategy == "no_op"

    def test_three_way_returns_result(self):
        """Three-way merge returns a MergeResult with details."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"base": b"0"})

        v2 = _wt(store)
        commit_to(v1, {"a": b"1"})

        result = commit_to(v2, {"b": b"2"})
        assert isinstance(result, MergeResult)
        assert result.strategy == "three_way"
        assert result.merged is True
        assert result.commit is not None

    def test_abandon_returns_falsy_result(self):
        """on_conflict='abandon' returns MergeResult with merged=False."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"x": b"1"})

        v2 = _wt(store)  # reads HEAD

        # Concurrent writer modifies the same key — merge conflict
        commit_to(v1, {"x": b"v1_conflict"})

        # v2 also modifies x — no merge function, so conflict → abandon
        result = commit_to(v2, {"x": b"v2_conflict"}, on_conflict="abandon")
        assert isinstance(result, MergeResult)
        assert not result
        assert result.merged is False


class TestBugFixes:
    def test_commit_info_only_creates_commit(self):
        """commit(info=...) with no data changes still creates a commit."""
        v = _wt()
        old_hash = v.head
        result = v.commit(info={"msg": "hi"})
        assert result.commit != old_hash
        assert v.repo.get_commit(v.head).info == {"msg": "hi"}

    def test_commit_no_changes_no_info_is_noop(self):
        """commit() with nothing at all is a no-op."""
        v = _wt()
        old_hash = v.head
        result = v.commit()
        assert result.strategy == "no_op"
        assert v.head == old_hash

    def test_merge_fn_exception_surfaces_in_conflict(self):
        """Merge fn exceptions are attached to MergeConflict."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"k": b"base"})

        v2 = _wt(store)
        commit_to(v1, {"k": b"v1"})

        def bad_fn(old, ours, theirs):
            raise ValueError("intentional error")

        with pytest.raises(MergeConflict) as exc_info:
            commit_to(v2, {"k": b"v2"}, merge_fns={"k": bad_fn})
        assert "k" in exc_info.value.conflicting_keys
        assert "k" in exc_info.value.merge_errors
        assert isinstance(exc_info.value.merge_errors["k"], ValueError)

    def test_commit_invalid_on_conflict(self):
        """Invalid on_conflict value raises ValueError."""
        v = _wt()
        with pytest.raises(ValueError, match="on_conflict"):
            commit_to(v, {"x": b"1"}, on_conflict="bogus")

    def test_state_clean_after_merge_conflict(self):
        """The worktree's head is unchanged after MergeConflict."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})

        original_commit = v.head

        # Concurrent writer modifies same key — no merge function → conflict
        v_other = _wt(store)
        commit_to(v_other, {"x": b"concurrent"})

        with pytest.raises(MergeConflict):
            commit_to(v, {"x": b"2"})

        # State should be unchanged
        assert v.head == original_commit
        assert _committed(v, "x") == b"1"

    def test_state_clean_after_abandon_on_conflict(self):
        """The worktree's head is unchanged after abandon on merge conflict."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})

        original_commit = v.head

        # Concurrent writer modifies same key — conflict
        v_other = _wt(store)
        commit_to(v_other, {"x": b"concurrent"})

        result = commit_to(v, {"x": b"2"}, on_conflict="abandon")
        assert result.merged is False
        assert v.head == original_commit

    def test_state_clean_after_merge_conflict_three_way(self):
        """The worktree's head is unchanged after MergeConflict on a
        three-way merge."""
        store = Memory()
        v1 = _wt(store)
        commit_to(v1, {"x": b"1"})

        v2 = _wt(store)
        # v1 advances HEAD — v2 is now behind
        commit_to(v1, {"x": b"v1"})

        original_commit = v2.head

        # v2 modifies same key — no merge function → conflict
        with pytest.raises(MergeConflict):
            commit_to(v2, {"x": b"v2"})

        assert v2.head == original_commit
        assert _committed(v2, "x") == b"1"


class TestErgonomics:
    def test_repr(self):
        v = _wt()
        commit_to(v, {"a": b"1", "b": b"2"})
        r = repr(v)
        assert "main" in r
        assert v.head[:8] in r

    def test_repr_different_branch(self):
        store = Memory()
        v = _wt(store, branch="dev")
        r = repr(v)
        assert "dev" in r


class TestCleanOrphans:
    def test_delete_branch_then_sweep_cleans(self):
        """Deleting a branch and sweeping removes what only it reached."""
        store = Memory()
        v = _wt(store)

        # Create a branch with unique data
        dev = fork(v, "dev")
        commit_to(dev, {"dev_key": b"dev_value"})
        commit_to(dev, {"dev_key2": b"dev_value2"})

        # Count keys before deletion
        keys_before = len(list(store.keys()))

        v.repo.branches.delete("dev")
        v.repo.gc()

        keys_after = len(list(store.keys()))
        assert keys_after < keys_before

    def test_delete_branch_preserves_shared_commits(self):
        """A sweep after deleting a branch keeps commits other branches
        still reach."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"shared": b"data"})

        # Both branches fork from the same commit
        v.repo.branches.create("dev", at=v.head)
        v.repo.branches.create("staging", at=v.head)

        # Add unique data to dev
        dev = _wt(store, branch="dev")
        commit_to(dev, {"dev_only": b"value"})

        # Delete dev — shared commits survive (still reachable from staging)
        v.repo.branches.delete("dev")
        v.repo.gc()

        # staging should still work fine
        staging = _wt(store, branch="staging")
        assert staging.get("shared") == b"data"

    def test_gc_explicit(self):
        """gc() reclaims commits whose branch head vanished."""
        store = Memory()
        v = _wt(store)
        v.repo.branches.create("temp", at=v.head)
        temp = _wt(store, branch="temp")
        commit_to(temp, {"t": b"val"})

        # Remove HEAD pointer manually (simulating external deletion)
        store.remove(BRANCH_HEAD % "temp")

        cleaned = v.repo.gc(min_age=0)
        assert cleaned >= 1

    def test_gc_respects_min_age(self):
        """Recent orphans should NOT be cleaned when min_age is large."""
        store = Memory()
        v = _wt(store)
        v.repo.branches.create("temp", at=v.head)
        temp = _wt(store, branch="temp")
        commit_to(temp, {"t": b"val"})

        store.remove(BRANCH_HEAD % "temp")

        # With a very large min_age, nothing should be cleaned
        cleaned = v.repo.gc(min_age=999999)
        assert cleaned == 0


class TestHeadRecovery:
    """Tests for corrupt HEAD detection and recovery."""

    def test_prev_head_written_on_commit(self):
        """Committing writes a prev HEAD backup."""
        from kvgit._store import BRANCH_HEAD_PREV

        store = Memory()
        v = _wt(store)
        first = v.head
        commit_to(v, {"x": b"1"})

        prev_bytes = store.get(BRANCH_HEAD_PREV % "main")
        assert prev_bytes is not None
        from kvgit.encoding import loads

        assert loads(prev_bytes) == first

    def test_recover_from_empty_head(self):
        """Opening a worktree recovers from empty HEAD bytes via prev HEAD."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})
        commit_to(v, {"x": b"2"})  # prev HEAD now has x=1

        # Corrupt HEAD
        store.set(BRANCH_HEAD % "main", b"")

        # Should recover from prev HEAD
        v2 = _repo(store).worktree("main")
        assert v2.get("x") == b"1"

    def test_recover_from_invalid_json_head(self):
        """Opening a worktree recovers from invalid JSON HEAD bytes."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})
        commit_to(v, {"x": b"2"})  # prev HEAD now points to commit with x=1

        store.set(BRANCH_HEAD % "main", b"not-json{{{")
        v2 = _repo(store).worktree("main")
        # Recovers to prev HEAD (one commit behind)
        assert v2.get("x") == b"1"

    def test_recover_from_dangling_head(self):
        """Opening a worktree recovers when HEAD points to a nonexistent
        commit."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})
        commit_to(v, {"x": b"2"})

        store.set(BRANCH_HEAD % "main", dumps("deadbeef" * 5))
        v2 = _repo(store).worktree("main")
        assert v2.get("x") == b"1"

    def test_snapshot_survives_corrupt_branch(self):
        """A snapshot of a corrupt branch recovers instead of crashing."""
        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})
        v.repo.branches.create("other", at=v.head)
        other = _wt(store, branch="other")
        commit_to(other, {"y": b"2"})
        commit_to(other, {"y": b"3"})  # ensure prev HEAD has data

        # Corrupt the other branch
        store.set(BRANCH_HEAD % "other", b"")

        # The snapshot should not crash — reads via prev HEAD recovery
        result = v.repo.snapshot(branch="other").get("y")
        assert result == b"2"

    def test_open_other_branch_survives_corrupt_head(self):
        """Opening a worktree on another branch recovers from its
        corrupt HEAD."""
        store = Memory()
        v = _wt(store)
        v.repo.branches.create("dev", at=v.head)
        dev = _wt(store, branch="dev")
        commit_to(dev, {"d": b"1"})
        commit_to(dev, {"d": b"2"})

        store.set(BRANCH_HEAD % "dev", b"")

        assert v.repo.worktree("dev").get("d") == b"1"

    def test_no_recovery_when_no_prev_head(self):
        """A corrupt HEAD with no backup is unrecoverable by default: the
        commit scan answers only when asked for (see
        ``TestScanRecoveryIsOptIn`` in ``test_head_recovery.py``)."""
        from kvgit._store import BRANCH_HEAD_PREV

        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})

        # Corrupt HEAD and remove prev
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            _repo(store).worktree("main")

    def test_scan_recovery_when_no_prev_head_if_asked_for(self):
        """The scan still recovers when handed in explicitly."""
        from kvgit._store import BRANCH_HEAD_PREV, recover_by_commit_scan

        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})

        # Corrupt HEAD and remove prev
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")

        v2 = _repo(store, recover_from_corrupt_head=recover_by_commit_scan).worktree(
            "main"
        )
        assert v2.get("x") == b"1"

    def test_reset_saves_prev_head(self):
        """reset() preserves prev HEAD for recovery."""
        from kvgit._store import BRANCH_HEAD_PREV
        from kvgit.encoding import loads

        store = Memory()
        v = _wt(store)
        commit_to(v, {"x": b"1"})
        first = v.head
        commit_to(v, {"x": b"2"})
        second = v.head

        v.reset(first)
        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        assert prev == second


def _encode_chunked(value, sink) -> bytes:
    """Bytes stored as given; a ``(blob, chunk)`` pair stores the blob and
    sends the chunk to the commit's sink."""
    if isinstance(value, bytes):
        return value
    blob, chunk = value
    sink.put(chunk)
    return blob


def _decode_chunked(raw: bytes, reader) -> bytes:
    return raw


class TestMergeCommitMeta:
    """A merge commit's metadata must describe the blob it kept."""

    @staticmethod
    def _entry(store, commit, key):
        return entries(store, commit)[key]

    def test_their_side_key_keeps_their_size(self):
        store = Memory()
        other = _wt(store)
        commit_to(other, {"k": b"aaaa"})
        merger = _wt(store)

        commit_to(other, {"k": b"bbbbbbbbbb"})
        result = commit_to(merger, {"other": b"1"})

        entry = self._entry(store, result.commit, "k")
        assert entry.meta.size == len(store.get(entry.blob)) == 10

    def test_their_side_key_keeps_their_chunk_refs(self):
        store = Memory()
        codec = (_encode_chunked, _decode_chunked)
        other = worktree(store, codec=codec)
        commit_to(other, {"k": (b"base-blob", b"base chunk bytes")})
        merger = worktree(store, codec=codec)

        commit_to(other, {"k": (b"their-blob", b"their chunk bytes")})
        result = commit_to(merger, {"other": b"1"})

        entry = self._entry(store, result.commit, "k")
        assert store.get(entry.blob) == b"their-blob"
        assert entry.meta.chunks == [hash_bytes(b"their chunk bytes")]

    def test_our_side_key_keeps_our_meta(self):
        store = Memory()
        merger = _wt(store)
        commit_to(merger, {"k": b"aaaa"})
        other = _wt(store)

        commit_to(other, {"other": b"1"})
        result = commit_to(merger, {"k": b"bbbbbbbbbb"})

        entry = self._entry(store, result.commit, "k")
        assert entry.meta.size == len(store.get(entry.blob)) == 10

    def test_merged_value_gets_fresh_meta(self):
        store = Memory()
        other = _wt(store)
        commit_to(other, {"k": b"base"})
        merger = _wt(store)

        commit_to(other, {"k": b"theirs"})
        result = commit_to(
            merger,
            {"k": b"ours"},
            default_merge=lambda old, our, their: b"merged value",
        )

        entry = self._entry(store, result.commit, "k")
        assert store.get(entry.blob) == b"merged value"
        assert entry.meta.size == len(b"merged value")
        assert entry.meta.chunks is None
