"""Tests for Worktree: a branch checked out for work."""

import json
import pickle

import pytest
from support import fork, worktree

from kvgit import (
    ConcurrencyError,
    MergeChoice,
    MergeConflict,
    MergeResult,
    Repo,
    Status,
    UnknownBranchError,
    UnknownCommitError,
    text_merge,
)
from kvgit.encoding import dumps
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import BRANCH_HEAD, BRANCH_HEAD_PREV, ROOT_COMMIT


class TestReads:
    def test_set_and_get(self):
        wt = worktree()
        wt["k"] = "v"
        assert wt.get("k") == "v"

    def test_get_missing_and_default(self):
        wt = worktree()
        assert wt.get("nope") is None
        assert wt.get("nope", "fallback") == "fallback"

    def test_get_many(self):
        wt = worktree()
        wt["a"] = 1
        wt["b"] = 2
        wt.commit()
        wt["c"] = 3
        assert wt.get_many("a", "c", "missing") == {"a": 1, "c": 3}

    def test_contains_and_keys_include_pending(self):
        wt = worktree()
        wt["committed"] = 1
        wt.commit()
        wt["pending"] = 2
        assert "committed" in wt and "pending" in wt
        assert wt.keys() == {"committed", "pending"}
        assert 42 not in wt


class TestMutableMapping:
    def test_getitem_and_missing(self):
        wt = worktree()
        wt["k"] = "v"
        assert wt["k"] == "v"
        with pytest.raises(KeyError):
            wt["missing"]

    def test_delitem_and_missing(self):
        wt = worktree()
        wt["k"] = "v"
        del wt["k"]
        assert "k" not in wt
        with pytest.raises(KeyError):
            del wt["missing"]

    def test_iter_and_len(self):
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        wt["b"] = 2
        assert sorted(wt) == ["a", "b"]
        assert len(wt) == 2

    def test_delete_shadows_committed_and_set_after_delete(self):
        wt = worktree()
        wt["k"] = "v1"
        wt.commit()
        del wt["k"]
        assert wt.get("k") is None and "k" not in wt.keys()
        wt["k"] = "v2"
        assert wt["k"] == "v2"


class TestStatus:
    def test_clean_worktree_is_falsy(self):
        wt = worktree()
        assert not wt.status()
        assert wt.status() == Status(frozenset(), frozenset())

    def test_status_names_pending_keys(self):
        wt = worktree()
        wt["gone"] = 1
        wt.commit()
        wt["new"] = 2
        del wt["gone"]
        assert wt.status() == Status(frozenset({"new"}), frozenset({"gone"}))
        assert wt.status()

    def test_deleting_a_pending_only_key_leaves_nothing_pending(self):
        wt = worktree()
        wt["draft"] = 1
        del wt["draft"]
        assert not wt.status()
        assert "draft" not in wt

    def test_deleting_a_rewritten_committed_key_is_a_removal(self):
        wt = worktree()
        wt["k"] = 1
        wt.commit()
        wt["k"] = 2
        del wt["k"]
        assert wt.status() == Status(frozenset(), frozenset({"k"}))
        wt.commit()
        assert "k" not in wt


class TestCommit:
    def test_commit_persists_and_clears_pending(self):
        store = Memory()
        wt = worktree(store)
        wt["a"] = 1
        wt["b"] = 2
        result = wt.commit()
        assert isinstance(result, MergeResult) and result.merged
        assert not wt.status()
        reader = worktree(store)
        assert (reader["a"], reader["b"]) == (1, 2)

    def test_commit_with_removals(self):
        wt = worktree()
        wt["a"] = 1
        wt["b"] = 2
        wt.commit()
        del wt["a"]
        wt["d"] = 4
        assert wt.commit().merged
        assert wt.get("a") is None and wt["b"] == 2 and wt["d"] == 4

    def test_nothing_pending_is_a_no_op(self):
        wt = worktree()
        head = wt.head
        assert wt.commit().strategy == "no_op"
        assert wt.head == head

    def test_commit_info_lands_on_the_commit(self):
        wt = worktree()
        wt["k"] = "v"
        wt.commit(info={"author": "test"})
        assert wt.repo.get_commit(wt.head).info == {"author": "test"}

    def test_head_moves_with_each_commit(self):
        wt = worktree()
        assert wt.head == ROOT_COMMIT
        wt["k"] = 1
        wt.commit()
        assert wt.head != ROOT_COMMIT
        assert wt.repo.branches["main"] == wt.head


class TestPartialCommit:
    def test_only_named_keys_commit(self):
        store = Memory()
        wt = worktree(store)
        wt["a"] = 1
        wt["b"] = 2
        assert wt.commit(keys={"a", "ghost"}).merged
        assert wt.status().updated == {"b"}
        assert wt["b"] == 2
        reader = worktree(store)
        assert reader.get("a") == 1 and reader.get("b") is None

    def test_partial_commit_with_info_and_removals(self):
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        del wt["a"]
        wt["c"] = 3
        wt.commit(keys=["a"], info={"message": "just a"})
        assert wt.get("a") is None
        assert wt.status().updated == {"c"}
        assert wt.repo.get_commit(wt.head).info == {"message": "just a"}

    def test_a_partial_commit_drops_the_whole_read_cache(self):
        """The head moved, possibly over a concurrent write: cached values
        for keys the commit did not touch may be stale too."""
        store = Memory()
        wt = worktree(store)
        wt["x"] = "original_x"
        wt["y"] = "original_y"
        wt.commit()
        assert wt["y"] == "original_y"  # cached

        other = worktree(store)
        other["y"] = "updated_by_other"
        other.commit()

        wt["x"] = "new_x"
        wt.commit(keys={"x"})
        assert wt["y"] == "updated_by_other"


class TestDiscardResetRefresh:
    def test_discard_drops_pending_only(self):
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        wt["b"] = 2
        del wt["a"]
        wt.discard()
        assert not wt.status()
        assert wt["a"] == 1 and wt.get("b") is None

    def test_reset_moves_the_branch_and_drops_pending(self):
        wt = worktree()
        wt["k"] = "v1"
        wt.commit()
        first = wt.head
        wt["k"] = "v2"
        wt.commit()
        wt["k"] = "pending"

        wt.reset(first)
        assert not wt.status()
        assert wt["k"] == "v1"
        assert wt.head == first == wt.repo.branches["main"]

    def test_reset_to_an_unknown_commit_raises_and_keeps_pending(self):
        wt = worktree()
        wt["k"] = "v"
        wt.commit()
        wt["k"] = "pending"
        with pytest.raises(UnknownCommitError):
            wt.reset("0" * 40)
        assert wt["k"] == "pending"

    def test_refresh_moves_to_the_tip_and_drops_pending(self):
        store = Memory()
        wt = worktree(store)
        other = worktree(store)
        other["from_other"] = "data"
        other.commit()
        wt["mine"] = "pending"
        assert wt.get("from_other") is None
        wt.refresh()
        assert wt["from_other"] == "data"
        assert not wt.status()

    def test_refresh_of_a_deleted_branch_raises(self):
        wt = worktree()
        dev = fork(wt, "dev")
        wt.repo.branches.delete("dev")
        with pytest.raises(UnknownBranchError):
            dev.refresh()

    def test_reset_of_a_deleted_branch_raises_and_does_not_recreate_it(self):
        wt = worktree()
        dev = fork(wt, "dev")
        wt.repo.branches.delete("dev")
        with pytest.raises(UnknownBranchError):
            dev.reset(wt.head)
        assert "dev" not in wt.repo.branches

    def test_a_delete_landing_mid_reset_is_not_undone(self):
        """The delete lands between reset's read of HEAD and its write."""

        class DeleteFirst(Memory):
            armed = False

            def cas_many(self, expected, writes, removes=()):
                if self.armed and BRANCH_HEAD % "dev" in writes:
                    self.armed = False
                    self.remove_many(BRANCH_HEAD % "dev", BRANCH_HEAD_PREV % "dev")
                return super().cas_many(expected, writes, removes)

        store = DeleteFirst()
        wt = worktree(store)
        wt["k"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        store.armed = True
        with pytest.raises(UnknownBranchError):
            dev.reset(ROOT_COMMIT)
        assert "dev" not in wt.repo.branches


class TestSharedBranch:
    def test_a_commit_that_loses_the_race_merges(self):
        store = Memory()
        first = worktree(store)
        first["seed"] = "0"
        first.commit()
        second = worktree(store)

        first["mine"] = "2"
        second["other"] = "1"
        second.commit()
        result = first.commit()

        assert result.merged and result.strategy == "three_way"
        assert not first.status()
        assert (first["mine"], first["other"]) == ("2", "1")

    def test_a_commit_to_a_deleted_branch_raises(self):
        wt = worktree()
        dev = fork(wt, "dev")
        wt.repo.branches.delete("dev")
        assert dev.get("missing") is None  # still readable from its head
        dev["k"] = "v"
        with pytest.raises(UnknownBranchError):
            dev.commit()


class TestMerge:
    def _diverged(self):
        wt = worktree()
        wt["base"] = "b"
        wt["notes"] = "alpha\nbeta\n"
        wt.commit()
        dev = fork(wt, "dev")
        dev["dev_only"] = 1
        dev["notes"] = "alpha\nBETA\n"
        dev.commit()
        wt["notes"] = "ALPHA\nbeta\n"
        wt.commit()
        return wt, dev

    def test_merge_a_branch(self):
        wt, _ = self._diverged()
        result = wt.merge(branch="dev", default_merge=text_merge())
        assert result.merged
        assert wt["notes"] == "ALPHA\nBETA\n"
        assert wt["dev_only"] == 1
        assert len(wt.repo.get_commit(wt.head).parents) == 2

    def test_merge_a_commit_or_a_tag(self):
        wt, dev = self._diverged()
        wt.repo.tags.create("dev-v1", dev.head)
        assert wt.merge(tag="dev-v1", default_merge=text_merge()).merged
        wt2, dev2 = self._diverged()
        assert wt2.merge(commit=dev2.head, default_merge=text_merge()).merged

    def test_merge_needs_exactly_one_ref(self):
        wt, dev = self._diverged()
        with pytest.raises(ValueError, match="exactly one"):
            wt.merge()
        with pytest.raises(ValueError, match="exactly one"):
            wt.merge(branch="dev", commit=dev.head)

    def test_merge_refuses_with_pending_changes(self):
        wt, _ = self._diverged()
        wt["pending"] = 1
        with pytest.raises(ValueError, match="pending changes"):
            wt.merge(branch="dev")

    def test_unresolved_conflict_raises_and_changes_nothing(self):
        wt, _ = self._diverged()
        head = wt.head
        with pytest.raises(MergeConflict):
            wt.merge(branch="dev")
        assert wt.head == head == wt.repo.branches["main"]

    def test_abandon_returns_a_falsy_result(self):
        wt, _ = self._diverged()
        assert not wt.merge(branch="dev", on_conflict="abandon")


class TestFastForward:
    def _ahead(self):
        """``dev`` two commits ahead of ``main``, which has not moved."""
        wt = worktree()
        wt["a"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2
        dev.commit()
        dev["c"] = 3
        dev.commit()
        return wt, dev

    def test_a_branch_that_has_not_moved_fast_forwards(self):
        wt, dev = self._ahead()
        before = wt.head
        commits_before = set(wt.repo.store.keys("__commit_root__"))
        result = wt.merge(branch="dev", info={"msg": "unused"})
        assert result.merged
        assert result.strategy == "fast_forward"
        assert result.commit == dev.head == wt.head == wt.repo.branches["main"]
        assert (wt["b"], wt["c"]) == (2, 3)
        assert set(wt.repo.store.keys("__commit_root__")) == commits_before
        assert wt.repo.store.get(BRANCH_HEAD_PREV % "main") == dumps(before)

    def test_fast_forward_false_writes_a_merge_commit(self):
        wt, dev = self._ahead()
        before = wt.head
        result = wt.merge(branch="dev", fast_forward=False, info={"msg": "m"})
        assert result.strategy == "three_way"
        commit = wt.repo.get_commit(wt.head)
        assert commit.parents == (before, dev.head)
        assert commit.info == {"msg": "m"}
        assert (wt["b"], wt["c"]) == (2, 3)

    def test_merging_what_the_branch_already_contains_is_a_no_op(self):
        _, dev = self._ahead()
        dev.merge(branch="main")  # main's head is dev's ancestor
        head = dev.head
        result = dev.merge(branch="main")
        assert result.merged and result.strategy == "no_op"
        assert dev.head == head == dev.repo.branches["dev"]
        assert dev.merge(commit=head).strategy == "no_op"

    def test_a_no_op_checks_the_branch_has_not_moved(self):
        """Another worktree resets the branch to history without theirs:
        the merge must not report the branch as already containing it."""
        wt, dev = self._ahead()
        wt.merge(branch="dev")  # main == dev's head
        other = wt.repo.worktree("main")
        other.reset(ROOT_COMMIT)
        with pytest.raises(ConcurrencyError):
            wt.merge(commit=dev.head)
        assert not wt.merge(commit=dev.head, on_conflict="abandon")
        assert wt.repo.branches["main"] == ROOT_COMMIT

    def test_a_no_op_on_a_deleted_branch_raises(self):
        wt, dev = self._ahead()
        wt.merge(branch="dev")
        wt.repo.branches.delete("main")
        with pytest.raises(UnknownBranchError):
            wt.merge(commit=dev.head)
        assert "main" not in wt.repo.branches

    def test_a_branch_that_moved_meanwhile_is_not_fast_forwarded(self):
        wt, _ = self._ahead()
        other = wt.repo.worktree("main")
        other["x"] = 1
        other.commit()
        with pytest.raises(ConcurrencyError):
            wt.merge(branch="dev")
        assert not wt.merge(branch="dev", on_conflict="abandon")
        assert wt.repo.branches["main"] == other.head

    def test_fast_forwarding_a_deleted_branch_raises_and_does_not_recreate_it(self):
        wt, _ = self._ahead()
        wt.repo.branches.delete("main")
        with pytest.raises(UnknownBranchError):
            wt.merge(branch="dev")
        assert "main" not in wt.repo.branches

    def test_a_fast_forwarded_worktree_commits_on_top(self):
        wt, dev = self._ahead()
        wt.merge(branch="dev")
        wt["d"] = 4
        wt.commit()
        assert wt.repo.get_commit(wt.head).parents == (dev.head,)


class TestApply:
    def _history(self):
        wt = worktree()
        wt["a"] = 1
        wt["b"] = 1
        wt.commit()
        dev = fork(wt, "dev")
        dev["b"] = 2  # the change to pick
        dev["c"] = 3
        dev.commit()
        picked = dev.head
        dev["a"] = 99  # a later change that must not come along
        dev.commit()
        return wt, picked

    def test_cherry_pick_brings_one_commits_change(self):
        wt, picked = self._history()
        result = wt.cherry_pick(picked)
        assert result.merged and result.strategy == "apply"
        assert (wt["a"], wt["b"], wt["c"]) == (1, 2, 3)
        assert wt.repo.get_commit(wt.head).parents == (
            wt.repo.get_commit(wt.head).parents[0],
        )

    def test_revert_undoes_one_commits_change(self):
        wt = worktree()
        wt["k"] = "before"
        wt.commit()
        wt["k"] = "after"
        wt["new"] = 1
        wt.commit()
        change = wt.head
        wt["later"] = 2
        wt.commit()

        assert wt.revert(change).merged
        assert wt["k"] == "before"
        assert "new" not in wt
        assert wt["later"] == 2

    def test_apply_is_the_general_form(self):
        wt, picked = self._history()
        parent = wt.repo.get_commit(picked).parents[0]
        assert wt.apply(parent, picked).merged
        assert wt["b"] == 2

    def test_a_change_already_present_commits_nothing(self):
        wt, picked = self._history()
        wt.cherry_pick(picked)
        head = wt.head
        result = wt.cherry_pick(picked)
        assert result.strategy == "no_op"
        assert wt.head == head

    def test_conflicting_apply_raises_and_changes_nothing(self):
        wt, picked = self._history()
        wt["b"] = 7
        wt.commit()
        head = wt.head
        with pytest.raises(MergeConflict):
            wt.cherry_pick(picked)
        assert wt.head == head

    def test_apply_refuses_pending_changes_and_unknown_commits(self):
        wt, picked = self._history()
        with pytest.raises(UnknownCommitError):
            wt.cherry_pick("0" * 40)
        wt["pending"] = 1
        with pytest.raises(ValueError, match="pending changes"):
            wt.cherry_pick(picked)

    def test_apply_raises_on_a_race(self):
        """A worktree whose branch moved since its head cannot publish the
        applied change over the newer tip; nothing changes."""
        store = Memory()
        wt = worktree(store)
        wt["k"] = 1
        wt.commit()
        base = wt.head
        stale = worktree(store)  # opened at ``base``
        wt["k"] = 2
        wt.commit()
        target = wt.head
        wt["other"] = 1
        wt.commit()
        tip = wt.head

        with pytest.raises(ConcurrencyError):
            stale.apply(base, target, merge_fns={})
        assert stale.head == base
        assert wt.repo.branches["main"] == tip


class TestMergeRules:
    def test_repo_defaults_apply_to_every_worktree(self):
        repo = Repo(Memory(), merge_prefixes={"runs/": MergeChoice.OURS})
        wt = repo.worktree("main", create=True)
        wt["runs/1"] = "base"
        wt.commit()
        repo.branches.create("dev", at=wt.head)
        dev = repo.worktree("dev")
        dev["runs/1"] = "theirs"
        dev["runs/2"] = "theirs only"
        dev.commit()
        wt["runs/1"] = "ours"
        wt.commit()

        assert wt.merge(branch="dev").merged
        assert wt["runs/1"] == "ours"
        assert "runs/2" not in wt

    def test_worktree_rules_override_repo_defaults_and_calls_override_both(self):
        repo = Repo(Memory(), default_merge=lambda old, ours, theirs: "repo")
        wt = repo.worktree("main", create=True)
        wt["k"] = "base"
        wt.commit()

        def diverge():
            repo.branches.create(f"dev{diverge.n}", at=wt.head)
            dev = repo.worktree(f"dev{diverge.n}")
            dev["k"] = f"theirs{diverge.n}"
            dev.commit()
            wt["k"] = f"ours{diverge.n}"
            wt.commit()
            diverge.n += 1
            return dev.branch

        diverge.n = 0
        wt.merge(branch=diverge())
        assert wt["k"] == "repo"

        wt.set_default_merge(lambda old, ours, theirs: "worktree")
        wt.merge(branch=diverge())
        assert wt["k"] == "worktree"

        wt.merge(branch=diverge(), merge_fns={"k": lambda o, a, b: "call"})
        assert wt["k"] == "call"

    def test_merge_results_are_encoded_with_the_repo_codec(self):
        """A merge function's result is stored the way a write would be:
        under the bytes codec it stays bytes, not a pickle of bytes."""
        repo = Repo(Memory(), codec="bytes")
        wt = repo.worktree("main", create=True)
        wt["k"] = b"base"
        wt.commit()
        repo.branches.create("dev", at=wt.head)
        dev = repo.worktree("dev")
        dev["k"] = b"theirs"
        dev.commit()
        wt["k"] = b"ours"
        wt.commit()
        wt.merge(branch="dev", merge_fns={"k": lambda o, a, b: a + b"+" + b})
        assert wt["k"] == b"ours+theirs"
        assert repo.snapshot(branch="main").raw["k"] == b"ours+theirs"


class TestCodecs:
    def test_a_custom_pair(self):
        def encode(v):
            return json.dumps(v).encode()

        def decode(b):
            return json.loads(b)

        wt = worktree(codec=(encode, decode))
        wt["k"] = {"hello": "world"}
        wt.commit()
        assert wt["k"] == {"hello": "world"}
        assert wt.repo.snapshot(branch="main").raw["k"] == b'{"hello": "world"}'
        other = fork(wt, "other")
        assert other["k"] == {"hello": "world"}  # every worktree shares the codec

    def test_bytes_codec_stores_bytes_as_they_are(self):
        wt = worktree(codec="bytes")
        wt["k"] = b"\x00raw"
        wt.commit()
        assert wt.repo.snapshot(branch="main").raw["k"] == b"\x00raw"
        wt["bad"] = "not bytes"
        with pytest.raises(TypeError, match="bytes values only"):
            wt.commit()

    def test_pickle_is_the_default(self):
        wt = worktree()
        wt["k"] = [1, 2]
        wt.commit()
        assert wt.repo.snapshot(branch="main").raw["k"] == pickle.dumps([1, 2])

    def test_unknown_codec_names_are_refused(self):
        with pytest.raises(ValueError, match="unknown codec"):
            Repo(Memory(), codec="json")


def test_repr():
    wt = worktree()
    wt["k"] = 1
    assert "branch='main'" in repr(wt) and "pending=1" in repr(wt)
