"""Tests for cross-branch merge: Worktree.merge over raw bytes and decoded values."""

import pytest
from support import fork, worktree

from kvgit import MergeConflict, Repo, UnknownCommitError
from kvgit.kv.memory import Memory
from kvgit.merges import CantMark, text


def _commit(wt, updates):
    """Stage ``updates`` on ``wt``, then commit them."""
    wt.update(updates)
    return wt.commit()


def _branched_bytes():
    """Main + worker worktrees of one repository storing raw bytes."""
    main = worktree(codec="bytes")
    _commit(main, {"doc": b"a\nb\nc\nd\n"})
    worker = fork(main, "worker")
    return main, worker


def _branched():
    """Main + worker worktrees sharing one repo, forked after one commit."""
    main = worktree()
    main["doc"] = b"a\nb\nc\nd\n"
    main.commit()
    worker = fork(main, "worker")
    return main, worker


class TestMergeOverBytes:
    def test_fast_forward_takes_theirs(self):
        main, worker = _branched_bytes()
        _commit(worker, {"k": b"2"})

        result = main.merge(commit=worker.head)
        assert result.merged
        assert main.get("k") == b"2"

    def test_disjoint_changes_union(self):
        main, worker = _branched_bytes()
        _commit(main, {"a": b"1"})
        _commit(worker, {"b": b"2"})

        result = main.merge(commit=worker.head)
        assert result.merged
        assert main.get("a") == b"1"
        assert main.get("b") == b"2"

    def test_overlap_without_fn_conflicts(self):
        main, worker = _branched_bytes()
        _commit(main, {"doc": b"a\nB\nc\nd\n"})
        _commit(worker, {"doc": b"a\nb\nc\nD\n"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge(commit=worker.head)
        assert "doc" in exc_info.value.conflicting_keys

    def test_overlap_with_text_fn_marks(self):
        main, worker = _branched_bytes()
        _commit(main, {"doc": b"a\nB\nc\nd\n"})
        _commit(worker, {"doc": b"a\nb\nc\nD\n"})

        result = main.merge(commit=worker.head, default_merge=text)
        assert result.merged
        assert main.get("doc") == b"a\nB\nc\nD\n"

    def test_post_check_refusal_raises(self):
        main, worker = _branched_bytes()
        _commit(main, {"doc": b"a\nB\nc\nd\n"})
        _commit(worker, {"doc": b"a\nY\nc\nd\n"})

        before = main.head
        with pytest.raises(MergeConflict) as exc_info:
            main.merge(
                commit=worker.head,
                default_merge=text,
                post_check=lambda key, value: b"<<<<<<<" not in value,
            )
        assert "doc" in exc_info.value.conflicting_keys
        assert main.head == before

    def test_post_check_refusal_abandons(self):
        main, worker = _branched_bytes()
        _commit(main, {"doc": b"a\nB\nc\nd\n"})
        _commit(worker, {"doc": b"a\nY\nc\nd\n"})

        before = main.head
        result = main.merge(
            commit=worker.head,
            default_merge=text,
            post_check=lambda key, value: False,
            on_conflict="abandon",
        )
        assert not result.merged
        assert result.commit is None
        assert main.head == before

    def test_fresh_branches_share_empty_genesis(self):
        """Two fresh branches have no shared commits — but both descend
        from the deterministic empty genesis, so they still merge: same
        key added on both sides is an ordinary added/added conflict,
        not an unrelated-histories refusal."""
        repo = Repo(Memory(), codec="bytes")
        main = repo.worktree("aaa", create=True)
        _commit(main, {"k": b"1"})
        other = repo.worktree("bbb", create=True)
        _commit(other, {"k": b"2"})

        before = main.head
        with pytest.raises(MergeConflict) as exc_info:
            main.merge(commit=other.head)
        assert "k" in exc_info.value.conflicting_keys
        assert main.head == before

    def test_binary_conflict_carries_cantmark(self):
        main, worker = _branched_bytes()
        _commit(main, {"doc": b"a\x00b"})
        _commit(worker, {"doc": b"a\nb\nc\nD\n"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge(commit=worker.head, default_merge=text)
        assert isinstance(exc_info.value.merge_errors["doc"], CantMark)


class TestEngineChecksItsCommits:
    """A commit missing from the store reads as an empty tree, so the
    engine refuses one up front rather than applying a change "to empty"
    (removing every key) or reporting no common ancestor."""

    def test_unknown_commits_are_refused_and_nothing_is_published(self):
        store = Memory()
        engine = worktree(store, codec="bytes")
        _commit(engine, {"k": b"1"})
        known = engine.head
        missing = "f" * 40
        before = dict(store.items())

        for attempt in (
            lambda: engine.apply(known, missing),
            lambda: engine.apply(missing, known),
            lambda: engine.merge(commit=missing),
            lambda: engine.apply(known, missing, on_conflict="abandon"),
            lambda: engine.merge(commit=missing, on_conflict="abandon"),
        ):
            with pytest.raises(UnknownCommitError, match=missing):
                attempt()
        assert engine.repo.branches["main"] == known
        assert engine.get("k") == b"1"
        assert dict(store.items()) == before


class TestWorktreeMerge:
    def test_dirty_buffer_refuses(self):
        main, worker = _branched()
        worker["b"] = b"2"
        worker.commit()
        main["staged"] = b"pending"

        with pytest.raises(ValueError, match="pending changes"):
            main.merge(commit=worker.head)
        # Refusal changes nothing: the pending write is still pending.
        assert main["staged"] == b"pending"

    def test_end_to_end_cross_branch(self):
        main, worker = _branched()
        main["a"] = b"1"
        main.commit()
        worker["b"] = b"2"
        worker.commit()

        result = main.merge(commit=worker.head, default_merge=text)
        assert result.merged
        assert main["a"] == b"1"
        assert main["b"] == b"2"

    def test_cache_invalidated(self):
        main, worker = _branched()
        assert main["doc"] == b"a\nb\nc\nd\n"  # prime the read cache
        worker["doc"] = b"changed\n"
        worker.commit()

        main.merge(commit=worker.head, default_merge=text)
        assert main["doc"] == b"changed\n"

    def test_decoded_fn_wrapping(self):
        # `n` is added independently on both sides after the fork, so the
        # LCA has no `n`: add(None, 30, 20) == 50. Exercises the worktree's
        # decoded-value wrapping (not raw bytes) end to end.
        main, worker = _branched()
        main["n"] = 10
        main.commit()
        worker["n"] = 20
        worker.commit()

        main["n"] = 30
        main.commit()

        def add(old, ours, theirs):
            base = old if old is not None else 0
            return ours + theirs - base

        result = main.merge(commit=worker.head, merge_fns={"n": add})
        assert result.merged
        assert main["n"] == 50


class TestReviewRegression35:
    """Regression tests for PR #35 review findings (all reproduced)."""

    def test_merge_parents_put_ours_first(self):
        """Linear history must stay on the merging branch: the merge
        commit's first parent is our pre-merge head (git convention),
        so the branch's log doesn't divert onto the source branch."""
        main, worker = _branched_bytes()
        _commit(main, {"a": b"1"})
        _commit(worker, {"b": b"2"})
        our_head = main.head

        main.merge(commit=worker.head)
        assert main.repo.get_commit(main.head).parents == (our_head, worker.head)
        assert our_head in [c.hash for c in main.repo.log(branch=main.branch)]

    def test_worktree_uses_registered_fns(self):
        """set_default_merge applies to merge() without per-call args."""
        main, worker = _branched()
        main["doc"] = b"a\nB\nc\nd\n"
        main.commit()
        worker["doc"] = b"a\nY\nc\nd\n"
        worker.commit()

        main.set_default_merge(lambda old, ours, theirs: ours)
        result = main.merge(commit=worker.head)
        assert result.merged
        assert main["doc"] == b"a\nB\nc\nd\n"

    def test_bogus_on_conflict_rejected_before_mutating(self):
        main, worker = _branched_bytes()
        _commit(worker, {"b": b"2"})
        before = main.head

        with pytest.raises(ValueError, match="on_conflict"):
            main.merge(commit=worker.head, on_conflict="bogus")
        assert main.head == before


class TestBytesMerge:
    def test_raw_bytes_merge(self):
        """A merge function sees raw bytes on a bytes-codec repository."""
        v1 = worktree(codec="bytes")
        _commit(v1, {"k": b"base"})
        v2 = fork(v1, "dev")
        _commit(v2, {"k": b"dev"})

        result = v1.merge(commit=v2.head, default_merge=text)
        assert result.merged
        assert v1.get("k") == b"dev"


class TestMergeBase:
    def test_same_commit_is_its_own_base(self):
        main, _ = _branched_bytes()
        assert main.repo.merge_base(main.head, main.head) == (main.head)

    def test_diverged_branches_resolve_to_fork_point(self):
        main, worker = _branched_bytes()
        fork_point = main.head
        _commit(main, {"a": b"1"})
        _commit(worker, {"b": b"2"})

        assert main.repo.merge_base(main.head, worker.head) == fork_point
        assert worker.repo.merge_base(worker.head, main.head) == fork_point

    def test_linear_history_returns_ancestor(self):
        main, _ = _branched_bytes()
        base = main.head
        _commit(main, {"a": b"1"})

        assert main.repo.merge_base(base, main.head) == base

    def test_repo_merge_base_matches_the_engine(self):
        main, worker = _branched()
        fork_point = main.head
        main["a"] = "1"
        main.commit()
        worker["b"] = "2"
        worker.commit()

        assert main.repo.merge_base(main.head, worker.head) == fork_point
