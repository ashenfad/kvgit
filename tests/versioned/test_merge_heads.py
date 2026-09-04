"""Tests for cross-branch merge: VersionedKV.merge_heads + Staged.merge."""

import pytest

from kvgit import MergeConflict, VersionedKV as Versioned
from kvgit.kv.memory import Memory
from kvgit.merges import CantMark, text
from kvgit.store import store


def _branched_versioned():
    """Main + worker VersionedKV pair sharing one store (raw bytes)."""
    main = Versioned(Memory())
    main.commit({"doc": b"a\nb\nc\nd\n"})
    worker = main.create_branch("worker")
    return main, worker


def _branched():
    """Main + worker Staged pair sharing one memory store, diverged once."""
    main = store(kind="memory", branch="main")
    main["doc"] = b"a\nb\nc\nd\n"
    main.commit()
    worker = main.create_branch("worker")
    return main, worker


class TestMergeHeads:
    def test_fast_forward_takes_theirs(self):
        main, worker = _branched_versioned()
        worker.commit({"k": b"2"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("k") == b"2"

    def test_disjoint_changes_union(self):
        main, worker = _branched_versioned()
        main.commit({"a": b"1"})
        worker.commit({"b": b"2"})

        result = main.merge_heads(worker.current_commit)
        assert result.merged
        assert main.get("a") == b"1"
        assert main.get("b") == b"2"

    def test_overlap_without_fn_conflicts(self):
        main, worker = _branched_versioned()
        main.commit({"doc": b"a\nB\nc\nd\n"})
        worker.commit({"doc": b"a\nb\nc\nD\n"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit)
        assert "doc" in exc_info.value.conflicting_keys

    def test_overlap_with_text_fn_marks(self):
        main, worker = _branched_versioned()
        main.commit({"doc": b"a\nB\nc\nd\n"})
        worker.commit({"doc": b"a\nb\nc\nD\n"})

        result = main.merge_heads(worker.current_commit, default_merge=text)
        assert result.merged
        assert main.get("doc") == b"a\nB\nc\nD\n"

    def test_post_check_refusal_raises(self):
        main, worker = _branched_versioned()
        main.commit({"doc": b"a\nB\nc\nd\n"})
        worker.commit({"doc": b"a\nY\nc\nd\n"})

        before = main.current_commit
        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(
                worker.current_commit,
                default_merge=text,
                post_check=lambda key, value: b"<<<<<<<" not in value,
            )
        assert "doc" in exc_info.value.conflicting_keys
        assert main.current_commit == before

    def test_post_check_refusal_abandons(self):
        main, worker = _branched_versioned()
        main.commit({"doc": b"a\nB\nc\nd\n"})
        worker.commit({"doc": b"a\nY\nc\nd\n"})

        before = main.current_commit
        result = main.merge_heads(
            worker.current_commit,
            default_merge=text,
            post_check=lambda key, value: False,
            on_conflict="abandon",
        )
        assert not result.merged
        assert result.commit is None
        assert main.current_commit == before

    def test_fresh_branches_share_empty_genesis(self):
        """Two fresh branches have no shared commits — but both descend
        from the deterministic empty genesis, so they still merge: same
        key added on both sides is an ordinary added/added conflict,
        not an unrelated-histories refusal."""
        mem = Memory()
        main = Versioned(mem, branch="aaa")
        main.commit({"k": b"1"})
        other = Versioned(mem, branch="bbb")
        other.commit({"k": b"2"})

        before = main.current_commit
        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(other.current_commit)
        assert "k" in exc_info.value.conflicting_keys
        assert main.current_commit == before

    def test_binary_conflict_carries_cantmark(self):
        main, worker = _branched_versioned()
        main.commit({"doc": b"a\x00b"})
        worker.commit({"doc": b"a\nb\nc\nD\n"})

        with pytest.raises(MergeConflict) as exc_info:
            main.merge_heads(worker.current_commit, default_merge=text)
        assert isinstance(exc_info.value.merge_errors["doc"], CantMark)


class TestStagedMerge:
    def test_dirty_buffer_refuses(self):
        main, worker = _branched()
        worker["b"] = b"2"
        worker.commit()
        main["staged"] = b"pending"

        with pytest.raises(ValueError, match="staged changes"):
            main.merge(worker.versioned.current_commit)
        # Refusal changes nothing: the staged write is still staged.
        assert main["staged"] == b"pending"

    def test_end_to_end_cross_branch(self):
        main, worker = _branched()
        main["a"] = b"1"
        main.commit()
        worker["b"] = b"2"
        worker.commit()

        result = main.merge(worker.versioned.current_commit, default_merge=text)
        assert result.merged
        assert main["a"] == b"1"
        assert main["b"] == b"2"

    def test_cache_invalidated(self):
        main, worker = _branched()
        assert main["doc"] == b"a\nb\nc\nd\n"  # prime the read cache
        worker["doc"] = b"changed\n"
        worker.commit()

        main.merge(worker.versioned.current_commit, default_merge=text)
        assert main["doc"] == b"changed\n"

    def test_decoded_fn_wrapping(self):
        # `n` is added independently on both sides after the fork, so the
        # LCA has no `n`: add(None, 30, 20) == 50. Exercises Staged's
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

        result = main.merge(worker.versioned.current_commit, merge_fns={"n": add})
        assert result.merged
        assert main["n"] == 50


class TestReviewRegression35:
    """Regression tests for PR #35 review findings (all reproduced)."""

    def test_merge_parents_put_ours_first(self):
        """Linear history must stay on the merging branch: the merge
        commit's first parent is our pre-merge head (git convention),
        so history() doesn't divert onto the source branch."""
        main, worker = _branched_versioned()
        main.commit({"a": b"1"})
        worker.commit({"b": b"2"})
        our_head = main.current_commit

        main.merge_heads(worker.current_commit)
        assert main.parents() == (our_head, worker.current_commit)
        assert our_head in list(main.history())

    def test_staged_uses_registered_fns(self):
        """set_default_merge applies to merge() without per-call args."""
        main, worker = _branched()
        main["doc"] = b"a\nB\nc\nd\n"
        main.commit()
        worker["doc"] = b"a\nY\nc\nd\n"
        worker.commit()

        main.set_default_merge(lambda old, ours, theirs: ours)
        result = main.merge(worker.versioned.current_commit)
        assert result.merged
        assert main["doc"] == b"a\nB\nc\nd\n"

    def test_bogus_on_conflict_rejected_before_mutating(self):
        main, worker = _branched_versioned()
        worker.commit({"b": b"2"})
        before = main.current_commit

        with pytest.raises(ValueError, match="on_conflict"):
            main.merge_heads(worker.current_commit, on_conflict="bogus")
        assert main.current_commit == before


class TestVersionedMergeParity:
    def test_versioned_level_merge(self):
        """The engine verb works directly on VersionedKV too."""
        v1 = Versioned(Memory())
        v1.commit({"k": b"base"})
        v2 = v1.create_branch("dev")
        v2.commit({"k": b"dev"})

        result = v1.merge_heads(v2.current_commit, default_merge=text)
        assert result.merged
        assert v1.get("k") == b"dev"
