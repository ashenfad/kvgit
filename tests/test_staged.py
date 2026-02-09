"""Tests for the Staged buffered-write layer."""

import pytest

from kvit import MergeResult, Staged, Versioned
from kvit.kv.memory import Memory


class TestStagedBasic:
    def test_set_and_get(self):
        s = Staged(Versioned())
        s.set("k", "v")
        assert s.get("k") == "v"

    def test_get_missing(self):
        s = Staged(Versioned())
        assert s.get("nope") is None

    def test_get_default(self):
        s = Staged(Versioned())
        assert s.get("nope", "fallback") == "fallback"

    def test_get_many(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.set("b", 2)
        result = s.get_many("a", "b", "c")
        assert result == {"a": 1, "b": 2}

    def test_contains(self):
        s = Staged(Versioned())
        s.set("k", "v")
        assert "k" in s
        assert "nope" not in s

    def test_keys_includes_staged(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.commit()
        s.set("b", 2)
        assert set(s.keys()) == {"a", "b"}


class TestStagedMutableMapping:
    def test_getitem(self):
        s = Staged(Versioned())
        s["k"] = "v"
        assert s["k"] == "v"

    def test_getitem_missing_raises(self):
        s = Staged(Versioned())
        with pytest.raises(KeyError):
            s["nope"]

    def test_setitem(self):
        s = Staged(Versioned())
        s["k"] = "v"
        assert s.get("k") == "v"

    def test_delitem(self):
        s = Staged(Versioned())
        s["k"] = "v"
        del s["k"]
        assert s.get("k") is None

    def test_delitem_missing_raises(self):
        s = Staged(Versioned())
        with pytest.raises(KeyError):
            del s["nope"]

    def test_iter(self):
        s = Staged(Versioned())
        s["a"] = 1
        s["b"] = 2
        assert set(s) == {"a", "b"}

    def test_len(self):
        s = Staged(Versioned())
        assert len(s) == 0
        s["a"] = 1
        s["b"] = 2
        assert len(s) == 2

    def test_len_with_committed(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.set("b", 2)
        s.commit()
        s["c"] = 3
        assert len(s) == 3


class TestStagedRemove:
    def test_remove_shadows_committed(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.set("b", 2)
        s.commit()
        s.remove("a")
        assert s.get("a") is None
        assert s.get("b") is not None
        assert "a" not in s

    def test_remove_staged_key(self):
        s = Staged(Versioned())
        s.set("k", "v")
        s.remove("k")
        assert s.get("k") is None

    def test_keys_excludes_removed(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.set("b", 2)
        s.commit()
        s.remove("a")
        assert set(s.keys()) == {"b"}

    def test_set_after_remove(self):
        s = Staged(Versioned())
        s.set("k", "v1")
        s.remove("k")
        s.set("k", "v2")
        assert s.get("k") == "v2"


class TestStagedCommit:
    def test_commit_flushes_to_versioned(self):
        store = Memory()
        v = Versioned(store)
        s = Staged(v)
        s.set("a", 1)
        s.set("b", 2)
        result = s.commit()
        assert isinstance(result, MergeResult)
        assert result.merged

        # Verify persisted (read back through another Staged)
        s2 = Staged(Versioned(store))
        assert s2.get("a") == 1
        assert s2.get("b") == 2

    def test_commit_clears_staging(self):
        s = Staged(Versioned())
        s.set("a", 1)
        assert s.has_changes
        s.commit()
        assert not s.has_changes

    def test_commit_with_removals(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.set("b", 2)
        s.set("c", 3)
        s.commit()
        s.remove("a")
        s.set("d", 4)
        result = s.commit()
        assert result.merged
        assert s.get("a") is None
        assert s.get("b") == 2
        assert s.get("d") == 4

    def test_no_op_commit(self):
        s = Staged(Versioned())
        result = s.commit()
        assert result.strategy == "no_op"

    def test_commit_with_info(self):
        s = Staged(Versioned())
        s.set("k", "v")
        result = s.commit(info={"author": "test"})
        assert result.merged
        assert s.versioned.commit_info() == {"author": "test"}


class TestStagedReset:
    def test_reset_clears_staging(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.remove("b")
        s.reset()
        assert not s.has_changes
        assert s.get("a") is None

    def test_reset_does_not_affect_committed(self):
        s = Staged(Versioned())
        s.set("a", 1)
        s.commit()
        s.set("b", 2)
        s.reset()
        assert s.get("a") == 1
        assert s.get("b") is None


class TestStagedHasChanges:
    def test_empty_has_no_changes(self):
        s = Staged(Versioned())
        assert not s.has_changes

    def test_set_marks_has_changes(self):
        s = Staged(Versioned())
        s.set("k", "v")
        assert s.has_changes

    def test_remove_marks_has_changes(self):
        s = Staged(Versioned())
        s.remove("k")
        assert s.has_changes


class TestStagedProperties:
    def test_versioned_property(self):
        v = Versioned()
        s = Staged(v)
        assert s.versioned is v

    def test_current_commit(self):
        v = Versioned()
        s = Staged(v)
        assert s.current_commit == v.current_commit

    def test_base_commit(self):
        v = Versioned()
        s = Staged(v)
        assert s.base_commit == v.base_commit

    def test_last_merge_result(self):
        s = Staged(Versioned())
        s.set("k", "v")
        s.commit()
        assert s.last_merge_result is not None
        assert s.last_merge_result.merged


class TestStagedBranching:
    def test_create_branch_returns_staged(self):
        s = Staged(Versioned())
        s.set("k", "v")
        s.commit()
        worker = s.create_branch("worker")
        assert isinstance(worker, Staged)
        assert worker.get("k") == "v"

    def test_create_branch_independent_commits(self):
        s = Staged(Versioned())
        s.set("base", 1)
        s.commit()

        worker = s.create_branch("worker")
        worker.set("from_worker", 2)
        worker.commit()

        s.refresh()
        assert s.get("from_worker") is None
        assert worker.get("from_worker") == 2

    def test_checkout_returns_staged(self):
        s = Staged(Versioned())
        s.set("k", "v1")
        s.commit()
        old_hash = s.current_commit

        s.set("k", "v2")
        s.commit()

        old = s.checkout(old_hash)
        assert isinstance(old, Staged)
        assert old.get("k") == "v1"

    def test_checkout_invalid_returns_none(self):
        s = Staged(Versioned())
        assert s.checkout("nonexistent") is None

    def test_checkout_with_branch(self):
        s = Staged(Versioned())
        s.set("k", "v")
        s.commit()
        old = s.checkout(s.current_commit, branch="review")
        assert isinstance(old, Staged)
        assert old.versioned._branch == "review"

    def test_list_branches(self):
        s = Staged(Versioned())
        s.create_branch("dev")
        assert "dev" in s.list_branches()
        assert "main" in s.list_branches()


class TestStagedRefresh:
    def test_refresh_reloads_from_head(self):
        store = Memory()
        v1 = Versioned(store)
        s = Staged(v1)

        # Another Staged writer advances HEAD
        s2 = Staged(Versioned(store))
        s2.set("from_other", "data")
        s2.commit()

        # Staged doesn't see it yet
        assert s.get("from_other") is None

        # After refresh, it does
        s.refresh()
        assert s.get("from_other") == "data"

    def test_refresh_clears_staging(self):
        s = Staged(Versioned())
        s.set("k", "v")
        s.refresh()
        assert not s.has_changes


class TestStagedEncoder:
    def test_custom_encoder_decoder(self):
        import json

        def encode(v):
            return json.dumps(v).encode()

        def decode(b):
            return json.loads(b)

        s = Staged(Versioned(), encoder=encode, decoder=decode)
        s.set("k", {"hello": "world"})
        s.commit()
        assert s.get("k") == {"hello": "world"}

    def test_branch_propagates_encoder(self):
        import json

        def encode(v):
            return json.dumps(v).encode()

        def decode(b):
            return json.loads(b)

        s = Staged(Versioned(), encoder=encode, decoder=decode)
        s.set("k", "v")
        s.commit()
        worker = s.create_branch("worker")
        assert worker._encoder is encode
        assert worker._decoder is decode
