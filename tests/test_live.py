"""Tests for the Live immediate-write store."""

import pytest

from kvit import Live


class TestLiveBasic:
    def test_set_and_get(self):
        s = Live()
        s.set("k", "v")
        assert s.get("k") == "v"

    def test_get_missing(self):
        s = Live()
        assert s.get("nope") is None

    def test_get_default(self):
        s = Live()
        assert s.get("nope", "fallback") == "fallback"

    def test_get_many(self):
        s = Live()
        s.set("a", 1)
        s.set("b", 2)
        result = s.get_many("a", "b", "c")
        assert result == {"a": 1, "b": 2}

    def test_contains(self):
        s = Live()
        s.set("k", "v")
        assert "k" in s
        assert "nope" not in s

    def test_keys(self):
        s = Live()
        s.set("a", 1)
        s.set("b", 2)
        assert set(s.keys()) == {"a", "b"}


class TestLiveMutableMapping:
    def test_getitem(self):
        s = Live()
        s["k"] = "v"
        assert s["k"] == "v"

    def test_getitem_missing_raises(self):
        s = Live()
        with pytest.raises(KeyError):
            s["nope"]

    def test_setitem(self):
        s = Live()
        s["k"] = "v"
        assert s.get("k") == "v"

    def test_delitem(self):
        s = Live()
        s["k"] = "v"
        del s["k"]
        assert s.get("k") is None

    def test_delitem_missing_raises(self):
        s = Live()
        with pytest.raises(KeyError):
            del s["nope"]

    def test_iter(self):
        s = Live()
        s["a"] = 1
        s["b"] = 2
        assert set(s) == {"a", "b"}

    def test_len(self):
        s = Live()
        assert len(s) == 0
        s["a"] = 1
        s["b"] = 2
        assert len(s) == 2


class TestLiveRemove:
    def test_remove_key(self):
        s = Live()
        s.set("k", "v")
        s.remove("k")
        assert s.get("k") is None
        assert "k" not in s

    def test_remove_missing_key(self):
        s = Live()
        s.remove("nope")  # should not raise


class TestLiveImmediateWrites:
    def test_writes_are_immediately_visible(self):
        s = Live()
        s.set("k", "v1")
        assert s.get("k") == "v1"
        s.set("k", "v2")
        assert s.get("k") == "v2"


class TestLiveUnsupported:
    def test_commit_raises(self):
        s = Live()
        with pytest.raises(NotImplementedError, match="commit"):
            s.commit()

    def test_reset_raises(self):
        s = Live()
        with pytest.raises(NotImplementedError, match="reset"):
            s.reset()

    def test_create_branch_raises(self):
        s = Live()
        with pytest.raises(NotImplementedError, match="branching"):
            s.create_branch("dev")

    def test_checkout_raises(self):
        s = Live()
        with pytest.raises(NotImplementedError, match="checkout"):
            s.checkout("abc123")

    def test_list_branches_raises(self):
        s = Live()
        with pytest.raises(NotImplementedError, match="branching"):
            s.list_branches()
