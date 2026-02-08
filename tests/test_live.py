"""Tests for the Live immediate-write store."""

import pytest

from kvit import Live


class TestLiveBasic:
    def test_set_and_get(self):
        s = Live()
        s.set("k", b"v")
        assert s.get("k") == b"v"

    def test_get_missing(self):
        s = Live()
        assert s.get("nope") is None

    def test_get_many(self):
        s = Live()
        s.set("a", b"1")
        s.set("b", b"2")
        result = s.get_many("a", "b", "c")
        assert result == {"a": b"1", "b": b"2"}

    def test_contains(self):
        s = Live()
        s.set("k", b"v")
        assert "k" in s
        assert "nope" not in s

    def test_keys(self):
        s = Live()
        s.set("a", b"1")
        s.set("b", b"2")
        assert set(s.keys()) == {"a", "b"}


class TestLiveRemove:
    def test_remove_key(self):
        s = Live()
        s.set("k", b"v")
        s.remove("k")
        assert s.get("k") is None
        assert "k" not in s

    def test_remove_missing_key(self):
        s = Live()
        s.remove("nope")  # should not raise


class TestLiveImmediateWrites:
    def test_writes_are_immediately_visible(self):
        s = Live()
        s.set("k", b"v1")
        assert s.get("k") == b"v1"
        s.set("k", b"v2")
        assert s.get("k") == b"v2"


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
