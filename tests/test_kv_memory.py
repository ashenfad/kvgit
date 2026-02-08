"""Tests for the Memory KV store."""

import threading

from kvit.kv.memory import Memory


class TestMemoryBasic:
    def test_set_get(self):
        m = Memory()
        m.set("k", b"v")
        assert m.get("k") == b"v"

    def test_get_missing(self):
        m = Memory()
        assert m.get("nope") is None

    def test_contains(self):
        m = Memory()
        m.set("k", b"v")
        assert "k" in m
        assert "nope" not in m

    def test_keys(self):
        m = Memory()
        m.set("a", b"1")
        m.set("b", b"2")
        assert set(m.keys()) == {"a", "b"}

    def test_items(self):
        m = Memory()
        m.set("a", b"1")
        m.set("b", b"2")
        assert dict(m.items()) == {"a": b"1", "b": b"2"}

    def test_set_many_get_many(self):
        m = Memory()
        m.set_many(a=b"1", b=b"2", c=b"3")
        result = m.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_overwrite(self):
        m = Memory()
        m.set("k", b"old")
        m.set("k", b"new")
        assert m.get("k") == b"new"

    def test_clear(self):
        m = Memory()
        m.set_many(a=b"1", b=b"2")
        m.clear()
        assert m.get("a") is None
        assert list(m.keys()) == []


class TestMemoryRemove:
    def test_remove(self):
        m = Memory()
        m.set("k", b"v")
        m.remove("k")
        assert m.get("k") is None

    def test_remove_missing(self):
        m = Memory()
        m.remove("nope")  # should not raise

    def test_remove_many(self):
        m = Memory()
        m.set_many(a=b"1", b=b"2", c=b"3")
        m.remove_many("a", "c", "missing")
        assert m.get("a") is None
        assert m.get("b") == b"2"
        assert m.get("c") is None


class TestMemoryCAS:
    def test_cas_success(self):
        m = Memory()
        m.set("k", b"old")
        assert m.cas("k", b"new", expected=b"old")
        assert m.get("k") == b"new"

    def test_cas_failure(self):
        m = Memory()
        m.set("k", b"old")
        assert not m.cas("k", b"new", expected=b"wrong")
        assert m.get("k") == b"old"

    def test_cas_create(self):
        m = Memory()
        assert m.cas("k", b"val", expected=None)
        assert m.get("k") == b"val"

    def test_cas_create_fails_if_exists(self):
        m = Memory()
        m.set("k", b"existing")
        assert not m.cas("k", b"new", expected=None)
        assert m.get("k") == b"existing"

    def test_cas_thread_safety(self):
        m = Memory()
        m.set("counter", b"0")
        wins = []

        def try_cas(thread_id):
            if m.cas("counter", f"thread-{thread_id}".encode(), expected=b"0"):
                wins.append(thread_id)

        threads = [threading.Thread(target=try_cas, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(wins) == 1
