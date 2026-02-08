"""Tests for the Disk KV store."""

import shutil
import tempfile

import pytest

from kvit.kv.disk import Disk


@pytest.fixture
def disk_store():
    tmpdir = tempfile.mkdtemp()
    store = Disk(tmpdir)
    yield store, tmpdir
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestDiskBasic:
    def test_set_get(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        assert store.get("k") == b"v"

    def test_get_missing(self, disk_store):
        store, _ = disk_store
        assert store.get("nope") is None

    def test_contains(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        assert "k" in store
        assert "nope" not in store

    def test_set_many_get_many(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2", c=b"3")
        result = store.get_many("a", "c", "missing")
        assert result == {"a": b"1", "c": b"3"}

    def test_type_error_on_non_bytes(self, disk_store):
        store, _ = disk_store
        with pytest.raises(TypeError, match="Expected bytes"):
            store.set("k", "not bytes")  # type: ignore

    def test_clear(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2")
        store.clear()
        assert store.get("a") is None


class TestDiskPersistence:
    def test_survives_reload(self, disk_store):
        store, tmpdir = disk_store
        store.set("k", b"persistent")
        del store
        store2 = Disk(tmpdir)
        assert store2.get("k") == b"persistent"

    def test_cas_persists(self, disk_store):
        store, tmpdir = disk_store
        store.set("k", b"old")
        assert store.cas("k", b"new", expected=b"old")
        del store
        store2 = Disk(tmpdir)
        assert store2.get("k") == b"new"


class TestDiskRemove:
    def test_remove(self, disk_store):
        store, _ = disk_store
        store.set("k", b"v")
        store.remove("k")
        assert store.get("k") is None

    def test_remove_many(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2", c=b"3")
        store.remove_many("a", "c")
        assert store.get("a") is None
        assert store.get("b") == b"2"


class TestDiskCAS:
    def test_cas_success(self, disk_store):
        store, _ = disk_store
        store.set("k", b"old")
        assert store.cas("k", b"new", expected=b"old")
        assert store.get("k") == b"new"

    def test_cas_failure(self, disk_store):
        store, _ = disk_store
        store.set("k", b"old")
        assert not store.cas("k", b"new", expected=b"wrong")
        assert store.get("k") == b"old"
