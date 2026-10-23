"""Tests for the Disk KV store."""

import shutil
import tempfile

import pytest

from kvgit.kv.disk import Disk


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


class TestDiskBulkCallForms:
    """Disk backend supports both variadic and container call forms."""

    def test_set_many_mapping_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2"})
        assert store.get("a") == b"1"
        assert store.get("b") == b"2"

    def test_set_many_kwargs_form(self, disk_store):
        store, _ = disk_store
        store.set_many(a=b"1", b=b"2")
        assert store.get("a") == b"1"
        assert store.get("b") == b"2"

    def test_get_many_iterable_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2"})
        assert store.get_many(["a", "b"]) == {"a": b"1", "b": b"2"}

    def test_remove_many_iterable_form(self, disk_store):
        store, _ = disk_store
        store.set_many({"a": b"1", "b": b"2", "c": b"3"})
        store.remove_many(["a", "c"])
        assert store.get("a") is None
        assert store.get("b") == b"2"


class TestDiskSizeLimit:
    """The default ``Disk()`` constructor must not silently cap storage.

    Regression: a previous version defaulted to a 1 GiB cap, and the
    factory function passed size_limit=0 (which diskcache interprets
    as "0 bytes allowed", evicting everything). The default is now
    effectively unbounded; explicit caps must be opted into.
    """

    def test_default_size_limit_does_not_evict(self):
        with tempfile.TemporaryDirectory() as d:
            store = Disk(d)
            store.set("k", b"v" * 10000)
            assert store.get("k") == b"v" * 10000

    def test_explicit_none_means_unbounded(self):
        with tempfile.TemporaryDirectory() as d:
            store = Disk(d, size_limit=None)
            store.set("k", b"v")
            assert store.get("k") == b"v"

    def test_explicit_cap_still_works(self):
        # Pass a cap large enough to fit the test data; verify it
        # doesn't evict our small write.
        with tempfile.TemporaryDirectory() as d:
            store = Disk(d, size_limit=10 * 1024 * 1024)  # 10 MiB
            store.set("k", b"v")
            assert store.get("k") == b"v"
