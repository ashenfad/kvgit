"""Tests for the vkv.store() factory function."""

import pytest

from vkv import Live, Staged, store


class TestStoreFactory:
    def test_default_returns_staged(self):
        s = store()
        assert isinstance(s, Staged)

    def test_versioned_type(self):
        s = store(type="versioned")
        assert isinstance(s, Staged)

    def test_live_type(self):
        s = store(type="live")
        assert isinstance(s, Live)

    def test_invalid_type(self):
        with pytest.raises(ValueError, match="Unknown type"):
            store(type="bogus")

    def test_invalid_storage(self):
        with pytest.raises(ValueError, match="Unknown storage"):
            store(storage="redis")

    def test_disk_requires_path(self):
        with pytest.raises(ValueError, match="path is required"):
            store(storage="disk")

    def test_gc_params_only_for_versioned(self):
        with pytest.raises(ValueError, match="GC parameters"):
            store(type="live", high_water_bytes=1000)

    def test_live_commit_raises(self):
        s = store(type="live")
        s.set("k", b"v")
        with pytest.raises(NotImplementedError):
            s.commit()

    def test_gc_versioned(self):
        from vkv import GCVersioned

        s = store(high_water_bytes=5000)
        assert isinstance(s, Staged)
        assert isinstance(s.versioned, GCVersioned)

    def test_branch_parameter(self):
        s = store(branch="dev")
        assert isinstance(s, Staged)
        assert s.versioned._branch == "dev"


class TestStoreFactoryRoundTrip:
    def test_versioned_set_commit_get(self):
        s = store()
        s.set("greeting", b"hello")
        result = s.commit()
        assert result.merged
        assert s.get("greeting") == b"hello"

    def test_live_set_get(self):
        s = store(type="live")
        s.set("greeting", b"hello")
        assert s.get("greeting") == b"hello"

    def test_versioned_create_branch(self):
        s = store()
        s.set("k", b"v")
        s.commit()
        worker = s.create_branch("worker")
        assert isinstance(worker, Staged)
        assert worker.get("k") == b"v"
