"""Tests for the kvgit.store() factory function."""

import pytest

from kvgit import Staged, store


class TestStoreFactory:
    def test_default_returns_staged(self):
        s = store()
        assert isinstance(s, Staged)

    def test_invalid_kind(self):
        with pytest.raises(ValueError, match="Unknown kind"):
            store(kind="redis")

    def test_disk_requires_path(self):
        with pytest.raises(ValueError, match="path is required"):
            store(kind="disk")

    def test_gc_versioned(self):
        from kvgit.gc import GCVersioned

        s = store(high_water_bytes=5000)
        assert isinstance(s, Staged)
        assert isinstance(s.versioned, GCVersioned)

    def test_branch_parameter(self):
        s = store(branch="dev")
        assert isinstance(s, Staged)
        assert s.versioned._branch == "dev"


class TestStoreFactoryRoundTrip:
    def test_set_commit_get(self):
        s = store()
        s["greeting"] = "hello"
        result = s.commit()
        assert result.merged
        assert s.get("greeting") == "hello"

    def test_create_branch(self):
        s = store()
        s["k"] = "v"
        s.commit()
        worker = s.create_branch("worker")
        assert isinstance(worker, Staged)
        assert worker.get("k") == "v"

    def test_mutable_mapping(self):
        s = store()
        s["k"] = {"hello": "world"}
        s.commit()
        assert s["k"] == {"hello": "world"}
