"""Tests for the kvgit.store() factory function."""

import os
import tempfile

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


class TestDiskFactory:
    """Round-trip tests against the disk-backed factory.

    Regression: prior to this, store(kind='disk', path=...) passed
    size_limit=0 to the diskcache backend, which is "0 bytes allowed"
    rather than "no limit", so every write was evicted immediately
    and the store appeared empty after commit.
    """

    def test_disk_factory_round_trip_within_session(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["greeting"] = "hello"
            s["count"] = 42
            result = s.commit()
            assert result.merged
            assert s.get("greeting") == "hello"
            assert s.get("count") == 42

    def test_disk_factory_persists_across_reopens(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")

            s1 = store(kind="disk", path=p)
            s1["greeting"] = "hello"
            s1.commit()

            # Re-open the same path; data must still be there.
            s2 = store(kind="disk", path=p)
            assert s2.get("greeting") == "hello"

    def test_disk_factory_branches_persist(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")

            s1 = store(kind="disk", path=p)
            s1["base"] = "ok"
            s1.commit()
            worker = s1.create_branch("worker")
            worker["work"] = "done"
            worker.commit()

            # Re-open and switch to the branch
            s2 = store(kind="disk", path=p, branch="worker")
            assert s2.get("base") == "ok"
            assert s2.get("work") == "done"
