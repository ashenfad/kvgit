"""Tests for the kvgit.store() one-liner."""

import os
import tempfile

import pytest

from kvgit import Repo, Worktree, store
from kvgit.kv.memory import Memory


class TestStoreFactory:
    def test_default_returns_a_worktree_on_main(self):
        wt = store()
        assert isinstance(wt, Worktree)
        assert isinstance(wt.repo, Repo)
        assert isinstance(wt.repo.store, Memory)
        assert wt.branch == "main"

    def test_invalid_kind(self):
        with pytest.raises(ValueError, match="Unknown kind"):
            store(kind="redis")  # type: ignore[arg-type]

    def test_disk_requires_path(self):
        with pytest.raises(ValueError, match="path is required"):
            store(kind="disk")

    def test_branch_parameter_creates_the_branch(self):
        wt = store(branch="dev")
        assert wt.branch == "dev"
        assert wt.repo.branches() == ["dev"]

    def test_opening_an_existing_branch_does_not_recreate_it(self):
        with tempfile.TemporaryDirectory() as path:
            first = store(kind="disk", path=path)
            first["k"] = "v"
            first.commit()
            head = first.head
            first.repo.close()

            again = store(kind="disk", path=path)
            assert again.head == head
            assert again["k"] == "v"
            again.repo.close()

    def test_codec_is_passed_to_the_repo(self):
        wt = store(codec="bytes")
        wt["k"] = b"raw"
        wt.commit()
        assert wt.repo.snapshot(branch="main").raw["k"] == b"raw"
        with pytest.raises(TypeError, match="bytes values only"):
            wt["n"] = 1
            wt.commit()


class TestStoreFactoryRoundTrip:
    def test_set_commit_get(self):
        wt = store()
        wt["greeting"] = "hello"
        result = wt.commit()
        assert result.merged
        assert wt.get("greeting") == "hello"

    def test_mutable_mapping(self):
        wt = store()
        wt["k"] = {"hello": "world"}
        wt.commit()
        assert wt["k"] == {"hello": "world"}


class TestDiskFactory:
    """Round trips against the disk-backed factory.

    A regression once passed size_limit=0 to the diskcache backend, which
    means "0 bytes allowed" rather than "no limit", so every write was
    evicted immediately and the store appeared empty after commit.
    """

    def test_disk_factory_round_trip_within_session(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            wt = store(kind="disk", path=p)
            wt["greeting"] = "hello"
            wt["count"] = 42
            assert wt.commit().merged
            assert wt.get("greeting") == "hello"
            assert wt.get("count") == 42
            wt.repo.close()

    def test_disk_factory_persists_across_reopens(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            first = store(kind="disk", path=p)
            first["greeting"] = "hello"
            first.commit()
            first.repo.close()

            again = store(kind="disk", path=p)
            assert again.get("greeting") == "hello"
            again.repo.close()

    def test_disk_factory_branches_persist(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            first = store(kind="disk", path=p)
            first["base"] = "ok"
            first.commit()
            first.repo.create_branch("worker", at=first.head)
            worker = first.repo.worktree("worker")
            worker["work"] = "done"
            worker.commit()
            first.repo.close()

            again = store(kind="disk", path=p, branch="worker")
            assert again.get("base") == "ok"
            assert again.get("work") == "done"
            again.repo.close()
