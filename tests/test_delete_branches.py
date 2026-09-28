"""Deleting branches through a Repo, and what the sweep takes afterwards."""

import os
import tempfile

import pytest
from support import fork, worktree

from kvgit import Repo, UnknownBranchError
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    VersionedKV,
    _resolve_head,
    blob_key,
    clean_orphans,
)


class TestDeleteBranch:
    def test_delete_the_only_branch(self):
        """Legal: a Repo needs no branch to stand on."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                wt["x"] = "1"
                wt.commit()

            with Repo(Disk(p)) as repo:
                repo.branches.delete("main")
                assert list(repo.branches) == []
                fresh = repo.worktree("main", create=True)
                assert fresh.get("x") is None

    def test_delete_some_branches(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                wt["base"] = "ok"
                wt.commit()
                for name in ("dev", "feature"):
                    repo.branches.create(name, at=wt.head)
                assert list(repo.branches) == ["dev", "feature", "main"]

            with Repo(Disk(p)) as repo:
                repo.branches.delete("dev")
                repo.branches.delete("feature")
                assert list(repo.branches) == ["main"]
                assert repo.snapshot(branch="main")["base"] == "ok"

    def test_deleting_an_unknown_branch_raises(self):
        repo = Repo(Memory())
        repo.worktree("main", create=True)
        with pytest.raises(UnknownBranchError):
            repo.branches.delete("never-existed")
        assert list(repo.branches) == ["main"]

    def test_the_backup_goes_with_the_head(self):
        """Left behind, a same-named branch created later could 'recover'
        the deleted state through head resolution's fallback."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                dev = fork(wt, "dev")
                dev["secret"] = "deleted data"
                dev.commit()
                dev["more"] = "moves prev-HEAD off the fork point"
                dev.commit()

            with Repo(Disk(p)) as repo:
                repo.branches.delete("dev")
                again = repo.worktree("dev", create=True)
                assert again.get("secret") is None
                assert again.get("more") is None
                assert repo.store.get(BRANCH_HEAD_PREV % "dev") is None

    def test_delete_then_gc_at_min_age_zero_reclaims_immediately(self):
        """Deleting takes the branch; the next gc takes what only it held,
        at once when min_age is 0."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                wt["base"] = "ok"
                wt.commit()
                dev = fork(wt, "dev")
                dev["secret"] = "unique-blob-value"
                dev.commit()
                dev_commit = dev.head
                pointer = dev._engine._commit_keys["secret"]

            with Repo(Disk(p)) as repo:
                repo.branches.delete("dev")
                assert repo.store.get(pointer) is not None
                repo.gc(min_age=0)
                assert repo.store.get(COMMIT_ROOT % dev_commit) is None
                assert repo.store.get(pointer) is None

    def test_reopen_after_close_is_not_locked(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                repo.worktree("main", create=True)
                repo.branches.delete("main")
            with Repo(Disk(p)) as repo:
                assert list(repo.branches) == []


class TestSharedOrphanSweep:
    def test_the_sweep_reclaims_a_deleted_branchs_commits_and_blobs(self):
        backend = Memory()
        v = VersionedKV(backend)  # main
        dev = v.create_branch("dev")
        dev.commit({"secret": b"unique-blob-value"})
        pointer = blob_key(b"unique-blob-value")
        assert backend.get(pointer) == b"unique-blob-value"

        backend.remove_many([BRANCH_HEAD % "dev", BRANCH_HEAD_PREV % "dev"])
        removed = clean_orphans(backend, min_age=0)

        assert removed >= 1
        assert backend.get(COMMIT_ROOT % dev.current_commit) is None
        assert backend.get(pointer) is None

    def test_repo_gc_and_the_module_sweep_agree(self):
        wt = worktree()
        dev = fork(wt, "dev")
        dev["k"] = "v"
        dev.commit()
        wt.repo.branches.delete("dev")
        assert _resolve_head(wt.repo.store, "dev") is None
        assert wt.repo.gc(min_age=0) >= 1
        assert clean_orphans(wt.repo.store, min_age=0) == 0
