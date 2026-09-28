"""Tests for Repo: branches, tags, the commit graph, snapshots and gc."""

import os
import tempfile
from collections.abc import Mapping

import pytest
from support import fork, worktree

import kvgit
from kvgit import (
    BranchExistsError,
    Commit,
    CorruptHeadError,
    GcBusy,
    KvgitError,
    Repo,
    Snapshot,
    StorageVersionError,
    TagExistsError,
    UnknownBranchError,
    UnknownCommitError,
    UnknownTagError,
)
from kvgit.encoding import dumps
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    ROOT_COMMIT,
    STORAGE_VERSION_KEY,
    _acquire_gc_lease,
)


def repo_with_history():
    """main: root -> c1 (a=1) -> c2 (a=2, b=1); dev from c1: c3 (d=1)."""
    wt = worktree()
    repo = wt.repo
    wt["a"] = 1
    wt.commit(info={"n": 1})
    c1 = wt.head
    wt["a"] = 2
    wt["b"] = 1
    wt.commit(info={"n": 2})
    c2 = wt.head
    repo.branches.create("dev", at=c1)
    dev = repo.worktree("dev")
    dev["d"] = 1
    dev.commit()
    return repo, wt, dev, c1, c2


class TestOpening:
    def test_a_fresh_store_has_no_branches_until_one_is_made(self):
        repo = Repo(Memory())
        assert list(repo.branches) == []
        with pytest.raises(UnknownBranchError):
            repo.worktree("main")
        wt = repo.worktree("main", create=True)
        assert wt.head == ROOT_COMMIT
        assert list(repo.branches) == ["main"]

    def test_create_is_open_or_create(self):
        repo = Repo(Memory())
        first = repo.worktree("main", create=True)
        first["k"] = 1
        first.commit()
        again = repo.worktree("main", create=True)
        assert again.head == first.head

    def test_opening_does_not_write(self):
        repo, *_ = repo_with_history()
        before = dict(repo.store.items())
        Repo(repo.store).worktree("main")
        Repo(repo.store).snapshot(branch="dev")
        assert dict(repo.store.items()) == before

    def test_a_store_stamped_too_high_is_refused(self):
        backend = Memory()
        backend.set(STORAGE_VERSION_KEY, dumps(99))
        with pytest.raises(StorageVersionError):
            Repo(backend)

    def test_context_manager_closes_the_backend(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "store")
            with Repo(Disk(path)) as repo:
                wt = repo.worktree("main", create=True)
                wt["k"] = "v"
                wt.commit()
            with Repo(Disk(path)) as repo:
                assert repo.snapshot(branch="main")["k"] == "v"

    def test_every_state_error_is_a_kvgit_error(self):
        for error in (
            BranchExistsError,
            CorruptHeadError,
            GcBusy,
            StorageVersionError,
            TagExistsError,
            UnknownBranchError,
            UnknownCommitError,
            UnknownTagError,
        ):
            assert issubclass(error, KvgitError)
            assert not issubclass(error, ValueError)


class TestBranches:
    def test_create_at_root_by_default(self):
        repo = Repo(Memory())
        assert repo.branches.create("dev") == ROOT_COMMIT
        assert repo.branches["dev"] == ROOT_COMMIT
        assert "dev" in repo.branches

    def test_create_at_a_commit(self):
        repo, _, _, c1, _ = repo_with_history()
        assert repo.branches.create("from-c1", at=c1) == c1
        assert repo.snapshot(branch="from-c1")["a"] == 1

    def test_create_errors(self):
        repo, *_ = repo_with_history()
        with pytest.raises(BranchExistsError):
            repo.branches.create("dev")
        with pytest.raises(UnknownCommitError):
            repo.branches.create("x", at="0" * 40)

    def test_branches_are_sorted_and_exclude_tags(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        assert list(repo.branches) == ["dev", "main"]

    def test_delete_any_branch(self):
        repo, *_ = repo_with_history()
        repo.branches.delete("main")  # even one with a worktree open
        assert list(repo.branches) == ["dev"]
        assert "main" not in repo.branches
        with pytest.raises(UnknownBranchError):
            repo.branches.delete("main")

    def test_delete_does_not_sweep(self):
        repo, _, dev, *_ = repo_with_history()
        orphan = dev.head
        repo.branches.delete("dev")
        assert repo.get_commit(orphan)  # collectable, not yet collected
        assert repo.gc(min_age=0) == 1
        with pytest.raises(UnknownCommitError):
            repo.get_commit(orphan)

    def test_head_errors(self):
        repo, *_ = repo_with_history()
        with pytest.raises(UnknownBranchError):
            repo.branches["nope"]
        repo.store.set(BRANCH_HEAD % "main", b"garbage")
        repo.store.remove(BRANCH_HEAD_PREV % "main")
        with pytest.raises(CorruptHeadError):
            repo.branches["main"]

    def test_repair_head(self):
        repo, wt, *_ = repo_with_history()
        good = wt.head
        repo.store.set(BRANCH_HEAD % "main", b"garbage")
        assert repo.branches["main"] != good  # recovered in memory, from the backup
        assert repo.repair_head("main") is not None
        assert repo.store.get(BRANCH_HEAD % "main") != b"garbage"


class TestRefCollections:
    """``repo.branches`` and ``repo.tags`` are live mappings, shaped like
    pygit2's ``repo.branches``."""

    def test_branches_is_a_mapping_of_name_to_tip(self):
        repo, wt, dev, *_ = repo_with_history()
        assert isinstance(repo.branches, Mapping)
        assert dict(repo.branches) == {"dev": dev.head, "main": wt.head}
        assert len(repo.branches) == 2
        assert repo.branches.get("nope") is None
        assert repo.branches.get("main") == wt.head
        assert 42 not in repo.branches

    def test_a_missing_ref_is_a_key_error_that_reads_cleanly(self):
        repo, *_ = repo_with_history()
        with pytest.raises(KeyError) as missing_branch:
            repo.branches["nope"]
        assert isinstance(missing_branch.value, UnknownBranchError)
        assert str(missing_branch.value) == "Branch 'nope' does not exist"
        with pytest.raises(KeyError) as missing_tag:
            repo.tags["nope"]
        assert isinstance(missing_tag.value, UnknownTagError)
        assert str(missing_tag.value) == "Tag 'nope' does not exist"

    def test_damage_is_not_absence(self):
        repo, *_ = repo_with_history()
        repo.store.set(BRANCH_HEAD % "main", b"garbage")
        repo.store.remove(BRANCH_HEAD_PREV % "main")
        assert "main" in repo.branches
        with pytest.raises(CorruptHeadError):
            repo.branches.get("main")

    def test_views_are_live(self):
        repo, wt, *_ = repo_with_history()
        branches, tags = repo.branches, repo.tags
        other = Repo(repo.store)
        other.branches.create("late", wt.head)
        other.tags.create("v9", wt.head)
        assert "late" in branches and branches["late"] == wt.head
        assert list(tags) == ["v9"] and dict(tags.items()) == {"v9": wt.head}
        moved = other.worktree("late")
        moved["x"] = 1
        moved.commit()
        assert branches["late"] == moved.head

    def test_create_takes_the_commit_positionally_too(self):
        repo, wt, *_ = repo_with_history()
        assert repo.branches.create("pos", wt.head) == wt.head
        assert repo.branches["pos"] == wt.head

    def test_a_dangling_tag_still_maps_to_its_commit(self):
        repo, _, dev, *_ = repo_with_history()
        repo.tags.create("keep", dev.head)
        repo.store.remove(COMMIT_ROOT % dev.head)
        assert repo.tags["keep"] == dev.head
        assert repo.tags.info("keep").dangling


class TestTags:
    def test_create_list_info_delete(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head, info={"why": "release"})
        assert dict(repo.tags) == {"v1": wt.head}
        info = repo.tags.info("v1")
        assert info.commit == wt.head and info.info == {"why": "release"}
        repo.tags.delete("v1")
        assert dict(repo.tags) == {}
        with pytest.raises(UnknownTagError):
            repo.tags.info("v1")

    def test_tag_errors(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        with pytest.raises(TagExistsError):
            repo.tags.create("v1", wt.head)
        with pytest.raises(UnknownCommitError):
            repo.tags.create("v2", "0" * 40)
        with pytest.raises(UnknownTagError):
            repo.tags.delete("v2")

    def test_a_tag_keeps_its_commit_alive(self):
        repo, _, dev, *_ = repo_with_history()
        repo.tags.create("keep", dev.head)
        repo.branches.delete("dev")
        repo.gc(min_age=0)
        assert repo.snapshot(tag="keep")["d"] == 1

    def test_branch_and_tag_namespaces_are_separate(self):
        repo, wt, dev, *_ = repo_with_history()
        repo.tags.create("dev", wt.head)
        assert repo.snapshot(branch="dev").commit == dev.head
        assert repo.snapshot(tag="dev").commit == wt.head

    def test_the_branch_api_refuses_a_tags_reserved_name(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        reserved = "refs/tags/v1"
        for attempt in (
            lambda: repo.worktree(reserved),
            lambda: repo.branches.create(reserved),
            lambda: repo.branches.delete(reserved),
            lambda: repo.branches[reserved],
            lambda: repo.repair_head(reserved),
            lambda: repo.snapshot(branch=reserved),
        ):
            with pytest.raises(ValueError, match="reserved"):
                attempt()
        assert reserved not in repo.branches
        assert dict(repo.tags) == {"v1": wt.head}


class TestCommits:
    def test_get_commit(self):
        repo, _, _, c1, c2 = repo_with_history()
        record = repo.get_commit(c2)
        assert isinstance(record, Commit)
        assert record.hash == c2
        assert record.parents == (c1,)
        assert record.info == {"n": 2}
        assert isinstance(record.time, float)
        assert record.root == repo.get_commit(c2).root
        with pytest.raises(UnknownCommitError):
            repo.get_commit("0" * 40)

    def test_equal_contents_have_equal_roots(self):
        repo, wt, _, c1, _ = repo_with_history()
        wt.reset(c1)
        wt["a"] = 1  # the same state c1 had, in a new commit
        wt["extra"] = 0
        del wt["extra"]
        wt.commit(info={"again": True})
        assert wt.head != c1
        assert repo.get_commit(wt.head).root == repo.get_commit(c1).root

    def test_log_newest_first(self):
        repo, _, _, c1, c2 = repo_with_history()
        assert [c.hash for c in repo.log(branch="main")] == [c2, c1, ROOT_COMMIT]
        assert [c.hash for c in repo.log(commit=c1)] == [c1, ROOT_COMMIT]
        assert [c.hash for c in repo.log(branch="main", limit=2)] == [c2, c1]

    def test_log_follows_every_parent_unless_first_parent(self):
        repo, wt, dev, c1, c2 = repo_with_history()
        wt.merge(branch="dev")
        merge = wt.head
        everything = [c.hash for c in repo.log(branch="main")]
        assert everything[0] == merge and dev.head in everything
        linear = [c.hash for c in repo.log(branch="main", first_parent=True)]
        assert linear == [merge, c2, c1, ROOT_COMMIT]

    def test_log_from_a_tag_and_errors(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        assert next(repo.log(tag="v1")).hash == wt.head
        with pytest.raises(ValueError, match="exactly one"):
            list(repo.log())
        with pytest.raises(UnknownTagError):
            list(repo.log(tag="nope"))

    def test_diff(self):
        repo, _, _, c1, c2 = repo_with_history()
        change = repo.diff(c1, c2)
        assert change.added == {"b"} and change.modified == {"a"}
        assert repo.diff(c2, c1).removed == {"b"}
        assert repo.diff(c1, c1).added == frozenset()
        with pytest.raises(UnknownCommitError):
            repo.diff(c1, "0" * 40)

    def test_merge_base(self):
        repo, _, dev, c1, c2 = repo_with_history()
        assert repo.merge_base(c2, dev.head) == c1
        assert repo.merge_base(c1, c2) == c1
        assert repo.merge_base(c2, c2) == c2

    def test_merge_base_of_a_missing_commit_raises(self):
        repo, _, _, c1, _ = repo_with_history()
        missing = "0" * 40
        for a, b in ((missing, missing), (missing, c1), (c1, missing)):
            with pytest.raises(UnknownCommitError):
                repo.merge_base(a, b)


class TestSnapshots:
    def test_a_snapshot_is_a_read_only_mapping(self):
        repo, _, _, _, c2 = repo_with_history()
        snap = repo.snapshot(commit=c2)
        assert isinstance(snap, Snapshot)
        assert dict(snap) == {"a": 2, "b": 1}
        assert len(snap) == 2 and "b" in snap and "zzz" not in snap
        assert snap.get("zzz") is None
        assert snap.get_many("a", "zzz") == {"a": 2}
        with pytest.raises(KeyError):
            snap["zzz"]
        with pytest.raises(TypeError):
            snap["a"] = 3  # type: ignore[index]

    def test_a_snapshot_is_pinned(self):
        repo, wt, *_ = repo_with_history()
        snap = repo.snapshot(branch="main")
        wt["a"] = 100
        wt.commit()
        assert snap["a"] == 2
        assert repo.snapshot(branch="main")["a"] == 100

    def test_raw_is_the_stored_bytes(self):
        import pickle

        repo, _, _, _, c2 = repo_with_history()
        raw = repo.snapshot(commit=c2).raw
        assert raw["a"] == pickle.dumps(2)
        assert raw.get_many("a", "b") == {"a": pickle.dumps(2), "b": pickle.dumps(1)}
        assert sorted(raw) == ["a", "b"] and len(raw) == 2
        with pytest.raises(KeyError):
            raw["zzz"]

    def test_snapshot_refs_and_errors(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        assert repo.snapshot(tag="v1").commit == wt.head
        with pytest.raises(ValueError, match="exactly one"):
            repo.snapshot(branch="main", tag="v1")
        with pytest.raises(UnknownBranchError):
            repo.snapshot(branch="nope")
        with pytest.raises(UnknownCommitError):
            repo.snapshot(commit="0" * 40)
        with pytest.raises(UnknownTagError):
            repo.snapshot(tag="nope")

    def test_a_tag_naming_a_missing_commit_is_unknown(self):
        repo, wt, *_ = repo_with_history()
        repo.tags.create("v1", wt.head)
        repo.store.set(BRANCH_HEAD % "refs/tags/v1", dumps("0" * 40))
        with pytest.raises(UnknownCommitError):
            repo.snapshot(tag="v1")


class TestGc:
    def test_gc_reclaims_a_deleted_branch(self):
        repo, _, dev, *_ = repo_with_history()
        orphan = dev.head
        repo.branches.delete("dev")
        assert repo.gc() == 0  # younger than the default hour
        assert repo.gc(min_age=0) == 1
        assert repo.store.get(COMMIT_ROOT % orphan) is None

    def test_deep_scans_for_unreferenced_content(self):
        repo, *_ = repo_with_history()
        stray = "kvgit:blob:" + "0" * 64
        repo.store.set(stray, b"nothing references this")
        repo.gc(min_age=0)
        assert repo.store.get(stray) is not None
        repo.gc(min_age=0, deep=True)
        assert repo.store.get(stray) is None

    def test_wait_false_raises_while_another_sweep_runs(self):
        repo, *_ = repo_with_history()
        _acquire_gc_lease(repo.store, 60.0)
        with pytest.raises(GcBusy):
            repo.gc(wait=False)


def test_open_returns_a_worktree_of_a_repo():
    wt = kvgit.open()
    assert isinstance(wt.repo, Repo)
    other = fork(wt, "other")
    assert other.repo is wt.repo
