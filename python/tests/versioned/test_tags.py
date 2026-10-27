"""Tests for tags: immutable names for commits, and GC roots.

Covers the tag API on ``Repo``, the reachability rule that keeps a
tagged commit's ancestry alive, and the storage-version gate that stops
older code from sweeping a store whose tags it cannot see.
"""

import os
import pickle
import tempfile

import pytest
from support import clean_orphans, fork, pointers, worktree

from kvgit import Repo, Worktree
from kvgit._store import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    STORAGE_VERSION_KEY,
    TAG_BRANCH_PREFIX,
    TAG_INFO_KEY,
    blob_key,
    tag_info,
)
from kvgit.encoding import dumps, safe_loads
from kvgit.errors import (
    StorageVersionError,
    TagExistsError,
    UnknownCommitError,
    UnknownTagError,
)
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory

SECRET = blob_key(b"tagged value")


def _tag_head(name: str) -> str:
    """The key a tag's commit pointer lives under."""
    return BRANCH_HEAD % (TAG_BRANCH_PREFIX + name)


def _version(backend):
    raw = backend.get(STORAGE_VERSION_KEY)
    return safe_loads(raw) if raw is not None else None


def _main(store=None) -> Worktree:
    """A worktree on ``main`` that stores values as given."""
    return worktree(store, codec="bytes")


def _commit(wt: Worktree, updates: dict[str, bytes]):
    """Write ``updates`` into ``wt`` and commit them."""
    wt.update(updates)
    return wt.commit()


class TestTagCreateListDelete:
    def test_round_trip(self):
        store = Memory()
        v = _main(store)
        repo = v.repo
        result = _commit(v, {"x": b"1"})

        repo.tags.create("v1", v.head, info={"by": "ann"})

        assert repo.tags["v1"] == result.commit
        assert dict(repo.tags.items()) == {"v1": result.commit}

        record = repo.tags.info("v1")
        assert record.name == "v1"
        assert record.commit == result.commit
        assert record.info == {"by": "ann"}
        assert record.time is not None
        assert record.dangling is False

        repo.tags.delete("v1")
        assert dict(repo.tags.items()) == {}
        with pytest.raises(UnknownTagError):
            repo.tags.info("v1")

    def test_tag_at_specific_commit(self):
        v = _main()
        first = _commit(v, {"x": b"1"}).commit
        _commit(v, {"x": b"2"})
        v.repo.tags.create("first", first)
        assert v.repo.tags["first"] == first
        assert dict(v.repo.tags.items())["first"] == first

    def test_tag_without_info(self):
        v = _main()
        v.repo.tags.create("bare", v.head)
        record = v.repo.tags.info("bare")
        assert record.info is None
        assert record.time is not None

    def test_duplicate_name_raises(self):
        """Tags are immutable: moving one is delete + create, spelled out."""
        v = _main()
        second = _commit(v, {"x": b"1"}).commit
        v.repo.tags.create("v1", v.head)
        with pytest.raises(TagExistsError, match="already exists"):
            v.repo.tags.create("v1", second)

    def test_recreate_after_delete_moves_the_name(self):
        v = _main()
        first = v.head
        v.repo.tags.create("latest", first)
        second = _commit(v, {"x": b"1"}).commit

        v.repo.tags.delete("latest")
        v.repo.tags.create("latest", second)

        assert dict(v.repo.tags.items())["latest"] == second

    def test_unknown_commit_raises(self):
        v = _main()
        with pytest.raises(UnknownCommitError, match="does not exist"):
            v.repo.tags.create("bad", "0" * 40)

    def test_delete_unknown_tag_raises(self):
        v = _main()
        with pytest.raises(UnknownTagError, match="does not exist"):
            v.repo.tags.delete("never-existed")

    def test_tag_info_unknown_name(self):
        v = _main()
        with pytest.raises(UnknownTagError, match="does not exist"):
            v.repo.tags.info("nope")
        assert tag_info(v.repo.store, "nope") is None

    def test_empty_name_raises(self):
        v = _main()
        with pytest.raises(ValueError, match="non-empty string"):
            v.repo.tags.create("", v.head)

    def test_percent_in_name_raises(self):
        """Tag keys are built by %-formatting a template."""
        v = _main()
        with pytest.raises(ValueError, match="must not contain"):
            v.repo.tags.create("v1%s", v.head)

    def test_slash_in_name_allowed(self):
        """Embedders namespace their own tags."""
        v = _main()
        v.repo.tags.create("pub/v1", v.head)
        assert "pub/v1" in dict(v.repo.tags.items())

    def test_info_must_be_json_serializable(self):
        """Same rule as commit info — and the tag is not created."""
        v = _main()
        with pytest.raises(TypeError):
            v.repo.tags.create("v1", v.head, info={"fn": object()})
        assert dict(v.repo.tags.items()) == {}

    def test_tags_and_branches_are_separate_namespaces(self):
        store = Memory()
        v = _main(store)
        v.repo.branches.create("release", at=v.head)
        v.repo.tags.create("release", v.head)
        assert "release" in list(v.repo.branches)
        assert "release" in dict(v.repo.tags.items())

    def test_info_key_of_one_tag_is_not_another_tag(self):
        """``__tag_info__x`` must not read back as the tag ``_info__x``."""
        v = _main()
        v.repo.tags.create("x", v.head, info={"real": True})
        assert list(v.repo.tags) == ["x"]


class TestTagsAsGCRoots:
    def _store_with_tagged_orphan(self):
        """A commit reachable only through the tag ``v1``."""
        backend = Memory()
        v = _main(backend)
        dev = fork(v, "dev")
        _commit(dev, {"secret": b"tagged value"})
        tagged = dev.head
        v.repo.tags.create("v1", tagged)
        v.repo.branches.delete("dev")
        return backend, v.repo, tagged

    def test_gc_keeps_a_tagged_commit(self):
        backend, repo, tagged = self._store_with_tagged_orphan()

        assert repo.gc(min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None
        assert backend.get(SECRET) == b"tagged value"

    def test_deep_gc_keeps_a_tagged_commit(self):
        backend, repo, tagged = self._store_with_tagged_orphan()

        assert repo.gc(min_age=0, deep=True, wait=False) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None
        assert repo.snapshot(tag="v1").get("secret") == b"tagged value"

    def test_delete_tag_releases_the_commit(self):
        backend, repo, tagged = self._store_with_tagged_orphan()

        repo.tags.delete("v1")

        assert backend.get(_tag_head("v1")) is None
        assert backend.get(TAG_INFO_KEY % "v1") is None
        assert repo.gc(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None
        repo.gc(min_age=0, deep=True, wait=False)
        assert backend.get(SECRET) is None

    def test_dangling_tag_keeps_nothing_alive(self):
        """A tag naming a commit the store does not have marks nothing.

        The store no longer says what that root pointed at, so the sweep
        has nothing to walk — the same rule an unresolvable branch HEAD
        gets.
        """
        backend, repo, tagged = self._store_with_tagged_orphan()

        # Damage the tag: it now names a commit that is not in the store.
        backend.set(_tag_head("v1"), dumps("0" * 40))
        assert repo.tags.info("v1").dangling is True

        assert repo.gc(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None

    def test_dangling_tag_is_still_listed(self):
        """Omitting it would make damage look like deletion."""
        v = _main()
        v.repo.tags.create("v1", v.head)
        v.repo.store.set(_tag_head("v1"), dumps("0" * 40))
        assert dict(v.repo.tags.items()) == {"v1": "0" * 40}

    def test_module_sweep_sees_tags_without_a_repo(self):
        backend, _repo, tagged = self._store_with_tagged_orphan()
        assert clean_orphans(backend, min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None


class TestRepoTagPaths:
    """Deleting through a Repo, with no worktree open."""

    def test_deleting_every_branch_leaves_a_tag_readable(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                wt["keep"] = "tagged value"
                wt.commit()
                repo.tags.create("v1", wt.head)

            with Repo(Disk(p)) as repo:
                repo.branches.delete("main")
                repo.gc(min_age=0)
                assert repo.snapshot(tag="v1")["keep"] == "tagged value"
                assert repo.worktree("main", create=True).get("keep") is None

    def test_deleting_the_tag_too_frees_its_commit_and_content(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            with Repo(Disk(p)) as repo:
                wt = repo.worktree("main", create=True)
                wt["keep"] = "tagged value"
                wt.commit()
                repo.tags.create("v1", wt.head)
                tagged = wt.head
                pointer = pointers(wt.repo.store, wt.head)["keep"]

            with Repo(Disk(p)) as repo:
                repo.branches.delete("main")
                repo.tags.delete("v1")
                repo.gc(min_age=0)
                assert dict(repo.tags) == {}
                assert repo.store.get(TAG_INFO_KEY % "v1") is None
                assert repo.store.get(COMMIT_ROOT % tagged) is None
                assert repo.store.get(pointer) is None


class TestSnapshotAtTag:
    def test_snapshot_returns_the_tagged_state(self):
        v = _main()
        _commit(v, {"x": b"tagged"})
        v.repo.tags.create("v1", v.head)
        tagged = v.head
        _commit(v, {"x": b"later"})

        at_tag = v.repo.snapshot(tag="v1")
        assert at_tag.get("x") == b"tagged"
        assert at_tag.commit == tagged

    def test_unknown_tag_raises(self):
        with pytest.raises(UnknownTagError):
            _main().repo.snapshot(tag="nope")

    def test_dangling_tag_raises(self):
        v = _main()
        v.repo.tags.create("v1", v.head)
        v.repo.store.set(_tag_head("v1"), dumps("0" * 40))
        with pytest.raises(UnknownCommitError):
            v.repo.snapshot(tag="v1")

    def test_both_commit_and_tag_raises(self):
        v = _main()
        v.repo.tags.create("v1", v.head)
        with pytest.raises(ValueError, match="exactly one"):
            v.repo.snapshot(commit=v.head, tag="v1")

    def test_neither_commit_nor_tag_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            _main().repo.snapshot()


class TestReadAtTag:
    def test_reads_the_tagged_value(self):
        v = _main()
        _commit(v, {"config": b"v1"})
        v.repo.tags.create("v1", v.head)
        _commit(v, {"config": b"v2"})

        assert v.repo.snapshot(tag="v1")["config"] == b"v1"
        assert v.get("config") == b"v2"

    def test_missing_key_or_tag(self):
        v = _main()
        _commit(v, {"config": b"v1"})
        v.repo.tags.create("v1", v.head)
        assert v.repo.snapshot(tag="v1").get("absent") is None
        with pytest.raises(UnknownTagError):
            v.repo.snapshot(tag="nope")

    def test_both_branch_and_tag_raises(self):
        v = _main()
        v.repo.tags.create("v1", v.head)
        with pytest.raises(ValueError, match="exactly one"):
            v.repo.snapshot(branch="main", tag="v1")

    def test_neither_branch_nor_tag_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            _main().repo.snapshot()


class TestTagKeyLayout:
    """A tag is a branch head under a reserved name. That is the
    cross-version contract, so it is asserted directly."""

    def test_a_tag_is_a_branch_head_under_refs_tags(self):
        """Every kvgit decides reachability by walking branch heads. A
        tag stored as one is therefore read, and kept alive, by versions
        that predate tags — including their admin sweep, which no
        version stamp in this repo could have held back. Change this key
        and that property is gone."""
        backend = Memory()
        v = _main(backend)
        tagged = v.head
        v.repo.tags.create("v1", tagged)

        assert backend.get(BRANCH_HEAD % "refs/tags/v1") == dumps(tagged)
        assert TAG_BRANCH_PREFIX == "refs/tags/"

    def test_tagging_does_not_change_the_storage_version(self):
        """A tagged store must still open under versions that predate
        tags, so nothing about a tag raises the stamp."""
        backend = Memory()
        v = _main(backend)
        _commit(v, {"x": b"1"})
        before = _version(backend)

        v.repo.tags.create("v1", v.head)
        assert _version(backend) == before

    def test_info_record_survives_a_deep_gc(self):
        """The record lives under its own key kind that no sweep — this
        version's or an older one's — scans."""
        backend = Memory()
        v = _main(backend)
        v.repo.tags.create("v1", v.head, info={"by": "ann"})
        v.repo.gc(min_age=0, deep=True, wait=False)
        assert backend.get(TAG_INFO_KEY % "v1") is not None
        assert v.repo.tags.info("v1").info == {"by": "ann"}

    def test_delete_tag_removes_head_backup_and_record(self):
        """The prev-HEAD backup goes too. This code never writes one for
        a tag, but something treating the tag as an ordinary branch
        could have, and a backup outliving its head would resurrect the
        deleted commit under a later tag of the same name."""
        backend = Memory()
        v = _main(backend)
        _commit(v, {"x": b"1"})
        v.repo.tags.create("v1", v.head)
        # Planted by hand: what an older client leaves behind if it
        # switches onto the tag and commits.
        backend.set(BRANCH_HEAD_PREV % "refs/tags/v1", dumps(v.head))

        v.repo.tags.delete("v1")

        assert backend.get(_tag_head("v1")) is None
        assert backend.get(BRANCH_HEAD_PREV % "refs/tags/v1") is None
        assert backend.get(TAG_INFO_KEY % "v1") is None

    def test_repo_delete_tag_removes_the_backup_too(self):
        repo = Repo(Memory())
        wt = repo.worktree("main", create=True)
        repo.tags.create("v1", wt.head)
        repo.store.set(BRANCH_HEAD_PREV % "refs/tags/v1", dumps(wt.head))

        repo.tags.delete("v1")

        assert repo.store.get(_tag_head("v1")) is None
        assert repo.store.get(BRANCH_HEAD_PREV % "refs/tags/v1") is None
        assert repo.store.get(TAG_INFO_KEY % "v1") is None


class TestReservedBranchNames:
    """Tags hide from the branch API, and the branch API refuses to
    hand out names inside their namespace."""

    def test_branch_listings_hide_tags(self):
        store_ = Memory()
        v = _main(store_)
        v.repo.branches.create("dev", at=v.head)
        v.repo.tags.create("v1", v.head)

        assert list(v.repo.branches) == ["dev", "main"]
        assert list(Repo(store_, codec="bytes").branches) == ["dev", "main"]
        assert dict(v.repo.tags.items()) == {"v1": v.head}

    def test_creating_a_worktree_refuses_a_reserved_branch(self):
        repo = Repo(Memory(), codec="bytes")
        with pytest.raises(ValueError, match="reserved for tags"):
            repo.worktree("refs/tags/v1", create=True)

    def test_create_branch_refuses_a_reserved_name(self):
        v = _main()
        with pytest.raises(ValueError, match="reserved for tags"):
            v.repo.branches.create("refs/tags/v1", at=v.head)

    def test_opening_a_worktree_refuses_a_reserved_name(self):
        v = _main()
        v.repo.tags.create("v1", v.head)
        with pytest.raises(ValueError, match="reserved for tags"):
            v.repo.worktree("refs/tags/v1")

    def test_delete_branch_refuses_a_reserved_name(self):
        v = _main()
        v.repo.tags.create("v1", v.head)
        with pytest.raises(ValueError, match="reserved for tags"):
            v.repo.branches.delete("refs/tags/v1")

    def test_snapshot_refuses_a_reserved_branch(self):
        v = _main()
        _commit(v, {"x": b"1"})
        v.repo.tags.create("v1", v.head)
        with pytest.raises(ValueError, match="reserved for tags"):
            v.repo.snapshot(branch="refs/tags/v1")


class TestUnknownStorageVersionIsRefused:
    """Version hygiene, no longer what protects tags: code must not
    sweep a store whose layout it does not know."""

    def test_sweep_refuses_an_unknown_version(self):
        backend = Memory()
        v = _main(backend)
        v.repo.tags.create("v1", v.head)
        backend.set(STORAGE_VERSION_KEY, dumps(99))

        with pytest.raises(StorageVersionError, match="storage version"):
            clean_orphans(backend, min_age=0)

    def test_a_repo_refuses_an_unknown_version_before_touching_it(self):
        """Refused on opening, so no delete or sweep can run against a
        store whose tags this code may not see."""
        backend = Memory()
        wt = Repo(backend).worktree("main", create=True)
        wt.repo.tags.create("v1", wt.head)
        backend.set(STORAGE_VERSION_KEY, dumps(99))
        before = dict(backend.items())

        with pytest.raises(StorageVersionError, match="storage version"):
            Repo(backend)
        assert dict(backend.items()) == before


class TestRepoTagOps:
    def test_tag_round_trip(self):
        wt = worktree()
        wt["x"] = "hello"
        wt.commit()
        repo = wt.repo

        repo.tags.create("v1", wt.head, info={"by": "ann"})

        assert dict(repo.tags) == {"v1": wt.head}
        assert repo.tags.info("v1").info == {"by": "ann"}
        repo.tags.delete("v1")
        assert dict(repo.tags) == {}

    def test_a_tag_names_a_commit_not_pending_changes(self):
        wt = worktree()
        wt["x"] = "committed"
        wt.commit()
        wt["x"] = "pending only"

        wt.repo.tags.create("v1", wt.head)

        assert wt.repo.snapshot(tag="v1")["x"] == "committed"
        assert wt["x"] == "pending only"

    def test_reading_at_a_tag(self):
        wt = worktree()
        wt["title"] = "first"
        wt.commit()
        wt.repo.tags.create("v1", wt.head)
        wt["title"] = "second"
        wt.commit()

        assert wt.repo.snapshot(tag="v1")["title"] == "first"
        assert wt["title"] == "second"
        with pytest.raises(ValueError, match="exactly one"):
            wt.repo.snapshot(branch="main", tag="v1")

    def test_delete_tag_releases_the_commit(self):
        wt = worktree()
        wt["x"] = "base"
        wt.commit()
        dev = fork(wt, "dev")
        dev["secret"] = "tagged value"
        dev.commit()
        repo = wt.repo
        repo.tags.create("v1", dev.head)
        repo.branches.delete("dev")

        assert repo.gc(min_age=0) == 0
        pointer = blob_key(pickle.dumps("tagged value"))
        assert repo.store.get(pointer) is not None

        repo.tags.delete("v1")
        assert repo.gc(min_age=0) >= 1
        assert repo.store.get(pointer) is None
