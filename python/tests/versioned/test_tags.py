"""Tests for tags: immutable names for commits, and GC roots.

Covers the tag API on ``VersionedKV`` and on ``Repo``, the reachability
rule that keeps a tagged commit's ancestry alive, and the storage-version
gate that stops older code from sweeping a store whose tags it cannot see.
"""

import os
import pickle
import tempfile

import pytest
from support import fork, worktree

from kvgit import MergeConflict, Repo
from kvgit.encoding import dumps, safe_loads
from kvgit.errors import (
    StorageVersionError,
    TagExistsError,
    UnknownCommitError,
    UnknownTagError,
)
from kvgit.kv.disk import Disk
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    STORAGE_VERSION_KEY,
    TAG_BRANCH_PREFIX,
    TAG_INFO_KEY,
    VersionedKV as Versioned,
    _stamp_version_at_least,
    blob_key,
    clean_orphans,
)

SECRET = blob_key(b"tagged value")


def _tag_head(name: str) -> str:
    """The key a tag's commit pointer lives under."""
    return BRANCH_HEAD % (TAG_BRANCH_PREFIX + name)


def _version(backend):
    raw = backend.get(STORAGE_VERSION_KEY)
    return safe_loads(raw) if raw is not None else None


class TestTagCreateListDelete:
    def test_round_trip(self):
        store = Memory()
        v = Versioned(store)
        result = v.commit({"x": b"1"})

        tagged = v.tag("v1", info={"by": "ann"})

        assert tagged == result.commit
        assert v.tags() == {"v1": result.commit}

        record = v.tag_info("v1")
        assert record.name == "v1"
        assert record.commit == result.commit
        assert record.info == {"by": "ann"}
        assert record.time is not None
        assert record.dangling is False

        v.delete_tag("v1")
        assert v.tags() == {}
        assert v.tag_info("v1") is None

    def test_tag_defaults_to_current_commit(self):
        v = Versioned()
        v.commit({"x": b"1"})
        assert v.tag("here") == v.current_commit

    def test_tag_at_specific_commit(self):
        v = Versioned()
        first = v.commit({"x": b"1"}).commit
        v.commit({"x": b"2"})
        assert v.tag("first", at=first) == first
        assert v.tags()["first"] == first

    def test_tag_without_info(self):
        v = Versioned()
        v.tag("bare")
        record = v.tag_info("bare")
        assert record.info is None
        assert record.time is not None

    def test_duplicate_name_raises(self):
        """Tags are immutable: moving one is delete + create, spelled out."""
        v = Versioned()
        second = v.commit({"x": b"1"}).commit
        v.tag("v1")
        with pytest.raises(TagExistsError, match="already exists"):
            v.tag("v1", at=second)

    def test_recreate_after_delete_moves_the_name(self):
        v = Versioned()
        first = v.current_commit
        v.tag("latest", at=first)
        second = v.commit({"x": b"1"}).commit

        v.delete_tag("latest")
        v.tag("latest", at=second)

        assert v.tags()["latest"] == second

    def test_unknown_commit_raises(self):
        v = Versioned()
        with pytest.raises(UnknownCommitError, match="does not exist"):
            v.tag("bad", at="0" * 40)

    def test_delete_unknown_tag_raises(self):
        v = Versioned()
        with pytest.raises(UnknownTagError, match="does not exist"):
            v.delete_tag("never-existed")

    def test_tag_info_unknown_name_is_none(self):
        assert Versioned().tag_info("nope") is None

    def test_empty_name_raises(self):
        v = Versioned()
        with pytest.raises(ValueError, match="non-empty string"):
            v.tag("")

    def test_percent_in_name_raises(self):
        """Tag keys are built by %-formatting a template."""
        v = Versioned()
        with pytest.raises(ValueError, match="must not contain"):
            v.tag("v1%s")

    def test_slash_in_name_allowed(self):
        """Embedders namespace their own tags."""
        v = Versioned()
        v.tag("pub/v1")
        assert "pub/v1" in v.tags()

    def test_info_must_be_json_serializable(self):
        """Same rule as commit info — and the tag is not created."""
        v = Versioned()
        with pytest.raises(TypeError):
            v.tag("v1", info={"fn": object()})
        assert v.tags() == {}

    def test_tags_and_branches_are_separate_namespaces(self):
        store = Memory()
        v = Versioned(store)
        v.create_branch("release")
        v.tag("release")
        assert "release" in v.list_branches()
        assert "release" in v.tags()

    def test_info_key_of_one_tag_is_not_another_tag(self):
        """``__tag_info__x`` must not read back as the tag ``_info__x``."""
        v = Versioned()
        v.tag("x", info={"real": True})
        assert list(v.tags()) == ["x"]


class TestTagsAsGCRoots:
    def _store_with_tagged_orphan(self):
        """A commit reachable only through the tag ``v1``."""
        backend = Memory()
        v = Versioned(backend)
        dev = v.create_branch("dev")
        dev.commit({"secret": b"tagged value"})
        tagged = dev.current_commit
        dev.tag("v1")
        v.delete_branch("dev")
        return backend, v, tagged

    def test_clean_orphans_keeps_a_tagged_commit(self):
        backend, v, tagged = self._store_with_tagged_orphan()

        assert v.clean_orphans(min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None
        assert backend.get(SECRET) == b"tagged value"

    def test_deep_clean_keeps_a_tagged_commit(self):
        backend, v, tagged = self._store_with_tagged_orphan()

        assert v.deep_clean(min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None
        assert v.checkout(tag="v1").get("secret") == b"tagged value"

    def test_delete_tag_releases_the_commit(self):
        backend, v, tagged = self._store_with_tagged_orphan()

        v.delete_tag("v1")

        assert backend.get(_tag_head("v1")) is None
        assert backend.get(TAG_INFO_KEY % "v1") is None
        assert v.clean_orphans(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None
        v.deep_clean(min_age=0)
        assert backend.get(SECRET) is None

    def test_dangling_tag_keeps_nothing_alive(self):
        """A tag naming a commit the store does not have marks nothing.

        The store no longer says what that root pointed at, so the sweep
        has nothing to walk — the same rule an unresolvable branch HEAD
        gets.
        """
        backend, v, tagged = self._store_with_tagged_orphan()

        # Damage the tag: it now names a commit that is not in the store.
        backend.set(_tag_head("v1"), dumps("0" * 40))
        assert v.tag_info("v1").dangling is True

        assert v.clean_orphans(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None

    def test_dangling_tag_is_still_listed(self):
        """Omitting it would make damage look like deletion."""
        v = Versioned()
        v.tag("v1")
        v.store.set(_tag_head("v1"), dumps("0" * 40))
        assert v.tags() == {"v1": "0" * 40}

    def test_module_sweep_sees_tags_without_a_handle(self):
        backend, _v, tagged = self._store_with_tagged_orphan()
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
                pointer = wt._engine._commit_keys["keep"]

            with Repo(Disk(p)) as repo:
                repo.branches.delete("main")
                repo.tags.delete("v1")
                repo.gc(min_age=0)
                assert dict(repo.tags) == {}
                assert repo.store.get(TAG_INFO_KEY % "v1") is None
                assert repo.store.get(COMMIT_ROOT % tagged) is None
                assert repo.store.get(pointer) is None


class TestCheckoutTag:
    def test_checkout_returns_the_tagged_state(self):
        v = Versioned()
        v.commit({"x": b"tagged"})
        v.tag("v1")
        v.commit({"x": b"later"})

        at_tag = v.checkout(tag="v1")
        assert at_tag.get("x") == b"tagged"
        assert at_tag.current_branch == v.current_branch

    def test_commit_from_tag_handle_lands_when_branch_has_not_moved(self):
        store = Memory()
        v = Versioned(store)
        v.commit({"x": b"tagged"})
        v.tag("v1")

        at_tag = v.checkout(tag="v1")
        result = at_tag.commit({"y": b"new"})

        assert result.strategy == "fast_forward"
        assert v.latest_head == at_tag.current_commit

    def test_commit_from_tag_handle_conflicts_when_branch_moved(self):
        """A tag handle is a normal writable handle on the branch, so a
        commit from it goes through the ordinary HEAD CAS."""
        store = Memory()
        v = Versioned(store)
        v.commit({"x": b"tagged"})
        v.tag("v1")
        at_tag = v.checkout(tag="v1")

        v.commit({"x": b"moved on"})

        with pytest.raises(MergeConflict) as exc_info:
            at_tag.commit({"x": b"from the tag"})
        assert "x" in exc_info.value.conflicting_keys

    def test_unknown_tag_returns_none(self):
        assert Versioned().checkout(tag="nope") is None

    def test_dangling_tag_returns_none(self):
        v = Versioned()
        v.tag("v1")
        v.store.set(_tag_head("v1"), dumps("0" * 40))
        assert v.checkout(tag="v1") is None

    def test_both_commit_and_tag_raises(self):
        v = Versioned()
        v.tag("v1")
        with pytest.raises(ValueError, match="exactly one"):
            v.checkout(v.current_commit, tag="v1")

    def test_neither_commit_nor_tag_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            Versioned().checkout()


class TestPeekTag:
    def test_peek_reads_the_tagged_value(self):
        v = Versioned()
        v.commit({"config": b"v1"})
        v.tag("v1")
        v.commit({"config": b"v2"})

        assert v.peek("config", tag="v1") == b"v1"
        assert v.get("config") == b"v2"

    def test_peek_missing_key_or_tag_is_none(self):
        v = Versioned()
        v.commit({"config": b"v1"})
        v.tag("v1")
        assert v.peek("absent", tag="v1") is None
        assert v.peek("config", tag="nope") is None

    def test_both_branch_and_tag_raises(self):
        v = Versioned()
        v.tag("v1")
        with pytest.raises(ValueError, match="exactly one"):
            v.peek("config", branch="main", tag="v1")

    def test_neither_branch_nor_tag_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            Versioned().peek("config")


class TestTagKeyLayout:
    """A tag is a branch head under a reserved name. That is the
    cross-version contract, so it is asserted directly."""

    def test_a_tag_is_a_branch_head_under_refs_tags(self):
        """Every kvgit decides reachability by walking branch heads. A
        tag stored as one is therefore read, and kept alive, by versions
        that predate tags — including their anchor-free admin sweep,
        which no version stamp in this repo could have held back. Change
        this key and that property is gone."""
        backend = Memory()
        v = Versioned(backend)
        tagged = v.tag("v1")

        assert backend.get(BRANCH_HEAD % "refs/tags/v1") == dumps(tagged)
        assert TAG_BRANCH_PREFIX == "refs/tags/"

    def test_tagging_does_not_change_the_storage_version(self):
        """A tagged store must still open under versions that predate
        tags, so nothing about a tag raises the stamp."""
        backend = Memory()
        v = Versioned(backend)
        v.commit({"x": b"1"})
        before = _version(backend)

        v.tag("v1")
        assert _version(backend) == before

    def test_info_record_survives_a_deep_clean(self):
        """The record lives under its own key kind that no sweep — this
        version's or an older one's — scans."""
        backend = Memory()
        v = Versioned(backend)
        v.tag("v1", info={"by": "ann"})
        v.deep_clean(min_age=0)
        assert backend.get(TAG_INFO_KEY % "v1") is not None
        assert v.tag_info("v1").info == {"by": "ann"}

    def test_delete_tag_removes_head_backup_and_record(self):
        """The prev-HEAD backup goes too. This code never writes one for
        a tag, but something treating the tag as an ordinary branch
        could have, and a backup outliving its head would resurrect the
        deleted commit under a later tag of the same name."""
        backend = Memory()
        v = Versioned(backend)
        v.commit({"x": b"1"})
        v.tag("v1")
        # Planted by hand: what an older client leaves behind if it
        # switches onto the tag and commits.
        backend.set(BRANCH_HEAD_PREV % "refs/tags/v1", dumps(v.current_commit))

        v.delete_tag("v1")

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
        v = Versioned(store_)
        v.create_branch("dev")
        v.tag("v1")

        assert v.list_branches() == ["dev", "main"]
        assert Versioned.branches(store_) == ["dev", "main"]
        assert v.tags() == {"v1": v.current_commit}

    def test_constructor_refuses_a_reserved_branch(self):
        with pytest.raises(ValueError, match="reserved for tags"):
            Versioned(Memory(), branch="refs/tags/v1")

    def test_create_branch_refuses_a_reserved_name(self):
        v = Versioned()
        with pytest.raises(ValueError, match="reserved for tags"):
            v.create_branch("refs/tags/v1")

    def test_switch_branch_refuses_a_reserved_name(self):
        v = Versioned()
        v.tag("v1")
        with pytest.raises(ValueError, match="reserved for tags"):
            v.switch_branch("refs/tags/v1")

    def test_delete_branch_refuses_a_reserved_name(self):
        v = Versioned()
        v.tag("v1")
        with pytest.raises(ValueError, match="reserved for tags"):
            v.delete_branch("refs/tags/v1")

    def test_peek_refuses_a_reserved_branch(self):
        v = Versioned()
        v.commit({"x": b"1"})
        v.tag("v1")
        with pytest.raises(ValueError, match="reserved for tags"):
            v.peek("x", branch="refs/tags/v1")


class TestVersionStamp:
    """The stamp helper still guards the v2 -> v3 chunk upgrade."""

    def test_stamping_is_monotonic(self):
        backend = Memory()
        backend.set(STORAGE_VERSION_KEY, dumps(3))
        _stamp_version_at_least(backend, 2)
        assert _version(backend) == 3

    def test_stamping_an_unstamped_store_writes_the_version(self):
        backend = Memory()
        _stamp_version_at_least(backend, 3)
        assert _version(backend) == 3

    def test_a_stale_writer_cannot_lower_the_stamp(self):
        """Read-then-set would let two first-time writers interleave so
        the lower version lands last, leaving a store whose stamp
        under-describes what is in it — exactly the store older readers
        are willing to open. The write is a CAS against the bytes the
        decision was made from, so a stale writer loses and re-reads."""

        class StaleReadStore(Memory):
            """Serves one stale read of the version stamp.

            Stands in for a writer that read the stamp before another
            writer raised it and only reached its own write afterwards.
            """

            def __init__(self) -> None:
                super().__init__()
                self.stale_reads = 1

            def get(self, key):
                if key == STORAGE_VERSION_KEY and self.stale_reads:
                    self.stale_reads -= 1
                    return dumps(2)
                return super().get(key)

        backend = StaleReadStore()
        backend.set(STORAGE_VERSION_KEY, dumps(4))  # the other writer won

        _stamp_version_at_least(backend, 3)

        assert backend.stale_reads == 0  # the stale value really was served
        assert _version(backend) == 4


class TestUnknownStorageVersionIsRefused:
    """Version hygiene, no longer what protects tags: code must not
    sweep a store whose layout it does not know."""

    def test_sweep_refuses_an_unknown_version(self):
        backend = Memory()
        Versioned(backend).tag("v1")
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
