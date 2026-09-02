"""Tests for tags: immutable names for commits, and GC roots.

Covers the tag API on ``VersionedKV`` and ``Staged``, the reachability
rule that keeps a tagged commit's ancestry alive, the anchor-free admin
paths, and the storage-version gate that stops older code from sweeping
a store whose tags it cannot see.
"""

import os
import tempfile

import pytest

import kvgit
from kvgit import MergeConflict, VersionedKV as Versioned, store
from kvgit.encoding import dumps, safe_loads
from kvgit.kv.memory import Memory
from kvgit.versioned import kv as kv_module
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    COMMIT_ROOT,
    STORAGE_VERSION_KEY,
    TAG_INFO_KEY,
    TAG_KEY,
    clean_orphans,
)


def _blob_keys(backend) -> set[str]:
    """Versioned blob keys (``<commit>:<user_key>``) currently in the store."""
    return {
        k
        for k in backend.keys()
        if isinstance(k, str)
        and ":" in k
        and not k.startswith("__")
        and "kvgit:" not in k
    }


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
        with pytest.raises(ValueError, match="already exists"):
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
        with pytest.raises(ValueError, match="does not exist"):
            v.tag("bad", at="0" * 40)

    def test_delete_unknown_tag_raises(self):
        v = Versioned()
        with pytest.raises(ValueError, match="does not exist"):
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
        assert {k for k in _blob_keys(backend) if k.endswith(":secret")}

    def test_deep_clean_keeps_a_tagged_commit(self):
        backend, v, tagged = self._store_with_tagged_orphan()

        assert v.deep_clean(min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None
        assert v.checkout(tag="v1").get("secret") == b"tagged value"

    def test_delete_tag_releases_the_commit(self):
        backend, v, tagged = self._store_with_tagged_orphan()

        v.delete_tag("v1")

        assert backend.get(TAG_KEY % "v1") is None
        assert backend.get(TAG_INFO_KEY % "v1") is None
        assert v.clean_orphans(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None
        assert not {k for k in _blob_keys(backend) if k.endswith(":secret")}

    def test_dangling_tag_keeps_nothing_alive(self):
        """A tag naming a commit the store does not have marks nothing.

        The store no longer says what that root pointed at, so the sweep
        has nothing to walk — the same rule an unresolvable branch HEAD
        gets.
        """
        backend, v, tagged = self._store_with_tagged_orphan()

        # Damage the tag: it now names a commit that is not in the store.
        backend.set(TAG_KEY % "v1", dumps("0" * 40))
        assert v.tag_info("v1").dangling is True

        assert v.clean_orphans(min_age=0) >= 1
        assert backend.get(COMMIT_ROOT % tagged) is None

    def test_dangling_tag_is_still_listed(self):
        """Omitting it would make damage look like deletion."""
        v = Versioned()
        v.tag("v1")
        v.store.set(TAG_KEY % "v1", dumps("0" * 40))
        assert v.tags() == {"v1": "0" * 40}

    def test_module_sweep_sees_tags_without_a_handle(self):
        backend, _v, tagged = self._store_with_tagged_orphan()
        assert clean_orphans(backend, min_age=0) == 0
        assert backend.get(COMMIT_ROOT % tagged) is not None


class TestAnchorFreeTagPaths:
    def test_delete_branches_respects_tags(self):
        """The admin sweep marks from tags like every other caller."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["keep"] = "tagged value"
            s.commit()
            s.tag("v1")
            s.versioned.store.close()

            kvgit.delete_branches("main", kind="disk", path=p, min_age=0)

            s2 = store(kind="disk", path=p)  # mints a fresh empty main
            assert s2.get("keep") is None
            assert s2.checkout(tag="v1")["keep"] == "tagged value"

    def test_delete_tags_removes_both_keys_and_sweeps(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["keep"] = "tagged value"
            s.commit()
            s.tag("v1")
            backend = s.versioned.store
            backend.close()

            kvgit.delete_branches("main", kind="disk", path=p, min_age=0)
            kvgit.delete_tags("v1", kind="disk", path=p, min_age=0)

            s2 = store(kind="disk", path=p)
            assert s2.tags() == {}
            assert s2.versioned.store.get(TAG_INFO_KEY % "v1") is None
            assert not {
                k for k in _blob_keys(s2.versioned.store) if k.endswith(":keep")
            }

    def test_delete_tags_unknown_name_is_a_noop(self):
        """Teardown is idempotent, like delete_branches."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            store(kind="disk", path=p)["x"] = "1"
            kvgit.delete_tags(["never", "existed"], kind="disk", path=p)
            assert store(kind="disk", path=p).tags() == {}

    def test_delete_tags_bare_string_is_one_name(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s.commit()
            s.tag("v1")
            s.versioned.store.close()

            kvgit.delete_tags("v1", kind="disk", path=p)

            assert store(kind="disk", path=p).tags() == {}

    def test_delete_tags_empty_names_early_return(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            kvgit.delete_tags([], kind="disk", path=p)
            assert not os.path.exists(p)


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
        v.store.set(TAG_KEY % "v1", dumps("0" * 40))
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


class TestStorageVersionGate:
    def test_store_stays_v3_until_the_first_tag(self):
        backend = Memory()
        v = Versioned(backend)
        v.commit({"x": b"1"})
        assert _version(backend) == 3

        v.tag("v1")
        assert _version(backend) == 4

    def test_stamp_lands_before_the_tag(self):
        """A tag in a store still claiming v3 is exactly what older code
        is willing to open and sweep, so the stamp is never written
        second."""
        backend = Memory()
        v = Versioned(backend)
        writes: list[str] = []
        original = backend.set

        def recording_set(key, value):
            writes.append(key)
            return original(key, value)

        backend.set = recording_set  # type: ignore[method-assign]
        v.tag("v1")

        assert writes.index(STORAGE_VERSION_KEY) < writes.index(TAG_INFO_KEY % "v1")

    def test_reader_without_v4_refuses_a_tagged_store(self, monkeypatch):
        backend = Memory()
        Versioned(backend).tag("v1")

        monkeypatch.setattr(kv_module, "SUPPORTED_READ_VERSIONS", frozenset({2, 3}))
        with pytest.raises(ValueError, match="storage version"):
            Versioned(backend)

    def test_sweep_without_v4_refuses_a_tagged_store(self, monkeypatch):
        """The whole point of the stamp: an older sweep would see every
        tagged commit as garbage."""
        backend = Memory()
        Versioned(backend).tag("v1")

        monkeypatch.setattr(kv_module, "SUPPORTED_READ_VERSIONS", frozenset({2, 3}))
        with pytest.raises(ValueError, match="storage version"):
            clean_orphans(backend, min_age=0)

    def test_anchor_free_delete_without_v4_refuses_a_tagged_store(self, monkeypatch):
        """No handle is constructed on that path, so it checks the
        version itself — before removing anything."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s.commit()
            s.tag("v1")
            s.versioned.store.close()

            monkeypatch.setattr(kv_module, "SUPPORTED_READ_VERSIONS", frozenset({2, 3}))
            with pytest.raises(ValueError, match="storage version"):
                kvgit.delete_branches("main", kind="disk", path=p)
            with pytest.raises(ValueError, match="storage version"):
                kvgit.delete_tags("v1", kind="disk", path=p)

            monkeypatch.undo()
            reopened = store(kind="disk", path=p)
            assert reopened.versioned.store.get(BRANCH_HEAD % "main") is not None
            assert reopened.tags() != {}


class TestStagedTagOps:
    def test_staged_tag_round_trip(self):
        s = store()
        s["x"] = "hello"
        s.commit()

        tagged = s.tag("v1", info={"by": "ann"})

        assert tagged == s.current_commit
        assert s.tags() == {"v1": s.current_commit}
        assert s.tag_info("v1").info == {"by": "ann"}

        s.delete_tag("v1")
        assert s.tags() == {}

    def test_staged_tag_names_the_commit_not_the_staging_buffer(self):
        s = store()
        s["x"] = "committed"
        s.commit()
        s["x"] = "staged only"

        s.tag("v1")

        assert s.checkout(tag="v1")["x"] == "committed"
        assert s["x"] == "staged only"

    def test_staged_checkout_tag_returns_staged(self):
        s = store()
        s["x"] = "tagged"
        s.commit()
        s.tag("v1")
        s["x"] = "later"
        s.commit()

        at_tag = s.checkout(tag="v1")
        assert isinstance(at_tag, kvgit.Staged)
        assert at_tag["x"] == "tagged"
        assert s.checkout(tag="nope") is None

    def test_staged_peek_tag(self):
        s = store()
        s["title"] = "first"
        s.commit()
        s.tag("v1")
        s["title"] = "second"
        s.commit()

        assert s.peek("title", tag="v1") == "first"
        with pytest.raises(ValueError, match="exactly one"):
            s.peek("title", branch="main", tag="v1")

    def test_staged_delete_tag_releases_the_commit(self):
        s = store()
        s["x"] = "base"
        s.commit()
        dev = s.create_branch("dev")
        dev["secret"] = "tagged value"
        dev.commit()
        dev.tag("v1")
        s.delete_branch("dev")

        backend = s.versioned.store
        assert s.versioned.clean_orphans(min_age=0) == 0

        s.delete_tag("v1")
        assert s.versioned.clean_orphans(min_age=0) >= 1
        assert not {k for k in _blob_keys(backend) if k.endswith(":secret")}
