"""Tests for the anchor-free admin deletion API (``kvgit.delete_branches``)
and the module-level ``clean_orphans`` it shares with ``VersionedKV``.
"""

import os
import tempfile

import kvgit
from kvgit import VersionedKV, store
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    _resolve_head,
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


class TestDeleteBranches:
    def test_delete_sole_branch(self):
        """Deleting the store's ONLY branch is legal — the exact case the
        instance method can't reach (no anchor left to open on)."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)  # only branch: main
            s["x"] = "1"
            s.commit()
            s.versioned.store.close()

            kvgit.delete_branches(["main"], kind="disk", path=p)

            # Reopening mints a fresh empty main.
            s2 = store(kind="disk", path=p)
            assert s2.list_branches() == ["main"]
            assert s2.get("x") is None

    def test_delete_batch(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["base"] = "ok"
            s.commit()
            s.create_branch("dev")["d"] = "1"
            s.create_branch("feature")["f"] = "1"
            assert set(s.list_branches()) == {"main", "dev", "feature"}
            s.versioned.store.close()

            kvgit.delete_branches(["dev", "feature"], kind="disk", path=p)

            s2 = store(kind="disk", path=p)
            assert s2.list_branches() == ["main"]
            assert s2.get("base") == "ok"

    def test_nonexistent_names_noop(self):
        """Teardown is idempotent: unknown names don't raise (unlike the
        instance method's ValueError)."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            store(kind="disk", path=p)["x"] = "1"  # materialize main
            kvgit.delete_branches(["never", "existed"], kind="disk", path=p)
            # main untouched
            assert store(kind="disk", path=p).list_branches() == ["main"]

    def test_delete_all_branches_legal(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s.commit()
            s.create_branch("dev")
            s.versioned.store.close()

            kvgit.delete_branches(["main", "dev"], kind="disk", path=p)

            s2 = store(kind="disk", path=p)
            assert s2.list_branches() == ["main"]  # fresh empty main

    def test_prev_head_cleanup_no_resurrection(self):
        """The prev-HEAD recovery backup must go with the head, or a
        same-named branch created later would 'recover' deleted state
        through _resolve_head's fallback."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s.commit()  # main genesis
            dev = s.create_branch("dev")
            dev["secret"] = "deleted data"
            dev.commit()
            dev["more"] = "moves prev-HEAD off genesis"
            dev.commit()
            s.versioned.store.close()

            kvgit.delete_branches(["dev"], kind="disk", path=p)

            s2 = store(kind="disk", path=p, branch="dev")  # recreate the name
            assert s2.get("secret") is None
            assert s2.get("more") is None
            assert s2.versioned.store.get(BRANCH_HEAD_PREV % "dev") is None

    def test_memory_kind_smoke(self):
        """kind='memory' constructs a fresh empty Memory each call, so
        this is a no-op — but it must not raise."""
        kvgit.delete_branches(["anything"], kind="memory")

    def test_bare_string_is_one_name(self):
        """A bare string is treated as ONE branch name — iterated, it
        would 'delete' each character as a branch and silently no-op
        the real request."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["base"] = "ok"
            s.commit()
            s.create_branch("dev")["d"] = "1"
            s.versioned.store.close()

            kvgit.delete_branches("dev", kind="disk", path=p)

            s2 = store(kind="disk", path=p)
            assert s2.list_branches() == ["main"]
            assert s2.get("base") == "ok"

    def test_empty_names_early_return(self):
        """An empty iterable returns before the backend opens — no
        store directory is created as a side effect."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            kvgit.delete_branches([], kind="disk", path=p)
            assert not os.path.exists(p)

    def test_min_age_zero_reclaims_immediately(self):
        """min_age=0 lets an admin who knows the store is quiet reclaim
        a just-committed branch's blobs in the same call, instead of
        waiting out the one-hour concurrent-writer guard."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            s = store(kind="disk", path=p)
            s["base"] = "ok"
            s.commit()
            dev = s.create_branch("dev")
            dev["secret"] = "unique-blob-value"
            dev.commit()
            backend = s.versioned.store
            assert {k for k in _blob_keys(backend) if k.endswith(":secret")}
            backend.close()

            kvgit.delete_branches("dev", kind="disk", path=p, min_age=0)

            s2 = store(kind="disk", path=p)
            after = {k for k in _blob_keys(s2.versioned.store) if k.endswith(":secret")}
            assert not after  # reclaimed without waiting out the guard

    def test_reopen_after_delete_not_locked(self):
        """The disk handle is released (finally-close), so the next
        opener on the same directory is not blocked."""
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "store")
            store(kind="disk", path=p).commit()
            kvgit.delete_branches(["main"], kind="disk", path=p)
            # Would wedge if delete_branches leaked the diskcache handle.
            store(kind="disk", path=p).commit()


class TestSharedOrphanSweep:
    def test_orphan_gc_reclaims_deleted_branch_blobs(self):
        """The deleted branch's unique blobs are gone after the shared
        sweep. Uses min_age=0 to bypass the age guard on fresh commits."""
        backend = Memory()
        v = VersionedKV(backend)  # main
        dev = v.create_branch("dev")
        dev.commit({"secret": b"unique-blob-value"})

        # A blob key referencing the dev-only value exists.
        before = {k for k in _blob_keys(backend) if k.endswith(":secret")}
        assert before

        # Mimic delete_branches' removals, then the shared sweep at min_age=0.
        backend.remove(BRANCH_HEAD % "dev")
        backend.remove(BRANCH_HEAD_PREV % "dev")
        removed = clean_orphans(backend, min_age=0)

        assert removed >= 1
        after = {k for k in _blob_keys(backend) if k.endswith(":secret")}
        assert not after  # reclaimed

    def test_module_clean_orphans_matches_instance(self):
        """The instance method delegates to the module function; both
        return the orphan count for the same store."""
        backend = Memory()
        v = VersionedKV(backend)
        dev = v.create_branch("dev")
        dev.commit({"k": b"v"})
        backend.remove(BRANCH_HEAD % "dev")
        backend.remove(BRANCH_HEAD_PREV % "dev")

        # No live branch references dev's commit anymore.
        assert _resolve_head(backend, "dev") is None
        assert clean_orphans(backend, min_age=0) >= 1
