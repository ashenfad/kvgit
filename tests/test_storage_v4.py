"""Storage v4 over a store an older kvgit wrote.

``fixtures/v3_store.json`` was written by the released kvgit 0.3.9 (see
``fixtures/make_v3_store.py``): blobs keyed ``<commit>:<key>``, keyset
entries carrying ``created_at``, commit hashes from the old scheme, a
tag, a merge, a chunked branch, and the orphaned commits of a deleted
branch. Everything here opens that store with this code.
"""

import base64
import json
import pickle
from pathlib import Path

import pytest

from kvgit import Repo, Worktree, text_merge
from kvgit.encoding import safe_loads
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BLOB_PREFIX,
    BLOB_STORAGE_VERSION,
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    ROOT_COMMIT,
    STORAGE_VERSION_KEY,
    VersionedKV,
    _load_root,
    blob_key,
    commit_hash,
)

FIXTURE = Path(__file__).parent / "fixtures" / "v3_store.json"


def load_v3() -> tuple[Memory, dict]:
    data = json.loads(FIXTURE.read_text())
    store = Memory()
    store.set_many({k: base64.b64decode(v) for k, v in data["store"].items()})
    return store, data["manifest"]


def version(store) -> int:
    return safe_loads(store.get(STORAGE_VERSION_KEY))


def legacy_blobs(store) -> set[str]:
    return {k for k in store.keys() if ":" in k and not k.startswith(("__", "kvgit:"))}


def content_blobs(store) -> set[str]:
    return {k for k in store.keys() if k.startswith(BLOB_PREFIX)}


def entries(store, commit: str) -> dict:
    return dict(Keyset(store, root=_load_root(store, commit)).items())


def values(mapping) -> dict:
    return {k: mapping[k] for k in sorted(mapping.keys())}


def main_of(store, **repo_options) -> Worktree:
    return Repo(store, **repo_options).worktree("main")


def check_every_branch(store, manifest) -> None:
    """Every branch the older kvgit wrote still reads as it wrote it."""
    repo = Repo(store)
    for name, expected in manifest["branches"].items():
        if "values" in expected:
            assert values(repo.snapshot(branch=name)) == expected["values"], name


def pickled(value) -> bytes:
    return pickle.dumps(value)


class TestTheFixture:
    def test_it_is_what_an_older_kvgit_wrote(self):
        store, manifest = load_v3()
        assert manifest["kvgit"] == "0.3.9"
        assert version(store) == 3
        assert legacy_blobs(store)
        assert not content_blobs(store)
        main = manifest["branches"]["main"]["head"]
        assert all(e.meta.created_at is not None for e in entries(store, main).values())


class TestReading:
    def test_opening_writes_nothing(self):
        store, manifest = load_v3()
        before = dict(store.items())
        check_every_branch(store, manifest)
        assert dict(store.items()) == before

    def test_heads_and_history_are_the_old_hashes(self):
        store, manifest = load_v3()
        repo = Repo(store)
        for name, expected in manifest["branches"].items():
            assert repo.head(name) == expected["head"]
            assert repo.worktree(name).head == expected["head"]
            assert [c.hash for c in repo.log(branch=name)] == expected["history"]

    def test_every_branch_descends_from_the_shared_root_commit(self):
        _, manifest = load_v3()
        for expected in manifest["branches"].values():
            assert ROOT_COMMIT in expected["history"]
        assert VersionedKV(Memory()).current_commit == ROOT_COMMIT

    def test_tags(self):
        store, manifest = load_v3()
        repo = Repo(store)
        assert repo.tags() == manifest["tags"]
        expected = manifest["branches"]["main"]["values"]
        assert values(repo.snapshot(tag="v1")) == expected

    def test_commit_records(self):
        store, manifest = load_v3()
        repo = Repo(store)
        head = repo.get_commit(manifest["branches"]["main"]["head"])
        assert head.info == {"step": "main edit"}
        assert head.parents and head.time is not None
        assert repo.get_commit(ROOT_COMMIT).parents == ()

    def test_chunked_branch(self):
        np = pytest.importorskip("numpy")
        from kvgit.codecs import compose
        from kvgit.codecs.numpy import NumpyCodec

        store, _ = load_v3()
        repo = Repo(store, codec=compose(NumpyCodec(min_bytes=64)))
        sci = repo.snapshot(branch="sci")
        np.testing.assert_array_equal(sci["arr"], np.arange(4096, dtype="float64"))
        np.testing.assert_array_equal(sci["arr_copy"], sci["arr"])


class TestWriting:
    def test_first_commit_stamps_v4_and_extends_the_old_history(self):
        store, manifest = load_v3()
        main = main_of(store)
        main["count"] = 3
        main.commit()

        assert version(store) == BLOB_STORAGE_VERSION
        old = manifest["branches"]["main"]["history"]
        assert [c.hash for c in main.repo.log(branch="main")] == [main.head, *old]
        expected = dict(manifest["branches"]["main"]["values"], count=3)
        assert values(main_of(store)) == expected

    def test_one_keyset_holds_both_kinds_of_blob(self):
        store, manifest = load_v3()
        head = manifest["branches"]["main"]["head"]
        old = entries(store, head)
        main = main_of(store)
        main["count"] = 3
        main.commit()

        new = entries(store, main.head)
        assert new["count"].blob == blob_key(pickled(3))
        assert new["count"].meta.created_at is None
        # An untouched entry is carried as it was stored, timestamp included.
        assert new["greeting"] == old["greeting"]
        assert not new["greeting"].blob.startswith(BLOB_PREFIX)

    def test_other_branches_and_tags_are_untouched(self):
        store, manifest = load_v3()
        main = main_of(store)
        main["count"] = 3
        main.commit()

        assert main.repo.head("dev") == manifest["branches"]["dev"]["head"]
        assert main.repo.tags() == manifest["tags"]
        check_every_branch(store, {"branches": {"dev": manifest["branches"]["dev"]}})

    def test_equal_bytes_share_one_blob(self):
        store, _ = load_v3()
        main = main_of(store)
        main["copy_one"] = "hello"
        main["copy_two"] = "hello"
        main.commit()

        ptrs = {k: e.blob for k, e in entries(store, main.head).items()}
        assert ptrs["copy_one"] == ptrs["copy_two"] == blob_key(pickled("hello"))
        # The legacy blob holding the same bytes is left as it is.
        assert ptrs["greeting"] != ptrs["copy_one"]
        assert main["greeting"] == main["copy_one"]

    def test_fork_from_a_legacy_head(self):
        store, manifest = load_v3()
        repo = Repo(store)
        repo.create_branch("fork", at=repo.head("dev"))
        fork = repo.worktree("fork")
        fork["extra"] = "v4"
        fork.commit()
        assert values(fork) == dict(manifest["branches"]["dev"]["values"], extra="v4")


class TestKeysSharingABlob:
    """Keys holding equal bytes point at one blob; every read path must
    still answer for each key."""

    def test_versioned_get_many_answers_every_key(self):
        v = VersionedKV(Memory())
        v.commit({"a": b"x", "b": b"x", "c": b"y"})
        assert v.get_many("a", "b", "c", "missing") == {
            "a": b"x",
            "b": b"x",
            "c": b"y",
        }
        assert v.get_many("a", "a") == {"a": b"x"}

    def test_worktree_get_many_decodes_each_key_on_its_own(self):
        wt = Repo(Memory()).worktree("main", create=True)
        wt["a"] = [1, 2]
        wt["b"] = [1, 2]
        wt.commit()
        reader = main_of(wt.repo.store)

        got = reader.get_many("a", "b")
        assert got == {"a": [1, 2], "b": [1, 2]}
        got["a"].append(3)
        assert reader["b"] == [1, 2]

    def test_snapshot_get_many_answers_every_key(self):
        wt = Repo(Memory()).worktree("main", create=True)
        wt["a"] = [1, 2]
        wt["b"] = [1, 2]
        wt.commit()
        snap = wt.repo.snapshot(branch="main")
        assert snap.get_many("a", "b") == {"a": [1, 2], "b": [1, 2]}
        assert snap.raw.get_many("a", "b") == {
            "a": pickled([1, 2]),
            "b": pickled([1, 2]),
        }

    def test_a_legacy_store_extended_with_a_shared_blob(self):
        store, _ = load_v3()
        main = main_of(store)
        main["twin_one"] = "twin"
        main["twin_two"] = "twin"
        main.commit()
        reader = main_of(store)
        got = reader.get_many("greeting", "twin_one", "twin_two")
        assert got == {"greeting": "hello", "twin_one": "twin", "twin_two": "twin"}


class TestMergingAcrossFormats:
    def test_legacy_and_content_pointers_to_equal_bytes_merge_clean(self):
        """Both sides changed ``notes`` to the same bytes: dev under a
        legacy pointer, main under a content pointer. The pointers differ,
        the bytes do not, so the merge is clean with no merge function.

        Main writes dev's stored bytes as they are rather than encoding
        the same string again: pickle's output depends on the Python
        version (its default protocol moved in 3.14), so re-encoding
        would not reliably reproduce what the older kvgit stored.
        """
        store, manifest = load_v3()
        dev_head = manifest["branches"]["dev"]["head"]
        main = VersionedKV(store)
        theirs = main._load_keyset(dev_head)["notes"]
        main.commit({"notes": store.get(theirs)})
        ours = main._load_keyset(main.current_commit)["notes"]
        assert ours.startswith(BLOB_PREFIX) and not theirs.startswith(BLOB_PREFIX)

        result = main.merge_heads(dev_head)
        assert result.merged
        assert "notes" not in result.auto_merged_keys
        merged = main_of(store)
        assert merged["notes"] == "alpha\nBETA\ngamma\n"
        assert merged["dev_only"] == "only on dev"

    def test_text_merge_across_formats(self):
        store, _ = load_v3()
        main = main_of(store)
        main["notes"] = "ALPHA\nbeta\ngamma\n"
        main.commit()

        result = main.merge(branch="dev", default_merge=text_merge())
        assert result.merged
        assert main["notes"] == "ALPHA\nBETA\ngamma\n"

    def test_two_writers_making_the_same_change_merge_on_pointers(self):
        store, _ = load_v3()
        first = VersionedKV(store)
        second = VersionedKV(store)
        first.commit({"same": b"bytes"})
        result = second.commit({"same": b"bytes"})

        assert result.merged
        assert result.strategy == "three_way"
        assert "same" not in result.auto_merged_keys
        assert VersionedKV(store).get("same") == b"bytes"


class TestHonestCommitHash:
    def test_the_hash_is_recomputable_from_what_is_stored(self):
        store, _ = load_v3()
        main = main_of(store)
        main["count"] = 3
        main.commit(info={"why": "check"})
        main.merge(branch="dev", default_merge=text_merge())

        for record in list(main.repo.log(branch="main"))[:2]:
            recomputed = commit_hash(
                record.parents, record.root, record.time, record.info
            )
            assert recomputed == record.hash

    def test_a_commit_rewrites_nothing_already_stored(self):
        """Every key a commit writes is new or holds the bytes it had, so
        everything but branch heads can be cached forever."""
        store, _ = load_v3()
        main = VersionedKV(store)
        main.commit({"k": b"v"})
        base = main.current_commit
        before = dict(store.items())

        # The same change on the same parent, twice over.
        main.reset_to(base)
        main.commit({"k": b"v"})
        other = VersionedKV(store, commit_hash=base)
        other.commit({"k": b"v"}, on_conflict="abandon")

        mutable = (BRANCH_HEAD.replace("%s", ""), BRANCH_HEAD_PREV.replace("%s", ""))
        changed = [
            k
            for k, v in before.items()
            if store.get(k) != v and not k.startswith(mutable)
        ]
        assert changed == []

    def test_new_entries_carry_no_timestamp(self):
        v = VersionedKV(Memory())
        v.commit({"k": b"v"})
        (entry,) = entries(v.store, v.current_commit).values()
        assert entry.meta.created_at is None


class TestSweepingAMixedStore:
    def test_clean_orphans_takes_a_legacy_orphan_and_its_blobs(self):
        store, manifest = load_v3()
        orphan = manifest["orphan"]
        assert store.get(f"{orphan}:gone_only") is not None

        assert Repo(store).gc(min_age=0) == 1
        assert store.get(COMMIT_ROOT % orphan) is None
        assert store.get(f"{orphan}:gone_only") is None
        check_every_branch(store, manifest)

    def test_a_content_orphan_goes_with_its_commit(self):
        store, manifest = load_v3()
        repo = Repo(store)
        repo.create_branch("scratch", at=repo.head("main"))
        scratch = repo.worktree("scratch")
        scratch["tmp"] = "throwaway"
        scratch.commit()
        pointer = blob_key(pickled("throwaway"))
        repo.delete_branch("scratch")

        repo.gc(min_age=0)
        assert store.get(pointer) is None
        check_every_branch(store, manifest)

    def test_deep_clean_keeps_everything_live(self):
        np = pytest.importorskip("numpy")
        from kvgit.codecs import compose
        from kvgit.codecs.numpy import NumpyCodec

        store, manifest = load_v3()
        repo = Repo(store)
        main = repo.worktree("main")
        main["count"] = 3
        main.commit()
        live = dict(manifest["branches"]["main"]["values"], count=3)

        # An orphan holding the same bytes as a live key: shared content.
        repo.create_branch("shared", at=main.head)
        shared = repo.worktree("shared")
        shared["also_three"] = 3
        shared.commit()
        repo.delete_branch("shared")

        repo.gc(min_age=0, deep=True)
        assert store.get(blob_key(pickled(3))) is not None
        assert values(main_of(store)) == live
        dev_only = {"dev": manifest["branches"]["dev"]}
        check_every_branch(store, {"branches": dev_only})
        expected = manifest["branches"]["main"]["values"]
        assert values(repo.snapshot(tag="v1")) == expected
        sci = Repo(store, codec=compose(NumpyCodec(min_bytes=64))).snapshot(
            branch="sci"
        )
        np.testing.assert_array_equal(sci["arr"], np.arange(4096, dtype="float64"))
