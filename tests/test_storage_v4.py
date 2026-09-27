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

from kvgit import Staged, VersionedKV, text_merge
from kvgit.encoding import loads, safe_loads
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BLOB_PREFIX,
    BLOB_STORAGE_VERSION,
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    COMMIT_TIME,
    INFO_KEY,
    PARENT_COMMIT,
    ROOT_COMMIT,
    STORAGE_VERSION_KEY,
    _load_root,
    blob_key,
    clean_orphans,
    commit_hash,
    deep_clean,
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


def values(s: Staged) -> dict:
    return {k: s[k] for k in sorted(s.keys())}


def check_every_branch(store, manifest) -> None:
    """Every branch the older kvgit wrote still reads as it wrote it."""
    for name, expected in manifest["branches"].items():
        if "values" in expected:
            assert (
                values(Staged(VersionedKV(store, branch=name))) == (expected["values"])
            ), name


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
        for name, expected in manifest["branches"].items():
            v = VersionedKV(store, branch=name)
            assert v.current_commit == expected["head"]
            assert list(v.history(all_parents=True)) == expected["history"]

    def test_every_branch_descends_from_the_shared_root_commit(self):
        _, manifest = load_v3()
        for expected in manifest["branches"].values():
            assert ROOT_COMMIT in expected["history"]
        assert VersionedKV(Memory()).current_commit == ROOT_COMMIT

    def test_tags(self):
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        assert main.tags() == manifest["tags"]
        assert values(main.checkout(tag="v1")) == manifest["branches"]["main"]["values"]

    def test_chunked_branch(self):
        np = pytest.importorskip("numpy")
        from kvgit.codecs import compose
        from kvgit.codecs.numpy import NumpyCodec

        store, _ = load_v3()
        encoder, decoder = compose(NumpyCodec(min_bytes=64))
        sci = Staged(VersionedKV(store, branch="sci"), encoder=encoder, decoder=decoder)
        np.testing.assert_array_equal(sci["arr"], np.arange(4096, dtype="float64"))
        np.testing.assert_array_equal(sci["arr_copy"], sci["arr"])


class TestWriting:
    def test_first_commit_stamps_v4_and_extends_the_old_history(self):
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["count"] = 3
        main.commit()

        assert version(store) == BLOB_STORAGE_VERSION
        old = manifest["branches"]["main"]["history"]
        assert list(main.history(all_parents=True)) == [main.current_commit, *old]
        expected = dict(manifest["branches"]["main"]["values"], count=3)
        assert values(Staged(VersionedKV(store))) == expected

    def test_one_keyset_holds_both_kinds_of_blob(self):
        store, manifest = load_v3()
        head = manifest["branches"]["main"]["head"]
        old = entries(store, head)
        main = Staged(VersionedKV(store))
        main["count"] = 3
        main.commit()

        new = entries(store, main.current_commit)
        assert new["count"].blob == blob_key(pickled(3))
        assert new["count"].meta.created_at is None
        # An untouched entry is carried as it was stored, timestamp included.
        assert new["greeting"] == old["greeting"]
        assert not new["greeting"].blob.startswith(BLOB_PREFIX)

    def test_other_branches_and_tags_are_untouched(self):
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["count"] = 3
        main.commit()

        dev = VersionedKV(store, branch="dev")
        assert dev.current_commit == manifest["branches"]["dev"]["head"]
        assert main.tags() == manifest["tags"]
        check_every_branch(store, {"branches": {"dev": manifest["branches"]["dev"]}})

    def test_equal_bytes_share_one_blob(self):
        store, _ = load_v3()
        main = Staged(VersionedKV(store))
        main["copy_one"] = "hello"
        main["copy_two"] = "hello"
        main.commit()

        ptrs = main.versioned._load_keyset(main.current_commit)
        assert ptrs["copy_one"] == ptrs["copy_two"] == blob_key(pickled("hello"))
        # The legacy blob holding the same bytes is left as it is.
        assert ptrs["greeting"] != ptrs["copy_one"]
        assert main["greeting"] == main["copy_one"]

    def test_fork_from_a_legacy_head(self):
        store, manifest = load_v3()
        dev = Staged(VersionedKV(store, branch="dev"))
        fork = dev.create_branch("fork")
        fork["extra"] = "v4"
        fork.commit()
        assert values(fork) == dict(manifest["branches"]["dev"]["values"], extra="v4")


class TestMergingAcrossFormats:
    def test_legacy_and_content_pointers_to_equal_bytes_merge_clean(self):
        """Both sides changed ``notes`` to the same text: dev under a
        legacy pointer, main under a content pointer. The pointers differ,
        the bytes do not, so the merge is clean with no merge function."""
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["notes"] = manifest["branches"]["dev"]["values"]["notes"]
        main.commit()

        dev_head = manifest["branches"]["dev"]["head"]
        ours = main.versioned._load_keyset(main.current_commit)["notes"]
        theirs = main.versioned._load_keyset(dev_head)["notes"]
        assert ours.startswith(BLOB_PREFIX) and not theirs.startswith(BLOB_PREFIX)
        result = main.merge(dev_head)
        assert result.merged
        assert "notes" not in result.auto_merged_keys
        assert main["notes"] == "alpha\nBETA\ngamma\n"
        assert main["dev_only"] == "only on dev"

    def test_text_merge_across_formats(self):
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["notes"] = "ALPHA\nbeta\ngamma\n"
        main.commit()

        result = main.merge(
            manifest["branches"]["dev"]["head"], default_merge=text_merge()
        )
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
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["count"] = 3
        main.commit(info={"why": "check"})
        main.merge(manifest["branches"]["dev"]["head"], default_merge=text_merge())

        for commit in list(main.history(all_parents=True))[:2]:
            recomputed = commit_hash(
                tuple(loads(store.get(PARENT_COMMIT % commit))),
                loads(store.get(COMMIT_ROOT % commit)),
                loads(store.get(COMMIT_TIME % commit)),
                loads(store.get(INFO_KEY % commit))
                if store.get(INFO_KEY % commit) is not None
                else None,
            )
            assert recomputed == commit

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

        assert clean_orphans(store, min_age=0) == 1
        assert store.get(COMMIT_ROOT % orphan) is None
        assert store.get(f"{orphan}:gone_only") is None
        check_every_branch(store, manifest)

    def test_a_content_orphan_waits_for_deep_clean(self):
        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        scratch = main.create_branch("scratch")
        scratch["tmp"] = "throwaway"
        scratch.commit()
        pointer = blob_key(pickled("throwaway"))
        main.delete_branch("scratch")

        clean_orphans(store, min_age=0)
        assert store.get(pointer) is not None
        deep_clean(store, min_age=0, grace=0)
        assert store.get(pointer) is None
        check_every_branch(store, manifest)

    def test_deep_clean_keeps_everything_live(self):
        np = pytest.importorskip("numpy")
        from kvgit.codecs import compose
        from kvgit.codecs.numpy import NumpyCodec

        store, manifest = load_v3()
        main = Staged(VersionedKV(store))
        main["count"] = 3
        main.commit()
        live = dict(manifest["branches"]["main"]["values"], count=3)

        # An orphan holding the same bytes as a live key: shared content.
        shared = main.create_branch("shared")
        shared["also_three"] = 3
        shared.commit()
        main.delete_branch("shared")

        deep_clean(store, min_age=0, grace=0)
        assert store.get(blob_key(pickled(3))) is not None
        assert values(Staged(VersionedKV(store))) == live
        dev_only = {"dev": manifest["branches"]["dev"]}
        check_every_branch(store, {"branches": dev_only})
        assert values(main.checkout(tag="v1")) == manifest["branches"]["main"]["values"]
        encoder, decoder = compose(NumpyCodec(min_bytes=64))
        sci = Staged(VersionedKV(store, branch="sci"), encoder=encoder, decoder=decoder)
        np.testing.assert_array_equal(sci["arr"], np.arange(4096, dtype="float64"))
