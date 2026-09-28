"""Tests for opening v2 stores and for chunked entries.

The contract:

* Current code opens a v2 store transparently, and opening writes
  nothing: the stamp moves only when a commit lands (see
  ``tests/test_storage_v4.py`` for stores written by a released kvgit).
* Chunked entries and plain entries coexist in the same store.
* Importing values from a store without chunks into one with a chunked
  codec naturally dedups equal buffers — the migration win.
"""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")

from support import worktree

from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec
from kvgit.encoding import dumps, safe_loads
from kvgit.errors import StorageVersionError
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BLOB_STORAGE_VERSION,
    CHUNK_PREFIX,
    CHUNK_STORAGE_VERSION,
    STORAGE_VERSION_KEY,
    VersionedKV,
)


def chunked_pair():
    return compose(NumpyCodec(min_bytes=64))


class TestV2StoreOpenedByV3Code:
    def test_open_existing_v2_store_no_stamp_change(self):
        store = Memory()
        store.set(STORAGE_VERSION_KEY, dumps(2))

        s = worktree(store)
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == 2
        assert list(s.keys()) == []

    def test_first_commit_stamps_v4_and_chunks_keep_it(self):
        store = Memory()
        store.set(STORAGE_VERSION_KEY, dumps(2))

        s = worktree(store)
        s["x"] = "plain"
        s.commit()
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == BLOB_STORAGE_VERSION

        encoder, decoder = chunked_pair()
        s2 = worktree(store, codec=(encoder, decoder))
        s2["arr"] = np.arange(2048, dtype="float64")
        s2.commit()
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == BLOB_STORAGE_VERSION
        assert BLOB_STORAGE_VERSION >= CHUNK_STORAGE_VERSION

    def test_mixed_pickle_and_chunked_entries_coexist(self):
        encoder, decoder = chunked_pair()
        store = Memory()
        s = worktree(store, codec=(encoder, decoder))

        s["plain_string"] = "a string"
        s["numeric_array"] = np.arange(2048, dtype="float64")
        s["small_dict"] = {"a": 1, "b": [2, 3]}
        s.commit()

        # Re-read — both kinds round-trip.
        s.discard()
        s._cache.clear()
        assert s["plain_string"] == "a string"
        np.testing.assert_array_equal(
            s["numeric_array"], np.arange(2048, dtype="float64")
        )
        assert s["small_dict"] == {"a": 1, "b": [2, 3]}


class TestUnsupportedVersion:
    def test_v1_or_unknown_version_raises(self):
        store = Memory()
        store.set(STORAGE_VERSION_KEY, dumps(1))
        with pytest.raises(StorageVersionError, match="storage version"):
            VersionedKV(store)


class TestImportMigration:
    def test_import_dedups_duplicate_arrays_from_v2(self):
        """The import-as-migration story: v2 store with N copies of an
        array becomes a v3 store with one chunk."""
        # Build a v2 source store with 5 keys all holding the same
        # logical array, each pickled independently → 5x the bytes.
        v2 = Memory()
        s_v2 = worktree(v2)
        arr = np.arange(2048, dtype="float64")
        for k in ("a", "b", "c", "d", "e"):
            s_v2[k] = arr
        s_v2.commit()

        # Confirm we really do have v2 (no chunk namespace populated).
        assert [k for k in v2.keys() if k.startswith(CHUNK_PREFIX)] == []

        # New v3 target store with chunked codec.
        encoder, decoder = chunked_pair()
        v3 = Memory()
        s_v3 = worktree(v3, codec=(encoder, decoder))

        # Read each key from source, write into target.
        for k in s_v2.keys():
            s_v3[k] = s_v2[k]
        s_v3.commit()

        # Result: one chunk in the target despite 5 keys.
        chunk_count = len([k for k in v3.keys() if k.startswith(CHUNK_PREFIX)])
        assert chunk_count == 1

        # And every key still round-trips equal.
        for k in ("a", "b", "c", "d", "e"):
            np.testing.assert_array_equal(s_v3[k], arr)
