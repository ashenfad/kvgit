"""Tests that ``Staged`` correctly drives a chunked codec end-to-end."""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")

from kvgit import Staged, VersionedKV
from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    CHUNK_PREFIX,
    STORAGE_VERSION,
    STORAGE_VERSION_KEY,
)
from kvgit.encoding import safe_loads


@pytest.fixture
def chunked():
    """Build a Staged on a fresh Memory store with the numpy codec."""
    encoder, decoder = compose(NumpyCodec(min_bytes=64))
    store = Memory()
    s = Staged(VersionedKV(store), encoder=encoder, decoder=decoder)
    return s, store


class TestArityDetection:
    def test_chunked_encoder_detected(self, chunked):
        s, _ = chunked
        assert s._encoder_chunked is True
        assert s._decoder_chunked is True

    def test_default_pickle_not_detected_as_chunked(self):
        store = Memory()
        s = Staged(VersionedKV(store))
        assert s._encoder_chunked is False
        assert s._decoder_chunked is False


class TestMergeWithChunkedCodec:
    """The wrapped merge fn re-encodes via plain pickle (no sink in scope).
    The chunked decoder must still read those plain blobs back, and the
    merge result must round-trip equal.

    Three-way merge in kvgit triggers when a Staged commits to a branch
    whose HEAD has advanced since this Staged read it — typically modeled
    in tests by two Staged instances over the same store + branch.
    """

    def _build_pair(self, store):
        encoder, decoder = compose(NumpyCodec(min_bytes=64))
        return Staged(VersionedKV(store), encoder=encoder, decoder=decoder)

    def test_custom_merge_fn_resolves_conflict_with_chunked_codec(self):
        store = Memory()

        # Seed with a baseline value.
        bootstrap = self._build_pair(store)
        bootstrap["arr"] = np.zeros(2048, dtype="float64")
        bootstrap.commit()

        # Two writers on the same branch, both reading the same base.
        s1 = self._build_pair(store)
        s2 = self._build_pair(store)
        s2.set_merge_fn("arr", lambda old, ours, theirs: ours + theirs)

        # s1 advances HEAD.
        s1["arr"] = np.ones(2048, dtype="float64")
        s1.commit()

        # s2's commit must three-way-merge against s1's HEAD: 1 + 2 = 3.
        s2["arr"] = np.full(2048, 2.0, dtype="float64")
        result = s2.commit()
        assert result.strategy == "three_way"

        # Re-read via the chunked decoder; the merged blob is plain pickle
        # but the chunked decoder handles plain pickle bytes naturally
        # (persistent_id simply never fires).
        s2.reset()
        s2._cache.clear()
        np.testing.assert_array_equal(s2["arr"], np.full(2048, 3.0, dtype="float64"))

    def test_merge_result_blob_has_no_chunks_meta(self):
        """Documented limitation: merge results are encoded inline,
        not as chunks. ``MetaEntry.chunks`` should be empty for the
        merge output."""
        store = Memory()

        bootstrap = self._build_pair(store)
        bootstrap["arr"] = np.zeros(2048, dtype="float64")
        bootstrap.commit()

        s1 = self._build_pair(store)
        s2 = self._build_pair(store)
        s2.set_merge_fn("arr", lambda old, ours, theirs: ours + theirs)

        s1["arr"] = np.ones(2048, dtype="float64")
        s1.commit()

        s2["arr"] = np.full(2048, 2.0, dtype="float64")
        result = s2.commit()
        assert result.strategy == "three_way"

        assert not s2._versioned._meta["arr"].chunks


class TestBackendCompatibility:
    def test_chunked_codec_rejects_non_kv_backend(self):
        """Pairing chunked codec with a non-KV Versioned must fail loud."""
        encoder, decoder = compose(NumpyCodec(min_bytes=64))

        class FakeVersioned:
            """Stub Versioned that isn't a VersionedKV — should be rejected."""

            store = None
            current_commit = "x"
            base_commit = "x"
            current_branch = "main"
            initial_commit = "x"
            last_merge_result = None

            def get(self, k):
                return None

            def get_many(self, *keys):
                return {}

            def keys(self):
                return ()

            def __contains__(self, k):
                return False

            def commit(self, *a, **kw):
                raise NotImplementedError

        with pytest.raises(TypeError, match="VersionedKV"):
            Staged(FakeVersioned(), encoder=encoder, decoder=decoder)


class TestRoundTripThroughStore:
    def test_round_trip(self, chunked):
        s, _ = chunked
        arr = np.arange(2048, dtype="float64")
        s["x"] = arr
        s.commit()
        # Fresh staged, force a re-read from underlying store.
        s.reset()
        s._cache.clear()
        np.testing.assert_array_equal(s["x"], arr)

    def test_dedup_across_keys_in_one_commit(self, chunked):
        s, store = chunked
        arr = np.arange(2048, dtype="float64")
        s["a"] = arr
        s["b"] = arr
        s["c"] = arr
        s.commit()
        # Exactly one chunk written under kvgit:chunk:
        chunk_keys = [k for k in store.keys() if k.startswith(CHUNK_PREFIX)]
        assert len(chunk_keys) == 1

    def test_dedup_across_commits(self, chunked):
        s, store = chunked
        arr = np.arange(2048, dtype="float64")
        s["a"] = arr
        s.commit()
        s["b"] = arr
        s.commit()
        chunk_keys = [k for k in store.keys() if k.startswith(CHUNK_PREFIX)]
        assert len(chunk_keys) == 1

    def test_view_dedups_against_parent(self, chunked):
        s, store = chunked
        parent = np.arange(8192, dtype="float64")
        child = parent[1000:2000]
        s["parent"] = parent
        s["child"] = child
        s.commit()
        chunk_keys = [k for k in store.keys() if k.startswith(CHUNK_PREFIX)]
        assert len(chunk_keys) == 1


class TestStorageVersioning:
    def test_chunked_write_stamps_v3(self, chunked):
        s, store = chunked
        arr = np.arange(2048, dtype="float64")
        s["x"] = arr
        s.commit()
        version_raw = store.get(STORAGE_VERSION_KEY)
        assert safe_loads(version_raw) == STORAGE_VERSION

    def test_pickle_only_writes_dont_force_v3_on_v2_store(self):
        """Opening a v2 store with v3 code keeps it v2 until a chunk lands."""
        from kvgit.encoding import dumps

        store = Memory()
        # Simulate an existing v2 store.
        store.set(STORAGE_VERSION_KEY, dumps(2))

        s = Staged(VersionedKV(store))  # default pickle, not chunked
        s["x"] = "hello"
        s.commit()
        # Still v2 — no chunked write occurred.
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == 2

    def test_v2_store_then_chunked_write_upgrades(self):
        from kvgit.encoding import dumps

        store = Memory()
        store.set(STORAGE_VERSION_KEY, dumps(2))

        # Plain pickle commit first.
        s_plain = Staged(VersionedKV(store))
        s_plain["plain"] = {"a": 1}
        s_plain.commit()
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == 2

        # Now open with chunked codec and write an array.
        encoder, decoder = compose(NumpyCodec(min_bytes=64))
        s_chunked = Staged(VersionedKV(store), encoder=encoder, decoder=decoder)
        s_chunked["arr"] = np.arange(2048, dtype="float64")
        s_chunked.commit()
        assert safe_loads(store.get(STORAGE_VERSION_KEY)) == STORAGE_VERSION

        # Both keys still readable.
        assert s_chunked["plain"] == {"a": 1}
        np.testing.assert_array_equal(
            s_chunked["arr"], np.arange(2048, dtype="float64")
        )


class TestMetaEntryChunks:
    def test_chunks_field_populated(self, chunked):
        s, _ = chunked
        s["x"] = np.arange(2048, dtype="float64")
        s.commit()
        meta = s._versioned._meta["x"]
        assert meta.chunks
        assert isinstance(meta.chunks, list)
        assert all(isinstance(r, str) for r in meta.chunks)

    def test_chunks_field_omitted_for_plain_pickle(self):
        store = Memory()
        s = Staged(VersionedKV(store))
        s["x"] = "hello"
        s.commit()
        meta = s._versioned._meta["x"]
        assert meta.chunks is None
