"""Tests for ``kvgit.store(codecs=...)`` named-preset shortcut."""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")

import kvgit
from kvgit.codecs import scientific


class TestScientificFactory:
    def test_scientific_returns_chunk_aware_pair(self):
        encoder, decoder = scientific()
        # Sanity: chunked encoders take (value, sink); decoders take (blob, reader).
        import inspect

        enc_params = list(inspect.signature(encoder).parameters)
        dec_params = list(inspect.signature(decoder).parameters)
        assert len(enc_params) == 2
        assert len(dec_params) == 2


class TestStoreCodecsArg:
    def test_scientific_preset_round_trips_array(self):
        s = kvgit.store(codecs="scientific")
        arr = np.arange(2048, dtype="float64")
        s["x"] = arr
        s.commit()
        s.reset()
        s._cache.clear()
        np.testing.assert_array_equal(s["x"], arr)

    def test_scientific_preset_dedups(self):
        from kvgit.versioned.kv import CHUNK_PREFIX

        s = kvgit.store(codecs="scientific")
        big = np.arange(2048, dtype="float64")
        s["a"] = big
        s["b"] = big
        s.commit()
        chunk_keys = [k for k in s.versioned.store.keys() if k.startswith(CHUNK_PREFIX)]
        assert len(chunk_keys) == 1

    def test_unknown_preset_raises(self):
        with pytest.raises(ValueError, match="unknown codec preset 'bogus'"):
            kvgit.store(codecs="bogus")

    def test_codecs_with_explicit_encoder_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            kvgit.store(codecs="scientific", encoder=lambda v: v)

    def test_codecs_with_explicit_decoder_raises(self):
        with pytest.raises(ValueError, match="mutually exclusive"):
            kvgit.store(codecs="scientific", decoder=lambda b: b)

    def test_default_factory_unaffected(self):
        """No regression: store() without codecs= still uses pickle."""
        s = kvgit.store()
        assert s._encoder_chunked is False
        assert s._decoder_chunked is False
