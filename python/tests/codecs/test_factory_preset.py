"""Tests for the ``codec="scientific"`` preset."""

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


class TestScientificCodec:
    def test_scientific_preset_round_trips_array(self):
        wt = kvgit.open(codec="scientific")
        arr = np.arange(2048, dtype="float64")
        wt["x"] = arr
        wt.commit()
        wt.discard()
        wt._cache.clear()
        np.testing.assert_array_equal(wt["x"], arr)

    def test_scientific_preset_dedups(self):
        from kvgit.versioned.kv import CHUNK_PREFIX

        wt = kvgit.open(codec="scientific")
        big = np.arange(2048, dtype="float64")
        wt["a"] = big
        wt["b"] = big
        wt.commit()
        chunk_keys = [k for k in wt.repo.store.keys() if k.startswith(CHUNK_PREFIX)]
        assert len(chunk_keys) == 1

    def test_unknown_codec_raises(self):
        with pytest.raises(ValueError, match="unknown codec 'bogus'"):
            kvgit.open(codec="bogus")

    def test_default_codec_is_plain_pickle(self):
        wt = kvgit.open()
        assert wt._codec.encoder_chunked is False
        assert wt._codec.decoder_chunked is False
