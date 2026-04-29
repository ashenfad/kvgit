"""Tests for pandas DataFrames and Series via the numpy codec.

We deliberately don't ship a separate pandas codec: pickling a
DataFrame visits its block ndarrays as Python objects, which the
numpy codec catches via ``persistent_id`` before reduction. These
tests verify that pipeline end-to-end.
"""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec
from kvgit.codecs.pandas import PandasCodec  # alias-of-NumpyCodec

from conftest import DictSink, reader_for  # noqa: E402


@pytest.fixture
def codec_pair():
    return compose(NumpyCodec(min_bytes=64))


class TestSeries:
    def test_int_series_round_trip(self, codec_pair):
        encoder, decoder = codec_pair
        s = pd.Series(np.arange(2048, dtype="int64"), name="x")
        sink = DictSink()
        out = decoder(encoder(s, sink), reader_for(sink))
        pd.testing.assert_series_equal(out, s)
        assert sink.chunks  # something got externalized

    def test_float_series_round_trip(self, codec_pair):
        encoder, decoder = codec_pair
        s = pd.Series(np.random.default_rng(0).normal(size=2048), name="r")
        sink = DictSink()
        out = decoder(encoder(s, sink), reader_for(sink))
        pd.testing.assert_series_equal(out, s)


class TestDataFrame:
    def test_simple_round_trip(self, codec_pair):
        encoder, decoder = codec_pair
        rng = np.random.default_rng(0)
        df = pd.DataFrame(
            {
                "a": rng.normal(size=1024),
                "b": rng.integers(0, 100, size=1024).astype("int64"),
                "c": rng.normal(size=1024).astype("float32"),
            }
        )
        sink = DictSink()
        out = decoder(encoder(df, sink), reader_for(sink))
        pd.testing.assert_frame_equal(out, df)
        # At least one chunk per dtype-block (pandas groups same-dtype columns).
        assert len(sink.chunks) >= 1

    def test_string_column_handled(self, codec_pair):
        """Object/string columns pass through pickle gracefully."""
        encoder, decoder = codec_pair
        df = pd.DataFrame(
            {
                "x": np.arange(1024, dtype="float64"),
                "label": ["row_%d" % i for i in range(1024)],
            }
        )
        sink = DictSink()
        out = decoder(encoder(df, sink), reader_for(sink))
        pd.testing.assert_frame_equal(out, df)

    def test_multiindex_round_trip(self, codec_pair):
        encoder, decoder = codec_pair
        idx = pd.MultiIndex.from_product([["a", "b"], range(512)], names=["g", "i"])
        df = pd.DataFrame(
            {"x": np.arange(1024, dtype="float64")},
            index=idx,
        )
        sink = DictSink()
        out = decoder(encoder(df, sink), reader_for(sink))
        pd.testing.assert_frame_equal(out, df)


class TestDedup:
    def test_two_dataframes_with_same_blocks_share_chunks(self, codec_pair):
        """Two distinct DataFrames built from the same arrays dedup."""
        encoder, _ = codec_pair
        a = np.arange(2048, dtype="float64")
        df1 = pd.DataFrame({"x": a, "y": a + 1})
        df2 = pd.DataFrame({"x": a, "y": a + 1})
        sink = DictSink()
        encoder({"df1": df1, "df2": df2}, sink)
        # Same buffer contents → same hash → one chunk per distinct block.
        # Critically not 2x chunks.
        assert len(sink.chunks) <= 2

    def test_slice_of_dataframe_dedups(self, codec_pair):
        """Row slice of a DataFrame shares block buffers with parent."""
        encoder, _ = codec_pair
        rng = np.random.default_rng(0)
        df = pd.DataFrame(rng.normal(size=(8192, 4)), columns=list("abcd"))
        # iloc row slice — pandas often shares blocks via numpy views.
        slice_a = df.iloc[0:1000]
        slice_b = df.iloc[1000:2000]
        sink = DictSink()
        encoder(
            {"parent": df, "a": slice_a, "b": slice_b},
            sink,
        )
        # All three reference the same single block buffer (one float64 block).
        # We allow up to a few chunks for index buffers etc., but the dominant
        # data block must dedup — so chunk count should be small relative to
        # 3 full copies. df is 8192*4*8 = 256KB; 3 copies would be ~768KB.
        total_chunk_bytes = sum(len(v) for v in sink.chunks.values())
        assert total_chunk_bytes < 320 * 1024, (
            f"chunked output too large: {total_chunk_bytes} bytes; "
            "row-slice dedup against parent block is not happening"
        )


class TestAlias:
    def test_pandas_codec_is_numpy_codec(self):
        # Documented as an alias; ensure it stays one.
        assert PandasCodec is NumpyCodec
