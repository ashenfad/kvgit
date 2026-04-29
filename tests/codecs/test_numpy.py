"""Tests for the NumpyCodec."""

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")

from dataclasses import dataclass

from kvgit.codecs import compose
from kvgit.codecs.numpy import NumpyCodec

from conftest import DictSink, reader_for


@dataclass
class _Bundle:
    """Module-scope so pickle can resolve it during round-trip tests."""

    features: "np.ndarray"
    labels: "np.ndarray"


@pytest.fixture
def codec():
    # Lower MIN_BYTES so small test arrays still get externalized.
    return NumpyCodec(min_bytes=64)


@pytest.fixture
def codec_pair(codec):
    return compose(codec)


class TestRoundTrip:
    @pytest.mark.parametrize(
        "dtype",
        ["float64", "float32", "int64", "int32", "int8", "uint8", "bool", "complex64"],
    )
    def test_dtype_round_trip(self, codec_pair, dtype):
        encoder, decoder = codec_pair
        arr = (
            np.arange(256, dtype=dtype) if dtype != "bool" else np.arange(256) % 2 == 0
        )
        if dtype == "bool":
            arr = arr.astype("bool")
        sink = DictSink()
        blob = encoder(arr, sink)
        out = decoder(blob, reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    @pytest.mark.parametrize(
        "shape",
        [(1024,), (32, 32), (4, 8, 16), (2, 3, 4, 5)],
    )
    def test_shape_round_trip(self, codec_pair, shape):
        encoder, decoder = codec_pair
        arr = np.arange(int(np.prod(shape))).reshape(shape).astype("float64")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.shape == shape
        np.testing.assert_array_equal(out, arr)

    def test_endian(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.arange(128, dtype=">i4")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    def test_empty_array(self, codec_pair):
        # Empty arrays — even chunked, must round-trip.
        encoder, decoder = codec_pair
        arr = np.array([], dtype="float64")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        np.testing.assert_array_equal(out, arr)
        assert out.dtype == arr.dtype


class TestSmallArraysSkipped:
    def test_tiny_standalone_passes_through_pickle(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.array([1, 2, 3], dtype="int32")  # 12 bytes — well below 64
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        # No chunks, but value still round-trips via pickle.
        assert sink.chunks == {}
        np.testing.assert_array_equal(out, arr)


class TestDedup:
    def test_identical_arrays_share_chunk(self, codec_pair):
        encoder, _ = codec_pair
        a = np.arange(1000, dtype="int64")
        b = np.arange(1000, dtype="int64")  # different ndarray, same content
        sink = DictSink()
        encoder({"a": a, "b": b}, sink)
        assert len(sink.chunks) == 1

    def test_different_content_different_chunks(self, codec_pair):
        encoder, _ = codec_pair
        a = np.arange(1000, dtype="int64")
        b = np.arange(1000, dtype="int64") + 1
        sink = DictSink()
        encoder({"a": a, "b": b}, sink)
        assert len(sink.chunks) == 2


class TestViews:
    def test_slice_view_dedups_to_root(self, codec_pair):
        """A view shares the root buffer's chunk."""
        encoder, decoder = codec_pair
        parent = np.arange(2048, dtype="int64")
        child = parent[100:200]
        assert child.base is parent
        sink = DictSink()
        encoder({"parent": parent, "child": child}, sink)
        # Both reference the same root chunk.
        assert len(sink.chunks) == 1
        # Round-trip: child reconstructs to data-equal slice.
        blob = encoder(child, sink)
        out_child = decoder(blob, reader_for(sink))
        np.testing.assert_array_equal(out_child, parent[100:200])
        assert out_child.shape == (100,)
        assert out_child.dtype == parent.dtype

    def test_2d_row_slice_view(self, codec_pair):
        encoder, decoder = codec_pair
        parent = np.arange(64 * 16, dtype="float32").reshape(64, 16)
        child = parent[8:24]  # row range — typically a view in numpy
        assert child.base is not None
        sink = DictSink()
        encoder({"parent": parent, "child": child}, sink)
        assert len(sink.chunks) == 1
        # Round-trip child.
        sink2 = DictSink()
        blob = encoder(child, sink2)
        # Read via combined reader (has both buffers; sink2 is enough though).
        out = decoder(blob, reader_for(sink2))
        np.testing.assert_array_equal(out, parent[8:24])
        assert out.shape == (16, 16)
        assert out.dtype == parent.dtype

    def test_transposed_view_round_trips(self, codec_pair):
        encoder, decoder = codec_pair
        parent = np.arange(1024, dtype="float64").reshape(32, 32)
        view = parent.T  # transpose — same data, different strides
        sink = DictSink()
        out = decoder(encoder(view, sink), reader_for(sink))
        # Logical content matches.
        np.testing.assert_array_equal(out, parent.T)


class TestNonContiguous:
    def test_non_contiguous_array_round_trips(self, codec_pair):
        encoder, decoder = codec_pair
        big = np.arange(1024, dtype="int64").reshape(32, 32)
        sliced = big[:, ::2]  # non-contig view
        assert not sliced.flags["C_CONTIGUOUS"]
        sink = DictSink()
        out = decoder(encoder(sliced, sink), reader_for(sink))
        np.testing.assert_array_equal(out, big[:, ::2])


class TestFortranOrder:
    """F-contig roots must round-trip without data corruption.

    Regression: an earlier version of the codec stored
    ``root.tobytes()`` (always C-ordered) for non-C-contig roots but
    recorded ``obj.strides`` from the original layout. Applying
    F-strides to C-ordered bytes during materialization produced
    garbage. The fix records the layout order alongside the bytes.
    """

    def test_standalone_f_contig_round_trips(self, codec_pair):
        encoder, decoder = codec_pair
        f = np.arange(64, dtype="float64").reshape(8, 8, order="F")
        assert f.flags["F_CONTIGUOUS"] and not f.flags["C_CONTIGUOUS"]
        sink = DictSink()
        out = decoder(encoder(f, sink), reader_for(sink))
        np.testing.assert_array_equal(out, f)

    def test_view_of_f_contig_parent_round_trips(self, codec_pair):
        """The bug case: row-slice of an F-contig parent.

        Pre-fix this stored C-ordered parent bytes (via tobytes) but
        recorded F-strides on the view; reapplying F-strides to the
        C-ordered buffer produced wrong values on read.
        """
        encoder, decoder = codec_pair
        parent = np.arange(64, dtype="float64").reshape(8, 8, order="F")
        assert parent.flags["F_CONTIGUOUS"] and not parent.flags["C_CONTIGUOUS"]
        view = parent[1:5]
        assert isinstance(view.base, np.ndarray)
        sink = DictSink()
        out = decoder(encoder(view, sink), reader_for(sink))
        np.testing.assert_array_equal(out, parent[1:5])

    def test_view_of_f_contig_dedups_to_parent_chunk(self, codec_pair):
        """F-contig parent and its view must share one chunk."""
        encoder, _ = codec_pair
        parent = np.arange(2048, dtype="float64").reshape(32, 64, order="F")
        view = parent[4:20]
        sink = DictSink()
        encoder({"parent": parent, "view": view}, sink)
        assert len(sink.chunks) == 1

    def test_column_slice_of_f_contig_round_trips(self, codec_pair):
        encoder, decoder = codec_pair
        parent = np.arange(64, dtype="float64").reshape(8, 8, order="F")
        col = parent[:, 2:5]  # contiguous in F-memory
        sink = DictSink()
        out = decoder(encoder(col, sink), reader_for(sink))
        np.testing.assert_array_equal(out, parent[:, 2:5])


class TestPathologicalRoots:
    """Roots that are neither C- nor F-contig fall back to per-obj
    storage. Dedup against the parent is lost but data must still be
    correct."""

    def test_non_contig_root_falls_back_safely(self, codec_pair):
        """A view whose .base chain ends at a non-contig array."""
        encoder, decoder = codec_pair
        # Build a non-contig array via as_strided that has no parent —
        # then take a view of it. Walking .base lands on the non-contig
        # one; the codec must canonicalize and round-trip correctly.
        base = np.arange(64, dtype="float64")
        weird = np.lib.stride_tricks.as_strided(
            base, shape=(4, 4), strides=(16, 8), writeable=False
        )  # writes out non-canonical layout (still readable)
        view = weird[1:3]
        sink = DictSink()
        out = decoder(encoder(view, sink), reader_for(sink))
        np.testing.assert_array_equal(out, weird[1:3])


class TestObjectDtype:
    def test_object_dtype_passes_through(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.array(["hello", "world", 42, [1, 2]], dtype=object)
        sink = DictSink()
        blob = encoder(arr, sink)
        # No chunk for an object array — pickled element-wise instead.
        assert sink.chunks == {}
        out = decoder(blob, reader_for(sink))
        assert list(out) == list(arr)


class TestIntegrationWithContainers:
    def test_dict_of_arrays(self, codec_pair):
        encoder, decoder = codec_pair
        data = {
            "x": np.arange(2048, dtype="float64"),
            "y": np.arange(2048, dtype="float64") * 2,
            "label": "foo",
        }
        sink = DictSink()
        out = decoder(encoder(data, sink), reader_for(sink))
        np.testing.assert_array_equal(out["x"], data["x"])
        np.testing.assert_array_equal(out["y"], data["y"])
        assert out["label"] == "foo"

    def test_nested_dataclass_like(self, codec_pair):
        b = _Bundle(
            features=np.arange(4096, dtype="float32").reshape(64, 64),
            labels=np.arange(64, dtype="int32"),
        )
        encoder, decoder = codec_pair
        sink = DictSink()
        out = decoder(encoder(b, sink), reader_for(sink))
        np.testing.assert_array_equal(out.features, b.features)
        np.testing.assert_array_equal(out.labels, b.labels)
