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


class TestWritableMaterialization:
    """Materialized arrays must behave like the result of plain
    ``pickle.loads``: independent, writable, no shared memory with
    sibling keys or the underlying chunk bytes. The dedup is purely
    a storage-layer property; the runtime objects are private."""

    def test_materialized_array_is_writable(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.arange(2048, dtype="float64")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.flags["WRITEABLE"]
        out[0] = 999.0  # must not raise
        assert out[0] == 999.0

    def test_materialized_view_is_writable(self, codec_pair):
        """Views of a parent buffer also materialize as writable copies."""
        encoder, decoder = codec_pair
        parent = np.arange(2048, dtype="int64").reshape(32, 64)
        view = parent[:, 8:24]  # non-contig view
        sink = DictSink()
        out = decoder(encoder(view, sink), reader_for(sink))
        assert out.flags["WRITEABLE"]
        out[0, 0] = -1
        assert out[0, 0] == -1

    def test_two_keys_decode_to_independent_arrays(self, codec_pair):
        """Mutating one key's materialized array must not affect another
        key that shares the same underlying chunk."""
        encoder, decoder = codec_pair
        shared = np.arange(2048, dtype="float64")
        sink = DictSink()
        # Both keys reference the same chunk; sink dedups on hash.
        encoder({"a": shared, "b": shared}, sink)
        # Decode each key independently from the same reader.
        a_blob = encoder(shared, sink)
        b_blob = encoder(shared, sink)
        reader = reader_for(sink)
        a = decoder(a_blob, reader)
        b = decoder(b_blob, reader)
        assert not np.shares_memory(a, b)
        a[0] = -42.0
        assert b[0] == 0.0  # unchanged


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
        # Layout flag also survives the round-trip (matches pickle).
        assert out.flags["F_CONTIGUOUS"] and not out.flags["C_CONTIGUOUS"]

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


class TestMultiDimByteOffset:
    """Regression: a row-slice of a multi-row C-contig parent decoded
    incorrectly because the materializer applied the byte offset to
    a multi-dim ``view(uint8)`` of the reshaped root, which silently
    sliced the wrong axis. The tail slice of a wide DataFrame-shaped
    block is the canonical reproduction.
    """

    def test_tail_slice_of_2d_root_decodes_correctly(self, codec_pair):
        encoder, decoder = codec_pair
        # Parent shape mirrors a pandas float64 BlockManager block:
        # (n_columns, n_rows). A row slice cuts along the fast axis.
        parent = np.arange(3 * 1000, dtype="float64").reshape(3, 1000)
        # Last 100 "rows" — the slice byte_offset would be 900*8 = 7200,
        # well beyond the leading dim, which is the failure trigger.
        view = parent[:, 900:]
        sink = DictSink()
        out = decoder(encoder(view, sink), reader_for(sink))
        np.testing.assert_array_equal(out, parent[:, 900:])

    def test_dataframe_tail_round_trips(self, codec_pair):
        """Same shape as the agent benchmark: 3-column block, tail slice."""
        pd = pytest.importorskip("pandas")
        encoder, decoder = codec_pair
        rng = np.random.default_rng(0)
        df = pd.DataFrame({"x": rng.normal(size=10_000), "y": rng.normal(size=10_000)})
        sink = DictSink()
        # Encode the head block + the tail block in the same sink so
        # they share the parent buffer.
        head = df.iloc[:1000]
        tail = df.iloc[-1000:]
        encoder({"head": head, "tail": tail}, sink)
        # Now decode each — they must round-trip to their respective halves.
        head_blob = encoder(head, sink)
        tail_blob = encoder(tail, sink)
        out_head = decoder(head_blob, reader_for(sink))
        out_tail = decoder(tail_blob, reader_for(sink))
        pd.testing.assert_frame_equal(out_head, head)
        pd.testing.assert_frame_equal(out_tail, tail)


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


class TestUncommonDtypes:
    """Coverage for less-common ndarray dtypes. The question for each
    is "round-trips bit-for-bit?" — anything that doesn't either has
    to be fixed or has to be documented as falling back to plain
    pickle."""

    def test_complex128(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.arange(256, dtype="complex128") * (1 + 2j)
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    @pytest.mark.parametrize("unit", ["ns", "us", "ms", "s", "D"])
    def test_datetime64_units(self, codec_pair, unit):
        """Time unit (``ns``/``us``/``s``/``D`` etc.) is part of the
        dtype identity and must survive a round-trip."""
        encoder, decoder = codec_pair
        # Build via int64 view → datetime64; np.arange doesn't work
        # for datetime64 directly.
        arr = np.arange(256, dtype="int64").view(f"datetime64[{unit}]")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    @pytest.mark.parametrize("unit", ["ns", "us", "ms", "s"])
    def test_timedelta64_units(self, codec_pair, unit):
        encoder, decoder = codec_pair
        arr = np.arange(256, dtype="int64").view(f"timedelta64[{unit}]")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    def test_fixed_width_bytes(self, codec_pair):
        """``S`` dtype: fixed-width raw bytes."""
        encoder, decoder = codec_pair
        arr = np.array([b"hello", b"world", b"foo", b"bar"] * 64, dtype="S5")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    def test_fixed_width_unicode(self, codec_pair):
        """``U`` dtype: fixed-width unicode (4 bytes per char)."""
        encoder, decoder = codec_pair
        arr = np.array(["α", "β", "γ", "δ"] * 64, dtype="U2")
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    def test_structured_dtype_with_fields(self, codec_pair):
        """Structured / record dtype with named fields."""
        encoder, decoder = codec_pair
        dt = np.dtype([("x", "f4"), ("y", "i8"), ("name", "S8")])
        arr = np.zeros(256, dtype=dt)
        arr["x"] = np.arange(256, dtype="f4")
        arr["y"] = np.arange(256, dtype="i8") * 2
        arr["name"] = [f"row{i:03d}".encode() for i in range(256)]
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)

    @pytest.mark.parametrize("byteorder", [">i4", "<i4", ">f8", "<f8"])
    def test_explicit_byteorder(self, codec_pair, byteorder):
        encoder, decoder = codec_pair
        arr = np.arange(256, dtype=byteorder)
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.dtype == arr.dtype
        np.testing.assert_array_equal(out, arr)


class TestUnusualLayouts:
    """Layout corner cases: scalars, singletons, negative strides,
    broadcast views — anything the stride-and-offset reconstruction
    needs to handle without silent corruption."""

    def test_zero_d_scalar_array(self, codec_pair):
        """``np.array(5.0)`` — shape (), one element."""
        encoder, decoder = codec_pair
        arr = np.array(5.0, dtype="float64")
        assert arr.shape == ()
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        # Tiny by definition; falls back to pickle. Round-trip is what matters.
        assert out.shape == ()
        assert out.dtype == arr.dtype
        assert out == arr

    def test_singleton_dim_first(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.arange(2048, dtype="float64").reshape(1, 2048)
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.shape == arr.shape
        np.testing.assert_array_equal(out, arr)

    def test_singleton_dim_last(self, codec_pair):
        encoder, decoder = codec_pair
        arr = np.arange(2048, dtype="float64").reshape(2048, 1)
        sink = DictSink()
        out = decoder(encoder(arr, sink), reader_for(sink))
        assert out.shape == arr.shape
        np.testing.assert_array_equal(out, arr)

    def test_negative_stride_reverse(self, codec_pair):
        """``arr[::-1]`` is a view with negative stride. Our
        ``_root_and_offset`` sanity check (offset < 0) should kick in
        and treat the reversed view as its own root."""
        encoder, decoder = codec_pair
        parent = np.arange(2048, dtype="float64")
        reversed_view = parent[::-1]
        assert reversed_view.strides[0] < 0
        sink = DictSink()
        out = decoder(encoder(reversed_view, sink), reader_for(sink))
        np.testing.assert_array_equal(out, parent[::-1])

    def test_broadcast_view(self, codec_pair):
        """``np.broadcast_to`` produces a view with stride-0 axes —
        another non-canonical case that should fall back to a copy."""
        encoder, decoder = codec_pair
        seed = np.arange(2048, dtype="float64")
        bcast = np.broadcast_to(seed, (4, 2048))
        assert 0 in bcast.strides  # broadcast axis has stride 0
        sink = DictSink()
        out = decoder(encoder(bcast, sink), reader_for(sink))
        np.testing.assert_array_equal(out, np.broadcast_to(seed, (4, 2048)))

    def test_diagonal_view(self, codec_pair):
        """``np.diag`` returns a non-contig view of a 2-D parent."""
        encoder, decoder = codec_pair
        parent = np.arange(2048, dtype="float64").reshape(32, 64)[:32, :32]
        diag = np.diag(parent)  # view in modern numpy
        sink = DictSink()
        out = decoder(encoder(diag, sink), reader_for(sink))
        np.testing.assert_array_equal(out, np.diag(parent))


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
