"""NumPy ndarray codec.

Externalizes the underlying buffer of an ndarray as a content-addressed
chunk; the token records the dtype, shape, layout order, and (for
views) the slice geometry needed to reconstruct the array against the
chunked root buffer.

Dedup story:

* Two arrays with identical bytes hash to the same chunk reference.
* A view (``arr.base is not None``) hashes its **root** buffer, not
  the slice. Multiple slices of the same parent therefore share one
  chunk on disk.
* Both C-contiguous and F-contiguous roots are stored verbatim
  (zero-copy via ``memoryview``) and the layout order is recorded
  so the strides recovered for any view remain valid against the
  exact bytes we wrote.
* Roots with non-canonical layouts (neither C- nor F-contig — rare,
  usually only created via ``as_strided`` abuse) are not safe to
  reuse as a shared buffer; the codec falls back to a C-contiguous
  copy of the input array, treating it as its own root. Dedup
  against the original parent is lost in that case.
* Object-dtype arrays hold Python pointers, not data — they fall
  through to plain pickling (the elements may themselves be
  externalized by other codecs).

Materialized arrays are independent, writable copies — same mutation
semantics as plain ``pickle.loads``. The dedup happens at the storage
layer; reads always allocate a fresh array. (Reads pay one memcpy per
key, equivalent to pickle's allocate-and-copy. The savings come from
storing each unique buffer once instead of N times.)
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import numpy as np

from .base import ChunkReader, ChunkSink

# Below this size, the chunk indirection costs more than the savings.
# Tunable via constructor; this default is conservative for IndexedDB
# where each entry has hundreds of bytes of fixed overhead.
DEFAULT_MIN_BYTES = 1024


def _root_and_offset(arr: "np.ndarray") -> tuple["np.ndarray", int]:
    """Walk ``.base`` to the root buffer; return (root, byte_offset).

    Returns ``(arr, 0)`` if the .base chain leads somewhere we can't
    safely interpret as a parent buffer (foreign owner, mismatched
    address arithmetic).
    """
    import numpy as np

    root = arr
    while isinstance(root.base, np.ndarray):
        root = root.base

    # If the root is identical to the input, no view computation needed.
    if root is arr:
        return arr, 0

    try:
        arr_addr = arr.__array_interface__["data"][0]
        root_addr = root.__array_interface__["data"][0]
    except (KeyError, AttributeError, TypeError):
        return arr, 0

    offset = arr_addr - root_addr
    if offset < 0 or offset + arr.nbytes > root.nbytes:
        # The view's memory extends outside the root we walked to —
        # likely a strided view with negative strides or some unusual
        # layout. Treat the array as its own root for safety.
        return arr, 0
    return root, offset


class NumpyCodec:
    """Codec for :class:`numpy.ndarray` instances."""

    name = "np"

    def __init__(self, min_bytes: int = DEFAULT_MIN_BYTES) -> None:
        self.min_bytes = min_bytes

    def try_externalize(self, obj: Any, sink: ChunkSink) -> Any | None:
        try:
            import numpy as np
        except ImportError:
            return None
        if not isinstance(obj, np.ndarray):
            return None
        # Object/struct-with-object dtypes contain Python pointers,
        # not data — let pickle handle element-wise so other codecs
        # can fire on the elements.
        if obj.dtype.hasobject:
            return None

        is_view = isinstance(obj.base, np.ndarray)
        # Small standalone arrays: chunk overhead exceeds savings.
        if not is_view and obj.nbytes < self.min_bytes:
            return None

        root, byte_offset = _root_and_offset(obj)

        # Pick a layout we can faithfully round-trip. C-contig and
        # F-contig roots are both contiguous blocks of bytes — we
        # store them verbatim and tag the order so materialize can
        # reshape correctly. A non-contiguous root would force us to
        # canonicalize the bytes (via ``tobytes()``) but the recorded
        # strides would no longer describe that byte layout, leading
        # to data corruption on reconstruction. Fall back to a
        # C-contig copy of ``obj`` instead, treating it as its own
        # root (loses dedup against the original parent for this
        # branch, but keeps correctness).
        if root.flags["C_CONTIGUOUS"]:
            ref = sink.put(memoryview(root).cast("B"))
            order = "C"
        elif root.flags["F_CONTIGUOUS"]:
            # ``memoryview.cast`` only accepts C-contig sources, so
            # collapse to a 1-D view in memory order. ``ravel(order='K')``
            # is zero-copy when strides are positive (always true for
            # F-contig arrays produced via numpy's normal APIs), and
            # the resulting 1-D view is itself C-contig.
            ref = sink.put(memoryview(np.ravel(root, order="K")).cast("B"))
            order = "F"
        else:
            canonical = np.ascontiguousarray(obj)
            ref = sink.put(memoryview(canonical).cast("B"))
            return {
                "ref": ref,
                "root_shape": list(canonical.shape),
                "root_dtype": canonical.dtype.str,
                "shape": list(obj.shape),
                "dtype": obj.dtype.str,
                "strides": list(canonical.strides),
                "offset": 0,
                "order": "C",
            }

        return {
            "ref": ref,
            "root_shape": list(root.shape),
            "root_dtype": root.dtype.str,
            "shape": list(obj.shape),
            "dtype": obj.dtype.str,
            "strides": list(obj.strides),
            "offset": int(byte_offset),
            "order": order,
        }

    def materialize(self, token: Any, reader: ChunkReader) -> Any:
        import numpy as np

        raw = reader.get(token["ref"])
        root_dtype = np.dtype(token["root_dtype"])
        # ``order`` defaults to 'C' for tokens written before the field
        # existed (those tokens are only correct for C-contig roots,
        # which is the only path the older code took without bugs).
        order = token.get("order", "C")

        offset = token["offset"]
        out_dtype = np.dtype(token["dtype"])
        out_shape = tuple(token["shape"])
        out_strides = tuple(token["strides"])
        root_shape = tuple(token["root_shape"])

        # Build a read-only view of the requested data first (zero-copy
        # against the chunk bytes), then ``np.array(view)`` allocates a
        # fresh, writable, independent copy. This matches plain
        # ``pickle.loads`` semantics: the array the caller gets back is
        # safe to mutate and won't surprise neighbouring keys that share
        # the same chunk on the underlying store. The dedup story still
        # holds — chunks are stored once on disk, decoded into private
        # copies on demand.

        # Fast path: the array IS the root (identical shape/dtype, no
        # offset). Reshape the bytes in the recorded memory order.
        if (
            offset == 0
            and out_shape == root_shape
            and token["dtype"] == token["root_dtype"]
        ):
            view = np.frombuffer(raw, dtype=root_dtype).reshape(root_shape, order=order)
            return np.array(view)

        # General path: reconstruct the view via stride tricks against
        # the raw bytes. Critically, we go through a 1-D uint8 view
        # of ``raw`` rather than a multi-dim view of the reshaped
        # root. ``ndarray.view(uint8)`` on a multi-dim array preserves
        # the leading dimensions, so ``[offset:]`` would silently
        # slice the wrong axis when the root has rank > 1 — producing
        # data corruption for any non-trivial offset (e.g., a
        # row-slice tail of a multi-column DataFrame block).
        flat_bytes = np.frombuffer(raw, dtype=np.uint8)
        if offset:
            flat_bytes = flat_bytes[offset:]
        typed = flat_bytes.view(out_dtype)
        view = np.lib.stride_tricks.as_strided(
            typed,
            shape=out_shape,
            strides=out_strides,
            writeable=False,
        )
        # ``np.array`` honours the view's strides while allocating
        # contiguous, writable storage — exactly the shape/contents
        # the caller would get from a fresh pickle.loads.
        return np.array(view)
