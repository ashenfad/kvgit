"""NumPy ndarray codec.

Externalizes the underlying buffer of an ndarray as a content-addressed
chunk; the token records the dtype, shape, and (for views) the slice
geometry needed to reconstruct the array against the chunked root
buffer.

Dedup story:

* Two arrays with identical bytes hash to the same chunk reference.
* A view (``arr.base is not None``) hashes its **root** buffer, not
  the slice. Multiple slices of the same parent therefore share one
  chunk on disk.
* Object-dtype arrays hold Python pointers, not data — they fall
  through to plain pickling (the elements may themselves be
  externalized by other codecs).

Materialized arrays are read-only because the underlying chunk bytes
are shared. Call ``.copy()`` to mutate.
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

        # Hash the root buffer's bytes. Use canonical C-order bytes so
        # two arrays with the same logical content (regardless of how
        # constructed) collide.
        if root.flags["C_CONTIGUOUS"]:
            ref = sink.put(memoryview(root).cast("B"))
        else:
            ref = sink.put(root.tobytes())

        return {
            "ref": ref,
            "root_shape": list(root.shape),
            "root_dtype": root.dtype.str,
            "shape": list(obj.shape),
            "dtype": obj.dtype.str,
            "strides": list(obj.strides),
            "offset": int(byte_offset),
        }

    def materialize(self, token: Any, reader: ChunkReader) -> Any:
        import numpy as np

        raw = reader.get(token["ref"])
        root_dtype = np.dtype(token["root_dtype"])
        # frombuffer over bytes returns a read-only ndarray sharing the
        # underlying buffer — the dedup-friendly default.
        root = np.frombuffer(raw, dtype=root_dtype).reshape(token["root_shape"])

        offset = token["offset"]
        out_dtype = np.dtype(token["dtype"])
        out_shape = tuple(token["shape"])
        out_strides = tuple(token["strides"])

        # Fast path: the array IS the root (identical shape/dtype, no offset).
        if (
            offset == 0
            and out_shape == root.shape
            and token["dtype"] == token["root_dtype"]
        ):
            return root

        # General path: reconstruct the view via stride tricks. We
        # cast through uint8 to apply a byte-level offset, then recast
        # to the target dtype before applying shape and strides.
        flat_bytes = root.view(np.uint8)
        if offset:
            flat_bytes = flat_bytes[offset:]
        typed = flat_bytes.view(out_dtype)
        if out_shape == typed.shape and out_strides == typed.strides:
            return typed
        return np.lib.stride_tricks.as_strided(
            typed,
            shape=out_shape,
            strides=out_strides,
            writeable=False,
        )
