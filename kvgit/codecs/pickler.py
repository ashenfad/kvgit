"""Pickle subclasses that route specific types through codecs.

The trick is pickle's ``persistent_id`` / ``persistent_load`` hooks:
they get a chance to replace any object with a serializable token
*before* pickle's normal reduction runs. We use that to externalize
ndarrays, DataFrames, Arrow tables, etc. as content-addressed chunks
plus a small token, while letting pickle handle everything else
(containers, primitives, custom classes).

Composition with arbitrarily nested structure works for free: pickle
walks the object graph and our hook intercepts only the types each
codec recognizes.
"""

from __future__ import annotations

import io
import pickle
from typing import Any, Iterable

from .base import ChunkReader, ChunkSink, Codec


class ChunkingPickler(pickle.Pickler):
    """Pickle hook that externalizes specific objects via codecs.

    Each visited object is offered to the codec chain in order. The
    first codec to return a non-None token wins; the token is wrapped
    as ``(codec.name, token)`` and emitted as a persistent ID.

    A per-pickler ``id()`` memo skips re-running the codec chain for
    repeated visits of the same object — important when one container
    holds the same ndarray under multiple keys.
    """

    def __init__(self, file, sink: ChunkSink, codecs: Iterable[Codec]) -> None:
        super().__init__(file, protocol=pickle.HIGHEST_PROTOCOL)
        self._sink = sink
        self._codecs: tuple[Codec, ...] = tuple(codecs)
        self._memo_by_id: dict[int, tuple[str, Any]] = {}

    def persistent_id(self, obj: Any) -> Any | None:
        oid = id(obj)
        cached = self._memo_by_id.get(oid)
        if cached is not None:
            return cached
        for codec in self._codecs:
            token = codec.try_externalize(obj, self._sink)
            if token is not None:
                pid = (codec.name, token)
                self._memo_by_id[oid] = pid
                return pid
        return None


class ChunkingUnpickler(pickle.Unpickler):
    """Pickle hook that materializes externalized objects via codecs.

    Resolves persistent IDs of the shape ``(codec_name, token)`` by
    looking up the codec by name and calling its ``materialize``.
    """

    def __init__(self, file, reader: ChunkReader, codecs: Iterable[Codec]) -> None:
        super().__init__(file)
        self._reader = reader
        self._codec_by_name: dict[str, Codec] = {c.name: c for c in codecs}

    def persistent_load(self, pid: Any) -> Any:
        if not (isinstance(pid, tuple) and len(pid) == 2):
            raise pickle.UnpicklingError(f"unexpected persistent_id shape: {pid!r}")
        name, token = pid
        codec = self._codec_by_name.get(name)
        if codec is None:
            raise pickle.UnpicklingError(
                f"codec {name!r} not registered. "
                f"Available: {sorted(self._codec_by_name)}. "
                "Register the matching codec when constructing the decoder."
            )
        return codec.materialize(token, self._reader)


def encode(value: Any, sink: ChunkSink, codecs: Iterable[Codec]) -> bytes:
    """Encode a value to bytes, emitting chunks to ``sink``."""
    buf = io.BytesIO()
    ChunkingPickler(buf, sink, codecs).dump(value)
    return buf.getvalue()


def decode(blob: bytes, reader: ChunkReader, codecs: Iterable[Codec]) -> Any:
    """Decode bytes back to a value, fetching chunks from ``reader``."""
    return ChunkingUnpickler(io.BytesIO(blob), reader, codecs).load()
