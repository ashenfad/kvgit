"""Codec protocols for chunked encoding.

A *codec* recognizes a specific value type during encoding, externalizes
its bulk data as content-addressed chunks, and emits a small
picklable token in place of the value. On decode it reads the token,
fetches the chunks it references, and rebuilds the value.

Codecs are composed via :func:`kvgit.codecs.compose`, which wraps them
in a ``ChunkingPickler`` / ``ChunkingUnpickler`` pair. Pickle handles
container traversal and primitive types; the codec only sees leaf
objects it cares about.

The kvgit core sees codecs as opaque ``(encoder, decoder)`` callables
on ``Staged``. Users opt in by passing them at construction.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ChunkSink(Protocol):
    """Encoder-side: emit a chunk, get back a content-addressed reference.

    Implementations hash the data once and cache the hash. Re-emitting
    the same bytes within an encode session must short-circuit to the
    same reference without rehashing — this matters for codecs that
    can cheaply produce the same bytes from the same Python object
    (e.g., the same ndarray reaching ``put`` twice via different
    container paths).
    """

    def put(self, data: bytes | memoryview) -> str:
        """Register a chunk and return its content-addressed reference."""
        ...


@runtime_checkable
class ChunkReader(Protocol):
    """Decoder-side: fetch chunks by reference."""

    def get(self, ref: str) -> bytes:
        """Fetch a single chunk. Raises ``KeyError`` if missing."""
        ...

    def get_many(self, refs: list[str]) -> dict[str, bytes]:
        """Bulk-fetch. Returns only refs that exist; missing refs absent."""
        ...

    def prefetch(self, refs: list[str]) -> None:
        """Hint that these refs will be needed soon. May be a no-op."""
        ...


@runtime_checkable
class Codec(Protocol):
    """A type-specific chunking codec.

    Attributes:
        name: Short tag stored in tokens (e.g. ``"np"``, ``"pa"``).
            Must be unique within a composed codec set.
    """

    name: str

    def try_externalize(self, obj: Any, sink: ChunkSink) -> Any | None:
        """Recognize ``obj`` and emit chunks; return a picklable token.

        Returns ``None`` to pass — the next codec in the chain (or
        plain pickling) will handle the object. The returned token
        becomes the second element of pickle's ``persistent_id``
        (the first element is :attr:`name`).
        """
        ...

    def materialize(self, token: Any, reader: ChunkReader) -> Any:
        """Reconstruct the value from a token by fetching its chunks."""
        ...
