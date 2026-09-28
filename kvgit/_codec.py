"""How a repository turns values into stored bytes and back."""

import inspect
import pickle
from collections.abc import Callable
from typing import Any

from .codecs._hash import hash_bytes
from .kv.base import KVStore
from .versioned.kv import CHUNK_PREFIX

CodecSpec = str | tuple[Callable[..., bytes], Callable[..., Any]]
"""``"pickle"``, ``"scientific"``, ``"bytes"``, or an ``(encoder, decoder)`` pair."""


class _ChunkSink:
    """Accumulates content-addressed chunks emitted during one commit.

    Built fresh per commit and shared across every value encoded in it,
    so dedup extends to the same buffer appearing under several keys.
    """

    def __init__(self) -> None:
        self.chunks: dict[str, bytes] = {}
        # per-encode key tracking — set externally between encode calls
        self.current_key: str | None = None
        self.refs_by_key: dict[str, list[str]] = {}

    def put(self, data) -> str:
        ref = hash_bytes(data)
        if ref not in self.chunks:
            # Materialize once on first sight; later puts of the same
            # chunk hash are free (same ref returned, no new bytes).
            self.chunks[ref] = bytes(data) if isinstance(data, memoryview) else data
        if self.current_key is not None:
            self.refs_by_key.setdefault(self.current_key, []).append(ref)
        return ref


class _ChunkReader:
    """Fetches chunks from the store by content-addressed key."""

    def __init__(self, kv: KVStore) -> None:
        self._kv = kv

    def get(self, ref: str) -> bytes:
        raw = self._kv.get(CHUNK_PREFIX + ref)
        if raw is None:
            raise KeyError(f"chunk not found: {ref!r}")
        return raw

    def get_many(self, refs):
        prefixed = [CHUNK_PREFIX + r for r in refs]
        raw = self._kv.get_many(*prefixed)
        # Strip the prefix back off so callers see the codec-level refs.
        return {k[len(CHUNK_PREFIX) :]: v for k, v in raw.items()}

    def prefetch(self, refs) -> None:
        # Backends with async fetch could override; the in-process
        # backends have nothing useful to do here.
        return None


def _is_chunk_aware(fn) -> bool:
    """Whether an encoder/decoder takes a required sink/reader argument.

    Chunk-aware encoders/decoders (built with ``kvgit.codecs.compose``)
    have exactly two **required** positional parameters: the value/blob
    and the sink/reader. ``pickle.dumps`` and friends have optional
    second arguments (``protocol=...``) and stay one-argument.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    required = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        and p.default is inspect.Parameter.empty
    ]
    return len(required) >= 2


def _bytes_encode(value: Any) -> bytes:
    if not isinstance(value, bytes):
        raise TypeError(
            f"codec 'bytes' stores bytes values only, got {type(value).__name__}"
        )
    return value


def _bytes_decode(raw: bytes) -> bytes:
    return raw


def _resolve(spec: CodecSpec) -> tuple[Callable[..., bytes], Callable[..., Any]]:
    if isinstance(spec, tuple):
        if len(spec) != 2 or not all(callable(f) for f in spec):
            raise ValueError("a codec pair is (encoder, decoder)")
        return spec
    if spec == "pickle":
        return pickle.dumps, pickle.loads
    if spec == "bytes":
        return _bytes_encode, _bytes_decode
    if spec == "scientific":
        from .codecs import _resolve_named

        return _resolve_named("scientific")
    raise ValueError(
        f"unknown codec {spec!r}: use 'pickle', 'scientific', 'bytes', "
        "or an (encoder, decoder) pair"
    )


class Codec:
    """A repository's encoder and decoder, bound to its store.

    Arity is detected: a one-argument ``encoder(value) -> bytes`` /
    ``decoder(bytes) -> value`` pair, or a chunk-aware
    ``encoder(value, sink)`` / ``decoder(bytes, reader)`` pair from
    :mod:`kvgit.codecs`, whose large buffers are stored once as
    content-addressed chunks.
    """

    def __init__(self, spec: CodecSpec, store: KVStore) -> None:
        self.encoder, self.decoder = _resolve(spec)
        self.encoder_chunked = _is_chunk_aware(self.encoder)
        self.decoder_chunked = _is_chunk_aware(self.decoder)
        self._reader = _ChunkReader(store) if self.decoder_chunked else None

    def new_sink(self) -> _ChunkSink | None:
        """A sink for one commit's chunks, or None for a plain codec."""
        return _ChunkSink() if self.encoder_chunked else None

    def encode(self, key: str, value: Any, sink: _ChunkSink | None) -> bytes:
        if sink is not None:
            sink.current_key = key
            try:
                return self.encoder(value, sink)
            finally:
                sink.current_key = None
        return self.encoder(value)

    def decode(self, raw: bytes) -> Any:
        if self.decoder_chunked:
            return self.decoder(raw, self._reader)
        return self.decoder(raw)

    def encode_merged(self, value: Any) -> bytes:
        """Encode a value a merge function produced.

        A merge runs below any commit's chunk sink, so there is nowhere to
        land chunks: a chunk-aware codec's merge output is stored as plain
        pickle (which its decoder reads) until the key is next written.
        Every other codec encodes merge output exactly as it encodes a
        write, so a ``bytes`` store stays bytes.
        """
        if self.encoder_chunked:
            return pickle.dumps(value)
        return self.encoder(value)
