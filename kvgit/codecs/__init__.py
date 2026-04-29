"""Chunked codecs for kvgit.

A chunked codec externalizes large sub-values (numpy buffers, Arrow
tables, ...) as content-addressed chunks stored in a separate
namespace, so equal data is stored once across keys, commits, and
branches.

Quick start::

    from kvgit import store
    from kvgit.codecs import compose
    from kvgit.codecs.numpy import NumpyCodec

    encoder, decoder = compose(NumpyCodec())
    s = store(encoder=encoder, decoder=decoder)

The encoder/decoder pair is plug-compatible with ``Staged``'s
existing slots: it accepts a ``ChunkSink`` / ``ChunkReader`` second
argument, and ``Staged`` detects the extended arity automatically.
"""

from __future__ import annotations

from .base import ChunkReader, ChunkSink, Codec
from .pickler import (
    ChunkingPickler,
    ChunkingUnpickler,
    decode as _decode,
    encode as _encode,
)


def compose(*codecs: Codec):
    """Build an ``(encoder, decoder)`` pair from a list of codecs.

    The codecs are tried in order during encoding; the first to
    return a non-None token from ``try_externalize`` wins. Plain
    pickling handles anything no codec claims — there is no need to
    register a "pickle codec" explicitly.

    Order matters when codecs claim overlapping types. Put the more
    specific codec first.
    """
    codec_list = list(codecs)

    def encoder(value, sink):
        return _encode(value, sink, codec_list)

    def decoder(blob, reader):
        return _decode(blob, reader, codec_list)

    return encoder, decoder


def scientific():
    """Return an ``(encoder, decoder)`` pair using scientific codecs.

    Currently composes the numpy codec, which transparently handles
    pandas ``DataFrame`` / ``Series`` block buffers via pandas' pickle
    path. Future scientific codecs (Arrow, etc.) will slot in here
    when both the codec and its dependency are available.

    Raises:
        ImportError: if numpy is not importable in this environment.
            Install with ``pip install kvgit[scientific]``.
    """
    try:
        from .numpy import NumpyCodec
    except ImportError as e:
        raise ImportError(
            "kvgit.codecs.scientific() requires numpy. "
            "Install with `pip install kvgit[scientific]`."
        ) from e
    return compose(NumpyCodec())


# Registry of named codec presets used by ``kvgit.store(codecs=...)``.
# Keep this sparse: each preset is a deliberate, well-documented bundle.
_NAMED_PRESETS = {
    "scientific": scientific,
}


def _resolve_named(name: str):
    """Resolve a named codec preset to an ``(encoder, decoder)`` pair.

    Internal helper used by ``kvgit.store(codecs=...)``. Raises
    ``ValueError`` for unknown names with a list of valid options.
    """
    factory = _NAMED_PRESETS.get(name)
    if factory is None:
        raise ValueError(
            f"unknown codec preset {name!r}. "
            f"Available presets: {sorted(_NAMED_PRESETS)}"
        )
    return factory()


__all__ = [
    "ChunkReader",
    "ChunkSink",
    "ChunkingPickler",
    "ChunkingUnpickler",
    "Codec",
    "compose",
    "scientific",
]
