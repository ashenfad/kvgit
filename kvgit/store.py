"""Store factory function."""

import pickle
from typing import Any, Callable, Literal

from .kv.base import KVStore
from .kv.memory import Memory
from .staged import Staged
from .versioned.kv import VersionedKV
from .versioned.protocol import Versioned


def store(
    kind: Literal["memory", "disk", "git", "indexeddb"] = "memory",
    *,
    path: str | None = None,
    db_name: str = "kvgit",
    branch: str = "main",
    encoder: Callable[..., bytes] = pickle.dumps,
    decoder: Callable[..., Any] = pickle.loads,
    codecs: str | None = None,
) -> Staged:
    """Create a Staged store with sensible defaults.

    Args:
        kind: ``"memory"`` (default), ``"disk"``, ``"git"``, or ``"indexeddb"``.
        path: Required when ``kind="disk"`` or ``kind="git"``.
            Directory path for the disk backend or repo path for git.
        db_name: IndexedDB database name (default ``"kvgit"``).
            Only used when ``kind="indexeddb"``.
        branch: Branch name (default ``"main"``).
        encoder: Value encoder (default ``pickle.dumps``).
        decoder: Value decoder (default ``pickle.loads``).
        codecs: Optional named codec preset. Currently supported:
            ``"scientific"`` — numpy/pandas chunked codecs (requires
            numpy; install with ``pip install kvgit[scientific]``).
            Mutually exclusive with explicit ``encoder`` / ``decoder``.

    Returns:
        A ``Staged`` store instance.

    Raises:
        ValueError: if ``codecs`` is given alongside non-default
            ``encoder`` / ``decoder``, or if ``codecs`` names an
            unknown preset.
        ImportError: if a codec preset's optional dependency is not
            installed.
    """
    if codecs is not None:
        if encoder is not pickle.dumps or decoder is not pickle.loads:
            raise ValueError(
                "codecs= is mutually exclusive with explicit encoder/decoder; "
                "pass one or the other"
            )
        from .codecs import _resolve_named

        encoder, decoder = _resolve_named(codecs)

    versioned: Versioned

    if kind == "git":
        if path is None:
            raise ValueError("path is required when kind='git'")
        from .versioned.gp import VersionedGP

        versioned = VersionedGP(path, branch=branch)
    else:
        # Build KV backend
        backend: KVStore
        if kind == "memory":
            backend = Memory()
        elif kind == "disk":
            if path is None:
                raise ValueError("path is required when kind='disk'")
            from .kv.disk import Disk

            backend = Disk(path)
        elif kind == "indexeddb":
            from .kv.indexeddb import IndexedDB

            backend = IndexedDB(db_name=db_name)
        else:
            raise ValueError(f"Unknown kind: {kind!r}")

        versioned = VersionedKV(backend, branch=branch)

    return Staged(versioned, encoder=encoder, decoder=decoder)
