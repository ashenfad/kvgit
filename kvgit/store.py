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
    encoder: Callable[[Any], bytes] = pickle.dumps,
    decoder: Callable[[bytes], Any] = pickle.loads,
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

    Returns:
        A ``Staged`` store instance.
    """
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
