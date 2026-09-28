"""One-line entry point: open a store, and a worktree on one of its branches."""

from typing import Literal

from ._codec import CodecSpec
from .cache import DEFAULT_CACHE_BYTES
from .kv.base import KVStore
from .kv.memory import Memory
from .repo import Repo
from .worktree import Worktree


def _make_backend(
    kind: Literal["memory", "disk", "indexeddb"],
    *,
    path: str | None,
    db_name: str,
) -> KVStore:
    """Construct a ``KVStore`` backend by kind."""
    if kind == "memory":
        return Memory()
    elif kind == "disk":
        if path is None:
            raise ValueError("path is required when kind='disk'")
        from .kv.disk import Disk

        return Disk(path)
    elif kind == "indexeddb":
        from .kv.indexeddb import IndexedDB

        return IndexedDB(db_name=db_name)
    else:
        raise ValueError(f"Unknown kind: {kind!r}")


def open(
    kind: Literal["memory", "disk", "indexeddb"] = "memory",
    *,
    path: str | None = None,
    db_name: str = "kvgit",
    branch: str = "main",
    codec: CodecSpec = "pickle",
    cache_bytes: int = DEFAULT_CACHE_BYTES,
) -> Worktree:
    """Open (or create) a store, and a worktree on ``branch`` of it.

    Like ``shelve.open``, it creates what is missing: the branch is
    created if it is new, unlike :meth:`Repo.worktree`, which asks for
    ``create=True``. Sugar for
    ``Repo(backend, codec=codec).worktree(branch, create=True)``
    over the backend ``kind`` names; ``wt.repo`` is the repository, and
    ``wt.repo.close()`` releases the backend. Build a :class:`Repo` for
    anything more — another backend, merge rules, several branches.

    Args:
        kind: ``"memory"`` (default), ``"disk"`` (``pip install
            kvgit[disk]``) or ``"indexeddb"`` (Pyodide).
        path: The directory, for ``kind="disk"``.
        db_name: The IndexedDB database name, for ``kind="indexeddb"``.
        branch: The branch to check out (default ``"main"``).
        codec: How values are stored; see :class:`Repo`.
        cache_bytes: Memory for what the store never changes; see
            :class:`Repo`.
    """
    backend = _make_backend(kind, path=path, db_name=db_name)
    return Repo(backend, codec=codec, cache_bytes=cache_bytes).worktree(
        branch, create=True
    )
