"""One-line entry point: a worktree on a fresh or existing store."""

from typing import Literal

from ._codec import CodecSpec
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


def store(
    kind: Literal["memory", "disk", "indexeddb"] = "memory",
    *,
    path: str | None = None,
    db_name: str = "kvgit",
    branch: str = "main",
    codec: CodecSpec = "pickle",
) -> Worktree:
    """A worktree on ``branch``, creating the branch if it is new.

    Sugar for ``Repo(backend, codec=codec).worktree(branch, create=True)``
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
    """
    backend = _make_backend(kind, path=path, db_name=db_name)
    return Repo(backend, codec=codec).worktree(branch, create=True)
