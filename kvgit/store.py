"""Store factory function."""

import pickle
from collections.abc import Iterable
from typing import Any, Callable, Literal

from .kv.base import KVStore
from .kv.memory import Memory
from .staged import Staged
from .versioned.kv import BRANCH_HEAD, BRANCH_HEAD_PREV, VersionedKV, clean_orphans


def _make_backend(
    kind: Literal["memory", "disk", "indexeddb"],
    *,
    path: str | None,
    db_name: str,
) -> KVStore:
    """Construct a raw ``KVStore`` backend by kind.

    Shared by :func:`store` and :func:`delete_branches` so both open the
    exact same backends from the same parameters.
    """
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
    encoder: Callable[..., bytes] = pickle.dumps,
    decoder: Callable[..., Any] = pickle.loads,
    codecs: str | None = None,
) -> Staged:
    """Create a Staged store with sensible defaults.

    Args:
        kind: ``"memory"`` (default), ``"disk"``, or ``"indexeddb"``.
        path: Required when ``kind="disk"``. Directory path for the
            disk backend.
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

    backend = _make_backend(kind, path=path, db_name=db_name)

    return Staged(VersionedKV(backend, branch=branch), encoder=encoder, decoder=decoder)


def delete_branches(
    names: Iterable[str],
    *,
    kind: Literal["memory", "disk", "indexeddb"] = "disk",
    path: str | None = None,
    db_name: str = "kvgit",
) -> None:
    """Delete branches with no branch anchor, then sweep orphans.

    :meth:`VersionedKV.delete_branch` refuses to delete the branch its
    handle is anchored on — and there is no way to open a handle
    *without* a branch. When the branch you want gone is the store's
    only branch, there is nothing safe to anchor on. This admin entry
    point sidesteps the whole problem: it opens the backend directly
    (no ``VersionedKV``, so no current branch) and edits the branch
    keys itself.

    For each name, ``__branch_head__<name>`` and its
    ``__branch_head_prev__<name>`` recovery backup are removed — the
    backup too, so a later same-named branch can't "recover" the
    deleted state through ``_resolve_head``'s fallback. A name with no
    head is simply skipped: this is teardown, and idempotency beats a
    ``ValueError`` here. Deleting every branch is legal — the store
    just mints a fresh empty ``main`` on next open.

    One :func:`clean_orphans` sweep runs after all removals, so commits
    that only the deleted branches referenced are reclaimed. All heads
    (and prev-heads) are gone before the sweep, so it can't mistake a
    doomed branch for a live one. The sweep keeps ``clean_orphans``'s
    default ``min_age`` guard (matching ``delete_branch``), so an
    in-flight commit from a concurrent writer on another branch is not
    swept out from under it.

    Args:
        names: Branch names to delete. Missing names are no-ops.
        kind: ``"disk"`` (default), ``"memory"``, or ``"indexeddb"``.
        path: Required when ``kind="disk"``. Store directory.
        db_name: IndexedDB database name (``kind="indexeddb"`` only).
    """
    backend = _make_backend(kind, path=path, db_name=db_name)
    try:
        for name in names:
            backend.remove(BRANCH_HEAD % name)
            backend.remove(BRANCH_HEAD_PREV % name)
        clean_orphans(backend)
    finally:
        close = getattr(backend, "close", None)
        if callable(close):
            close()
