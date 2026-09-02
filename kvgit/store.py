"""Store factory function."""

import pickle
from collections.abc import Callable, Iterable
from typing import Any, Literal

from .kv.base import KVStore
from .kv.memory import Memory
from .staged import Staged
from .versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    TAG_INFO_KEY,
    TAG_KEY,
    CorruptHeadRecoverer,
    VersionedKV,
    _assert_supported_version,
    clean_orphans,
)


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
    recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
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
        recover_from_corrupt_head: Optional last-resort recovery for a
            branch whose HEAD is unresolvable *and* whose prev-HEAD
            backup is gone. Off by default: with both gone the store
            does not know what the branch pointed at, so the honest
            answer is to fail rather than guess. Pass
            ``kvgit.versioned.kv.recover_by_commit_scan`` to opt into
            the scan, having read what it can return.

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

    return Staged(
        VersionedKV(
            backend,
            branch=branch,
            recover_from_corrupt_head=recover_from_corrupt_head,
        ),
        encoder=encoder,
        decoder=decoder,
    )


def delete_branches(
    names: str | Iterable[str],
    *,
    kind: Literal["memory", "disk", "indexeddb"] = "disk",
    path: str | None = None,
    db_name: str = "kvgit",
    min_age: float = 3600,
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
    that only the deleted branches referenced are reclaimed. Tags are
    roots for that sweep like any other caller of it, so a tagged commit
    survives the deletion of every branch that reached it. All heads
    (and prev-heads) are gone before the sweep, so it can't mistake a
    doomed branch for a live one. ``min_age`` defaults to
    ``clean_orphans``'s one-hour guard (matching ``delete_branch``), so
    an in-flight commit from a concurrent writer on another branch is
    not swept out from under it; an admin who knows the store is quiet
    can pass ``0`` to reclaim young commits immediately.

    That sweep does not reclaim chunks — the content-addressed bytes a
    chunked codec writes — because a chunk key is not the deleted
    branch's to give away. On a store using chunked codecs, follow up
    with ``deep_clean`` on a quiescent store to get that space back.

    Args:
        names: Branch names to delete — one name or an iterable of
            them. Missing names are no-ops. (A bare string is treated
            as ONE name, not iterated per character.)
        kind: ``"disk"`` (default), ``"memory"``, or ``"indexeddb"``.
        path: Required when ``kind="disk"``. Store directory.
        db_name: IndexedDB database name (``kind="indexeddb"`` only).
        min_age: Passed to :func:`clean_orphans` — commits younger than
            this many seconds survive the sweep.
    """
    # A bare string is a plausible one-branch call, and iterating it
    # would "delete" each character as a branch name — a silent no-op
    # at best. Treat it as a single name, like nontainer's
    # delete_workspace does one layer up.
    doomed = {names} if isinstance(names, str) else set(names)
    if not doomed:
        return  # nothing asked: skip the backend open and the sweep
    backend = _make_backend(kind, path=path, db_name=db_name)
    try:
        # No ``VersionedKV`` is constructed here, so nothing else on this
        # path checks the store's layout version. Checked before the
        # first removal rather than leaving it to the sweep: a store this
        # code cannot read must come out of the call untouched, not with
        # its branch keys already gone.
        _assert_supported_version(backend)
        for name in doomed:
            backend.remove(BRANCH_HEAD % name)
            backend.remove(BRANCH_HEAD_PREV % name)
        clean_orphans(backend, min_age=min_age)
    finally:
        close = getattr(backend, "close", None)
        if callable(close):
            close()


def delete_tags(
    names: str | Iterable[str],
    *,
    kind: Literal["memory", "disk", "indexeddb"] = "disk",
    path: str | None = None,
    db_name: str = "kvgit",
    min_age: float = 3600,
) -> None:
    """Delete tags with no branch anchor, then sweep orphans.

    The tag counterpart of :func:`delete_branches`, and anchor-free for
    the same reason: teardown should not need a handle, and a handle
    always has a current branch to open on. For each name,
    ``__tag__<name>`` and its ``__tag_info__<name>`` record are removed,
    then a single :func:`clean_orphans` sweep reclaims commits that only
    the deleted tags kept alive. A name with no tag is skipped —
    idempotency beats a ``ValueError`` in teardown.

    That sweep does not reclaim chunks; follow up with ``deep_clean`` on
    a quiescent store if the store uses chunked codecs.

    Args:
        names: Tag names to delete — one name or an iterable of them.
            Missing names are no-ops. (A bare string is treated as ONE
            name, not iterated per character.)
        kind: ``"disk"`` (default), ``"memory"``, or ``"indexeddb"``.
        path: Required when ``kind="disk"``. Store directory.
        db_name: IndexedDB database name (``kind="indexeddb"`` only).
        min_age: Passed to :func:`clean_orphans` — commits younger than
            this many seconds survive the sweep.
    """
    doomed = {names} if isinstance(names, str) else set(names)
    if not doomed:
        return  # nothing asked: skip the backend open and the sweep
    backend = _make_backend(kind, path=path, db_name=db_name)
    try:
        # Same reasoning as delete_branches: no handle is constructed on
        # this path, so the layout check has to happen here, before
        # anything is removed.
        _assert_supported_version(backend)
        for name in doomed:
            backend.remove(TAG_KEY % name)
            backend.remove(TAG_INFO_KEY % name)
        clean_orphans(backend, min_age=min_age)
    finally:
        close = getattr(backend, "close", None)
        if callable(close):
            close()
