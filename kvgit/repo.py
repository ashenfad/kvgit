"""Repo: a kvgit repository over one KVStore."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from ._codec import Codec, CodecSpec
from .content_types import MergeFn
from .encoding import loads, safe_loads
from .errors import (
    BranchExistsError,
    CorruptHeadError,
    UnknownBranchError,
    UnknownCommitError,
    UnknownTagError,
)
from .kv.base import KVStore
from .versioned import kv as _kv
from .versioned.helpers import changes_as_diff, walk_history
from .versioned.keyset import Keyset
from .versioned.kv import (
    BRANCH_HEAD,
    COMMIT_ROOT,
    COMMIT_TIME,
    GC_LEASE_TTL,
    INFO_KEY,
    PARENT_COMMIT,
    ROOT_COMMIT,
    CorruptHeadRecoverer,
    VersionedKV,
)
from .versioned.protocol import DiffResult, MergeChoice, TagInfo
from .worktree import Worktree

MergeRule = MergeFn | MergeChoice


@dataclass(frozen=True)
class Commit:
    """One commit: its parents, when it was made, what the caller said
    about it, and the root of its keyset — equal roots mean equal
    contents."""

    hash: str
    parents: tuple[str, ...]
    time: float | None
    info: dict | None
    root: str


class Repo:
    """A kvgit repository: every branch, tag and commit in one ``KVStore``.

    The repository owns the backend and holds settings every worktree
    shares: the codec that turns values into stored bytes, default merge
    rules, and HEAD recovery. None of them is written to the store.

    Args:
        backend: Any ``KVStore`` — ``Memory()``, ``Disk(path)``,
            ``Postgres(dsn)``, ``IndexedDB(...)``.
        codec: ``"pickle"`` (default: values are any picklable object),
            ``"scientific"`` (pickle plus chunked numpy/pandas buffers),
            ``"bytes"`` (values are bytes, stored as they are), or an
            ``(encoder, decoder)`` pair. Unpickling can execute code: for
            a store more than one party can write, prefer ``"bytes"``.
        merge_fns: Merge rules by key that every worktree inherits.
        merge_prefixes: Merge rules by key prefix that every worktree
            inherits.
        default_merge: The merge rule for keys no other rule covers.
        recover_from_corrupt_head: Last-resort recovery for a HEAD that is
            damaged and has no usable backup; see the API reference.

    Raises:
        StorageVersionError: if the store is stamped with a layout this
            code does not read.
    """

    def __init__(
        self,
        backend: KVStore,
        *,
        codec: CodecSpec = "pickle",
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
        recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
    ) -> None:
        _kv._check_storage_version(backend)
        self._store = backend
        self._codec = Codec(codec, backend)
        self._merge_fns = dict(merge_fns or {})
        self._merge_prefixes = dict(merge_prefixes or {})
        self._default_merge = default_merge
        self._recover = recover_from_corrupt_head

    def __repr__(self) -> str:
        return f"Repo({type(self._store).__name__})"

    @property
    def store(self) -> KVStore:
        """The backend."""
        return self._store

    def close(self) -> None:
        """Close the backend, if it has anything to close."""
        close = getattr(self._store, "close", None)
        if callable(close):
            close()

    def __enter__(self) -> Repo:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- Branches --

    def worktree(self, name: str, *, create: bool = False) -> Worktree:
        """Check out branch ``name`` for work.

        Raises:
            UnknownBranchError: if there is no such branch — unless
                ``create=True``, which creates it at the empty root commit.
            CorruptHeadError: if its HEAD is damaged beyond recovery.
        """
        if create and not self.has_branch(name):
            try:
                _kv.create_branch(self._store, name, ROOT_COMMIT)
            except BranchExistsError:
                pass  # created by someone else in the meantime
        engine = VersionedKV(
            self._store,
            branch=name,
            create=False,
            recover_from_corrupt_head=self._recover,
            check_version=False,
        )
        return Worktree(self, engine)

    def create_branch(self, name: str, *, at: str | None = None) -> str:
        """Create branch ``name`` at commit ``at`` (default: the empty root
        commit); return the commit it points at.

        Raises:
            BranchExistsError: if the name is taken.
            UnknownCommitError: if ``at`` is not in the store.
        """
        target = at or ROOT_COMMIT
        _kv.create_branch(self._store, name, target)
        return target

    def delete_branch(self, name: str) -> None:
        """Delete a branch. Its commits become collectable at the next
        :meth:`gc`; worktrees still holding it read from their heads and
        raise :class:`UnknownBranchError` on commit.

        Raises:
            UnknownBranchError: if there is no such branch.
        """
        _kv.delete_branch(self._store, name)

    def branches(self) -> list[str]:
        """Every branch name, sorted. Tags are not included."""
        return VersionedKV.branches(self._store)

    def has_branch(self, name: str) -> bool:
        """Whether a branch exists. Never writes."""
        return VersionedKV.exists(self._store, name)

    def head(self, name: str) -> str:
        """The commit branch ``name`` points at.

        Raises:
            UnknownBranchError: if there is no such branch.
            CorruptHeadError: if its HEAD is damaged beyond recovery.
        """
        _kv._reject_reserved_branch(name)
        commit = _kv._resolve_head(
            self._store, name, recover_from_corrupt_head=self._recover
        )
        if commit is not None:
            return commit
        if self._store.get(BRANCH_HEAD % name) is not None:
            raise CorruptHeadError(f"Branch '{name}' HEAD is corrupt and unrecoverable")
        raise UnknownBranchError(f"Branch '{name}' does not exist")

    def repair_head(self, name: str) -> str | None:
        """Persist a recovered HEAD for a damaged branch; return the commit
        it now names, or None if nothing was recoverable. Reads recover in
        memory only; this is the call that makes a recovery durable."""
        _kv._reject_reserved_branch(name)
        return _kv.repair_head(
            self._store, name, recover_from_corrupt_head=self._recover
        )

    # -- Tags --

    def create_tag(self, name: str, commit: str, *, info: dict | None = None) -> None:
        """Name ``commit`` permanently. A tag never moves and keeps its
        commit, and everything that commit descends from, alive.

        Raises:
            TagExistsError: if the name is taken.
            UnknownCommitError: if ``commit`` is not in the store.
        """
        _kv.create_tag(self._store, name, commit, info)

    def delete_tag(self, name: str) -> None:
        """Delete a tag. A commit it alone kept alive becomes collectable
        at the next :meth:`gc`.

        Raises:
            UnknownTagError: if there is no such tag.
        """
        _kv.delete_tag(self._store, name)

    def tags(self) -> dict[str, str]:
        """Every tag, mapped to the commit it names."""
        return _kv.tags(self._store)

    def tag_info(self, name: str) -> TagInfo | None:
        """A tag's commit, creation time and info; None if there is none."""
        return _kv.tag_info(self._store, name)

    # -- Commits and history --

    def get_commit(self, commit: str) -> Commit:
        """One commit's record.

        Raises:
            UnknownCommitError: if it is not in the store.
        """
        keys = (
            COMMIT_ROOT % commit,
            PARENT_COMMIT % commit,
            COMMIT_TIME % commit,
            INFO_KEY % commit,
        )
        found = self._store.get_many(*keys)
        root_raw = found.get(keys[0])
        if root_raw is None:
            raise UnknownCommitError(f"Commit '{commit}' does not exist")
        parents_raw = found.get(keys[1])
        parents = loads(parents_raw) if parents_raw is not None else None
        if isinstance(parents, str):
            parents = [parents]
        time_raw = found.get(keys[2])
        time_val = safe_loads(time_raw) if time_raw is not None else None
        info_raw = found.get(keys[3])
        return Commit(
            hash=commit,
            parents=tuple(parents or ()),
            time=float(time_val) if isinstance(time_val, (int, float)) else None,
            info=loads(info_raw) if info_raw is not None else None,
            root=loads(root_raw),
        )

    def log(
        self,
        *,
        commit: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
        limit: int | None = None,
        first_parent: bool = False,
    ) -> Iterator[Commit]:
        """Commits reachable from a commit, branch or tag, newest first.

        Name exactly one starting point. ``first_parent=True`` follows
        only each commit's first parent — the line of history made on the
        branch itself, skipping what merges brought in.
        """
        start = self._resolve_ref(commit=commit, branch=branch, tag=tag)
        records: dict[str, Commit] = {}

        def parents_of(h: str) -> tuple[str, ...]:
            record = records.pop(h, None) or self.get_commit(h)
            return record.parents

        for count, h in enumerate(
            walk_history(start, parents_of, all_parents=not first_parent)
        ):
            if limit is not None and count >= limit:
                return
            record = self.get_commit(h)
            records[h] = record
            yield record

    def diff(self, a: str, b: str) -> DiffResult:
        """Keys added, removed and modified going from commit ``a`` to ``b``.

        A structural diff: the cost follows the size of the change, not
        of the two keysets.

        Raises:
            UnknownCommitError: if either commit is not in the store.
        """
        found = self._store.get_many(COMMIT_ROOT % a, COMMIT_ROOT % b)
        for commit in (a, b):
            if COMMIT_ROOT % commit not in found:
                raise UnknownCommitError(f"Commit '{commit}' does not exist")
        return changes_as_diff(_kv.commit_changes(self._store, a, b))

    def merge_base(self, a: str, b: str) -> str | None:
        """The lowest common ancestor of two commits, or None if they share
        no history — the base a merge of the two would use.

        Raises:
            UnknownCommitError: if either commit is not in the store.
        """
        for commit in (a, b):
            if self._store.get(COMMIT_ROOT % commit) is None:
                raise UnknownCommitError(f"Commit '{commit}' does not exist")
        return _kv.merge_base(self._store, a, b)

    def snapshot(
        self,
        *,
        commit: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> Snapshot:
        """A read-only view of the state at a commit, branch or tag.

        Name exactly one. The snapshot is pinned to the commit resolved
        now: a branch that moves later does not move it.
        """
        return Snapshot(self, self._resolve_ref(commit=commit, branch=branch, tag=tag))

    # -- Maintenance --

    def gc(
        self,
        *,
        min_age: float = 3600,
        deep: bool = False,
        wait: bool = True,
        lease_ttl: float = GC_LEASE_TTL,
    ) -> int:
        """Reclaim what no branch, tag or in-flight commit reaches; return
        how many commits were removed.

        Orphans younger than ``min_age`` seconds are kept — how long
        abandoned work lingers; any value is safe beside writers.
        ``deep=True`` also scans for content no commit references (crash
        leftovers). If another sweep is running, ``wait=False`` raises
        :class:`GcBusy` instead of waiting. Writers wait while a sweep
        runs.
        """
        return _kv.gc(self._store, min_age, deep=deep, wait=wait, lease_ttl=lease_ttl)

    # -- Internal --

    def _resolve_ref(
        self,
        *,
        commit: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
    ) -> str:
        """The commit a ref names; exactly one must be given."""
        given = [ref for ref in (commit, branch, tag) if ref is not None]
        if len(given) != 1:
            raise ValueError("name exactly one of commit, branch or tag")
        if branch is not None:
            return self.head(branch)
        if tag is not None:
            tagged = _kv._resolve_tag(self._store, tag)
            if tagged is None:
                raise UnknownTagError(f"Tag '{tag}' does not exist")
            target = tagged
        else:
            target = commit  # type: ignore[assignment]
        if self._store.get(COMMIT_ROOT % target) is None:
            raise UnknownCommitError(f"Commit '{target}' does not exist")
        return target

    def _pointers(self, commit: str) -> dict[str, str]:
        """A commit's keyset as key -> blob pointer."""
        root_raw = self._store.get(COMMIT_ROOT % commit)
        if root_raw is None:
            raise UnknownCommitError(f"Commit '{commit}' does not exist")
        entries = Keyset(self._store, root=loads(root_raw)).materialize()
        return {key: entry.blob for key, entry in entries.items()}


class Snapshot(Mapping[str, Any]):
    """The state of the repository at one commit, read-only.

    A ``Mapping`` of decoded values, pinned to :attr:`commit`. :attr:`raw`
    is the same state as stored bytes, whatever the codec.
    """

    def __init__(self, repo: Repo, commit: str) -> None:
        self._repo = repo
        self.commit = commit
        self._pointers: dict[str, str] | None = None

    def __repr__(self) -> str:
        return f"Snapshot(commit={self.commit[:8]}...)"

    def _index(self) -> dict[str, str]:
        if self._pointers is None:
            self._pointers = self._repo._pointers(self.commit)
        return self._pointers

    def _raw_many(self, keys: tuple[str, ...]) -> dict[str, bytes]:
        index = self._index()
        wanted: dict[str, list[str]] = {}
        for key in keys:
            pointer = index.get(key)
            if pointer is not None:
                wanted.setdefault(pointer, []).append(key)
        found = self._repo.store.get_many(wanted.keys())
        return {key: raw for pointer, raw in found.items() for key in wanted[pointer]}

    def __getitem__(self, key: str) -> Any:
        found = self._raw_many((key,))
        if key not in found:
            raise KeyError(key)
        return self._repo._codec.decode(found[key])

    def get_many(self, *keys: str) -> dict[str, Any]:
        """The decoded values of the given keys that exist here."""
        decode = self._repo._codec.decode
        return {key: decode(raw) for key, raw in self._raw_many(keys).items()}

    def __iter__(self) -> Iterator[str]:
        return iter(self._index())

    def __len__(self) -> int:
        return len(self._index())

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key in self._index()

    @property
    def raw(self) -> RawSnapshot:
        """This snapshot as stored bytes, whatever the codec."""
        return RawSnapshot(self)


class RawSnapshot(Mapping[str, bytes]):
    """A snapshot's stored bytes, undecoded.

    For the ``"scientific"`` codec a value's stored bytes are an envelope
    referring to chunks elsewhere in the store, so they are not
    self-contained outside it; for ``"pickle"`` and ``"bytes"`` they are.
    """

    def __init__(self, snapshot: Snapshot) -> None:
        self._snapshot = snapshot

    def __getitem__(self, key: str) -> bytes:
        found = self._snapshot._raw_many((key,))
        if key not in found:
            raise KeyError(key)
        return found[key]

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """The stored bytes of the given keys that exist here."""
        return self._snapshot._raw_many(keys)

    def __iter__(self) -> Iterator[str]:
        return iter(self._snapshot)

    def __len__(self) -> int:
        return len(self._snapshot)

    def __contains__(self, key: object) -> bool:
        return key in self._snapshot
