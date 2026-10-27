"""Repo: a kvgit repository over one KVStore."""

from __future__ import annotations

import base64
import time
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from . import _core, _store as _kv
from ._codec import Codec, CodecSpec
from ._drive import drive
from ._engine import Engine, _lowered
from ._store import EMPTY_HASH, GC_LEASE_TTL, ROOT_COMMIT, Clock, CorruptHeadRecoverer
from ._types import BytesMergeFn, DiffResult, MergeChoice, Resolution
from .cache import (
    DEFAULT_CACHE_BYTES,
    DEFAULT_VALUE_CACHE_BYTES,
    CachedStore,
    ContentCache,
)
from .content_types import MergeFn
from .errors import (
    BranchExistsError,
    UnknownCommitError,
)
from .kv.base import KVStore
from .refs import Branches, Tags
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
            ``Postgres(dsn)``.
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
        cache_bytes: Memory for tree nodes and commit records read from
            the store, which never change once written (32 MB by
            default; ``0`` turns the cache off). A sweep by any process
            empties it before this one builds on what the sweep removed;
            see :class:`~kvgit.cache.ContentCache`.
        value_cache_bytes: Memory for the decoded values each worktree
            reads, held until its head moves (32 MB per worktree by
            default; ``0`` holds none). A value costing more than a
            quarter of it is never held; see
            :class:`~kvgit.cache.ValueCache`.
        now: The clock commit and tag times are read from, and a sweep's
            ``min_age`` is measured by: seconds since the epoch, as a
            float (``time.time`` by default). GC leases always expire by
            the wall clock, whatever this says.

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
        cache_bytes: int = DEFAULT_CACHE_BYTES,
        value_cache_bytes: int = DEFAULT_VALUE_CACHE_BYTES,
        now: Clock | None = None,
    ) -> None:
        _kv._check_storage_version(backend)
        if value_cache_bytes < 0:
            raise ValueError(
                f"value_cache_bytes must not be negative, got {value_cache_bytes}"
            )
        self._value_cache_bytes = value_cache_bytes
        self._backend = backend
        # 0 turns the cache off; anything else is a budget, and
        # ContentCache refuses one that is negative.
        self.cache: ContentCache | None = (
            ContentCache(cache_bytes) if cache_bytes != 0 else None
        )
        """What this repository remembers of its store, with its hit and
        miss counts; ``None`` with ``cache_bytes=0``."""
        # Everything the repository reads goes through the cache; the
        # backend itself stays what ``store`` hands out.
        self._store: KVStore = (
            CachedStore(backend, self.cache) if self.cache is not None else backend
        )
        self._codec = Codec(codec, backend)
        self._merge_fns = dict(merge_fns or {})
        self._merge_prefixes = dict(merge_prefixes or {})
        self._default_merge = default_merge
        self._recover = recover_from_corrupt_head
        self._now = now
        self.branches = Branches(self)
        """Every branch, as a live mapping of name to tip commit, with
        ``create`` and ``delete``."""
        self.tags = Tags(self)
        """Every tag, as a live mapping of name to commit, with ``create``,
        ``delete`` and ``info``."""

    def __repr__(self) -> str:
        return f"Repo({type(self._backend).__name__})"

    @property
    def now(self) -> Clock:
        """The clock commit and tag times are read from."""
        return self._now if self._now is not None else time.time

    @property
    def store(self) -> KVStore:
        """The backend.

        Reads made straight through it bypass the repository's cache,
        and so do removals: a key deleted here, rather than by
        :meth:`gc`, may still be answered from memory until the next
        sweep.
        """
        return self._backend

    def close(self) -> None:
        """Close the backend, if it has anything to close."""
        close = getattr(self._backend, "close", None)
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
        if create and name not in self.branches:
            try:
                _kv.create_branch(self._store, name, ROOT_COMMIT, now=self._now)
            except BranchExistsError:
                pass  # created by someone else in the meantime
        engine = Engine(self._store, name, recover=self._recover, now=self._now)
        return Worktree(self, engine)

    def repair_head(self, name: str) -> str | None:
        """Persist a recovered HEAD for a damaged branch; return the commit
        it now names, or None if nothing was recoverable. Reads recover in
        memory only; this is the call that makes a recovery durable."""
        _kv._reject_reserved_branch(name)
        return _kv.repair_head(
            self._store, name, recover_from_corrupt_head=self._recover
        )

    # -- Commits and history --

    def get_commit(self, commit: str) -> Commit:
        """One commit's record.

        Raises:
            UnknownCommitError: if it is not in the store.
        """
        return _commit(_kv._call("get_commit", self._store, commit=commit))

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
        records = _kv._stream(
            "log", self._store, start=start, limit=limit, first_parent=first_parent
        )
        for record in records:
            yield _commit(record)

    def diff(self, a: str, b: str) -> DiffResult:
        """Keys added, removed and modified going from commit ``a`` to ``b``.

        A structural diff: the cost follows the size of the change, not
        of the two keysets.

        Raises:
            UnknownCommitError: if either commit is not in the store.
        """
        found = _kv._call("diff", self._store, a=a, b=b)
        return DiffResult(
            added=frozenset(found["added"]),
            removed=frozenset(found["removed"]),
            modified=frozenset(found["modified"]),
        )

    def merge_rule(
        self,
        key: str,
        *,
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
    ) -> MergeRule | None:
        """The merge rule that governs ``key``: its exact key, else the
        longest registered prefix it starts with, else the default, from
        the call's rules layered over the repository's; None for none.

        A ``MergeChoice`` here is a standing policy, so a key it gives to
        one side never needs its values read.
        """
        fns = {**self._merge_fns, **(merge_fns or {})}
        if key in fns:
            return fns[key]
        prefixes = {**self._merge_prefixes, **(merge_prefixes or {})}
        covering = [p for p in prefixes if key.startswith(p)]
        if covering:
            return prefixes[max(covering, key=len)]
        return _first(default_merge, self._default_merge)

    def resolve(
        self,
        base: Mapping[str, Any],
        ours: Mapping[str, Any],
        theirs: Mapping[str, Any],
        *,
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
    ) -> Resolution:
        """Resolve a three-way merge of values the caller names, as a
        merge resolves branches: what changed on one side since ``base``
        is taken, keys both changed alike agree, and keys both changed
        differently go to the merge rules, the call's layered over the
        repository's.

        Each mapping is a side's state of the keys in question; a key
        missing from one is absent on that side. Two values are the same
        when the repository's codec encodes them to the same bytes.
        Nothing is read from or written to the store.

        Returns the change to ``ours``: the values it takes and the keys
        it loses.

        Raises:
            MergeConflict: keys both sides changed that no rule resolves.
        """
        held: dict[str, bytes] = {}
        # Each key's own objects, by blob: two keys holding equal bytes
        # share a blob, and each takes back the object it was given.
        values: dict[str, dict[str, Any]] = {}

        def side(state: Mapping[str, Any], key: str) -> tuple[str, int] | None:
            if key not in state:
                return None
            # As a merge function's output is stored: without chunks,
            # since a resolve writes nothing for them to land in.
            raw = self._codec.encode_merged(state[key])
            blob = _kv.blob_key(raw)
            held[blob] = raw
            values.setdefault(key, {}).setdefault(blob, state[key])
            return (blob, len(raw))

        our_changes = []
        their_changes = []
        for key in sorted({*base, *ours, *theirs}):
            old, mine, other = side(base, key), side(ours, key), side(theirs, key)
            if mine != old:
                our_changes.append((key, old, mine))
            if other != old:
                their_changes.append((key, old, other))
        fns = {**self._merge_fns, **(merge_fns or {})}
        prefixes = {**self._merge_prefixes, **(merge_prefixes or {})}
        default = _first(default_merge, self._default_merge)
        hosted: list[Any] = []
        op = _core.Op.resolve(
            our_changes,
            their_changes,
            *_lowered(
                hosted,
                {k: self._lower(r) for k, r in fns.items()},
                {k: self._lower(r) for k, r in prefixes.items()},
                self._lower(default) if default is not None else None,
            ),
        )
        done = drive(
            op,
            self._store,
            held=held,
            merge_fns=hosted,
            clock=time.time,
            sleep=time.sleep,
            now=self._now,
        )
        taken = {key: values[key][blob] for key, blob in done["updates"]}
        for key, blob in done["merged"]:
            taken[key] = self._codec.decode(held[blob])
        for key, text in done["made"]:
            taken[key] = self._codec.decode(base64.b64decode(text))
        return Resolution(
            values=taken,
            removals=frozenset(done["removals"]),
            auto_merged_keys=tuple(done["auto_merged"]),
            carried_keys=tuple(done["carried"]),
        )

    def merge_base(self, a: str, b: str) -> str | None:
        """The lowest common ancestor of two commits, or None if they share
        no history — the base a merge of the two would use.

        Raises:
            UnknownCommitError: if either commit is not in the store.
        """
        return _kv._call("merge_base", self._store, a=a, b=b, check=True)["base"]

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
        roots: dict[str, str] = {}
        target = self._resolve_ref(commit=commit, branch=branch, tag=tag, roots=roots)
        return Snapshot(self, target, root=roots.get(target))

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
        return _kv.gc(
            self._store,
            min_age,
            deep=deep,
            wait=wait,
            lease_ttl=lease_ttl,
            now=self._now,
        )

    # -- Internal --

    def _resolve_ref(
        self,
        *,
        commit: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
        roots: dict[str, str] | None = None,
    ) -> str:
        """The commit a ref names; exactly one must be given.

        Checking that the commit exists reads its keyset root; ``roots``,
        when given, keeps it, for a caller about to open that tree.
        """
        given = {
            name: ref
            for name, ref in (("commit", commit), ("branch", branch), ("tag", tag))
            if ref is not None
        }
        if len(given) != 1:
            raise ValueError("name exactly one of commit, branch or tag")
        recover = self._recover
        found = _kv._call(
            "resolve_ref", self._store, recover, recover=recover is not None, **given
        )
        if roots is not None and found["root"] is not None:
            roots[found["commit"]] = found["root"]
        return found["commit"]

    def _lower(self, rule: MergeRule) -> BytesMergeFn | MergeChoice:
        """A merge rule, as the bytes-level merge takes it.

        A ``MergeChoice`` names a side rather than computing a value, so
        it goes down untouched. A merge function is wrapped: each side is
        decoded with the repository's codec, and the result encoded back
        with it.
        """
        if isinstance(rule, MergeChoice):
            return rule
        decode = self._codec.decode
        encode = self._codec.encode_merged

        def lowered(
            old: bytes | None, ours: bytes | None, theirs: bytes | None
        ) -> bytes | MergeChoice:
            result = rule(
                decode(old) if old is not None else None,
                decode(ours) if ours is not None else None,
                decode(theirs) if theirs is not None else None,
            )
            if isinstance(result, MergeChoice):
                return result
            return encode(result)

        return lowered


def _first(*rules: MergeRule | None) -> MergeRule | None:
    """The first rule given; None if none is. A rule counts by being
    there, whatever its truth value."""
    return next((r for r in rules if r is not None), None)


def _commit(record: dict[str, Any]) -> Commit:
    """A commit record as the core gives it."""
    return Commit(
        hash=record["hash"],
        parents=tuple(record["parents"]),
        time=record["time"],
        info=record["info"],
        root=record["root"],
    )


class Snapshot(Mapping[str, Any]):
    """The state of the repository at one commit, read-only.

    A ``Mapping`` of decoded values, pinned to :attr:`commit`. :attr:`raw`
    is the same state as stored bytes, whatever the codec.
    """

    def __init__(self, repo: Repo, commit: str, *, root: str | None = None) -> None:
        self._repo = repo
        self.commit = commit
        # The keyset's root and the key index the first listing builds,
        # held by the core; made on first use when whoever made this
        # snapshot didn't already read the root.
        self._tree = None if root is None else _core.WorktreeState(root)

    def __repr__(self) -> str:
        return f"Snapshot(commit={self.commit[:8]}...)"

    def _on_tree(self, op: str, **args: Any) -> Any:
        if self._tree is None:
            tree = _core.WorktreeState(EMPTY_HASH)
            found = _kv._on_tree(
                tree, "wt_checkout", self._repo._store, commit=self.commit
            )
            if found["root"] is None:
                raise UnknownCommitError(f"Commit '{self.commit}' does not exist")
            self._tree = tree
        return _kv._on_tree(self._tree, op, self._repo._store, **args)

    def _raw_many(self, keys: tuple[str, ...]) -> dict[str, bytes]:
        # Keys holding equal bytes share one blob, which answers for
        # every key that points at it.
        wanted: dict[str, list[str]] = {}
        for key, (blob, _size, _chunks) in self._on_tree(
            "wt_lookup", keys=list(dict.fromkeys(keys))
        ):
            wanted.setdefault(blob, []).append(key)
        if not wanted:
            return {}
        found = self._repo._store.get_many(wanted.keys())
        return {key: raw for blob, raw in found.items() for key in wanted[blob]}

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
        return iter(self._on_tree("wt_keys"))

    def __len__(self) -> int:
        return len(self._on_tree("wt_keys"))

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self._on_tree("wt_contains", key=key)

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
