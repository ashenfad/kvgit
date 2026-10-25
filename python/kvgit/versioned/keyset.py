"""kvgit-specific wrapper around the generic HAMT.

A ``Keyset`` is a content-addressable map from user keys to
``KeysetEntry`` values, where each entry holds the versioned blob
key and its per-key metadata. This is what ``VersionedKV`` uses
to represent the state of a single commit.

The wrapper is a thin shim: encode/decode entries and delegate
everything else to ``Hamt``. The HAMT does the structural sharing
work; the Keyset just gives the API a kvgit-friendly shape.
"""

from collections.abc import Iterable, Iterator, Mapping
from typing import NamedTuple, cast

from .. import _core
from ..hamt import EMPTY_HASH, Hamt
from ..kv.base import KVStore

MetaEntry = _core.MetaEntry
"""Per-key metadata stored alongside a blob pointer in a Keyset:
``MetaEntry(size, created_at=None, chunks=None)``.

``chunks`` lists the content-addressed chunk references that the
blob references via the chunked-codec mechanism. ``None`` means
"this blob is opaque (e.g. plain pickle) — no chunks to track".
Stored only when non-empty.

``created_at`` is only ever set on entries written before storage
v4. Everything an entry holds must follow from the blob's bytes,
so that identical content gives identical HAMT nodes; a timestamp
made every node unique to the moment it was written. Old entries
keep theirs, so a subtree nobody touched re-encodes to the bytes
it was stored as.
"""

KeysetEntry = _core.KeysetEntry
"""One entry in a Keyset: ``KeysetEntry(blob, meta)``, a blob pointer
plus its metadata."""

encode_entry = _core.encode_entry
"""Serialize a KeysetEntry to bytes deterministically: ``size``
always, ``created_at`` and ``chunks`` only when set."""

decode_entry = _core.decode_entry
"""Deserialize bytes back into a KeysetEntry."""


class _EntryTree(Hamt[KeysetEntry]):
    """A HAMT whose values are ``KeysetEntry``: the core decodes and
    encodes them."""

    _entries = True


class KeysetDiff(NamedTuple):
    """Structural diff between two Keyset roots."""

    added: dict[str, KeysetEntry]
    removed: dict[str, KeysetEntry]
    modified: dict[str, tuple[KeysetEntry, KeysetEntry]]


class Keyset:
    """Immutable view of a kvgit keyset, backed by a HAMT.

    Mutations return a new ``Keyset`` whose ``pending`` dict carries
    any new node bytes not yet flushed to the store. Use ``flush()``
    or ``commit()`` to persist, or merge ``pending`` into a larger
    write batch.
    """

    DEFAULT_PREFIX = "kvgit:keyset:"

    def __init__(
        self,
        store: KVStore,
        root: str = EMPTY_HASH,
        *,
        prefix: str = DEFAULT_PREFIX,
        bucket_max: int = 8,
        pending: dict[str, bytes] | None = None,
    ) -> None:
        self._hamt: Hamt[KeysetEntry] = _EntryTree(
            store,
            root,
            prefix=prefix,
            bucket_max=bucket_max,
            pending=pending,
        )

    @classmethod
    def _wrap(cls, hamt: Hamt[KeysetEntry]) -> "Keyset":
        """Wrap an existing Hamt without re-allocating it."""
        ks = cls.__new__(cls)
        ks._hamt = hamt
        return ks

    # ---- properties ----

    @property
    def store(self) -> KVStore:
        return self._hamt.store

    @property
    def root(self) -> str:
        return self._hamt.root

    @property
    def prefix(self) -> str:
        return self._hamt.prefix

    @property
    def bucket_max(self) -> int:
        return self._hamt.bucket_max

    @property
    def pending(self) -> dict[str, bytes]:
        return self._hamt.pending

    # ---- reads ----

    def get(self, key: str) -> KeysetEntry | None:
        return self._hamt.get(key)

    def get_many(self, keys: Iterable[str]) -> dict[str, KeysetEntry]:
        """The entries of the keys that are present, reading their paths
        in one batched read per tree level; see ``Hamt.get_many``."""
        return self._hamt.get_many(keys)

    def get_blob(self, key: str) -> str | None:
        """Shortcut: just the blob pointer, no meta."""
        entry = self.get(key)
        return None if entry is None else entry.blob

    def __contains__(self, key: str) -> bool:
        return key in self._hamt

    def items(self) -> Iterator[tuple[str, KeysetEntry]]:
        """Iterate over all (key, entry) pairs lazily.

        See ``materialize()`` for a batched-read alternative when the
        underlying store has non-trivial per-call latency.
        """
        yield from self._hamt.items()

    def materialize(self) -> dict[str, KeysetEntry]:
        """Walk the entire keyset using batched reads.

        Returns ``{key: KeysetEntry}`` after one batched store fetch
        per HAMT level. See ``Hamt.materialize`` for the underlying
        mechanism and tradeoffs.
        """
        return self._hamt.materialize()

    def walk(
        self, skip_nodes: set[str] | None = None
    ) -> tuple[dict[str, KeysetEntry], set[str]]:
        """Single batched walk returning (entries, hamt_node_hashes).

        Equivalent to ``materialize()`` plus collecting every HAMT
        node hash, in one tree traversal. Used by GC mark phases
        that need both — see ``Hamt.walk`` (including the
        ``skip_nodes`` cumulative seen-set semantics).
        """
        return self._hamt.walk(skip_nodes=skip_nodes)

    def keys(self) -> Iterator[str]:
        return self._hamt.keys()

    def values(self) -> Iterator[KeysetEntry]:
        for _, entry in self.items():
            yield entry

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    def __len__(self) -> int:
        return len(self._hamt)

    # ---- writes ----

    def updated(
        self,
        updates: Mapping[str, KeysetEntry] | None = None,
        removals: Iterable[str] = (),
    ) -> tuple["Keyset", dict[str, bytes]]:
        """Apply updates and removals.

        Returns ``(new_keyset, pending_writes)`` where
        ``pending_writes`` is a dict of prefixed-key -> node-bytes
        ready to merge into a store write batch.
        """
        new_hamt, pending = self._hamt.updated(updates, removals)
        return Keyset._wrap(new_hamt), pending

    def persist(
        self,
        updates: Mapping[str, KeysetEntry] | None = None,
        removals: Iterable[str] = (),
    ) -> "Keyset":
        """Apply updates and write any new nodes to the store immediately.

        Distinct from ``VersionedKV.commit``: a Keyset has no notion of
        a commit history — this just flushes HAMT node bytes.
        """
        return Keyset._wrap(self._hamt.persist(updates, removals))

    def flush(self) -> "Keyset":
        """Persist any pending node writes. Returns a fresh ``Keyset``."""
        return Keyset._wrap(self._hamt.flush())

    # ---- structural ops ----

    def reachable_nodes(self) -> Iterator[str]:
        """Yield every HAMT node hash reachable from this root.

        Used by ``clean_orphans``' mark phase.
        """
        return self._hamt.reachable_nodes()

    def diff(self, other: "Keyset") -> KeysetDiff:
        """Structural diff against ``other``.

        Skips identical subtrees by hash equality, so the cost is
        proportional to the number of changed entries.
        """
        # The tree's values are entries, whatever HamtDiff says.
        added, removed, modified = cast(tuple, self._hamt.diff(other._hamt))
        return KeysetDiff(added=added, removed=removed, modified=modified)
