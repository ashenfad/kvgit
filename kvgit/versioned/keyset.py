"""kvgit-specific wrapper around the generic HAMT.

A ``Keyset`` is a content-addressable map from user keys to
``KeysetEntry`` values, where each entry holds the versioned blob
key and its per-key metadata. This is what ``VersionedKV`` uses
to represent the state of a single commit.

The wrapper is a thin shim: encode/decode entries and delegate
everything else to ``Hamt``. The HAMT does the structural sharing
work; the Keyset just gives the API a kvgit-friendly shape.
"""

import json
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from typing import NamedTuple

from ..encoding import MetaEntry
from ..hamt import EMPTY_HASH, Hamt
from ..kv.base import KVStore


@dataclass(frozen=True)
class KeysetEntry:
    """One entry in a Keyset: a blob pointer plus its metadata."""

    blob: str
    meta: MetaEntry


def encode_entry(entry: KeysetEntry) -> bytes:
    """Serialize a KeysetEntry to bytes deterministically."""
    payload = [entry.blob, asdict(entry.meta)]
    return json.dumps(payload, separators=(",", ":")).encode()


def decode_entry(raw: bytes) -> KeysetEntry:
    """Deserialize bytes back into a KeysetEntry."""
    blob, meta_dict = json.loads(raw)
    return KeysetEntry(blob=blob, meta=MetaEntry(**meta_dict))


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
        self._hamt = Hamt(
            store,
            root,
            prefix=prefix,
            bucket_max=bucket_max,
            pending=pending,
        )

    @classmethod
    def _wrap(cls, hamt: Hamt) -> "Keyset":
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
        raw = self._hamt.get(key)
        if raw is None:
            return None
        return decode_entry(raw)

    def get_blob(self, key: str) -> str | None:
        """Shortcut: just the blob pointer, no meta."""
        entry = self.get(key)
        return None if entry is None else entry.blob

    def __contains__(self, key: str) -> bool:
        return key in self._hamt

    def items(self) -> Iterator[tuple[str, KeysetEntry]]:
        for k, raw in self._hamt.items():
            yield k, decode_entry(raw)

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
        encoded_updates: dict[str, bytes] | None = None
        if updates:
            encoded_updates = {k: encode_entry(v) for k, v in updates.items()}
        new_hamt, pending = self._hamt.updated(encoded_updates, removals)
        return Keyset._wrap(new_hamt), pending

    def persist(
        self,
        updates: Mapping[str, KeysetEntry] | None = None,
        removals: Iterable[str] = (),
    ) -> "Keyset":
        """Apply updates and write any new nodes to the store immediately.

        Distinct from ``Versioned.commit``: a Keyset has no notion of
        a commit history — this just flushes HAMT node bytes.
        """
        encoded_updates: dict[str, bytes] | None = None
        if updates:
            encoded_updates = {k: encode_entry(v) for k, v in updates.items()}
        new_hamt = self._hamt.persist(encoded_updates, removals)
        return Keyset._wrap(new_hamt)

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
        raw = self._hamt.diff(other._hamt)
        return KeysetDiff(
            added={k: decode_entry(v) for k, v in raw.added.items()},
            removed={k: decode_entry(v) for k, v in raw.removed.items()},
            modified={
                k: (decode_entry(old), decode_entry(new))
                for k, (old, new) in raw.modified.items()
            },
        )
