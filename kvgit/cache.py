"""A repository's memory of what its store can never change.

Tree nodes and a commit's records — its parents, their generations,
its time and its info — are written once, under keys named by content
or by commit, and never rewritten. A read of one can be answered from
memory for as long as the store still holds it, which saves the round
trip to a networked backend that most reads of them are.

What the cache must not outlive is a deletion. Only a sweep deletes
these keys, and every sweep rewrites the GC lease record, which each
commit reads before it builds. So the cache remembers the record it
last saw and empties itself when a read of the lease finds another:
the sweep of any process on any host is noticed before this process
builds on what it may have removed. A sweep run through this process's
own store evicts what it removes as it removes it.

A commit's root is never cached. It is the store's answer to whether a
commit exists — what ``snapshot(commit=)`` and a tag's ``dangling``
ask — so it is always read, and whatever is reached through a root the
store still holds is safe to answer from here. Absence is never cached
either: a key missing now may be written by the next commit.
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from typing import Any

from .kv.base import KVStore
from .versioned.keyset import Keyset
from .versioned.kv import (
    COMMIT_TIME,
    GC_LEASE_KEY,
    INFO_KEY,
    PARENT_COMMIT,
    PARENT_GENS,
)

DEFAULT_CACHE_BYTES = 32 * 1024 * 1024
"""The default budget: tens of thousands of tree nodes."""

_CACHEABLE_PREFIXES = (
    Keyset.DEFAULT_PREFIX,
    PARENT_COMMIT.replace("%s", ""),
    PARENT_GENS.replace("%s", ""),
    COMMIT_TIME.replace("%s", ""),
    INFO_KEY.replace("%s", ""),
)

# What an entry costs beyond its bytes: the key, the dict slot, the
# object headers. An estimate, so the budget bounds memory rather than
# just payload.
_ENTRY_OVERHEAD = 96

_UNSEEN: Any = object()


def cacheable(key: str) -> bool:
    """Whether ``key`` is one the store never rewrites."""
    return key.startswith(_CACHEABLE_PREFIXES)


class ContentCache:
    """Least-recently-used memory of write-once keys, bounded in bytes.

    Shared by every worktree and snapshot of one :class:`~kvgit.Repo`,
    and safe to use from several threads.
    """

    def __init__(self, max_bytes: int = DEFAULT_CACHE_BYTES) -> None:
        if max_bytes <= 0:
            raise ValueError(f"max_bytes must be positive, got {max_bytes}")
        self.max_bytes = max_bytes
        self._entries: OrderedDict[str, bytes] = OrderedDict()
        self._bytes = 0
        self._lock = threading.Lock()
        self._lease: Any = _UNSEEN
        self.hits = 0
        """Reads answered from memory."""
        self.misses = 0
        """Reads of cacheable keys that went to the store."""
        self.clears = 0
        """How often a sweep emptied the cache."""

    def __repr__(self) -> str:
        return (
            f"ContentCache({len(self._entries)} entries, "
            f"{self._bytes}/{self.max_bytes} bytes)"
        )

    def __len__(self) -> int:
        return len(self._entries)

    @property
    def size_bytes(self) -> int:
        """What the entries held now cost, by the budget's estimate."""
        return self._bytes

    def clear(self) -> None:
        with self._lock:
            self._clear()

    def _clear(self) -> None:
        self._entries.clear()
        self._bytes = 0

    def lookup(self, keys: Iterable[str]) -> dict[str, bytes]:
        """The cached values of those ``keys`` held here."""
        found: dict[str, bytes] = {}
        with self._lock:
            for key in keys:
                value = self._entries.get(key)
                if value is not None:
                    self._entries.move_to_end(key)
                    found[key] = value
        return found

    def count(self, hits: int, misses: int) -> None:
        with self._lock:
            self.hits += hits
            self.misses += misses

    def remember(self, items: Mapping[str, bytes]) -> None:
        """Hold these values, evicting the least recently used to fit."""
        with self._lock:
            for key, value in items.items():
                if not isinstance(value, bytes | bytearray | memoryview):
                    continue
                cost = len(value) + len(key) + _ENTRY_OVERHEAD
                if cost > self.max_bytes:
                    continue
                old = self._entries.pop(key, None)
                if old is not None:
                    self._bytes -= len(old) + len(key) + _ENTRY_OVERHEAD
                self._entries[key] = bytes(value)
                self._bytes += cost
            while self._bytes > self.max_bytes and self._entries:
                key, value = self._entries.popitem(last=False)
                self._bytes -= len(value) + len(key) + _ENTRY_OVERHEAD

    def forget(self, keys: Iterable[str]) -> None:
        with self._lock:
            for key in keys:
                value = self._entries.pop(key, None)
                if value is not None:
                    self._bytes -= len(value) + len(key) + _ENTRY_OVERHEAD

    def saw_lease(self, record: bytes | None) -> None:
        """Note the GC lease record a read found; a different one than
        last time means a sweep has run, and what it removed may be
        held here."""
        with self._lock:
            if self._lease is not _UNSEEN and record != self._lease:
                self._clear()
                self.clears += 1
            self._lease = record


class CachedStore(KVStore):
    """A backend seen through a :class:`ContentCache`.

    Reads of write-once keys are answered from the cache where it can
    and remembered where it cannot; everything else passes through
    untouched. Successful writes of write-once keys are remembered too,
    so a commit's next read of its own tree is a hit, and removals
    evict. Anything else the backend offers (``close``, ``drop``, ...)
    is reached through this object as it would be on the backend.
    """

    def __init__(self, backend: KVStore, cache: ContentCache) -> None:
        self.backend = backend
        self.cache = cache

    def __repr__(self) -> str:
        return f"CachedStore({self.backend!r})"

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") or name in ("backend", "cache"):
            raise AttributeError(name)
        return getattr(self.backend, name)

    # -- reads -----------------------------------------------------------

    def get(self, key: str) -> bytes | None:
        if cacheable(key):
            held = self.cache.lookup((key,))
            if key in held:
                self.cache.count(1, 0)
                return held[key]
            value = self.backend.get(key)
            self.cache.count(0, 1)
            if value is not None:
                self.cache.remember({key: value})
            return value
        value = self.backend.get(key)
        if key == GC_LEASE_KEY:
            self.cache.saw_lease(value)
        return value

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = list(dict.fromkeys(self._normalize_keys(args)))
        wanted = [k for k in keys if cacheable(k)]
        if GC_LEASE_KEY in keys:
            # A read of the lease may reveal a sweep, and nothing the
            # cache held before it may be answered beside it: the whole
            # call goes to the store, and the record is noted before
            # anything it brought back is remembered.
            held: dict[str, bytes] = {}
            found = dict(self.backend.get_many(keys))
            self.cache.saw_lease(found.get(GC_LEASE_KEY))
        else:
            held = self.cache.lookup(wanted) if wanted else {}
            rest = [k for k in keys if k not in held]
            found = dict(self.backend.get_many(rest)) if rest else {}
        fetched = {k: v for k, v in found.items() if cacheable(k)}
        if wanted:
            self.cache.count(len(held), len(wanted) - len(held))
        if fetched:
            self.cache.remember(fetched)
        found.update(held)
        return found

    def items(self) -> Iterable[tuple[str, bytes]]:
        return self.backend.items()

    def keys(self, prefix: str = "") -> Iterable[str]:
        return self.backend.keys(prefix)

    def __contains__(self, key: str) -> bool:
        if cacheable(key) and self.cache.lookup((key,)):
            return True
        return key in self.backend

    # -- writes ----------------------------------------------------------

    def set(self, key: str, value: bytes) -> None:
        self.backend.set(key, value)
        self._wrote({key: value}, ())

    def set_many(
        self, items: Mapping[str, bytes] | None = None, /, **kwargs: bytes
    ) -> None:
        items = dict(self._normalize_items(items, kwargs))
        self.backend.set_many(items)
        self._wrote(items, ())

    def remove(self, key: str) -> None:
        self.backend.remove(key)
        self._wrote({}, (key,))

    def remove_many(self, *args) -> None:
        keys = list(self._normalize_keys(args))
        self.backend.remove_many(keys)
        self._wrote({}, keys)

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = list(removes)
        if not self.backend.cas_many(expected, writes, removes):
            return False
        self._wrote(writes, removes)
        return True

    def clear(self) -> None:
        self.backend.clear()
        self.cache.clear()

    def _wrote(self, writes: Mapping[str, bytes], removes: Iterable[str]) -> None:
        gone = [k for k in removes if cacheable(k)]
        if gone:
            self.cache.forget(gone)
        landed = {k: v for k, v in writes.items() if cacheable(k)}
        if landed:
            self.cache.remember(landed)
        if GC_LEASE_KEY in writes:
            # This process's own sweep taking or releasing the lease.
            self.cache.saw_lease(writes[GC_LEASE_KEY])
