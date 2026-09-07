"""N-tier composite cache over multiple KV stores."""

import logging
from collections.abc import Iterable, Mapping

from .base import KVStore

logger = logging.getLogger("kvgit.kv.composite")

# Exceptions we treat as programming bugs (a misconfigured tier, a
# protocol mismatch) rather than operational tier unavailability.
# These are re-raised so they surface instead of being silently masked
# by the cache-fallback machinery. Everything else under Exception
# (OSError, ConnectionError, Pyodide JsException, ...) is logged and
# treated as "tier unavailable, try next".
_BUG_EXCEPTIONS = (TypeError, AttributeError, AssertionError)


def _is_bug(exc: BaseException) -> bool:
    return isinstance(exc, _BUG_EXCEPTIONS)


MUTABLE_PREFIX = "__"
"""Key prefix marking a value that may change under a fixed key.

Every mutable key kvgit writes starts with it — branch heads, their
prev-HEAD backups, the storage version stamp, the GC lease — and so does
commit metadata, which is immutable but small enough that reading it
from the authoritative tier costs little. Everything else is derived
from its own content (``kvgit:keyset:``, ``kvgit:chunk:``,
``<commit>:<key>``): the same key always holds the same bytes, which is
what makes it cacheable.
"""


def _is_mutable(key: str) -> bool:
    return isinstance(key, str) and key.startswith(MUTABLE_PREFIX)


class Composite(KVStore):
    """N-tier cache composing any number of KV stores.

    **Cache tiers serve only immutable, content-derived keys.** A key
    starting with ``__`` names a value that changes under a fixed key —
    a branch head, its recovery backup, the storage version stamp, the
    GC lease — so ``get``, ``get_many`` and ``__contains__`` read those
    from Ln alone and never populate a cache with them. Cached, they
    would let a process keep serving a branch head that another process
    has already moved: the handle would take a ``ConcurrencyError`` on
    commit, call ``refresh()``, and read the same stale head back out of
    L1. Everything else is keyed by its own content
    (``kvgit:keyset:``, ``kvgit:chunk:``, ``<commit>:<key>``), so a hit
    at any tier is the right answer forever.

    On get: for a ``__`` key, read Ln. Otherwise check L1, L2, ..., Ln
    in order; on hit at tier i, populate L1..L(i-1) and return.

    On set: write to all tiers (most durable first).

    On cas: delegate to Ln (authoritative), update caches on success —
    except for ``__`` keys, which no cache tier serves.

    Tier failures (``OSError``, network errors, etc.) are logged at
    WARNING and the next tier is tried; programming-error exceptions
    (``TypeError``, ``AttributeError``, ``AssertionError``) propagate
    so they aren't silently swallowed.

    Args:
        stores: List of KV stores ordered fastest -> most durable.
    """

    def __init__(self, stores: list[KVStore]) -> None:
        if not stores:
            raise ValueError("Composite requires at least one store")
        self._stores = stores

    def _populate_caches(self, upto: int, items: Mapping[str, bytes]) -> None:
        """Best-effort write to faster tiers after a slow-tier hit."""
        for j in range(upto):
            try:
                self._stores[j].set_many(items)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite cache populate failed at tier %d: %s", j, e)

    def _first_read_tier(self, key: str) -> int:
        """Index of the highest tier allowed to answer for ``key``.

        A ``__``-prefixed key names a value that can change under a
        fixed key, so only the authoritative tier may answer it;
        everything else is content-derived and may be served by any
        tier.
        """
        return len(self._stores) - 1 if _is_mutable(key) else 0

    def get(self, key: str) -> bytes | None:
        start = self._first_read_tier(key)
        for i in range(start, len(self._stores)):
            store = self._stores[i]
            try:
                value = store.get(key)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite get failed at tier %d for %r: %s", i, key, e)
                continue
            if value is not None:
                # Nothing to populate when the read started at the
                # authoritative tier, which is where a mutable key's
                # read starts and ends.
                if i > start:
                    self._populate_caches(i, {key: value})
                return value
        return None

    def _get_many_from(self, remaining: set[str], start: int) -> dict[str, bytes]:
        """Bulk read ``remaining``, consulting tiers ``start``..Ln."""
        result: dict[str, bytes] = {}
        for i in range(start, len(self._stores)):
            if not remaining:
                break
            store = self._stores[i]
            try:
                # Delegate to the tier's bulk get — backends with high
                # per-call latency (Disk, IndexedDB) collapse N round-trips
                # into one. The protocol guarantees only existing keys
                # appear in the result.
                tier_values = store.get_many(remaining)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite get_many failed at tier %d: %s", i, e)
                continue
            if tier_values and i > start:
                self._populate_caches(i, tier_values)
            result.update(tier_values)
            remaining = remaining - tier_values.keys()
        return result

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = set(self._normalize_keys(args))
        mutable = {key for key in keys if _is_mutable(key)}
        if not mutable:
            return self._get_many_from(keys, 0)
        # Two passes rather than one: mutable keys must come from the
        # authoritative tier and must not be written into a cache, so
        # they cannot share a batch with content-derived keys.
        result = self._get_many_from(mutable, len(self._stores) - 1)
        result.update(self._get_many_from(keys - mutable, 0))
        return result

    def __contains__(self, key: str) -> bool:
        for i in range(self._first_read_tier(key), len(self._stores)):
            try:
                if key in self._stores[i]:
                    return True
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning(
                    "Composite contains failed at tier %d for %r: %s", i, key, e
                )
                continue
        return False

    def keys(self) -> Iterable[str]:
        return self._stores[-1].keys()

    def items(self) -> Iterable[tuple[str, bytes]]:
        return self._stores[-1].items()

    def set(self, key: str, value: bytes) -> None:
        # Authoritative tier first; failures here propagate (durability
        # is the contract of set()). Cache-tier failures are logged.
        self._stores[-1].set(key, value)
        for i, store in enumerate(self._stores[:-1]):
            try:
                store.set(key, value)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite set failed at tier %d for %r: %s", i, key, e)

    def set_many(
        self,
        items: Mapping[str, bytes] | None = None,
        /,
        **kwargs: bytes,
    ) -> None:
        items = self._normalize_items(items, kwargs)
        self._stores[-1].set_many(items)
        for i, store in enumerate(self._stores[:-1]):
            try:
                store.set_many(items)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite set_many failed at tier %d: %s", i, e)

    def remove(self, key: str) -> None:
        self._stores[-1].remove(key)
        for i, store in enumerate(self._stores[:-1]):
            try:
                store.remove(key)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning(
                    "Composite remove failed at tier %d for %r: %s", i, key, e
                )

    def remove_many(self, *args) -> None:
        keys = list(self._normalize_keys(args))
        self._stores[-1].remove_many(keys)
        for i, store in enumerate(self._stores[:-1]):
            try:
                store.remove_many(keys)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite remove_many failed at tier %d: %s", i, e)

    def clear(self) -> None:
        self._stores[-1].clear()
        for i, store in enumerate(self._stores[:-1]):
            try:
                store.clear()
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite clear failed at tier %d: %s", i, e)

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        success = self._stores[-1].cas(key, value, expected)
        # A ``__``-prefixed key is read from the authoritative tier
        # only, so writing it into a cache stores bytes nothing will
        # ever read, and a CAS is almost always against such a key.
        if success and not _is_mutable(key):
            for i, store in enumerate(self._stores[:-1]):
                try:
                    store.set(key, value)
                except Exception as e:
                    if _is_bug(e):
                        raise
                    logger.warning(
                        "Composite cas cache-update failed at tier %d for %r: %s",
                        i,
                        key,
                        e,
                    )
        return success
