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


class Composite(KVStore):
    """N-tier cache composing any number of KV stores.

    On get: check L1, L2, ..., Ln in order. On hit at tier i,
    populate L1..L(i-1) and return.

    On set: write to all tiers (most durable first).

    On cas: delegate to Ln (authoritative), update caches on success.

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

    def get(self, key: str) -> bytes | None:
        for i, store in enumerate(self._stores):
            try:
                value = store.get(key)
            except Exception as e:
                if _is_bug(e):
                    raise
                logger.warning("Composite get failed at tier %d for %r: %s", i, key, e)
                continue
            if value is not None:
                if i > 0:
                    self._populate_caches(i, {key: value})
                return value
        return None

    def get_many(self, *args) -> Mapping[str, bytes]:
        result: dict[str, bytes] = {}
        remaining = set(self._normalize_keys(args))
        for i, store in enumerate(self._stores):
            if not remaining:
                break
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
            if tier_values and i > 0:
                self._populate_caches(i, tier_values)
            result.update(tier_values)
            remaining -= tier_values.keys()
        return result

    def __contains__(self, key: str) -> bool:
        for i, store in enumerate(self._stores):
            try:
                if key in store:
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
        if success:
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
