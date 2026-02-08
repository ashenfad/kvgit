"""N-tier composite cache over multiple KV stores."""

from typing import Iterable, Mapping

from .base import KVStore


class Composite(KVStore):
    """N-tier cache composing any number of KV stores.

    On get: check L1, L2, ..., Ln in order. On hit at tier i,
    populate L1..L(i-1) and return.

    On set: write to all tiers (most durable first).

    On cas: delegate to Ln (authoritative), update caches on success.

    Args:
        stores: List of KV stores ordered fastest -> most durable.
    """

    def __init__(self, stores: list[KVStore]) -> None:
        if not stores:
            raise ValueError("Composite requires at least one store")
        self._stores = stores

    def get(self, key: str) -> bytes | None:
        for i, store in enumerate(self._stores):
            try:
                value = store.get(key)
                if value is not None:
                    if i > 0:
                        for j in range(i):
                            try:
                                self._stores[j].set(key, value)
                            except Exception:
                                pass
                    return value
            except Exception:
                continue
        return None

    def get_many(self, *args: str) -> Mapping[str, bytes]:
        result: dict[str, bytes] = {}
        remaining = set(args)
        for i, store in enumerate(self._stores):
            if not remaining:
                break
            try:
                tier_values: dict[str, bytes] = {}
                for key in remaining:
                    value = store.get(key)
                    if value is not None:
                        tier_values[key] = value
                if tier_values and i > 0:
                    for j in range(i):
                        try:
                            self._stores[j].set_many(**tier_values)
                        except Exception:
                            pass
                result.update(tier_values)
                remaining -= tier_values.keys()
            except Exception:
                continue
        return result

    def __contains__(self, key: str) -> bool:
        for store in self._stores:
            try:
                if key in store:
                    return True
            except Exception:
                continue
        return False

    def keys(self) -> Iterable[str]:
        return self._stores[-1].keys()

    def items(self) -> Iterable[tuple[str, bytes]]:
        return self._stores[-1].items()

    def set(self, key: str, value: bytes) -> None:
        self._stores[-1].set(key, value)
        for store in self._stores[:-1]:
            try:
                store.set(key, value)
            except Exception:
                pass

    def set_many(self, **kwargs: bytes) -> None:
        self._stores[-1].set_many(**kwargs)
        for store in self._stores[:-1]:
            try:
                store.set_many(**kwargs)
            except Exception:
                pass

    def remove(self, key: str) -> None:
        self._stores[-1].remove(key)
        for store in self._stores[:-1]:
            try:
                store.remove(key)
            except Exception:
                pass

    def remove_many(self, *keys: str) -> None:
        self._stores[-1].remove_many(*keys)
        for store in self._stores[:-1]:
            try:
                store.remove_many(*keys)
            except Exception:
                pass

    def clear(self) -> None:
        self._stores[-1].clear()
        for store in self._stores[:-1]:
            try:
                store.clear()
            except Exception:
                pass

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        success = self._stores[-1].cas(key, value, expected)
        if success:
            for store in self._stores[:-1]:
                try:
                    store.set(key, value)
                except Exception:
                    pass
        return success
