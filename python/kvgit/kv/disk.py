"""Disk-backed KV store using diskcache."""

from collections.abc import Iterable, Mapping
from typing import cast

from .base import KVStore

# diskcache has no native "unlimited" sentinel — its eviction policy
# is driven by a numeric byte cap. We use a value far above any
# realistic disk size as the effective "no limit" default. Callers
# that want a real cap pass an explicit size_limit.
_UNBOUNDED = 2**62  # ~4.6 exabytes


class Disk(KVStore):
    """KV store backed by diskcache (SQLite + mmap).

    By default the store has no practical size cap. Pass an explicit
    ``size_limit`` (in bytes) to enable diskcache's eviction policy.
    """

    def __init__(self, directory: str, size_limit: int | None = None) -> None:
        from diskcache import Cache as DiskCache

        if size_limit is None:
            size_limit = _UNBOUNDED
        self.store = DiskCache(directory, size_limit=size_limit)

    def get(self, key: str) -> bytes | None:
        return cast(bytes | None, self.store.get(key))

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        self.store[key] = value

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = self._normalize_keys(args)
        return {k: v for k in keys if (v := self.get(k)) is not None}

    def set_many(
        self,
        items: Mapping[str, bytes] | None = None,
        /,
        **kwargs: bytes,
    ) -> None:
        items = self._normalize_items(items, kwargs)
        for key, value in items.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")
        with self.store.transact():
            for key, value in items.items():
                self.set(key, value)

    def items(self) -> Iterable[tuple[str, bytes]]:
        for key in self.store.iterkeys():
            yield str(key), cast(bytes, self.store[key])

    def keys(self, prefix: str = "") -> Iterable[str]:
        for key in self.store.iterkeys():
            if str(key).startswith(prefix):
                yield str(key)

    def __contains__(self, key: str) -> bool:
        return key in self.store

    def remove(self, key: str) -> None:
        try:
            del self.store[key]
        except KeyError:
            pass

    def remove_many(self, *args) -> None:
        keys = self._normalize_keys(args)
        with self.store.transact():
            for key in keys:
                self.store.delete(key, retry=False)

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = self._check_batch(writes, removes)
        # One SQLite transaction, which diskcache opens with BEGIN
        # IMMEDIATE: it holds the write lock from the first read, so no
        # other process's write can land between the check and the batch.
        with self.store.transact():
            for key, value in expected.items():
                if cast(bytes | None, self.store.get(key)) != value:
                    return False
            for key, value in writes.items():
                self.store[key] = value
            for key in removes:
                self.store.delete(key, retry=False)
            return True

    def clear(self) -> None:
        self.store.clear()

    def close(self) -> None:
        # diskcache holds SQLite connections (one per thread) open until
        # closed. A short-lived Repo (a one-off gc or teardown) must
        # release the handle so the next opener on the same directory
        # isn't blocked.
        self.store.close()
