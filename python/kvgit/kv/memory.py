"""In-memory KV store."""

import threading
from collections.abc import Iterable, Mapping

from .base import KVStore


class Memory(KVStore):
    """A memory-backed KV store.

    All operations are protected by a single lock, making this
    implementation safe for concurrent readers and writers
    (including free-threaded Python 3.14+).
    """

    def __init__(self) -> None:
        self.memory: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> bytes | None:
        with self._lock:
            return self.memory.get(key)

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        with self._lock:
            self.memory[key] = value

    def get_many(self, *args) -> Mapping[str, bytes]:
        keys = self._normalize_keys(args)
        with self._lock:
            return {
                key: val for key in keys if (val := self.memory.get(key)) is not None
            }

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
        with self._lock:
            self.memory.update(items)

    def items(self) -> Iterable[tuple[str, bytes]]:
        with self._lock:
            return list(self.memory.items())

    def keys(self, prefix: str = "") -> Iterable[str]:
        with self._lock:
            return [k for k in self.memory if k.startswith(prefix)]

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self.memory

    def remove(self, key: str) -> None:
        with self._lock:
            self.memory.pop(key, None)

    def remove_many(self, *args) -> None:
        keys = self._normalize_keys(args)
        with self._lock:
            for key in keys:
                self.memory.pop(key, None)

    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        removes = self._check_batch(writes, removes)
        with self._lock:
            if any(self.memory.get(k) != v for k, v in expected.items()):
                return False
            self.memory.update(writes)
            for key in removes:
                self.memory.pop(key, None)
            return True

    def clear(self) -> None:
        with self._lock:
            self.memory.clear()
