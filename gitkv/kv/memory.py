"""In-memory KV store."""

import threading
from typing import Iterable, Mapping

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

    def get_many(self, *args: str) -> Mapping[str, bytes]:
        with self._lock:
            return {
                key: val for key in args if (val := self.memory.get(key)) is not None
            }

    def set_many(self, **kwargs: bytes) -> None:
        for key, value in kwargs.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")
        with self._lock:
            self.memory.update(kwargs)

    def items(self) -> Iterable[tuple[str, bytes]]:
        with self._lock:
            return list(self.memory.items())

    def keys(self) -> Iterable[str]:
        with self._lock:
            return list(self.memory.keys())

    def __contains__(self, key: str) -> bool:
        with self._lock:
            return key in self.memory

    def remove(self, key: str) -> None:
        with self._lock:
            self.memory.pop(key, None)

    def remove_many(self, *keys: str) -> None:
        with self._lock:
            for key in keys:
                self.memory.pop(key, None)

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        with self._lock:
            current = self.memory.get(key)
            if current == expected:
                self.memory[key] = value
                return True
            return False

    def clear(self) -> None:
        with self._lock:
            self.memory.clear()
