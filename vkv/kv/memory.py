"""In-memory KV store."""

import threading
from typing import Iterable, Mapping

from .base import KVStore


class Memory(KVStore):
    """A memory-backed KV store."""

    def __init__(self) -> None:
        self.memory: dict[str, bytes] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> bytes | None:
        return self.memory.get(key)

    def set(self, key: str, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        self.memory[key] = value

    def get_many(self, *args: str) -> Mapping[str, bytes]:
        return {key: val for key in args if (val := self.memory.get(key)) is not None}

    def set_many(self, **kwargs: bytes) -> None:
        for key, value in kwargs.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")
        self.memory.update(kwargs)

    def items(self) -> Iterable[tuple[str, bytes]]:
        return self.memory.items()

    def keys(self) -> Iterable[str]:
        return self.memory.keys()

    def __contains__(self, key: str) -> bool:
        return key in self.memory

    def remove(self, key: str) -> None:
        self.memory.pop(key, None)

    def remove_many(self, *keys: str) -> None:
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
        self.memory.clear()
