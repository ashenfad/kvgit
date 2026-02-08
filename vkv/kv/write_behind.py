"""Write-behind wrapper for latency masking."""

import queue
import sys
import threading
from typing import Iterable, Mapping

from .base import KVStore


class WriteBehind(KVStore):
    """Pushes writes to a background thread.

    Useful for masking the latency of slow storage backends
    by returning control to the caller immediately.
    """

    def __init__(self, store: KVStore) -> None:
        self.store = store
        self._queue: queue.Queue = queue.Queue()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _worker(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                break
            func_name, args, kwargs = item
            try:
                getattr(self.store, func_name)(*args, **kwargs)
            except Exception as e:
                print(f"WriteBehind error ({func_name}): {e}", file=sys.stderr)
            finally:
                self._queue.task_done()

    def get(self, key: str) -> bytes | None:
        self.flush()
        return self.store.get(key)

    def set(self, key: str, value: bytes) -> None:
        self._queue.put(("set", (key, value), {}))

    def get_many(self, *args: str) -> Mapping[str, bytes]:
        self.flush()
        return self.store.get_many(*args)

    def set_many(self, **kwargs: bytes) -> None:
        self._queue.put(("set_many", (), kwargs))

    def items(self) -> Iterable[tuple[str, bytes]]:
        self.flush()
        return self.store.items()

    def keys(self) -> Iterable[str]:
        self.flush()
        return self.store.keys()

    def __contains__(self, key: str) -> bool:
        self.flush()
        return key in self.store

    def remove(self, key: str) -> None:
        self._queue.put(("remove", (key,), {}))

    def remove_many(self, *keys: str) -> None:
        self._queue.put(("remove_many", keys, {}))

    def flush(self) -> None:
        """Wait for all pending writes to complete."""
        self._queue.join()

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        self.flush()
        return self.store.cas(key, value, expected)

    def clear(self) -> None:
        self.flush()
        self.store.clear()
