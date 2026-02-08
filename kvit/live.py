"""Live: immediate-write store with no versioning."""

from typing import Iterable

from .kv.base import KVStore
from .kv.memory import Memory


class Live:
    """Immediate-write store backed by a ``KVStore``.

    Writes take effect immediately. Versioning operations
    (``commit``, ``create_branch``, ``checkout``, ``reset``)
    raise ``NotImplementedError``.

    Implements the ``Store`` protocol.
    """

    def __init__(self, backend: KVStore | None = None) -> None:
        self._store = backend if backend is not None else Memory()

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    def get_many(self, *keys: str) -> dict[str, bytes]:
        return dict(self._store.get_many(*keys))

    def keys(self) -> Iterable[str]:
        return self._store.keys()

    def __contains__(self, key: str) -> bool:
        return key in self._store

    # -- Write operations --

    def set(self, key: str, value: bytes) -> None:
        self._store.set(key, value)

    def remove(self, key: str) -> None:
        self._store.remove(key)

    # -- Versioning operations (not supported) --

    def commit(self, **kwargs):
        """Not supported. Live writes are immediate."""
        raise NotImplementedError("Live store does not support commit")

    def reset(self) -> None:
        """Not supported. Live has no staging buffer."""
        raise NotImplementedError("Live store does not support reset")

    def create_branch(self, name: str):
        """Not supported. Live has no versioning."""
        raise NotImplementedError("Live store does not support branching")

    def checkout(self, commit_hash: str, *, branch: str | None = None):
        """Not supported. Live has no versioning."""
        raise NotImplementedError("Live store does not support checkout")

    def list_branches(self) -> list[str]:
        """Not supported. Live has no versioning."""
        raise NotImplementedError("Live store does not support branching")
