"""Abstract KV store interface."""

from abc import ABC, abstractmethod
from typing import Iterable, Mapping


class KVStore(ABC):
    """Key-value store operating on bytes only.

    All values are stored and retrieved as bytes. Serialization is
    handled at higher layers (e.g., Versioned).
    """

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Get bytes value for key, or None if not found."""

    @abstractmethod
    def set(self, key: str, value: bytes) -> None:
        """Set bytes value for key."""

    @abstractmethod
    def get_many(self, *args: str) -> Mapping[str, bytes]:
        """Get multiple keys, returning only keys that exist."""

    @abstractmethod
    def set_many(self, **kwargs: bytes) -> None:
        """Set multiple key-value pairs."""

    @abstractmethod
    def items(self) -> Iterable[tuple[str, bytes]]:
        """Iterate over all key-value pairs."""

    @abstractmethod
    def keys(self) -> Iterable[str]:
        """Iterate over all keys."""

    @abstractmethod
    def __contains__(self, key: str) -> bool:
        """Check if key exists in store."""

    @abstractmethod
    def remove(self, key: str) -> None:
        """Remove a key if present."""

    @abstractmethod
    def remove_many(self, *keys: str) -> None:
        """Remove multiple keys."""

    @abstractmethod
    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        """Atomic compare-and-swap.

        Set value only if current value equals expected.
        None means "key must not exist".

        Returns True if swap succeeded, False otherwise.
        """

    @abstractmethod
    def clear(self) -> None:
        """Remove all items from the store."""
