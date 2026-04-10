"""Abstract KV store interface."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping


class KVStore(ABC):
    """Key-value store operating on bytes only.

    All values are stored and retrieved as bytes. Serialization is
    handled at higher layers (e.g., Versioned).

    Bulk methods (``set_many`` / ``get_many`` / ``remove_many``)
    accept two equivalent call forms — pass a Mapping/Iterable
    directly, or use the variadic ``**kwargs`` / ``*args`` form:

        store.set_many({"a": b"1", "b": b"2"})
        store.set_many(a=b"1", b=b"2")

        store.get_many(["a", "b"])
        store.get_many("a", "b")

        store.remove_many(["a", "b"])
        store.remove_many("a", "b")

    The Mapping/Iterable form is preferred in hot paths because it
    avoids the dict/tuple allocation that ``**dict`` / ``*list``
    unpacking incurs at the call boundary.
    """

    @abstractmethod
    def get(self, key: str) -> bytes | None:
        """Get bytes value for key, or None if not found."""

    @abstractmethod
    def set(self, key: str, value: bytes) -> None:
        """Set bytes value for key."""

    @abstractmethod
    def get_many(self, *args) -> Mapping[str, bytes]:
        """Get multiple keys, returning only keys that exist.

        Accepts either a single iterable of keys or many string
        positional args. See class docstring for examples.
        """

    @abstractmethod
    def set_many(
        self,
        items: Mapping[str, bytes] | None = None,
        /,
        **kwargs: bytes,
    ) -> None:
        """Set multiple key-value pairs.

        Accepts either a single Mapping or keyword arguments. See
        class docstring for examples.
        """

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
    def remove_many(self, *args) -> None:
        """Remove multiple keys.

        Accepts either a single iterable of keys or many string
        positional args. See class docstring for examples.
        """

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

    # ---- protected helpers for subclass implementations ----

    @staticmethod
    def _normalize_keys(args) -> Iterable[str]:
        """Normalize ``*args`` from get_many/remove_many to an iterable
        of keys.

        Accepts either a single non-string iterable or many positional
        string args. Subclasses call this from their bulk methods to
        support both call forms with one line of code.
        """
        if (
            len(args) == 1
            and isinstance(args[0], Iterable)
            and not isinstance(args[0], (str, bytes))
        ):
            return args[0]
        return args

    @staticmethod
    def _normalize_items(
        items: Mapping[str, bytes] | None,
        kwargs: Mapping[str, bytes],
    ) -> Mapping[str, bytes]:
        """Normalize set_many's positional + kwargs into a single Mapping.

        Subclasses call this from ``set_many`` to merge the two call
        forms into one container.
        """
        if items is None:
            return kwargs
        if not kwargs:
            return items
        return {**items, **kwargs}
