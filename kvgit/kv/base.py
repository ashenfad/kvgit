"""Abstract KV store interface."""

from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping


class KVStore(ABC):
    """Key-value store operating on bytes only.

    All values are stored and retrieved as bytes. Serialization is
    handled at higher layers (a repository's codec).

    Beyond reads and writes, a backend owes kvgit two things:

    * ``cas_many`` — a write of several keys that applies only if
      several other keys hold what the caller expects, atomically. kvgit
      publishes every commit through it, with the GC lease among the
      expected keys, which is what lets a sweep run beside writers. Any
      store with a multi-key transaction or conditional batch can
      provide it (SQLite, Postgres, IndexedDB, Redis, DynamoDB).
    * ``keys(prefix)`` — the keys under a prefix, which a backend with an
      ordered index answers without scanning everything.

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
    def keys(self, prefix: str = "") -> Iterable[str]:
        """Iterate over all keys, or only those starting with ``prefix``."""

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
    def cas_many(
        self,
        expected: Mapping[str, bytes | None],
        writes: Mapping[str, bytes],
        removes: Iterable[str] = (),
    ) -> bool:
        """Apply ``writes`` and ``removes`` atomically, if and only if
        every key in ``expected`` currently holds its value.

        ``None`` as an expected value means "this key must not exist".
        Either every write and removal lands or none does, and no other
        writer's change can land between the check and the write. A key
        may appear in both ``expected`` and ``writes``; a key in both
        ``writes`` and ``removes`` is a caller error.

        Returns True if the batch was applied, False if any expectation
        failed (in which case nothing changed).
        """

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        """Atomic compare-and-swap of one key.

        Set value only if current value equals expected.
        None means "key must not exist".

        Returns True if swap succeeded, False otherwise.
        """
        if not isinstance(value, bytes):
            raise TypeError(f"Expected bytes, got {type(value).__name__}")
        return self.cas_many({key: expected}, {key: value})

    @abstractmethod
    def clear(self) -> None:
        """Remove all items from the store."""

    # ---- protected helpers for subclass implementations ----

    @staticmethod
    def _check_batch(writes: Mapping[str, bytes], removes: Iterable[str]) -> list[str]:
        """Validate a ``cas_many`` batch; return ``removes`` as a list."""
        for key, value in writes.items():
            if not isinstance(value, bytes):
                raise TypeError(f"Expected bytes for {key}, got {type(value).__name__}")
        removes = list(removes)
        clash = set(writes).intersection(removes)
        if clash:
            raise ValueError(f"keys both written and removed: {sorted(clash)}")
        return removes

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
