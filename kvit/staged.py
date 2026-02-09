"""Staged: buffered writes over a Versioned store."""

import pickle
from collections.abc import Iterator, MutableMapping
from typing import Any, Callable

from .content_types import MergeFn
from .versioned import BytesMergeFn, MergeResult, Versioned


class Staged(MutableMapping[str, Any]):
    """Buffered write layer over a ``Versioned`` store.

    Individual ``set()`` / ``remove()`` calls are staged in memory.
    ``commit()`` flushes them to the underlying ``Versioned`` as a
    single atomic commit + merge.

    Values are encoded to bytes on commit using the configured encoder.
    Implements ``MutableMapping[str, Any]`` and the ``Store`` protocol.
    """

    def __init__(
        self,
        versioned: Versioned,
        *,
        encoder: Callable[[Any], bytes] = pickle.dumps,
        decoder: Callable[[bytes], Any] = pickle.loads,
    ) -> None:
        self._versioned = versioned
        self._encoder = encoder
        self._decoder = decoder
        self._updates: dict[str, Any] = {}
        self._removals: set[str] = set()
        self._cache: dict[str, Any] = {}
        self._merge_fns: dict[str, MergeFn] = {}
        self._default_merge: MergeFn | None = None

    # -- Read operations --

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value, checking staged changes first."""
        if key in self._removals:
            return default
        if key in self._updates:
            return self._updates[key]
        if key in self._cache:
            return self._cache[key]
        raw = self._versioned.get(key)
        if raw is None:
            return default
        value = self._decoder(raw)
        self._cache[key] = value
        return value

    def get_many(self, *keys: str) -> dict[str, Any]:
        """Get multiple values, respecting staged state."""
        result: dict[str, Any] = {}
        for key in keys:
            if key in self:
                result[key] = self.get(key)
        return result

    def keys(self) -> set[str]:  # type: ignore[override]
        """All keys visible in the current state (committed + staged)."""
        seen: set[str] = set()
        for key in self._versioned.keys():
            if key not in self._removals:
                seen.add(key)
        seen.update(self._updates.keys())
        return seen

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key in self._removals:
            return False
        if key in self._updates:
            return True
        return key in self._versioned

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise KeyError(key)
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        self.remove(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    # -- Write operations --

    def set(self, key: str, value: Any) -> None:
        """Stage a key-value pair for the next commit."""
        self._removals.discard(key)
        self._updates[key] = value

    def remove(self, key: str) -> None:
        """Stage a key removal for the next commit."""
        self._updates.pop(key, None)
        self._removals.add(key)

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a specific key."""
        self._merge_fns[key] = fn

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function."""
        self._default_merge = fn

    def _wrap_merge_fn(self, fn: MergeFn) -> BytesMergeFn:
        """Wrap a user-level merge fn into a bytes-level merge fn."""
        encoder = self._encoder
        decoder = self._decoder

        def wrapped(
            old: bytes | None, ours: bytes | None, theirs: bytes | None
        ) -> bytes:
            old_val = decoder(old) if old is not None else None
            ours_val = decoder(ours) if ours is not None else None
            theirs_val = decoder(theirs) if theirs is not None else None
            return encoder(fn(old_val, ours_val, theirs_val))

        return wrapped

    # -- Commit / reset --

    def commit(
        self,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergeFn] | None = None,
        default_merge: MergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Flush staged changes to the underlying Versioned store.

        Encodes staged values to bytes, wraps merge functions, and
        calls ``Versioned.commit()``. On success, clears the staging
        buffer.

        Returns:
            A MergeResult (truthy when committed).
        """
        # Encode staged updates to bytes
        encoded_updates: dict[str, bytes] | None = None
        if self._updates:
            encoded_updates = {
                key: self._encoder(value)
                for key, value in self._updates.items()
            }

        removals = self._removals if self._removals else None

        # Build effective merge fns and wrap to bytes-level
        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_default = default_merge or self._default_merge

        bytes_merge_fns: dict[str, BytesMergeFn] | None = None
        if effective_fns:
            bytes_merge_fns = {
                key: self._wrap_merge_fn(fn)
                for key, fn in effective_fns.items()
            }

        bytes_default: BytesMergeFn | None = None
        if effective_default:
            bytes_default = self._wrap_merge_fn(effective_default)

        result = self._versioned.commit(
            encoded_updates,
            removals,
            on_conflict=on_conflict,
            merge_fns=bytes_merge_fns,
            default_merge=bytes_default,
            info=info,
        )
        if result.merged:
            self._updates.clear()
            self._removals.clear()
            self._cache.clear()
        return result

    def reset(self) -> None:
        """Discard all staged changes."""
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()

    @property
    def has_changes(self) -> bool:
        """Whether there are staged changes."""
        return bool(self._updates or self._removals)

    # -- Versioned pass-through --

    @property
    def versioned(self) -> Versioned:
        """The underlying Versioned instance."""
        return self._versioned

    @property
    def current_commit(self) -> str:
        return self._versioned.current_commit

    @property
    def base_commit(self) -> str:
        return self._versioned.base_commit

    @property
    def last_merge_result(self) -> MergeResult | None:
        return self._versioned.last_merge_result

    def create_branch(self, name: str) -> "Staged":
        """Fork the current commit onto a new branch. Returns a new Staged."""
        return Staged(
            self._versioned.create_branch(name),
            encoder=self._encoder,
            decoder=self._decoder,
        )

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "Staged | None":
        """Create a new Staged at a specific commit. Returns None if not found."""
        v = self._versioned.checkout(commit_hash, branch=branch)
        if v is None:
            return None
        return Staged(v, encoder=self._encoder, decoder=self._decoder)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return self._versioned.list_branches()

    def refresh(self) -> None:
        """Reload from HEAD and discard staged changes."""
        self._versioned.refresh()
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()
