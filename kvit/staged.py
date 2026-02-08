"""Staged: buffered writes over a Versioned store."""

from typing import Iterable

from .versioned import MergeFn, MergeResult, Versioned


class Staged:
    """Buffered write layer over a ``Versioned`` store.

    Individual ``set()`` / ``remove()`` calls are staged in memory.
    ``commit()`` flushes them to the underlying ``Versioned`` as a
    single atomic commit + merge.

    Implements the ``Store`` protocol.
    """

    def __init__(self, versioned: Versioned) -> None:
        self._versioned = versioned
        self._updates: dict[str, bytes] = {}
        self._removals: set[str] = set()

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value, checking staged changes first."""
        if key in self._removals:
            return None
        if key in self._updates:
            return self._updates[key]
        return self._versioned.get(key)

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values, respecting staged state."""
        result: dict[str, bytes] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result

    def keys(self) -> Iterable[str]:
        """All keys visible in the current state (committed + staged)."""
        seen: set[str] = set()
        for key in self._versioned.keys():
            if key not in self._removals:
                seen.add(key)
        seen.update(self._updates.keys())
        return seen

    def __contains__(self, key: str) -> bool:
        if key in self._removals:
            return False
        if key in self._updates:
            return True
        return key in self._versioned

    # -- Write operations --

    def set(self, key: str, value: bytes) -> None:
        """Stage a key-value pair for the next commit."""
        self._removals.discard(key)
        self._updates[key] = value

    def remove(self, key: str) -> None:
        """Stage a key removal for the next commit."""
        self._updates.pop(key, None)
        self._removals.add(key)

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

        Calls ``Versioned.commit()`` with the buffered updates and
        removals. On success, clears the staging buffer.

        Returns:
            A MergeResult (truthy when committed).
        """
        updates = self._updates if self._updates else None
        removals = self._removals if self._removals else None

        result = self._versioned.commit(
            updates,
            removals,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            default_merge=default_merge,
            info=info,
        )
        if result.merged:
            self._updates.clear()
            self._removals.clear()
        return result

    def reset(self) -> None:
        """Discard all staged changes."""
        self._updates.clear()
        self._removals.clear()

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

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a specific key."""
        self._versioned.set_merge_fn(key, fn)

    def set_content_type(self, key: str, ct) -> None:
        """Register a ContentType for a key."""
        self._versioned.set_content_type(key, ct)

    def get_content_type(self, key: str):
        """Retrieve the ContentType registered for a key, or None."""
        return self._versioned.get_content_type(key)

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function."""
        self._versioned.set_default_merge(fn)

    def create_branch(self, name: str) -> "Staged":
        """Fork the current commit onto a new branch. Returns a new Staged."""
        return Staged(self._versioned.create_branch(name))

    def checkout(self, commit_hash: str, *, branch: str | None = None) -> "Staged | None":
        """Create a new Staged at a specific commit. Returns None if not found."""
        v = self._versioned.checkout(commit_hash, branch=branch)
        if v is None:
            return None
        return Staged(v)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return self._versioned.list_branches()

    def refresh(self) -> None:
        """Reload from HEAD and discard staged changes."""
        self._versioned.refresh()
        self._updates.clear()
        self._removals.clear()
