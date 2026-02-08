"""Namespaced: key-prefixed view over a Store."""

from __future__ import annotations

from typing import Iterable

from .store import Store
from .versioned import MergeFn, MergeResult


class Namespaced:
    """A namespaced view over a Store.

    Keys are prefixed with ``namespace/``. Nested namespaces are
    supported by wrapping another Namespaced instance.

    Implements the ``Store`` protocol.

    Args:
        store: Any Store (Staged, Live, or another Namespaced).
        namespace: The namespace name (must not contain ``/``).
    """

    def __init__(self, store: Store, namespace: str) -> None:
        if "/" in namespace:
            raise ValueError("Namespace names cannot contain '/'")
        if not isinstance(store, Store):
            raise TypeError(
                f"Namespaced requires a Store, "
                f"not {type(store).__name__}"
            )

        self._store = store

        if isinstance(store, Namespaced):
            self.namespace = f"{store.namespace}/{namespace}"
        else:
            self.namespace = namespace

    def _prefixed(self, key: str) -> str:
        return f"{self.namespace}/{key}"

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the namespaced view."""
        return self._store.get(self._prefixed(key))

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the namespaced view."""
        prefixed = {self._prefixed(k): k for k in keys}
        result = self._store.get_many(*prefixed.keys())
        return {prefixed[pk]: v for pk, v in result.items()}

    def keys(self) -> Iterable[str]:
        """Direct child keys in this namespace (not nested)."""
        prefix = f"{self.namespace}/"
        for key in self._store.keys():
            if key.startswith(prefix):
                remainder = key[len(prefix):]
                if remainder and "/" not in remainder:
                    yield remainder

    def descendant_keys(self) -> Iterable[str]:
        """All keys under this namespace, including nested."""
        prefix = f"{self.namespace}/"
        for key in self._store.keys():
            if key.startswith(prefix):
                yield key[len(prefix):]

    def __contains__(self, key: str) -> bool:
        return self._prefixed(key) in self._store

    # -- Write operations --

    def set(self, key: str, value: bytes) -> None:
        """Set a value in the namespaced view."""
        self._store.set(self._prefixed(key), value)

    def remove(self, key: str) -> None:
        """Remove a key from the namespaced view."""
        self._store.remove(self._prefixed(key))

    # -- Commit / reset --

    def commit(self, **kwargs) -> MergeResult:
        """Commit changes (delegates to underlying store)."""
        return self._store.commit(**kwargs)

    def reset(self) -> None:
        """Reset the underlying store."""
        self._store.reset()

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a namespaced key."""
        if hasattr(self._store, "set_merge_fn"):
            self._store.set_merge_fn(self._prefixed(key), fn)

    def set_content_type(self, key: str, ct) -> None:
        """Register a ContentType for a namespaced key."""
        if hasattr(self._store, "set_content_type"):
            self._store.set_content_type(self._prefixed(key), ct)

    def get_content_type(self, key: str):
        """Retrieve the ContentType registered for a namespaced key, or None."""
        if hasattr(self._store, "get_content_type"):
            return self._store.get_content_type(self._prefixed(key))
        return None

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function (store-wide)."""
        if hasattr(self._store, "set_default_merge"):
            self._store.set_default_merge(fn)

    def create_branch(self, name: str):
        """Create a branch (delegates to underlying store)."""
        return self._store.create_branch(name)

    def checkout(self, commit_hash: str, *, branch: str | None = None):
        """Checkout a commit (delegates to underlying store)."""
        return self._store.checkout(commit_hash, branch=branch)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return self._store.list_branches()

    # -- Convenience properties --

    @property
    def current_commit(self) -> str | None:
        if hasattr(self._store, "current_commit"):
            return self._store.current_commit
        return None

    @property
    def base_commit(self) -> str | None:
        if hasattr(self._store, "base_commit"):
            return self._store.base_commit
        return None

    @property
    def last_merge_result(self) -> MergeResult | None:
        if hasattr(self._store, "last_merge_result"):
            return self._store.last_merge_result
        return None
