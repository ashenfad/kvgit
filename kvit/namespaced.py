"""Namespaced: key-prefixed view over a Store."""

from collections.abc import Iterator, MutableMapping
from typing import Any, Iterable

from .content_types import MergeFn
from .store import Store
from .versioned import MergeResult


class Namespaced(MutableMapping[str, Any]):
    """A namespaced view over a Store.

    Keys are prefixed with ``namespace/``. Nested namespaces are
    supported by wrapping another Namespaced instance.

    Implements ``MutableMapping[str, Any]`` and the ``Store`` protocol.

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

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the namespaced view."""
        return self._store.get(self._prefixed(key), default)

    def get_many(self, *keys: str) -> dict[str, Any]:
        """Get multiple values from the namespaced view."""
        prefixed = {self._prefixed(k): k for k in keys}
        result = self._store.get_many(*prefixed.keys())
        return {prefixed[pk]: v for pk, v in result.items()}

    def keys(self) -> set[str]:
        """Direct child keys in this namespace (not nested)."""
        prefix = f"{self.namespace}/"
        result: set[str] = set()
        for key in self._store.keys():
            if key.startswith(prefix):
                remainder = key[len(prefix):]
                if remainder and "/" not in remainder:
                    result.add(remainder)
        return result

    def descendant_keys(self) -> Iterable[str]:
        """All keys under this namespace, including nested."""
        prefix = f"{self.namespace}/"
        for key in self._store.keys():
            if key.startswith(prefix):
                yield key[len(prefix):]

    def __contains__(self, key: object) -> bool:
        return self._prefixed(key) in self._store

    def __getitem__(self, key: str) -> Any:
        return self._store[self._prefixed(key)]

    def __setitem__(self, key: str, value: Any) -> None:
        self._store[self._prefixed(key)] = value

    def __delitem__(self, key: str) -> None:
        del self._store[self._prefixed(key)]

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    # -- Write operations --

    def set(self, key: str, value: Any) -> None:
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
