"""Namespaced: key-prefixed view over a MutableMapping."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from typing import Any, Iterable


class Namespaced(MutableMapping[str, Any]):
    """A namespaced view over a MutableMapping.

    Keys are prefixed with ``namespace/``. Nested namespaces are
    supported by wrapping another Namespaced instance.

    Args:
        store: Any MutableMapping (Staged, Live, or another Namespaced).
        namespace: The namespace name (must not contain ``/``).
    """

    def __init__(self, store: MutableMapping[str, Any], namespace: str) -> None:
        if "/" in namespace:
            raise ValueError("Namespace names cannot contain '/'")
        if not isinstance(store, MutableMapping):
            raise TypeError(
                f"Namespaced requires a MutableMapping, "
                f"not {type(store).__name__}"
            )

        if isinstance(store, Namespaced):
            self.namespace = f"{store.namespace}/{namespace}"
            self._store: MutableMapping[str, Any] = store._store
        else:
            self.namespace = namespace
            self._store = store

    def _prefixed(self, key: str) -> str:
        return f"{self.namespace}/{key}"

    # -- Read operations --

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the namespaced view."""
        return self._store.get(self._prefixed(key), default)

    def get_many(self, *keys: str) -> dict[str, Any]:
        """Get multiple values from the namespaced view."""
        prefixed = {self._prefixed(k): k for k in keys}
        if hasattr(self._store, "get_many"):
            result = self._store.get_many(*prefixed.keys())
        else:
            result = {k: self._store[k] for k in prefixed if k in self._store}
        return {prefixed[pk]: v for pk, v in result.items()}

    def keys(self) -> set[str]:  # type: ignore[override]
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
        if not isinstance(key, str):
            return False
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
