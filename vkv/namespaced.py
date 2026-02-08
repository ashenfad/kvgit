"""Namespaced: key-prefixed view over a Versioned store."""

from __future__ import annotations

from typing import Iterable

from .versioned import Versioned


class Namespaced:
    """A namespaced view over a Versioned store.

    Keys are prefixed with ``namespace/``. Nested namespaces are
    supported by wrapping another Namespaced instance.

    Args:
        store: A Versioned or Namespaced instance to wrap.
        namespace: The namespace name (must not contain ``/``).
    """

    def __init__(self, store: Versioned | Namespaced, namespace: str) -> None:
        if "/" in namespace:
            raise ValueError("Namespace names cannot contain '/'")
        if not isinstance(store, (Versioned, Namespaced)):
            raise TypeError(
                f"Namespaced can only wrap Versioned or Namespaced, "
                f"not {type(store).__name__}"
            )

        self._store = store

        if isinstance(store, Namespaced):
            self.namespace = f"{store.namespace}/{namespace}"
        else:
            self.namespace = namespace

    @property
    def base_store(self) -> Versioned:
        """The underlying Versioned store (unwraps nesting)."""
        if isinstance(self._store, Namespaced):
            return self._store.base_store
        return self._store

    def _prefixed(self, key: str) -> str:
        return f"{self.namespace}/{key}"

    def get(self, key: str) -> bytes | None:
        """Get a value from the namespaced view."""
        return self.base_store.get(self._prefixed(key))

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the namespaced view."""
        prefixed = {self._prefixed(k): k for k in keys}
        result = self.base_store.get_many(*prefixed.keys())
        return {prefixed[pk]: v for pk, v in result.items()}

    def keys(self) -> Iterable[str]:
        """Direct child keys in this namespace (not nested)."""
        prefix = f"{self.namespace}/"
        for key in self.base_store.keys():
            if key.startswith(prefix):
                remainder = key[len(prefix):]
                if remainder and "/" not in remainder:
                    yield remainder

    def descendant_keys(self) -> Iterable[str]:
        """All keys under this namespace, including nested."""
        prefix = f"{self.namespace}/"
        for key in self.base_store.keys():
            if key.startswith(prefix):
                yield key[len(prefix):]

    def __contains__(self, key: str) -> bool:
        return self._prefixed(key) in self.base_store
