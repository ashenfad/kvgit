"""Namespaced: key-prefixed view over a Versioned store."""

from __future__ import annotations

from typing import Iterable

from .versioned import MergeFn, MergeResult, Versioned


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

    # -- Write operations --

    def snapshot(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
    ) -> str:
        """Create a commit with namespaced key changes."""
        prefixed_updates = (
            {self._prefixed(k): v for k, v in updates.items()}
            if updates else None
        )
        prefixed_removals = (
            {self._prefixed(k) for k in removals}
            if removals else None
        )
        return self.base_store.snapshot(
            prefixed_updates, prefixed_removals, info=info
        )

    def merge(
        self,
        on_conflict: str = "raise",
        *,
        merge_fns: dict[str, MergeFn] | None = None,
        default_merge: MergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Merge the underlying branch (delegates to base store).

        Per-key merge_fns are auto-prefixed with the namespace.
        """
        prefixed_fns = (
            {self._prefixed(k): v for k, v in merge_fns.items()}
            if merge_fns else None
        )
        return self.base_store.merge(
            on_conflict,
            merge_fns=prefixed_fns,
            default_merge=default_merge,
            info=info,
        )

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a namespaced key."""
        self.base_store.set_merge_fn(self._prefixed(key), fn)

    def set_content_type(self, key: str, ct) -> None:
        """Register a ContentType for a namespaced key."""
        self.base_store.set_content_type(self._prefixed(key), ct)

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function (store-wide)."""
        self.base_store.set_default_merge(fn)

    # -- Convenience properties --

    @property
    def current_commit(self) -> str:
        return self.base_store.current_commit

    @property
    def base_commit(self) -> str:
        return self.base_store.base_commit

    @property
    def last_merge_result(self) -> MergeResult | None:
        return self.base_store.last_merge_result
