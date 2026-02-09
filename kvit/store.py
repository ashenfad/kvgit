"""Store protocol and factory function."""

import pickle
from collections.abc import Iterator
from typing import Any, Callable, Iterable, Literal, Protocol, runtime_checkable

from .gc import GCVersioned
from .kv.memory import Memory
from .staged import Staged
from .versioned import MergeResult, Versioned


@runtime_checkable
class Store(Protocol):
    """Protocol for key-value stores.

    Implements ``MutableMapping[str, Any]`` semantics.
    Implementations: ``Staged``, ``Live``, ``Namespaced``.
    """

    def get(self, key: str, default: Any = None) -> Any: ...
    def get_many(self, *keys: str) -> dict[str, Any]: ...
    def keys(self) -> Iterable[str]: ...
    def __contains__(self, key: object) -> bool: ...
    def __getitem__(self, key: str) -> Any: ...
    def __setitem__(self, key: str, value: Any) -> None: ...
    def __delitem__(self, key: str) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def set(self, key: str, value: Any) -> None: ...
    def remove(self, key: str) -> None: ...


@runtime_checkable
class VersionedStore(Store, Protocol):
    """Store with commit semantics.

    Extends ``Store`` with versioning operations.
    Implementations: ``Staged``.
    """

    def commit(self, **kwargs: Any) -> "MergeResult": ...
    def reset(self) -> None: ...
    def create_branch(self, name: str) -> "VersionedStore": ...
    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "VersionedStore | None": ...
    def list_branches(self) -> list[str]: ...


def store(
    kind: Literal["memory", "disk"] = "memory",
    *,
    path: str | None = None,
    branch: str = "main",
    encoder: Callable[[Any], bytes] = pickle.dumps,
    decoder: Callable[[bytes], Any] = pickle.loads,
    high_water_bytes: int | None = None,
    low_water_bytes: int | None = None,
    is_protected: Callable[[str], bool] | None = None,
) -> Staged:
    """Create a Staged store with sensible defaults.

    Args:
        kind: ``"memory"`` (default) or ``"disk"``.
        path: Required when ``kind="disk"``. Directory path for
            the disk backend.
        branch: Branch name (default ``"main"``).
        encoder: Value encoder (default ``pickle.dumps``).
        decoder: Value decoder (default ``pickle.loads``).
        high_water_bytes: Enable GC with this high-water threshold.
        low_water_bytes: GC low-water threshold (defaults to 80%
            of high_water).
        is_protected: Callable that returns True for keys GC should
            never drop. Only used when ``high_water_bytes`` is set.
            Defaults to protecting keys starting with ``__``.

    Returns:
        A ``Staged`` store instance.
    """
    # Build backend
    if kind == "memory":
        backend = Memory()
    elif kind == "disk":
        if path is None:
            raise ValueError("path is required when kind='disk'")
        from .kv.disk import Disk

        backend = Disk(path, size_limit=0)
    else:
        raise ValueError(f"Unknown kind: {kind!r}")

    if high_water_bytes is not None:
        gc_kwargs: dict[str, Any] = {
            "branch": branch,
            "high_water_bytes": high_water_bytes,
            "low_water_bytes": low_water_bytes,
        }
        if is_protected is not None:
            gc_kwargs["is_protected"] = is_protected
        versioned = GCVersioned(backend, **gc_kwargs)
    else:
        versioned = Versioned(backend, branch=branch)

    return Staged(versioned, encoder=encoder, decoder=decoder)
