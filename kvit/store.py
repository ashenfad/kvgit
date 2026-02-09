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
    """Protocol for key-value stores with commit semantics.

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
    def commit(self, **kwargs: Any) -> "MergeResult": ...
    def reset(self) -> None: ...
    def create_branch(self, name: str) -> "Store": ...
    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "Store | None": ...
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
) -> "Staged":
    """Create a Store with sensible defaults.

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

        backend = Disk(path)
    else:
        raise ValueError(f"Unknown kind: {kind!r}")

    if high_water_bytes is not None:
        versioned = GCVersioned(
            backend,
            branch=branch,
            high_water_bytes=high_water_bytes,
            low_water_bytes=low_water_bytes,
        )
    else:
        versioned = Versioned(backend, branch=branch)

    return Staged(versioned, encoder=encoder, decoder=decoder)
