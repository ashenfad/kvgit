"""Store protocol and factory function."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .versioned import MergeResult


@runtime_checkable
class Store(Protocol):
    """Protocol for key-value stores with commit semantics.

    Implementations: ``Staged``, ``Live``, ``Namespaced``.
    """

    def get(self, key: str) -> bytes | None: ...
    def get_many(self, *keys: str) -> dict[str, bytes]: ...
    def keys(self) -> Iterable[str]: ...
    def __contains__(self, key: str) -> bool: ...
    def set(self, key: str, value: bytes) -> None: ...
    def remove(self, key: str) -> None: ...
    def commit(self, **kwargs) -> MergeResult: ...
    def reset(self) -> None: ...
    def create_branch(self, name: str) -> Store: ...
    def checkout(self, commit_hash: str, *, branch: str | None = None) -> Store | None: ...
    def list_branches(self) -> list[str]: ...


def store(
    type: Literal["versioned", "live"] = "versioned",
    storage: str = "memory",
    *,
    path: str | None = None,
    branch: str = "main",
    high_water_bytes: int | None = None,
    low_water_bytes: int | None = None,
) -> Store:
    """Create a Store with sensible defaults.

    Args:
        type: ``"versioned"`` (default) for a ``Staged`` store with
            full commit/merge/history support, or ``"live"`` for
            immediate writes with no versioning.
        storage: ``"memory"`` (default) or ``"disk"``.
        path: Required when ``storage="disk"``. Directory path for
            the disk backend.
        branch: Branch name (versioned only, default ``"main"``).
        high_water_bytes: Enable GC with this high-water threshold
            (versioned only).
        low_water_bytes: GC low-water threshold (versioned only,
            defaults to 80% of high_water).

    Returns:
        A ``Store`` instance (``Staged`` or ``Live``).
    """
    # Build backend
    if storage == "memory":
        from .kv.memory import Memory

        backend = Memory()
    elif storage == "disk":
        if path is None:
            raise ValueError("path is required when storage='disk'")
        from .kv.disk import Disk

        backend = Disk(path)
    else:
        raise ValueError(f"Unknown storage: {storage!r}")

    if type == "live":
        if high_water_bytes is not None or low_water_bytes is not None:
            raise ValueError("GC parameters are only valid for type='versioned'")
        from .live import Live

        return Live(backend)

    if type == "versioned":
        if high_water_bytes is not None:
            from .gc import GCVersioned

            versioned = GCVersioned(
                backend,
                branch=branch,
                high_water_bytes=high_water_bytes,
                low_water_bytes=low_water_bytes,
            )
        else:
            from .versioned import Versioned

            versioned = Versioned(backend, branch=branch)

        from .staged import Staged

        return Staged(versioned)

    raise ValueError(f"Unknown type: {type!r}")
