"""Store protocol and factory function."""

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Callable, Iterable, Protocol, runtime_checkable

if TYPE_CHECKING:
    from .versioned import MergeResult


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
    def commit(self, **kwargs) -> "MergeResult": ...
    def reset(self) -> None: ...
    def create_branch(self, name: str) -> "Store": ...
    def checkout(self, commit_hash: str, *, branch: str | None = None) -> "Store | None": ...
    def list_branches(self) -> list[str]: ...


def store(
    storage: str = "memory",
    *,
    path: str | None = None,
    branch: str = "main",
    encoder: Callable[[Any], bytes] | None = None,
    decoder: Callable[[bytes], Any] | None = None,
    high_water_bytes: int | None = None,
    low_water_bytes: int | None = None,
) -> Store:
    """Create a Store with sensible defaults.

    Args:
        storage: ``"memory"`` (default) or ``"disk"``.
        path: Required when ``storage="disk"``. Directory path for
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

    kwargs: dict[str, Any] = {}
    if encoder is not None:
        kwargs["encoder"] = encoder
    if decoder is not None:
        kwargs["decoder"] = decoder
    return Staged(versioned, **kwargs)
