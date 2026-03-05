"""Store factory function."""

import pickle
from typing import Any, Callable, Literal

from .kv.base import KVStore
from .kv.memory import Memory
from .staged import Staged
from .versioned.gc import GCVersionedKV
from .versioned.kv import VersionedKV


def store(
    kind: Literal["memory", "disk", "git"] = "memory",
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
        kind: ``"memory"`` (default), ``"disk"``, or ``"git"``.
        path: Required when ``kind="disk"`` or ``kind="git"``.
            Directory path for the disk backend or repo path for git.
        branch: Branch name (default ``"main"``).
        encoder: Value encoder (default ``pickle.dumps``).
        decoder: Value decoder (default ``pickle.loads``).
        high_water_bytes: Enable GC with this high-water threshold.
            Not supported with ``kind="git"``.
        low_water_bytes: GC low-water threshold (defaults to 80%
            of high_water). Not supported with ``kind="git"``.
        is_protected: Callable that returns True for keys GC should
            never drop. Only used when ``high_water_bytes`` is set.
            Defaults to protecting keys starting with ``__``.
            Not supported with ``kind="git"``.

    Returns:
        A ``Staged`` store instance.
    """
    versioned: VersionedKV

    if kind == "git":
        if (
            high_water_bytes is not None
            or low_water_bytes is not None
            or is_protected is not None
        ):
            raise ValueError("GC parameters are not supported with kind='git'")
        if path is None:
            raise ValueError("path is required when kind='git'")
        from .versioned.gp import VersionedGP

        versioned = VersionedGP(path, branch=branch)  # type: ignore[assignment]
    else:
        # Build KV backend
        backend: KVStore
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
            versioned = GCVersionedKV(backend, **gc_kwargs)
        else:
            versioned = VersionedKV(backend, branch=branch)

    return Staged(versioned, encoder=encoder, decoder=decoder)
