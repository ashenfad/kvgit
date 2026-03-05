"""Versioned store implementations."""

from .gc import GCVersionedKV
from .kv import VersionedKV
from .protocol import BytesMergeFn, DiffResult, MergeResult, Versioned

__all__ = [
    "BytesMergeFn",
    "DiffResult",
    "GCVersionedKV",
    "MergeResult",
    "Versioned",
    "VersionedKV",
]

try:
    from .gp import VersionedGP

    __all__ += ["VersionedGP"]
except ImportError:
    pass
