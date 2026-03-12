"""Versioned store implementations."""

from .kv import VersionedKV
from .protocol import BytesMergeFn, DiffResult, MergeResult, Versioned

__all__ = [
    "BytesMergeFn",
    "DiffResult",
    "MergeResult",
    "Versioned",
    "VersionedKV",
]

try:
    from .gp import VersionedGP

    __all__ += ["VersionedGP"]
except ImportError:
    pass
