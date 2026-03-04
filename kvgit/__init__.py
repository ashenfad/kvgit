"""kvgit: Versioned key-value store."""

from .content_types import MergeFn, counter, last_writer_wins
from .errors import ConcurrencyError, MergeConflict
from .live import Live
from .namespaced import Namespaced
from .protocol import MergeResult, Versioned
from .staged import Staged
from .store import store
from .versioned_kv import VersionedKV

try:
    from .versioned_gp import VersionedGP
except ImportError:
    pass

__all__ = [
    "ConcurrencyError",
    "Live",
    "MergeConflict",
    "MergeFn",
    "MergeResult",
    "Namespaced",
    "Staged",
    "Versioned",
    "VersionedGP",
    "VersionedKV",
    "counter",
    "last_writer_wins",
    "store",
]
