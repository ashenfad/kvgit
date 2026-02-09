"""kvit: Versioned key-value store."""

from .content_types import MergeFn, counter, last_writer_wins
from .errors import ConcurrencyError, MergeConflict
from .live import Live
from .namespaced import Namespaced
from .staged import Staged
from .store import Store, VersionedStore, store
from .versioned import MergeResult, Versioned

__all__ = [
    "ConcurrencyError",
    "Live",
    "MergeConflict",
    "MergeFn",
    "MergeResult",
    "Namespaced",
    "Staged",
    "Store",
    "Versioned",
    "VersionedStore",
    "counter",
    "last_writer_wins",
    "store",
]
