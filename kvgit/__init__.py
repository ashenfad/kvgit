"""kvgit: Versioned key-value store."""

from .content_types import MergeFn, counter, last_writer_wins
from .errors import ConcurrencyError, MergeConflict
from .live import Live
from .namespaced import Namespaced
from .versioned.protocol import MergeResult, Versioned
from .staged import Staged
from .store import store
from .versioned.kv import VersionedKV

__all__ = [
    "ConcurrencyError",
    "Live",
    "MergeConflict",
    "MergeFn",
    "MergeResult",
    "Namespaced",
    "Staged",
    "Versioned",
    "VersionedKV",
    "counter",
    "last_writer_wins",
    "store",
]

try:
    from .versioned.gp import VersionedGP

    __all__ += ["VersionedGP"]
except ImportError:
    pass
