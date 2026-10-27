"""KV store backends."""

from .base import KVStore
from .composite import Composite
from .disk import Disk
from .memory import Memory

__all__ = ["Composite", "Disk", "KVStore", "Memory"]

try:
    from .postgres import Postgres

    __all__ += ["Postgres"]
except ImportError:
    pass
