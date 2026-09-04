"""kvgit: Versioned key-value store."""

from .content_types import MergeFn, counter, last_writer_wins
from .errors import ConcurrencyError, MergeConflict
from .merges import CantMark, TextMergeFn, make_text_merge, text
from .namespaced import Namespaced
from .staged import Staged
from .store import delete_branches, delete_tags, store
from .versioned.kv import VersionedKV
from .versioned.protocol import MergeResult, TagInfo, Versioned

__all__ = [
    "CantMark",
    "ConcurrencyError",
    "MergeConflict",
    "MergeFn",
    "MergeResult",
    "Namespaced",
    "Staged",
    "TagInfo",
    "TextMergeFn",
    "Versioned",
    "VersionedKV",
    "counter",
    "delete_branches",
    "delete_tags",
    "last_writer_wins",
    "make_text_merge",
    "store",
    "text",
]
