"""kvgit: Versioned key-value store."""

from .content_types import MergeFn, counter, last_writer_wins, text_merge
from .errors import ConcurrencyError, MergeConflict
from .merges import CantMark, TextMergeFn, make_text_merge, ours, text, theirs
from .namespaced import Namespaced
from .staged import Staged
from .store import delete_branches, delete_tags, store
from .versioned.kv import VersionedKV
from .versioned.protocol import (
    MergeChoice,
    MergePolicy,
    MergeResult,
    TagInfo,
    Versioned,
)

__all__ = [
    "CantMark",
    "ConcurrencyError",
    "MergeChoice",
    "MergeConflict",
    "MergeFn",
    "MergePolicy",
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
    "ours",
    "store",
    "text",
    "text_merge",
    "theirs",
]
