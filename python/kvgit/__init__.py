"""kvgit: versioned key-value store."""

from ._open import open
from ._store import ROOT_COMMIT, CorruptHeadRecoverer, recover_by_commit_scan
from ._types import (
    DiffResult,
    MergeChoice,
    MergePolicy,
    MergeResult,
    TagInfo,
)
from .content_types import MergeFn, counter, last_writer_wins, text_merge
from .errors import (
    BranchExistsError,
    BranchMovedError,
    ConcurrencyError,
    CorruptHeadError,
    GcBusy,
    IntegrityError,
    KvgitError,
    MergeConflict,
    StorageVersionError,
    TagExistsError,
    UnknownBranchError,
    UnknownCommitError,
    UnknownTagError,
)
from .merges import (
    CantMark,
    TextMergeFn,
    make_text_merge,
    ours,
    text,
    text_merge_result,
    theirs,
)
from .namespaced import Namespaced
from .refs import Branches, Tags
from .repo import Commit, RawSnapshot, Repo, Snapshot
from .worktree import Status, Worktree

__all__ = [
    "ROOT_COMMIT",
    "BranchExistsError",
    "BranchMovedError",
    "Branches",
    "CantMark",
    "Commit",
    "ConcurrencyError",
    "CorruptHeadError",
    "CorruptHeadRecoverer",
    "DiffResult",
    "GcBusy",
    "IntegrityError",
    "KvgitError",
    "MergeChoice",
    "MergeConflict",
    "MergeFn",
    "MergePolicy",
    "MergeResult",
    "Namespaced",
    "RawSnapshot",
    "Repo",
    "Snapshot",
    "Status",
    "StorageVersionError",
    "TagExistsError",
    "TagInfo",
    "Tags",
    "TextMergeFn",
    "UnknownBranchError",
    "UnknownCommitError",
    "UnknownTagError",
    "Worktree",
    "counter",
    "last_writer_wins",
    "make_text_merge",
    "open",
    "ours",
    "recover_by_commit_scan",
    "text",
    "text_merge",
    "text_merge_result",
    "theirs",
]
