"""Shared three-way merge resolution."""

from dataclasses import dataclass
from typing import Callable

from ..errors import MergeConflict
from .protocol import BytesMergeFn, DiffResult


BlobReader = Callable[[str], bytes | None]
"""Read a blob by its content identifier (versioned key or hex SHA)."""


@dataclass
class MergeResolution:
    """Result of resolving a three-way merge at the keyset level.

    This is the pre-commit result: the caller still needs to persist
    the merged keyset, write merged_values as blobs, and create the
    merge commit.
    """

    merged_keyset: dict[str, str]
    merged_values: dict[str, bytes]
    auto_merged_keys: list[str]


def resolve_merge(
    lca_keyset: dict[str, str],
    our_keyset: dict[str, str],
    their_keyset: dict[str, str],
    our_diff: DiffResult,
    their_diff: DiffResult,
    blob_reader: BlobReader,
    merge_fns: dict[str, BytesMergeFn],
    default_merge: BytesMergeFn | None,
) -> MergeResolution:
    """Resolve a three-way merge between two diverged keysets.

    Implements the pure merge logic shared between VersionedKV and
    VersionedGP.  Does NOT create commits or advance HEAD -- the
    caller handles persistence.

    Args:
        lca_keyset: Keyset of the lowest common ancestor commit.
        our_keyset: Keyset of our (local) commit.
        their_keyset: Keyset of their (remote/HEAD) commit.
        our_diff: DiffResult from LCA to our commit.
        their_diff: DiffResult from LCA to their commit.
        blob_reader: Callable to read blob bytes by content ID.
        merge_fns: Per-key merge functions.
        default_merge: Fallback merge function for unregistered keys.

    Returns:
        MergeResolution with the merged keyset, values that need
        to be written as new blobs, and the list of auto-merged keys.

    Raises:
        MergeConflict: If any keys conflict without a merge function.
    """
    our_changed = our_diff.added | our_diff.removed | our_diff.modified
    their_changed = their_diff.added | their_diff.removed | their_diff.modified
    all_changed = our_changed | their_changed

    merged_keyset: dict[str, str] = {}
    merged_values: dict[str, bytes] = {}
    auto_merged: list[str] = []
    conflicts: set[str] = set()
    merge_errors: dict[str, Exception] = {}

    # Unchanged keys: carry from their keyset (HEAD)
    all_keys = set(our_keyset.keys()) | set(their_keyset.keys())
    for key in all_keys - all_changed:
        if key in their_keyset:
            merged_keyset[key] = their_keyset[key]
        elif key in our_keyset:
            merged_keyset[key] = our_keyset[key]

    # Changed only by us
    for key in our_changed - their_changed:
        if key not in our_diff.removed:
            merged_keyset[key] = our_keyset[key]
            auto_merged.append(key)

    # Changed only by them
    for key in their_changed - our_changed:
        if key not in their_diff.removed:
            merged_keyset[key] = their_keyset[key]

    # Contested: changed by both sides
    contested = our_changed & their_changed
    for key in contested:
        our_removed = key in our_diff.removed
        their_removed = key in their_diff.removed

        if our_removed and their_removed:
            continue

        # Same change on both sides
        if (
            not our_removed
            and not their_removed
            and our_keyset.get(key) == their_keyset.get(key)
        ):
            merged_keyset[key] = their_keyset[key]
            continue

        # Try merge function
        fn = merge_fns.get(key, default_merge)
        if fn is None:
            conflicts.add(key)
            continue

        old_val = blob_reader(lca_keyset[key]) if key in lca_keyset else None
        our_val = None if our_removed else blob_reader(our_keyset[key])
        their_val = None if their_removed else blob_reader(their_keyset[key])
        try:
            result_val = fn(old_val, our_val, their_val)
            merged_values[key] = result_val
            auto_merged.append(key)
        except Exception as e:
            conflicts.add(key)
            merge_errors[key] = e

    if conflicts:
        raise MergeConflict(conflicts, merge_errors)

    return MergeResolution(
        merged_keyset=merged_keyset,
        merged_values=merged_values,
        auto_merged_keys=auto_merged,
    )
