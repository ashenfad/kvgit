"""Shared three-way merge resolution."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..errors import MergeConflict
from .protocol import BytesMergeFn, DiffResult, MergeChoice

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


def pick_merge_fn(
    key: str,
    merge_fns: Mapping[str, BytesMergeFn],
    merge_prefixes: Mapping[str, BytesMergeFn],
    default_merge: BytesMergeFn | None,
) -> BytesMergeFn | None:
    """Choose the merge function for one contested key.

    The most specific registration wins: an exact key match first, then
    the longest registered prefix the key starts with, then the default.
    ``None`` means nothing is registered for the key, which the caller
    files as a conflict.
    """
    fn = merge_fns.get(key)
    if fn is not None:
        return fn

    best_fn: BytesMergeFn | None = None
    best_len = -1
    for prefix, candidate in merge_prefixes.items():
        if key.startswith(prefix) and len(prefix) > best_len:
            best_fn = candidate
            best_len = len(prefix)
    if best_fn is not None:
        return best_fn

    return default_merge


def resolve_merge(
    lca_keyset: dict[str, str],
    our_keyset: dict[str, str],
    their_keyset: dict[str, str],
    our_diff: DiffResult,
    their_diff: DiffResult,
    blob_reader: BlobReader,
    merge_fns: dict[str, BytesMergeFn],
    default_merge: BytesMergeFn | None,
    merge_prefixes: dict[str, BytesMergeFn] | None = None,
) -> MergeResolution:
    """Resolve a three-way merge between two diverged keysets.

    Implements the pure merge logic used by VersionedKV. Does NOT
    create commits or advance HEAD -- the caller handles persistence.

    Args:
        lca_keyset: Keyset of the lowest common ancestor commit.
        our_keyset: Keyset of our (local) commit.
        their_keyset: Keyset of their (remote/HEAD) commit.
        our_diff: DiffResult from LCA to our commit.
        their_diff: DiffResult from LCA to their commit.
        blob_reader: Callable to read blob bytes by content ID.
        merge_fns: Per-key merge functions (exact key match).
        default_merge: Fallback merge function for unregistered keys.
        merge_prefixes: Merge functions by key prefix, consulted when
            no exact key match applies. The longest matching prefix
            wins.

    Returns:
        MergeResolution with the merged keyset, values that need
        to be written as new blobs, and the list of auto-merged keys.

    Raises:
        MergeConflict: If any keys conflict without a merge function.
    """
    our_changed = our_diff.added | our_diff.removed | our_diff.modified
    their_changed = their_diff.added | their_diff.removed | their_diff.modified
    all_changed = our_changed | their_changed

    prefixes = merge_prefixes or {}
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

        our_val = None if our_removed else blob_reader(our_keyset[key])
        their_val = None if their_removed else blob_reader(their_keyset[key])

        # Equal bytes under different pointers are still the same change.
        # A blob identifier is commit-scoped, so two writers making the
        # identical write from the same base end up with different
        # pointers to identical content; take their pointer, with no
        # merge function and no conflict. Both sides reading as None
        # means both blobs are missing, which is damage rather than
        # agreement, so it stays contested.
        if (
            not our_removed
            and not their_removed
            and our_val is not None
            and our_val == their_val
        ):
            merged_keyset[key] = their_keyset[key]
            continue

        fn = pick_merge_fn(key, merge_fns, prefixes, default_merge)
        if fn is None:
            conflicts.add(key)
            continue

        old_val = blob_reader(lca_keyset[key]) if key in lca_keyset else None
        try:
            result_val = fn(old_val, our_val, their_val)
        except Exception as e:  # noqa: BLE001 — `fn` is caller-supplied;
            # any failure it raises is reported as a merge conflict.
            conflicts.add(key)
            merge_errors[key] = e
            continue

        # A MergeChoice keeps the chosen side's committed value: carry
        # that side's existing pointer, so the merge writes no new blob.
        # A side that removed the key has no pointer to carry, and
        # choosing it removes the key from the merge.
        if result_val is MergeChoice.OURS:
            if not our_removed:
                merged_keyset[key] = our_keyset[key]
        elif result_val is MergeChoice.THEIRS:
            if not their_removed:
                merged_keyset[key] = their_keyset[key]
        else:
            merged_values[key] = result_val
        auto_merged.append(key)

    if conflicts:
        raise MergeConflict(conflicts, merge_errors)

    return MergeResolution(
        merged_keyset=merged_keyset,
        merged_values=merged_values,
        auto_merged_keys=auto_merged,
    )
