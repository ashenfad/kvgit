"""Shared three-way merge resolution."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from ..errors import MergeConflict
from .protocol import DiffResult, MergeChoice, MergePolicy

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


def pick_merge_policy(
    key: str,
    merge_fns: Mapping[str, MergePolicy],
    merge_prefixes: Mapping[str, MergePolicy],
    default_merge: MergePolicy | None,
) -> MergePolicy | None:
    """Choose the registration that governs one key.

    The most specific registration wins: an exact key match first, then
    the longest registered prefix the key starts with, then the default.
    Merge functions and ``MergeChoice`` policies compete in that one
    order, so a longer prefix beats a shorter one whichever kind each
    holds. ``None`` means nothing is registered for the key, which
    leaves a contested key to be filed as a conflict.
    """
    policy = merge_fns.get(key)
    if policy is not None:
        return policy

    best: MergePolicy | None = None
    best_len = -1
    for prefix, candidate in merge_prefixes.items():
        if key.startswith(prefix) and len(prefix) > best_len:
            best = candidate
            best_len = len(prefix)
    if best is not None:
        return best

    return default_merge


def resolve_merge(
    lca_keyset: dict[str, str],
    our_keyset: dict[str, str],
    their_keyset: dict[str, str],
    our_diff: DiffResult,
    their_diff: DiffResult,
    blob_reader: BlobReader,
    merge_fns: dict[str, MergePolicy],
    default_merge: MergePolicy | None,
    merge_prefixes: dict[str, MergePolicy] | None = None,
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
        merge_fns: Per-key registrations (exact key match).
        default_merge: Fallback registration for unregistered keys.
        merge_prefixes: Registrations by key prefix, consulted when no
            exact key match applies. The longest matching prefix wins.

    A registration holds either a merge function, consulted only where
    both sides changed a key, or a ``MergeChoice``, which gives one side
    every key it covers.

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

    policies = {
        key: pick_merge_policy(key, merge_fns, prefixes, default_merge)
        for key in all_changed
    }

    # Keys a MergeChoice governs: that side's state stands as it is,
    # whether or not the other side touched the key. Reading the pointer
    # straight out of the chosen side's keyset says all of it — a key
    # absent there was either added by the other side (dropped) or
    # removed by the chosen side (stays removed), and either way it has
    # no pointer to carry. These keys never reach a merge function, so
    # nothing is read or decoded for them.
    chosen = {
        key for key, policy in policies.items() if isinstance(policy, MergeChoice)
    }
    for key in chosen:
        side = our_keyset if policies[key] is MergeChoice.OURS else their_keyset
        if key in side:
            merged_keyset[key] = side[key]
        auto_merged.append(key)

    # Unchanged keys: carry from their keyset (HEAD)
    all_keys = set(our_keyset.keys()) | set(their_keyset.keys())
    for key in all_keys - all_changed:
        if key in their_keyset:
            merged_keyset[key] = their_keyset[key]
        elif key in our_keyset:
            merged_keyset[key] = our_keyset[key]

    # Changed only by us
    for key in our_changed - their_changed - chosen:
        if key not in our_diff.removed:
            merged_keyset[key] = our_keyset[key]
            auto_merged.append(key)

    # Changed only by them
    for key in their_changed - our_changed - chosen:
        if key not in their_diff.removed:
            merged_keyset[key] = their_keyset[key]

    # Contested: changed by both sides. A merge function is consulted
    # here and nowhere else, so registering one never disturbs a change
    # only one side made.
    contested = (our_changed & their_changed) - chosen
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

        fn = policies[key]
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
