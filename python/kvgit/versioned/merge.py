"""Shared three-way merge resolution."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import NamedTuple

from ..errors import MergeConflict
from .keyset import KeysetEntry
from .protocol import MergeChoice, MergePolicy

BlobReader = Callable[[str], bytes | None]
"""Read a blob by its content identifier (versioned key or hex SHA)."""


class Change(NamedTuple):
    """One key's change from the common ancestor to one side: its entry
    there and its entry here, ``None`` where the key is absent."""

    old: KeysetEntry | None
    new: KeysetEntry | None


@dataclass
class MergeResolution:
    """A resolved three-way merge, as changes to make to our side.

    ``updates`` and ``removals`` turn our keyset into the merged one;
    ``merged_values`` are values merge functions produced, each still to
    be written as a blob and set under its key. Nothing here restates a
    key whose merged state is already ours.
    """

    updates: dict[str, KeysetEntry] = field(default_factory=dict)
    removals: set[str] = field(default_factory=set)
    merged_values: dict[str, bytes] = field(default_factory=dict)
    auto_merged_keys: tuple[str, ...] = ()
    carried_keys: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        """Whether the merge changes our side at all."""
        return bool(self.updates or self.removals or self.merged_values)


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
    ours: Mapping[str, Change],
    theirs: Mapping[str, Change],
    blob_reader: BlobReader,
    merge_fns: Mapping[str, MergePolicy],
    default_merge: MergePolicy | None,
    merge_prefixes: Mapping[str, MergePolicy] | None = None,
) -> MergeResolution:
    """Resolve a three-way merge from each side's changes since the
    common ancestor.

    Only changed keys are looked at: a key neither side changed is the
    same on both and stays as it is, so the cost follows the size of the
    change, not of the keyset. The result is expressed against our side.

    A registration holds either a merge function, consulted only where
    both sides changed a key, or a ``MergeChoice``, which gives one side
    every key it covers that either side changed.

    Raises:
        MergeConflict: If any keys conflict without a merge function.
    """
    prefixes = merge_prefixes or {}
    resolution = MergeResolution()
    auto_merged: list[str] = []
    carried: list[str] = []
    conflicts: set[str] = set()
    merge_errors: dict[str, Exception] = {}

    def take(key: str, entry: KeysetEntry | None, current: KeysetEntry | None) -> None:
        """Make ``entry`` the merged state of ``key``, whose state on our
        side is ``current``."""
        if entry == current:
            return
        if entry is None:
            resolution.removals.add(key)
        else:
            resolution.updates[key] = entry

    for key in sorted(ours.keys() | theirs.keys()):
        our_change = ours.get(key)
        their_change = theirs.get(key)
        ancestor = (our_change or their_change).old  # type: ignore[union-attr]
        our_entry = our_change.new if our_change else ancestor
        their_entry = their_change.new if their_change else ancestor
        policy = pick_merge_policy(key, merge_fns, prefixes, default_merge)

        # A MergeChoice registration: that side's state stands, whether or
        # not the other side touched the key. Nothing is read or decoded.
        if isinstance(policy, MergeChoice):
            if policy is MergeChoice.THEIRS:
                take(key, their_entry, our_entry)
            auto_merged.append(key)
            continue

        if their_change is None:
            continue  # changed only by us: ours stands
        if our_change is None:
            take(key, their_entry, our_entry)  # changed only by them
            carried.append(key)
            continue

        # Contested: changed by both sides. A merge function is consulted
        # here and nowhere else, so registering one never disturbs a
        # change only one side made.
        if our_entry is None and their_entry is None:
            continue  # both removed it
        if (
            our_entry is not None
            and their_entry is not None
            and our_entry.blob == their_entry.blob
        ):
            continue  # the same change on both sides

        our_val = None if our_entry is None else blob_reader(our_entry.blob)
        their_val = None if their_entry is None else blob_reader(their_entry.blob)

        # Equal bytes under different pointers are still the same change.
        # A blob written before storage v4 is keyed by the commit that
        # wrote it, so identical content can sit under two pointers — an
        # old blob on one side and a content-keyed one on the other, or
        # two old ones; take their pointer, with no merge function and no
        # conflict. Both sides reading as None means both blobs are
        # missing, which is damage rather than agreement, so it stays
        # contested.
        if (
            our_entry is not None
            and their_entry is not None
            and our_val is not None
            and our_val == their_val
        ):
            take(key, their_entry, our_entry)
            continue

        if policy is None:
            conflicts.add(key)
            continue

        old_val = None if ancestor is None else blob_reader(ancestor.blob)
        try:
            result_val = policy(old_val, our_val, their_val)
        except Exception as e:  # noqa: BLE001 — `policy` is caller-supplied;
            # any failure it raises is reported as a merge conflict.
            conflicts.add(key)
            merge_errors[key] = e
            continue

        # A MergeChoice keeps the chosen side's committed value: carry
        # that side's existing pointer, so the merge writes no new blob.
        # A side that removed the key has no pointer to carry, and
        # choosing it removes the key from the merge.
        if result_val is MergeChoice.THEIRS:
            take(key, their_entry, our_entry)
        elif result_val is not MergeChoice.OURS:
            resolution.merged_values[key] = result_val
        auto_merged.append(key)

    if conflicts:
        raise MergeConflict(conflicts, merge_errors)

    resolution.auto_merged_keys = tuple(auto_merged)
    resolution.carried_keys = tuple(carried)
    return resolution
