"""kvgit error types.

Everything kvgit raises about the state of a store derives from
:class:`KvgitError`. Arguments that are invalid on their face — a
malformed name, two refs where one is expected — raise ``ValueError``.
"""


class KvgitError(Exception):
    """Base class for kvgit's errors."""


class ConcurrencyError(KvgitError):
    """Raised when a concurrent write wins a race this call cannot merge past.

    Another writer moved the branch between when this call read it and
    when it tried to publish. Nothing was changed; refresh and retry.
    """


class MergeConflict(KvgitError):
    """Raised when a three-way merge encounters unresolvable conflicts.

    Attributes:
        conflicting_keys: The set of keys that could not be auto-merged.
        merge_errors: Exceptions raised by merge functions, by key.
    """

    def __init__(
        self,
        conflicting_keys: set[str],
        merge_errors: dict[str, Exception] | None = None,
    ) -> None:
        self.conflicting_keys = conflicting_keys
        self.merge_errors = merge_errors or {}
        keys_str = ", ".join(sorted(conflicting_keys))
        super().__init__(f"Merge conflict on keys: {keys_str}")


class UnknownBranchError(KvgitError):
    """Raised for a branch that does not exist — opening it, reading its
    head, deleting it, or committing to it after it was deleted."""


class UnknownTagError(KvgitError):
    """Raised for a tag that does not exist."""


class UnknownCommitError(KvgitError):
    """Raised for a commit that is not in the store."""


class BranchExistsError(KvgitError):
    """Raised when creating a branch under a name already taken."""


class TagExistsError(KvgitError):
    """Raised when creating a tag under a name already taken. Tags never
    move: delete the tag first to point the name elsewhere."""


class CorruptHeadError(KvgitError):
    """Raised for a branch whose HEAD is present but unusable and could
    not be recovered from its backup. ``Repo.repair_head`` is the way
    back; see the HEAD recovery notes in the API reference."""


class StorageVersionError(KvgitError):
    """Raised for a store stamped with a storage layout this code does not
    read, or one older than any it supports. Nothing is written to it."""


class GcBusy(KvgitError):
    """Raised when a sweep cannot take the store's GC lease because
    another sweep holds an unexpired one. ``Repo.gc(wait=False)`` raises
    it; by default ``gc`` waits instead. The lease carries an expiry, so
    a holder that dies without releasing it blocks nothing past that
    point."""
