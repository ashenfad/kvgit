"""kvgit error types."""


class ConcurrencyError(Exception):
    """Raised when a concurrent write conflict occurs during merge.

    Another process updated HEAD between when this branch started
    and when merge was attempted via CAS. The caller should reset
    and retry.
    """


class MergeConflict(Exception):
    """Raised when a three-way merge encounters unresolvable conflicts.

    Attributes:
        conflicting_keys: The set of keys that could not be auto-merged.
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


class GcBusy(Exception):
    """Raised when a deep clean cannot take the store's GC lease.

    Another process or thread holds an unexpired lease, so a namespace
    scan started now would run beside a sweep that is already deleting.
    Retry later; the lease carries an expiry, so a holder that dies
    without releasing it blocks nothing past that point.
    """
