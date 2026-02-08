"""vkv error types."""


class ConcurrencyError(Exception):
    """Raised when a concurrent write conflict occurs during merge.

    Another process updated HEAD between when this branch started
    and when merge was attempted via CAS. The caller should reset
    and retry.
    """
