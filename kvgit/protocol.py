"""Versioned protocol and shared types."""

import json
from dataclasses import asdict, dataclass
from typing import Callable, Iterable, Protocol, runtime_checkable


def _to_bytes(obj) -> bytes:
    """Encode a JSON-safe Python object to bytes."""
    return json.dumps(obj, separators=(",", ":")).encode()


def _from_bytes(raw: bytes):
    """Decode bytes to a Python object."""
    return json.loads(raw)


BytesMergeFn = Callable[[bytes | None, bytes | None, bytes | None], bytes]
"""Merge function: (old_value, our_value, their_value) -> merged_value.

Any argument can be None (key absent or removed on that side).
"""


@dataclass(frozen=True)
class DiffResult:
    """Key-level differences between two commits."""

    added: frozenset[str]
    removed: frozenset[str]
    modified: frozenset[str]


@dataclass(frozen=True)
class MergeResult:
    """Result of a merge operation."""

    merged: bool
    commit: str | None
    strategy: str  # "no_op", "fast_forward", "three_way"
    auto_merged_keys: tuple[str, ...]
    carried_keys: tuple[str, ...]

    def __bool__(self) -> bool:
        return self.merged


@dataclass
class MetaEntry:
    """Metadata for a single key in versioned state."""

    last_touch: int
    size: int | None
    created_at: float


def _meta_to_bytes(meta: dict[str, "MetaEntry"]) -> bytes:
    """Serialize the per-key metadata dict to JSON bytes."""
    return _to_bytes({k: asdict(v) for k, v in meta.items()})


def _meta_from_bytes(raw: bytes) -> dict[str, "MetaEntry"]:
    """Deserialize JSON bytes to a per-key metadata dict."""
    return {k: MetaEntry(**v) for k, v in _from_bytes(raw).items()}


@runtime_checkable
class Versioned(Protocol):
    """Protocol for versioned key-value stores.

    Defines the common interface implemented by ``VersionedKV``
    (KVStore-backed) and ``VersionedGP`` (GitPython-backed).
    """

    last_merge_result: MergeResult | None

    @property
    def current_commit(self) -> str: ...

    @property
    def base_commit(self) -> str: ...

    @property
    def current_branch(self) -> str: ...

    @property
    def latest_head(self) -> str | None: ...

    # -- Read operations --

    def get(self, key: str) -> bytes | None: ...

    def get_many(self, *keys: str) -> dict[str, bytes]: ...

    def keys(self) -> Iterable[str]: ...

    def __contains__(self, key: str) -> bool: ...

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: BytesMergeFn) -> None: ...

    def set_default_merge(self, fn: BytesMergeFn) -> None: ...

    # -- Write operations --

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, BytesMergeFn] | None = None,
        default_merge: BytesMergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult: ...

    def refresh(self) -> None: ...

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "Versioned | None": ...

    def create_branch(self, name: str, *, at: str | None = None) -> "Versioned": ...

    def delete_branch(self, name: str) -> None: ...

    def switch_branch(self, name: str) -> None: ...

    def peek(self, key: str, *, branch: str) -> bytes | None: ...

    def reset_to(self, commit_hash: str) -> bool: ...

    def history(
        self, commit_hash: str | None = None, *, all_parents: bool = False
    ) -> Iterable[str]: ...

    def list_branches(self) -> list[str]: ...

    def commit_info(self, commit_hash: str | None = None) -> dict | None: ...

    def diff(self, commit_a: str, commit_b: str) -> DiffResult: ...

    def parents(self, commit_hash: str | None = None) -> tuple[str, ...]: ...
