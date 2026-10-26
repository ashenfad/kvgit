from typing import Any

class MetaEntry:
    """Per-key metadata stored beside a blob pointer in a keyset."""

    size: int | None
    created_at: float | int | None
    chunks: list[str] | None
    def __init__(
        self,
        size: int | None,
        created_at: float | int | None = None,
        chunks: list[str] | None = None,
    ) -> None: ...

class KeysetEntry:
    """One entry in a keyset: a blob pointer and its metadata."""

    @property
    def blob(self) -> str: ...
    @property
    def meta(self) -> MetaEntry: ...
    def __init__(self, blob: str, meta: MetaEntry) -> None: ...

def encode_entry(entry: KeysetEntry) -> bytes:
    """An entry's bytes, as both packages write them."""

def decode_entry(raw: bytes) -> KeysetEntry:
    """The entry stored as ``raw``."""

def version() -> str:
    """The version of the core, which is the version of the package."""

def empty_hash() -> str:
    """The root of the empty keyset."""

def commit_hash(
    parents: list[str], root: str, time: float, info: dict | None = None
) -> str:
    """The 40-hex hash naming a commit."""

class TreeOp:
    """A tree operation in progress: ``resume`` returns ``("get", keys)``
    while it needs nodes, to be answered with the stored values of those
    keys, and ``("done", result)`` once it is finished."""

    @staticmethod
    def get_many(
        prefix: str, bucket_max: int, root: str, keys: list[str], entries: bool = False
    ) -> TreeOp: ...
    @staticmethod
    def walk(
        prefix: str, bucket_max: int, root: str, skip: set[str], entries: bool = False
    ) -> TreeOp: ...
    @staticmethod
    def diff(
        prefix: str, bucket_max: int, a: str, b: str, entries: bool = False
    ) -> TreeOp: ...
    @staticmethod
    def update(
        prefix: str,
        bucket_max: int,
        root: str,
        updates: list[tuple[str, Any]],
        removals: list[str],
        unwritten: dict[str, bytes] | None = None,
        entries: bool = False,
    ) -> TreeOp: ...
    def resume(self, reply: dict[str, bytes] | None = None) -> tuple[str, Any]: ...

MAX_MARK_BYTES: int
"""The most bytes, all sides together, the text merge will mark."""

def text_merge(
    old: bytes | None,
    ours: bytes | None,
    theirs: bytes | None,
    ours_label: str,
    theirs_label: str,
    cant_mark: type[Exception],
) -> tuple[bytes, bool]:
    """The three-way text merge of a value's sides, and whether it marked
    a conflict. Values that can't be marked raise ``cant_mark``; a label
    holding a line break raises ``ValueError``."""

def check_text_labels(ours_label: str, theirs_label: str) -> None:
    """Raise ``ValueError`` if either label holds a line break."""

def check_branch_name(name: str) -> None:
    """Raise ``ValueError`` for a name that can't be a branch."""

def check_tag_name(name: str) -> None:
    """Raise ``ValueError`` for a name that can't be a tag."""

def cacheable(key: str) -> bool:
    """Whether ``key`` is one the store never rewrites."""

class ContentCache:
    """A repository's memory of write-once keys, shared by every
    operation given it; see ``kvgit.cache``."""

    def __init__(self, max_bytes: int) -> None: ...
    @property
    def max_bytes(self) -> int: ...
    @property
    def size_bytes(self) -> int: ...
    @property
    def hits(self) -> int: ...
    @property
    def misses(self) -> int: ...
    @property
    def clears(self) -> int: ...
    def __len__(self) -> int: ...
    def clear(self) -> None: ...
    def lookup(self, keys: list[str]) -> dict[str, bytes]: ...
    def count(self, hits: int, misses: int) -> None: ...
    def remember(self, items: dict[str, bytes]) -> None: ...
    def forget(self, keys: list[str]) -> None: ...
    def saw_lease(self, record: bytes | None) -> None: ...

class Op:
    """An operation of the core in progress, started by name with its
    arguments as a dict, or by ``Op.commit``. ``resume`` returns the next
    request as a tuple, to be answered by the next call, until it
    returns ``("done", outcome, notes)``; see ``kvgit.versioned._drive``."""

    def __init__(self, name: str, args: dict[str, Any]) -> None: ...
    @staticmethod
    def commit(
        branch: str,
        base: str,
        current: str,
        updates: list[tuple[str, KeysetEntry]],
        removals: list[str],
        chunks: list[str],
        info: dict | None,
        on_moved: str,
        on_conflict: str,
        merge_fns: dict[str, Any],
        merge_prefixes: list[tuple[str, Any]],
        default_merge: Any,
        recover: bool,
        stamped: bool,
        roots: list[tuple[str, str]],
        generations: list[tuple[str, int]],
    ) -> Op: ...
    @staticmethod
    def merge(
        branch: str,
        ours: str,
        theirs: str,
        from_: str | None,
        fast_forward: bool,
        info: dict | None,
        on_conflict: str,
        merge_fns: dict[str, Any],
        merge_prefixes: list[tuple[str, Any]],
        default_merge: Any,
        post_check: bool,
        recover: bool,
        stamped: bool,
        roots: list[tuple[str, str]],
        generations: list[tuple[str, int]],
    ) -> Op: ...
    def use_cache(self, cache: ContentCache) -> None: ...
    def resume(
        self, wall: float, reply: Any = None, failed: bool = False
    ) -> tuple[Any, ...]: ...
