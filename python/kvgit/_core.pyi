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
