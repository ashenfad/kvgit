"""Content-addressable Hash Array Mapped Trie (HAMT).

A persistent ``str -> bytes`` map laid out in a ``KVStore`` so that
unchanged subtrees are shared across versions by hash equality.

Each node is JSON-serialized and stored under its SHA-256 hash. A
HAMT is identified by its root node hash; mutations produce a new
root and a set of new node bytes that the caller persists (atomically,
if desired) by writing them to the underlying store.

The tree itself lives in the core (``kvgit._core``): each operation
there asks for the nodes it needs, one tree level at a time, and this
module answers from the view's unwritten nodes and then the store.

Layering: this module knows nothing about kvgit's commit semantics.
It is a generic content-addressable map. See ``kvgit/versioned/keyset.py``
for the kvgit-specific wrapper that adds blob/meta entry semantics.
"""

from collections.abc import Iterable, Iterator, Mapping
from typing import Any, Generic, NamedTuple, TypeVar

from . import _core
from .kv.base import KVStore

# The empty HAMT is represented by this hash; its node is never written
# to the store.
EMPTY_HASH: str = _core.empty_hash()


V = TypeVar("V")


class HamtDiff(NamedTuple):
    """Structural diff between two HAMT roots."""

    added: dict[str, bytes]
    removed: dict[str, bytes]
    modified: dict[str, tuple[bytes, bytes]]  # key -> (old, new)


def _drive(op: Any, read: Any) -> Any:
    """Run a core tree operation, answering each of its reads with
    ``read(keys)``."""
    kind, payload = op.resume()
    while kind == "get":
        kind, payload = op.resume(read(payload))
    return payload


class Hamt(Generic[V]):
    """Immutable, content-addressable HAMT view over a ``KVStore``.

    Mutating methods (``updated``) return a new ``Hamt`` whose
    ``pending`` dict contains any new node bytes not yet flushed to
    the store. Reads on the new view resolve through ``pending``
    first, falling back to the store. Use ``flush()`` or ``commit()``
    to persist, or merge ``pending`` into a larger write batch.

    Two HAMTs with the same logical contents and the same
    ``bucket_max`` will have the same root hash, regardless of how
    they were constructed. This invariant is what enables structural
    sharing across versions.

    The ``bucket_max`` parameter controls how many entries fit in a
    leaf before it splits into a branch. Larger buckets mean fewer
    nodes but larger leaves; smaller buckets mean more nodes with
    finer-grained sharing. Note: a HAMT built with one ``bucket_max``
    will hash differently from the same logical contents built with
    another ``bucket_max``.
    """

    store: KVStore
    root: str
    prefix: str
    bucket_max: int
    pending: dict[str, bytes]  # prefixed key -> node bytes

    # Whether the values are keyset entries, which the core decodes and
    # encodes on the way through, rather than bytes.
    _entries = False

    def __init__(
        self,
        store: KVStore,
        root: str = EMPTY_HASH,
        *,
        prefix: str = "hamt:",
        bucket_max: int = 8,
        pending: dict[str, bytes] | None = None,
    ) -> None:
        if bucket_max < 1:
            raise ValueError(f"bucket_max must be >= 1, got {bucket_max}")
        self.store = store
        self.root = root
        self.prefix = prefix
        self.bucket_max = bucket_max
        self.pending = pending if pending is not None else {}

    def _read(self, keys: list[str]) -> dict[str, bytes]:
        """Nodes by prefixed key: unwritten ones first, then the store
        underneath any content cache, which the operation reads through
        itself."""
        found = {k: self.pending[k] for k in keys if k in self.pending}
        rest = [k for k in keys if k not in found]
        if rest:
            store = self.store
            if getattr(store, "_core_cache", None) is not None:
                store = store.backend  # type: ignore[attr-defined]
            found.update(store.get_many(rest))
        return found

    def _run(self, op: Any) -> Any:
        cache = getattr(self.store, "_core_cache", None)
        if cache is not None:
            op.use_cache(cache)
        return _drive(op, self._read)

    # ---- reads ----

    def get(self, key: str) -> V | None:
        """Look up a key. Returns None if absent."""
        return self.get_many([key]).get(key)

    def get_many(self, keys: Iterable[str]) -> dict[str, V]:
        """The values of the keys that are present, as ``{key: value}``.

        Reads the paths to all of them level by level, one batched read
        per level, rather than walking the tree once per key.
        """
        keys = list(dict.fromkeys(keys))
        if not keys or self.root == EMPTY_HASH:
            return {}
        return self._run(
            _core.TreeOp.get_many(
                self.prefix, self.bucket_max, self.root, keys, self._entries
            )
        )

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def items(self) -> Iterator[tuple[str, V]]:
        """Iterate over all (key, value) pairs."""
        yield from self.walk()[0].items()

    def materialize(self) -> dict[str, V]:
        """Walk the entire HAMT and return its contents as a dict.

        Uses batched store reads — one ``get_many`` call per tree
        level — so the cost is roughly O(log_branching N) round-trips
        instead of one per node. Equivalent to ``walk()[0]``.
        """
        return self.walk()[0]

    def walk(self, skip_nodes: set[str] | None = None) -> tuple[dict[str, V], set[str]]:
        """Walk the entire HAMT, returning (items, node_hashes).

        Single batched BFS that collects both the key→value entries
        and the set of every visited node hash, one ``get_many`` call
        per tree level. Used by GC mark phases that want both.

        ``skip_nodes`` is an optional set of node hashes to treat as
        already-visited. Skipped subtrees are not fetched, not
        recursed into, and not included in the returned ``nodes``
        set. Items beneath skipped subtrees are also omitted from
        the returned ``items`` dict. Pass a cumulative seen-set
        across multiple ``walk()`` calls (e.g. across the commits
        of a branch's history) to share work where the underlying
        HAMTs share structure — turns N-walks-over-shared-tree from
        O(N · subtree) into O(unique nodes).
        """
        skip = skip_nodes if skip_nodes is not None else set()
        if self.root == EMPTY_HASH or self.root in skip:
            return {}, set()
        return self._run(
            _core.TreeOp.walk(
                self.prefix, self.bucket_max, self.root, skip, self._entries
            )
        )

    def keys(self) -> Iterator[str]:
        for k, _ in self.items():
            yield k

    def values(self) -> Iterator[V]:
        for _, v in self.items():
            yield v

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    def __len__(self) -> int:
        """Total entry count. O(N) — walks the tree."""
        return len(self.walk()[0])

    # ---- writes ----

    def updated(
        self,
        updates: Mapping[str, V] | None = None,
        removals: Iterable[str] = (),
    ) -> tuple["Hamt[V]", dict[str, bytes]]:
        """Apply updates and removals.

        Returns ``(new_hamt, pending_writes)`` where ``pending_writes``
        is a dict of prefixed-key -> node-bytes ready to merge into a
        store write batch. The returned ``new_hamt.pending`` is the
        same dict, so reads on the new view work before flushing.

        Every insert and delete walks from the root to its key; all of
        those paths are read up front, one batched read per level, so a
        commit that touches many keys costs the depth of the tree in
        round trips rather than a read per node per key.
        """
        n = len(self.prefix)
        unwritten = {k[n:]: v for k, v in self.pending.items()}
        root, nodes = self._run(
            _core.TreeOp.update(
                self.prefix,
                self.bucket_max,
                self.root,
                list((updates or {}).items()),
                list(removals),
                unwritten,
                self._entries,
            )
        )
        pending = {self.prefix + h: raw for h, raw in nodes.items()}
        new_hamt = type(self)(
            self.store,
            root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
            pending=pending,
        )
        return new_hamt, pending

    def persist(
        self,
        updates: Mapping[str, V] | None = None,
        removals: Iterable[str] = (),
    ) -> "Hamt[V]":
        """Apply updates and write any new nodes to the store immediately.

        Convenience for callers that don't need to batch writes with
        other store operations. Returns a fresh ``Hamt`` with empty
        pending. Distinct from ``VersionedKV.commit``: a HAMT has no
        notion of a commit history — this just flushes node bytes.
        """
        new_hamt, pending = self.updated(updates, removals)
        if pending:
            self.store.set_many(pending)
        return type(self)(
            self.store,
            new_hamt.root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
        )

    def flush(self) -> "Hamt[V]":
        """Persist any pending node writes. Returns a fresh ``Hamt``."""
        if self.pending:
            self.store.set_many(**self.pending)
        return type(self)(
            self.store,
            self.root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
        )

    # ---- structural ops ----

    def reachable_nodes(self) -> Iterator[str]:
        """Yield every node hash reachable from this root.

        Used by GC layers to mark live nodes. Includes pending nodes,
        so this works correctly on a Hamt that hasn't been flushed.
        """
        yield from self.walk()[1]

    def diff(self, other: "Hamt[V]") -> HamtDiff:
        """Structural diff against ``other``.

        Cost is O(changes + log N), not O(N), because identical
        subtrees (same node hash) are skipped wholesale. This is the
        primary payoff of structural sharing.

        Both trees are walked level by level, with one batched fetch per
        level, so the round trips are the depth of the trees rather than
        the number of nodes that differ. Where one tree has a leaf and
        the other a branch, the leaf's entries are split among the
        branch's children by key hash and compared further down.
        """
        if self.root == other.root:
            return HamtDiff(added={}, removed={}, modified={})
        n = len(self.prefix)

        def read(keys: list[str]) -> dict[str, bytes]:
            # Nodes are content addressed, so either tree's copy will do.
            found = self._read(keys)
            rest = [k for k in keys if k not in found]
            if rest:
                theirs = other._read([other.prefix + k[n:] for k in rest])
                m = len(other.prefix)
                found.update({self.prefix + k[m:]: v for k, v in theirs.items()})
            return found

        added, removed, modified = _drive(
            _core.TreeOp.diff(
                self.prefix, self.bucket_max, self.root, other.root, self._entries
            ),
            read,
        )
        return HamtDiff(added=added, removed=removed, modified=modified)
