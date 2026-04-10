"""Content-addressable Hash Array Mapped Trie (HAMT).

A persistent ``str -> bytes`` map laid out in a ``KVStore`` so that
unchanged subtrees are shared across versions by hash equality.

Each node is JSON-serialized and stored under its SHA-256 hash. A
HAMT is identified by its root node hash; mutations produce a new
root and a set of new node bytes that the caller persists (atomically,
if desired) by writing them to the underlying store.

Layering: this module knows nothing about kvgit's commit semantics.
It is a generic content-addressable map. See ``kvgit/versioned/keyset.py``
for the kvgit-specific wrapper that adds blob/meta entry semantics.
"""

import base64
import hashlib
import json
from collections.abc import Iterable, Iterator, Mapping
from typing import NamedTuple

from .kv.base import KVStore

# SHA-256 hex digest length. Each nibble is consumed once as the trie
# is descended, so this also bounds the maximum trie depth.
_HASH_LEN = 64


def _node_bytes(node: dict) -> bytes:
    """Serialize a node deterministically."""
    return json.dumps(node, sort_keys=True, separators=(",", ":")).encode()


def _hash_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _key_hash(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def _encode_value(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _decode_value(s: str) -> bytes:
    return base64.b64decode(s.encode("ascii"))


# Canonical empty leaf. Computed once at module load. The empty HAMT
# is represented by this hash; the node itself is never written to the
# store. Reads short-circuit on EMPTY_HASH; writes materialize a fresh
# leaf when needed.
_EMPTY_LEAF = {"items": {}, "kind": "leaf"}
_EMPTY_LEAF_BYTES = _node_bytes(_EMPTY_LEAF)
EMPTY_HASH: str = _hash_bytes(_EMPTY_LEAF_BYTES)


class HamtDiff(NamedTuple):
    """Structural diff between two HAMT roots."""

    added: dict[str, bytes]
    removed: dict[str, bytes]
    modified: dict[str, tuple[bytes, bytes]]  # key -> (old, new)


class Hamt:
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

    # ---- internal helpers ----

    def _load(
        self, node_hash: str, pending: dict[str, bytes] | None = None
    ) -> dict | None:
        """Load a node by hash. Checks the supplied pending dict first
        (used during in-progress batch updates), then ``self.pending``,
        then the store."""
        if node_hash == EMPTY_HASH:
            return {"items": {}, "kind": "leaf"}
        prefixed = self.prefix + node_hash
        if pending is not None and prefixed in pending:
            return json.loads(pending[prefixed])
        if prefixed in self.pending:
            return json.loads(self.pending[prefixed])
        raw = self.store.get(prefixed)
        if raw is None:
            return None
        return json.loads(raw)

    def _store_leaf(
        self, encoded_items: Mapping[str, str], pending: dict[str, bytes]
    ) -> str:
        """Materialize a leaf with the given (already-encoded) items."""
        node = {"items": dict(encoded_items), "kind": "leaf"}
        b = _node_bytes(node)
        h = _hash_bytes(b)
        pending[self.prefix + h] = b
        return h

    def _store_branch(
        self, children: Mapping[str, str], pending: dict[str, bytes]
    ) -> str:
        """Materialize a branch with the given child hashes."""
        node = {"children": dict(children), "kind": "branch"}
        b = _node_bytes(node)
        h = _hash_bytes(b)
        pending[self.prefix + h] = b
        return h

    # ---- reads ----

    def get(self, key: str) -> bytes | None:
        """Look up a key. Returns None if absent."""
        if self.root == EMPTY_HASH:
            return None
        kh = _key_hash(key)
        node_hash = self.root
        depth = 0
        while True:
            node = self._load(node_hash)
            if node is None:
                return None
            if node["kind"] == "leaf":
                encoded = node["items"].get(key)
                if encoded is None:
                    return None
                return _decode_value(encoded)
            chunk = kh[depth]
            if chunk not in node["children"]:
                return None
            node_hash = node["children"][chunk]
            depth += 1

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def items(self) -> Iterator[tuple[str, bytes]]:
        """Iterate over all (key, value) pairs lazily.

        One store read per visited node. Use ``materialize()`` if
        you want the whole map and the underlying store has
        non-trivial per-call latency.
        """
        if self.root == EMPTY_HASH:
            return
        yield from self._items_from(self.root)

    def _items_from(self, node_hash: str) -> Iterator[tuple[str, bytes]]:
        node = self._load(node_hash)
        if node is None:
            return
        if node["kind"] == "leaf":
            for k, v in node["items"].items():
                yield k, _decode_value(v)
        else:
            for child_hash in node["children"].values():
                yield from self._items_from(child_hash)

    def materialize(self) -> dict[str, bytes]:
        """Walk the entire HAMT and return its contents as a dict.

        Uses batched store reads — one ``get_many`` call per tree
        level — so the cost is roughly O(log_branching N) round-trips
        instead of one per node. For backends with non-trivial
        per-call latency (Redis, IndexedDB) this is dramatically
        faster than draining ``items()``.

        For local backends (Memory, Disk) the speedup over ``items()``
        is small because there's no per-call latency to amortize.
        Use ``items()`` when you want laziness (e.g. to break out
        early); use ``materialize()`` when you know you want the
        whole map.
        """
        if self.root == EMPTY_HASH:
            return {}

        result: dict[str, bytes] = {}
        current_level: list[str] = [self.root]

        while current_level:
            # Partition this level: nodes already in pending vs
            # nodes that need to be fetched from the store.
            cached_nodes: dict[str, dict] = {}
            to_fetch: list[str] = []
            for node_hash in current_level:
                if node_hash == EMPTY_HASH:
                    continue
                prefixed = self.prefix + node_hash
                if prefixed in self.pending:
                    cached_nodes[node_hash] = json.loads(self.pending[prefixed])
                else:
                    to_fetch.append(prefixed)

            # Single batched fetch for everything at this level.
            fetched: Mapping[str, bytes] = (
                self.store.get_many(to_fetch) if to_fetch else {}
            )

            # Walk the level: leaves contribute entries, branches
            # contribute the next level's node hashes.
            next_level: list[str] = []
            for node_hash in current_level:
                if node_hash == EMPTY_HASH:
                    continue
                if node_hash in cached_nodes:
                    node = cached_nodes[node_hash]
                else:
                    raw = fetched.get(self.prefix + node_hash)
                    if raw is None:
                        continue  # missing — skip rather than crash
                    node = json.loads(raw)

                if node["kind"] == "leaf":
                    for k, v in node["items"].items():
                        result[k] = _decode_value(v)
                else:  # branch
                    next_level.extend(node["children"].values())

            current_level = next_level

        return result

    def keys(self) -> Iterator[str]:
        for k, _ in self.items():
            yield k

    def values(self) -> Iterator[bytes]:
        for _, v in self.items():
            yield v

    def __iter__(self) -> Iterator[str]:
        return self.keys()

    def __len__(self) -> int:
        """Total entry count. O(N) — walks the tree."""
        return sum(1 for _ in self.items())

    # ---- writes ----

    def updated(
        self,
        updates: Mapping[str, bytes] | None = None,
        removals: Iterable[str] = (),
    ) -> tuple["Hamt", dict[str, bytes]]:
        """Apply updates and removals.

        Returns ``(new_hamt, pending_writes)`` where ``pending_writes``
        is a dict of prefixed-key -> node-bytes ready to merge into a
        store write batch. The returned ``new_hamt.pending`` is the
        same dict, so reads on the new view work before flushing.
        """
        pending = dict(self.pending)
        current_root = self.root

        for key, value in (updates or {}).items():
            current_root = self._insert(current_root, key, value, pending)
        for key in removals:
            current_root = self._delete(current_root, key, pending)

        # Drop any pending node that's no longer reachable from the new root
        # (intermediate nodes that were superseded by later updates).
        reachable_pending = self._filter_pending(current_root, pending)

        new_hamt = Hamt(
            self.store,
            current_root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
            pending=reachable_pending,
        )
        return new_hamt, reachable_pending

    def persist(
        self,
        updates: Mapping[str, bytes] | None = None,
        removals: Iterable[str] = (),
    ) -> "Hamt":
        """Apply updates and write any new nodes to the store immediately.

        Convenience for callers that don't need to batch writes with
        other store operations. Returns a fresh ``Hamt`` with empty
        pending. Distinct from ``Versioned.commit``: a HAMT has no
        notion of a commit history — this just flushes node bytes.
        """
        new_hamt, pending = self.updated(updates, removals)
        if pending:
            self.store.set_many(pending)
        return Hamt(
            self.store,
            new_hamt.root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
        )

    def flush(self) -> "Hamt":
        """Persist any pending node writes. Returns a fresh ``Hamt``."""
        if self.pending:
            self.store.set_many(**self.pending)
        return Hamt(
            self.store,
            self.root,
            prefix=self.prefix,
            bucket_max=self.bucket_max,
        )

    # ---- insert ----

    def _insert(
        self, root_hash: str, key: str, value: bytes, pending: dict[str, bytes]
    ) -> str:
        if root_hash == EMPTY_HASH:
            return self._store_leaf({key: _encode_value(value)}, pending)
        kh = _key_hash(key)
        return self._insert_at(root_hash, 0, kh, key, value, pending)

    def _insert_at(
        self,
        node_hash: str,
        depth: int,
        key_hash: str,
        key: str,
        value: bytes,
        pending: dict[str, bytes],
    ) -> str:
        node = self._load(node_hash, pending)
        if node is None:
            # Dangling reference — treat as missing and materialize a leaf.
            return self._store_leaf({key: _encode_value(value)}, pending)

        if node["kind"] == "leaf":
            encoded = _encode_value(value)
            existing = node["items"].get(key)
            if existing == encoded:
                return node_hash  # no-op
            new_items = dict(node["items"])
            new_items[key] = encoded
            if len(new_items) <= self.bucket_max:
                return self._store_leaf(new_items, pending)
            # Overflow: split into a branch.
            return self._split_leaf(new_items, depth, pending)

        # branch
        chunk = key_hash[depth]
        existing_children = node["children"]
        if chunk in existing_children:
            new_child_hash = self._insert_at(
                existing_children[chunk], depth + 1, key_hash, key, value, pending
            )
            if new_child_hash == existing_children[chunk]:
                return node_hash
            new_children = dict(existing_children)
            new_children[chunk] = new_child_hash
        else:
            new_leaf_hash = self._store_leaf({key: _encode_value(value)}, pending)
            new_children = dict(existing_children)
            new_children[chunk] = new_leaf_hash
        return self._store_branch(new_children, pending)

    def _split_leaf(
        self,
        encoded_items: Mapping[str, str],
        depth: int,
        pending: dict[str, bytes],
    ) -> str:
        """Convert an overflowing leaf at ``depth`` into a branch."""
        if depth >= _HASH_LEN:
            # Hash exhausted — full SHA-256 collision. Astronomically rare;
            # we just keep them in one (over-sized) leaf to avoid recursing
            # forever.
            return self._store_leaf(encoded_items, pending)

        groups: dict[str, dict[str, str]] = {}
        for k, v in encoded_items.items():
            nibble = _key_hash(k)[depth]
            groups.setdefault(nibble, {})[k] = v

        if len(groups) == 1:
            # All entries share the next nibble too — recurse deeper, then
            # wrap in a single-child branch at this depth.
            nibble, group_items = next(iter(groups.items()))
            child_hash = self._split_leaf(group_items, depth + 1, pending)
            return self._store_branch({nibble: child_hash}, pending)

        children: dict[str, str] = {}
        for nibble, group_items in groups.items():
            if len(group_items) <= self.bucket_max:
                children[nibble] = self._store_leaf(group_items, pending)
            else:
                children[nibble] = self._split_leaf(group_items, depth + 1, pending)
        return self._store_branch(children, pending)

    # ---- delete ----

    def _delete(self, root_hash: str, key: str, pending: dict[str, bytes]) -> str:
        if root_hash == EMPTY_HASH:
            return EMPTY_HASH
        kh = _key_hash(key)
        result = self._delete_at(root_hash, 0, kh, key, pending)
        return EMPTY_HASH if result is None else result

    def _delete_at(
        self,
        node_hash: str,
        depth: int,
        key_hash: str,
        key: str,
        pending: dict[str, bytes],
    ) -> str | None:
        """Delete ``key`` from the subtree. Returns new node hash, or
        None if the subtree is now empty."""
        node = self._load(node_hash, pending)
        if node is None:
            return node_hash

        if node["kind"] == "leaf":
            if key not in node["items"]:
                return node_hash
            new_items = {k: v for k, v in node["items"].items() if k != key}
            if not new_items:
                return None
            return self._store_leaf(new_items, pending)

        # branch
        chunk = key_hash[depth]
        existing_children = node["children"]
        if chunk not in existing_children:
            return node_hash

        new_child_hash = self._delete_at(
            existing_children[chunk], depth + 1, key_hash, key, pending
        )
        if new_child_hash == existing_children[chunk]:
            return node_hash

        new_children = dict(existing_children)
        if new_child_hash is None:
            del new_children[chunk]
        else:
            new_children[chunk] = new_child_hash

        if not new_children:
            return None

        # Canonicalization: if all children are leaves and their combined
        # entries fit in a single bucket, collapse the whole branch into
        # one leaf. This preserves the invariant that the same logical
        # contents always produce the same root hash.
        collapsed = self._try_collapse(new_children, pending)
        if collapsed is not None:
            return collapsed

        return self._store_branch(new_children, pending)

    def _try_collapse(
        self, children: Mapping[str, str], pending: dict[str, bytes]
    ) -> str | None:
        """If every child is a leaf and the union of their entries fits
        in ``bucket_max``, return the merged leaf hash. Otherwise None."""
        merged: dict[str, str] = {}
        for child_hash in children.values():
            child = self._load(child_hash, pending)
            if child is None or child["kind"] != "leaf":
                return None
            for k, v in child["items"].items():
                if k not in merged:
                    merged[k] = v
                if len(merged) > self.bucket_max:
                    return None
        return self._store_leaf(merged, pending)

    # ---- pending management ----

    def _filter_pending(self, root: str, pending: dict[str, bytes]) -> dict[str, bytes]:
        """Walk from ``root``, returning only pending entries that are
        actually reachable. Drops orphans created by superseded inserts."""
        if root == EMPTY_HASH:
            return {}
        result: dict[str, bytes] = {}
        queue = [root]
        while queue:
            h = queue.pop()
            prefixed = self.prefix + h
            if prefixed in result or prefixed not in pending:
                # Either already visited or already in the store — done with this branch.
                continue
            node_bytes = pending[prefixed]
            result[prefixed] = node_bytes
            node = json.loads(node_bytes)
            if node["kind"] == "branch":
                queue.extend(node["children"].values())
        return result

    # ---- structural ops ----

    def reachable_nodes(self) -> Iterator[str]:
        """Yield every node hash reachable from this root.

        Used by GC layers to mark live nodes. Includes pending nodes,
        so this works correctly on a Hamt that hasn't been flushed.
        """
        if self.root == EMPTY_HASH:
            return
        seen: set[str] = set()
        queue = [self.root]
        while queue:
            h = queue.pop()
            if h in seen:
                continue
            seen.add(h)
            yield h
            node = self._load(h)
            if node is None:
                continue
            if node["kind"] == "branch":
                queue.extend(node["children"].values())

    def diff(self, other: "Hamt") -> HamtDiff:
        """Structural diff against ``other``.

        Cost is O(changes + log N), not O(N), because identical
        subtrees (same node hash) are skipped wholesale. This is the
        primary payoff of structural sharing.
        """
        added: dict[str, bytes] = {}
        removed: dict[str, bytes] = {}
        modified: dict[str, tuple[bytes, bytes]] = {}
        self._diff_walk(self.root, other.root, other, added, removed, modified)
        return HamtDiff(added=added, removed=removed, modified=modified)

    def _diff_walk(
        self,
        a_hash: str,
        b_hash: str,
        other: "Hamt",
        added: dict[str, bytes],
        removed: dict[str, bytes],
        modified: dict[str, tuple[bytes, bytes]],
    ) -> None:
        if a_hash == b_hash:
            return  # identical subtrees — skip entirely

        if a_hash == EMPTY_HASH:
            for k, v in other._items_from(b_hash):
                added[k] = v
            return
        if b_hash == EMPTY_HASH:
            for k, v in self._items_from(a_hash):
                removed[k] = v
            return

        a_node = self._load(a_hash)
        b_node = other._load(b_hash)
        if a_node is None or b_node is None:
            # Missing node — fall back to full walk for whichever side is intact.
            if a_node is not None:
                for k, v in self._items_from(a_hash):
                    removed[k] = v
            if b_node is not None:
                for k, v in other._items_from(b_hash):
                    added[k] = v
            return

        if a_node["kind"] == "leaf" and b_node["kind"] == "leaf":
            a_items = {k: _decode_value(v) for k, v in a_node["items"].items()}
            b_items = {k: _decode_value(v) for k, v in b_node["items"].items()}
            for k, v in a_items.items():
                if k not in b_items:
                    removed[k] = v
                elif b_items[k] != v:
                    modified[k] = (v, b_items[k])
            for k, v in b_items.items():
                if k not in a_items:
                    added[k] = v
            return

        if a_node["kind"] == "branch" and b_node["kind"] == "branch":
            chunks = set(a_node["children"]) | set(b_node["children"])
            for chunk in chunks:
                a_child = a_node["children"].get(chunk, EMPTY_HASH)
                b_child = b_node["children"].get(chunk, EMPTY_HASH)
                self._diff_walk(a_child, b_child, other, added, removed, modified)
            return

        # Mixed kinds (one leaf, one branch). Walk both fully and reconcile.
        a_items = dict(self._items_from(a_hash))
        b_items = dict(other._items_from(b_hash))
        for k, v in a_items.items():
            if k not in b_items:
                removed[k] = v
            elif b_items[k] != v:
                modified[k] = (v, b_items[k])
        for k, v in b_items.items():
            if k not in a_items:
                added[k] = v
