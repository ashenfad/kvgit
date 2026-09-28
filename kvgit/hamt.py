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
from typing import Any, NamedTuple

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
_EMPTY_LEAF: dict[str, Any] = {"items": {}, "kind": "leaf"}
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

        Equivalent to ``walk()[0]``.
        """
        return self.walk()[0]

    def walk(
        self, skip_nodes: set[str] | None = None
    ) -> tuple[dict[str, bytes], set[str]]:
        """Walk the entire HAMT, returning (items, node_hashes).

        Single batched BFS that collects both the key→value entries
        and the set of every visited node hash. Equivalent to
        calling ``materialize()`` and ``reachable_nodes()`` separately
        but in one tree traversal — used by GC mark phases that want
        both, like ``clean_orphans``.

        Same batching properties as ``materialize()``: one
        ``get_many`` call per tree level, O(log_branching N)
        round-trips total.

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

        items: dict[str, bytes] = {}
        nodes: set[str] = set()
        current_level: list[str] = [self.root]

        while current_level:
            # Partition this level: nodes already in pending vs
            # nodes that need to be fetched from the store. Drop
            # anything in skip_nodes — those subtrees have already
            # been visited by a prior walk.
            cached_nodes: dict[str, dict] = {}
            to_fetch: list[str] = []
            for node_hash in current_level:
                if node_hash == EMPTY_HASH or node_hash in skip:
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
            # contribute the next level's node hashes. Track every
            # node hash we successfully load.
            next_level: list[str] = []
            for node_hash in current_level:
                if node_hash == EMPTY_HASH or node_hash in skip:
                    continue
                if node_hash in cached_nodes:
                    node = cached_nodes[node_hash]
                else:
                    raw = fetched.get(self.prefix + node_hash)
                    if raw is None:
                        continue  # missing — skip rather than crash
                    node = json.loads(raw)

                nodes.add(node_hash)

                if node["kind"] == "leaf":
                    for k, v in node["items"].items():
                        items[k] = _decode_value(v)
                else:  # branch
                    next_level.extend(node["children"].values())

            current_level = next_level

        return items, nodes

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
        pending. Distinct from ``VersionedKV.commit``: a HAMT has no
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

        Both trees are walked level by level, with one batched fetch per
        level, so the round trips are the depth of the trees rather than
        the number of nodes that differ. Where one tree has a leaf and
        the other a branch, the leaf's entries are split among the
        branch's children by key hash and compared further down.
        """
        added: dict[str, bytes] = {}
        removed: dict[str, bytes] = {}
        modified: dict[str, tuple[bytes, bytes]] = {}
        # A side is a node hash, or the encoded entries of a leaf that
        # was split to line up with the other side's branch.
        level: list[tuple[str | dict[str, str], str | dict[str, str], int]] = (
            [(self.root, other.root, 0)] if self.root != other.root else []
        )
        while level:
            nodes = self._load_level(
                [a for a, _, _ in level if isinstance(a, str)],
                other,
                [b for _, b, _ in level if isinstance(b, str)],
            )
            next_level: list[
                tuple[str | dict[str, str], str | dict[str, str], int]
            ] = []
            for a, b, depth in level:
                if isinstance(a, str) and isinstance(b, str) and a == b:
                    continue  # identical subtrees — skip entirely
                a_node = nodes[0].get(a) if isinstance(a, str) else _as_leaf(a)
                b_node = nodes[1].get(b) if isinstance(b, str) else _as_leaf(b)
                # A missing node reads as empty: its side's entries under
                # it are unknown, and the other side's show as added or
                # removed.
                a_node = a_node or _EMPTY_LEAF
                b_node = b_node or _EMPTY_LEAF
                a_branch = a_node["kind"] == "branch"
                b_branch = b_node["kind"] == "branch"
                if not a_branch and not b_branch:
                    _diff_items(
                        a_node["items"], b_node["items"], added, removed, modified
                    )
                    continue
                a_children = (
                    a_node["children"]
                    if a_branch
                    else _split_items(a_node["items"], depth)
                )
                b_children = (
                    b_node["children"]
                    if b_branch
                    else _split_items(b_node["items"], depth)
                )
                for chunk in a_children.keys() | b_children.keys():
                    a_child = a_children.get(chunk, EMPTY_HASH)
                    b_child = b_children.get(chunk, EMPTY_HASH)
                    if a_child != b_child:  # identical subtrees are never read
                        next_level.append((a_child, b_child, depth + 1))
            level = next_level
        return HamtDiff(added=added, removed=removed, modified=modified)

    def _load_level(
        self, ours: list[str], other: "Hamt", theirs: list[str]
    ) -> tuple[dict[str, dict | None], dict[str, dict | None]]:
        """Load one level of nodes from each tree, in as few reads as the
        two trees' stores allow."""
        shared = other.store is self.store and other.prefix == self.prefix
        wanted: dict[str, None] = {}
        for tree, hashes in ((self, ours), (other, theirs)):
            for h in hashes:
                key = tree.prefix + h
                if h != EMPTY_HASH and key not in tree.pending:
                    wanted[key] = None
        fetched: dict[str, Mapping[str, bytes]] = {}
        if shared:
            got = self.store.get_many(list(wanted)) if wanted else {}
            fetched = {"self": got, "other": got}
        else:
            for name, tree in (("self", self), ("other", other)):
                keys = [k for k in wanted if k.startswith(tree.prefix)]
                fetched[name] = tree.store.get_many(keys) if keys else {}

        def decode(tree: "Hamt", got: Mapping[str, bytes], h: str) -> dict | None:
            if h == EMPTY_HASH:
                return _EMPTY_LEAF
            key = tree.prefix + h
            raw = tree.pending.get(key) or got.get(key)
            return json.loads(raw) if raw is not None else None

        return (
            {h: decode(self, fetched["self"], h) for h in ours},
            {h: decode(other, fetched["other"], h) for h in theirs},
        )


def _as_leaf(items: dict[str, str]) -> dict:
    return {"items": items, "kind": "leaf"}


def _split_items(items: Mapping[str, str], depth: int) -> dict[str, dict[str, str]]:
    """A leaf's encoded entries, grouped as a branch at ``depth`` would
    hold them."""
    groups: dict[str, dict[str, str]] = {}
    for k, v in items.items():
        groups.setdefault(_key_hash(k)[depth], {})[k] = v
    return groups


def _diff_items(
    a: Mapping[str, str],
    b: Mapping[str, str],
    added: dict[str, bytes],
    removed: dict[str, bytes],
    modified: dict[str, tuple[bytes, bytes]],
) -> None:
    for k, v in a.items():
        if k not in b:
            removed[k] = _decode_value(v)
        elif b[k] != v:
            modified[k] = (_decode_value(v), _decode_value(b[k]))
    for k, v in b.items():
        if k not in a:
            added[k] = _decode_value(v)
