"""KVStore-backed versioned state.

Storage layout (v2):

- ``__kvgit_version__``                — storage version sentinel
- ``__branch_head__<branch>``          — current HEAD commit hash
- ``__branch_head_prev__<branch>``     — previous HEAD (recovery backup)
- ``__commit_root__<commit>``          — keyset HAMT root hash
- ``__parent_commit__<commit>``        — list of parent commit hashes
- ``__commit_time__<commit>``          — wall time the commit was created
- ``__info__<commit>``                 — optional caller-supplied info dict
- ``kvgit:keyset:<node_hash>``         — HAMT node bytes
- ``<commit_hash>:<user_key>``         — blob value bytes

The keyset (key -> blob_pointer + meta) is stored as a content-addressable
HAMT, so unchanged subtrees are shared across commits by hash equality. A
single-key change writes O(log N) new nodes instead of rewriting a full
keyset snapshot per commit.

Storage is **not backward-compatible** with the v1 layout. Stores written
by an earlier version raise on open and need to be rebuilt fresh.
"""

import hashlib
import json
import logging
import time

from ..encoding import MetaEntry, from_bytes, to_bytes
from ..hamt import EMPTY_HASH
from ..kv.base import KVStore
from ..kv.memory import Memory
from .base import VersionedBase
from .keyset import Keyset, KeysetEntry
from .merge import MergeResolution

PARENT_COMMIT = "__parent_commit__%s"
COMMIT_ROOT = "__commit_root__%s"
COMMIT_TIME = "__commit_time__%s"
BRANCH_HEAD = "__branch_head__%s"
BRANCH_HEAD_PREV = "__branch_head_prev__%s"
INFO_KEY = "__info__%s"

STORAGE_VERSION_KEY = "__kvgit_version__"
STORAGE_VERSION = 2


def content_hash(
    parents: tuple[str, ...],
    keyset: dict[str, str],
    updates: dict[str, bytes],
    info: dict | None = None,
) -> str:
    """Compute a content-addressable commit hash.

    Hashes the parent pointers, keyset preview, update blob digests,
    and optional info to produce a deterministic 40-hex-char commit
    hash. The keyset passed here is the in-memory placeholder dict
    (with ``<pending:key>`` markers for not-yet-written blobs), the
    same shape v1 used.
    """
    h = hashlib.sha256()
    h.update(json.dumps(list(parents), separators=(",", ":")).encode())
    h.update(json.dumps(sorted(keyset.items()), separators=(",", ":")).encode())
    for key in sorted(updates):
        h.update(key.encode())
        h.update(updates[key])
    if info is not None:
        h.update(json.dumps(info, sort_keys=True, separators=(",", ":")).encode())
    return h.hexdigest()[:40]


logger = logging.getLogger("kvgit")


def _safe_from_bytes(raw: bytes):
    """Like from_bytes but returns None on any decode/parse error."""
    try:
        return from_bytes(raw)
    except Exception:
        return None


def _check_storage_version(store: KVStore) -> None:
    """Verify the store's kvgit version is compatible.

    Stamps the version on a fresh store. Raises on any pre-v2 layout
    (detected by the presence of branch heads without a version sentinel).
    """
    raw = store.get(STORAGE_VERSION_KEY)
    if raw is not None:
        version = _safe_from_bytes(raw)
        if version != STORAGE_VERSION:
            raise ValueError(
                f"Store has kvgit storage version {version!r}, "
                f"expected {STORAGE_VERSION}. Use a fresh store."
            )
        return

    # No version sentinel. Either fresh, or pre-v2.
    branch_prefix = BRANCH_HEAD.replace("%s", "")
    has_existing = any(
        isinstance(k, str) and k.startswith(branch_prefix) for k in store.keys()
    )
    if has_existing:
        raise ValueError(
            "Store appears to use an older kvgit storage format. "
            f"This version requires storage v{STORAGE_VERSION}. "
            "Use a fresh store."
        )
    store.set(STORAGE_VERSION_KEY, to_bytes(STORAGE_VERSION))


def _load_root(store: KVStore, commit_hash: str) -> str | None:
    """Load the keyset HAMT root hash for a commit, or None if missing."""
    raw = store.get(COMMIT_ROOT % commit_hash)
    if raw is None:
        return None
    val = _safe_from_bytes(raw)
    return val if isinstance(val, str) else None


def _resolve_head(store: KVStore, branch: str, *, repair: bool = True) -> str | None:
    """Resolve a branch HEAD, falling back to prev HEAD or commit scan.

    When *repair* is True (default), a corrupt HEAD is automatically
    healed by writing the recovered commit hash back to the store.
    Pass ``repair=False`` for side-effect-free reads (e.g. properties).

    Returns a valid commit hash, or None if unrecoverable.
    """
    # 1. Try current HEAD
    head_bytes = store.get(BRANCH_HEAD % branch)
    if head_bytes is not None:
        commit_hash = _safe_from_bytes(head_bytes)
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            return commit_hash

    # 2. Try previous HEAD (backup written before each CAS)
    prev_bytes = store.get(BRANCH_HEAD_PREV % branch)
    if prev_bytes is not None:
        commit_hash = _safe_from_bytes(prev_bytes)
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            logger.warning(
                "Branch '%s': HEAD corrupt, recovered from prev HEAD", branch
            )
            if repair:
                store.set(BRANCH_HEAD % branch, to_bytes(commit_hash))
            return commit_hash

    # 3. HEAD existed but is corrupt and no prev — scan for best commit
    if head_bytes is not None:
        commit_hash = _scan_for_best_commit(store, branch)
        if commit_hash is not None:
            logger.warning(
                "Branch '%s': HEAD corrupt, recovered via commit scan", branch
            )
            if repair:
                store.set(BRANCH_HEAD % branch, to_bytes(commit_hash))
            return commit_hash

    return None


def _scan_for_best_commit(store: KVStore, branch: str) -> str | None:
    """Scan the store for the best valid commit for a corrupt branch.

    Finds all valid commits, excludes those reachable from healthy branches,
    and returns the most recent tip (by ``__commit_time__``).
    """
    root_prefix = COMMIT_ROOT.replace("%s", "")
    all_commits: dict[str, float] = {}
    for key in store.keys():
        if not isinstance(key, str) or not key.startswith(root_prefix):
            continue
        h = key[len(root_prefix) :]
        if not h:
            continue
        time_bytes = store.get(COMMIT_TIME % h)
        ts = 0.0
        if time_bytes is not None:
            try:
                val = _safe_from_bytes(time_bytes)
                if isinstance(val, (int, float)):
                    ts = float(val)
            except Exception:
                pass
        all_commits[h] = ts

    if not all_commits:
        return None

    # Exclude commits reachable from healthy branches
    claimed: set[str] = set()
    head_prefix = BRANCH_HEAD.replace("%s", "")
    for key in store.keys():
        if not isinstance(key, str) or not key.startswith(head_prefix):
            continue
        other = key[len(head_prefix) :]
        if other == branch or not other:
            continue
        hb = store.get(key)
        if hb is None:
            continue
        h = _safe_from_bytes(hb)
        if not isinstance(h, str) or store.get(COMMIT_ROOT % h) is None:
            continue
        # Walk parent chain
        stack = [h]
        while stack:
            c = stack.pop()
            if c in claimed:
                continue
            claimed.add(c)
            pb = store.get(PARENT_COMMIT % c)
            if pb is not None:
                parsed = _safe_from_bytes(pb)
                if isinstance(parsed, str):
                    stack.append(parsed)
                elif isinstance(parsed, list):
                    stack.extend(p for p in parsed if isinstance(p, str))

    candidates = {h for h in all_commits if h not in claimed}
    if not candidates:
        candidates = set(all_commits)

    # Find tips (not a parent of any other candidate)
    all_parents: set[str] = set()
    for h in candidates:
        pb = store.get(PARENT_COMMIT % h)
        if pb is not None:
            parsed = _safe_from_bytes(pb)
            if isinstance(parsed, str):
                all_parents.add(parsed)
            elif isinstance(parsed, list):
                all_parents.update(p for p in parsed if isinstance(p, str))
    tips = candidates - all_parents
    if not tips:
        tips = candidates

    return max(tips, key=lambda h: all_commits.get(h, 0))


class VersionedKV(VersionedBase):
    """A commit log over a KV store.

    The caller owns the working state. VersionedKV provides:
    - ``get()`` / ``get_many()`` to read from the current commit
    - ``commit()`` to atomically write changes and advance HEAD
    - ``refresh()`` to reload from HEAD
    - ``checkout()`` / ``history()`` for navigating commits
    """

    def __init__(
        self,
        store: KVStore | None = None,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
    ) -> None:
        if store is None:
            store = Memory()
        self.store = store

        _check_storage_version(store)

        if commit_hash is None:
            commit_hash = _resolve_head(store, branch)
            if commit_hash is None and store.get(BRANCH_HEAD % branch) is not None:
                raise ValueError(f"Branch '{branch}' HEAD is corrupt and unrecoverable")
            if commit_hash is None:
                # Create initial empty commit
                commit_hash = content_hash((), {}, {})
                initial = {
                    COMMIT_ROOT % commit_hash: to_bytes(EMPTY_HASH),
                    PARENT_COMMIT % commit_hash: to_bytes([]),
                    COMMIT_TIME % commit_hash: to_bytes(time.time()),
                    BRANCH_HEAD % branch: to_bytes(commit_hash),
                }
                store.set_many(**initial)

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )

        super().__init__(branch=branch, commit_hash=commit_hash)

        # Materialize keyset + meta from the HAMT
        self._meta: dict[str, MetaEntry] = {}
        self._touch_counter = 0
        self._populate_state(commit_hash)

    def _populate_state(self, commit_hash: str) -> None:
        """Walk the commit's HAMT and populate ``_commit_keys`` / ``_meta``."""
        root = _load_root(self.store, commit_hash)
        if root is None:
            self._commit_keys = {}
            self._meta = {}
            self._touch_counter = 0
            return

        ks = Keyset(self.store, root=root)
        commit_keys: dict[str, str] = {}
        meta: dict[str, MetaEntry] = {}
        for key, entry in ks.items():
            commit_keys[key] = entry.blob
            meta[key] = entry.meta
        self._commit_keys = commit_keys
        self._meta = meta
        self._touch_counter = (
            max((e.last_touch for e in meta.values()), default=0) if meta else 0
        )

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        return _resolve_head(self.store, self._branch, repair=False)

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the current commit. Updates touch for GC."""
        versioned_key = self._commit_keys.get(key)
        if versioned_key is None:
            return None
        value = self.store.get(versioned_key)
        if value is not None:
            self._touch(key)
        return value

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the current commit."""
        # Map user keys -> versioned keys, skipping missing
        vk_to_key: dict[str, str] = {}
        for key in keys:
            vk = self._commit_keys.get(key)
            if vk is not None:
                vk_to_key[vk] = key

        if not vk_to_key:
            return {}

        raw = self.store.get_many(*vk_to_key.keys())
        result: dict[str, bytes] = {}
        for vk, value in raw.items():
            key = vk_to_key[vk]
            result[key] = value
            self._touch(key)
        return result

    # -- Abstract method implementations --

    def _snapshot_state(self) -> tuple:
        """Capture in-memory state before a commit attempt."""
        return (
            self._current_commit,
            dict(self._commit_keys),
            dict(self._meta),
            self._touch_counter,
        )

    def _restore_state(self, saved: tuple) -> None:
        """Restore in-memory state after a failed commit attempt."""
        self._current_commit, self._commit_keys, self._meta, self._touch_counter = saved

    def _create_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
    ) -> str:
        """Create a new local commit with the given changes.

        Does not advance HEAD. Use ``commit()`` for the public API.

        Returns:
            The new commit hash.
        """
        updates = updates or {}
        removals = removals or set()

        # Build new in-memory dicts: carry forward, apply removals, apply updates
        new_commit_keys: dict[str, str] = {}
        new_meta: dict[str, MetaEntry] = {}

        for key, versioned_key in self._commit_keys.items():
            if key in removals:
                continue
            new_commit_keys[key] = versioned_key
            if key in self._meta:
                new_meta[key] = self._meta[key]

        # Compute content-addressable hash from a placeholder keyset
        # (real versioned blob keys depend on the commit hash itself).
        preview_keys = dict(new_commit_keys)
        for key in updates:
            preview_keys[key] = f"<pending:{key}>"
        new_hash = content_hash(
            (self._current_commit,), preview_keys, updates, info=info
        )

        # Resolve real versioned blob keys for new updates
        diffs: dict[str, bytes] = {}
        for key, value in updates.items():
            versioned_key = f"{new_hash}:{key}"
            diffs[versioned_key] = value
            new_commit_keys[key] = versioned_key
            size = len(value)
            if key in new_meta:
                new_meta[key] = MetaEntry(
                    last_touch=new_meta[key].last_touch,
                    size=size,
                    created_at=new_meta[key].created_at,
                )
            else:
                self._touch_counter += 1
                new_meta[key] = MetaEntry(
                    last_touch=self._touch_counter,
                    size=size,
                    created_at=time.time(),
                )

        # Build the new keyset by applying changes to the parent's HAMT.
        # Only the explicitly changed keys generate new entries; structural
        # sharing reuses unchanged subtrees from the parent commit.
        parent_root = _load_root(self.store, self._current_commit) or EMPTY_HASH
        parent_ks = Keyset(self.store, root=parent_root)
        keyset_updates = {
            key: KeysetEntry(blob=new_commit_keys[key], meta=new_meta[key])
            for key in updates
        }
        new_ks, pending = parent_ks.updated(updates=keyset_updates, removals=removals)
        diffs.update(pending)

        # Commit metadata
        diffs[COMMIT_ROOT % new_hash] = to_bytes(new_ks.root)
        diffs[PARENT_COMMIT % new_hash] = to_bytes([self._current_commit])
        diffs[COMMIT_TIME % new_hash] = to_bytes(time.time())
        if info is not None:
            diffs[INFO_KEY % new_hash] = to_bytes(info)

        # Write everything atomically
        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    def _create_merge_commit(
        self,
        resolution: MergeResolution,
        parents: tuple[str, ...],
        info: dict | None,
    ) -> str:
        """Create a merge commit from a resolved three-way merge."""
        merged_keyset = resolution.merged_keyset
        merged_values = resolution.merged_values

        preview_keys = dict(merged_keyset)
        for key in merged_values:
            preview_keys[key] = f"<pending:{key}>"

        merge_hash = content_hash(parents, preview_keys, merged_values, info)

        # Build write batch
        diffs: dict[str, bytes] = {}
        for key, value in merged_values.items():
            vk = f"{merge_hash}:{key}"
            merged_keyset[key] = vk
            diffs[vk] = value

        # Build merged meta from the parents' meta. ``self._meta`` is
        # already our parent's meta (in memory). Their parent's meta
        # we have to walk via the HAMT.
        their_root = _load_root(self.store, parents[0])
        their_meta: dict[str, MetaEntry] = {}
        if their_root is not None:
            their_ks = Keyset(self.store, root=their_root)
            for key, entry in their_ks.items():
                their_meta[key] = entry.meta

        merged_meta: dict[str, MetaEntry] = {}
        for key in merged_keyset:
            if key in merged_values:
                self._touch_counter += 1
                merged_meta[key] = MetaEntry(
                    last_touch=self._touch_counter,
                    size=len(merged_values[key]),
                    created_at=time.time(),
                )
            elif key in self._meta:
                merged_meta[key] = self._meta[key]
            elif key in their_meta:
                merged_meta[key] = their_meta[key]

        # Apply the merge result on top of our parent's HAMT. We compute
        # the minimal updates and removals so structural sharing kicks in
        # for unchanged subtrees.
        our_root = _load_root(self.store, self._current_commit) or EMPTY_HASH
        parent_ks = Keyset(self.store, root=our_root)

        keyset_updates: dict[str, KeysetEntry] = {}
        for key, blob in merged_keyset.items():
            new_entry = KeysetEntry(blob=blob, meta=merged_meta[key])
            old_blob = self._commit_keys.get(key)
            old_meta = self._meta.get(key)
            if old_blob != new_entry.blob or old_meta != new_entry.meta:
                keyset_updates[key] = new_entry

        keyset_removals = {key for key in self._commit_keys if key not in merged_keyset}

        new_ks, pending = parent_ks.updated(
            updates=keyset_updates, removals=keyset_removals
        )
        diffs.update(pending)

        diffs[COMMIT_ROOT % merge_hash] = to_bytes(new_ks.root)
        diffs[PARENT_COMMIT % merge_hash] = to_bytes(list(parents))
        diffs[COMMIT_TIME % merge_hash] = to_bytes(time.time())
        if info is not None:
            diffs[INFO_KEY % merge_hash] = to_bytes(info)

        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = merged_keyset
        self._current_commit = merge_hash
        self._meta = merged_meta

        return merge_hash

    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD via KVStore CAS.

        Saves the current HEAD as prev HEAD before advancing, so a
        corrupt write can be recovered from.
        """
        branch_key = BRANCH_HEAD % self._branch
        prev_key = BRANCH_HEAD_PREV % self._branch
        self.store.set(prev_key, to_bytes(expected))
        return self.store.cas(
            branch_key, to_bytes(new_head), expected=to_bytes(expected)
        )

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        """Load just the keyset for a commit (key -> versioned_key mapping).

        Used by the merge layer; returns a flat dict, dropping meta.
        """
        root = _load_root(self.store, commit_hash)
        if root is None:
            return {}
        ks = Keyset(self.store, root=root)
        return {key: entry.blob for key, entry in ks.items()}

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""
        parent_bytes = self.store.get(PARENT_COMMIT % commit_hash)
        if parent_bytes is None:
            return ()
        raw = from_bytes(parent_bytes)
        if raw is None:
            return ()
        if isinstance(raw, str):
            return (raw,)
        return tuple(raw)

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits."""
        if commit_a == commit_b:
            return commit_a

        from collections import deque

        seen_a: set[str] = {commit_a}
        seen_b: set[str] = {commit_b}
        queue_a: deque[str] = deque([commit_a])
        queue_b: deque[str] = deque([commit_b])

        while queue_a or queue_b:
            if queue_a:
                current = queue_a.popleft()
                if current in seen_b:
                    return current
                for p in self._load_parents(current):
                    if p not in seen_a:
                        seen_a.add(p)
                        queue_a.append(p)
                        if p in seen_b:
                            return p

            if queue_b:
                current = queue_b.popleft()
                if current in seen_a:
                    return current
                for p in self._load_parents(current):
                    if p not in seen_b:
                        seen_b.add(p)
                        queue_b.append(p)
                        if p in seen_a:
                            return p

        return None

    def _read_blob(self, content_id: str) -> bytes | None:
        """Read a blob by its versioned key."""
        return self.store.get(content_id)

    # -- Navigation --

    def refresh(self) -> None:
        """Reload state from HEAD."""
        commit_hash = _resolve_head(self.store, self._branch)
        if commit_hash is None:
            raise ValueError("No HEAD commit found for branch %s" % self._branch)
        self._load_commit(commit_hash, update_base=True)

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "VersionedKV | None":
        """Return a new VersionedKV at a specific commit."""
        if self.store.get(COMMIT_ROOT % commit_hash) is None:
            return None
        return VersionedKV(
            self.store,
            commit_hash=commit_hash,
            branch=branch or self._branch,
        )

    def create_branch(self, name: str, *, at: str | None = None) -> "VersionedKV":
        """Fork a commit onto a new branch.

        Returns a new VersionedKV instance on the new branch.
        """
        branch_key = BRANCH_HEAD % name
        target = at or self._current_commit
        if at is not None and self.store.get(COMMIT_ROOT % at) is None:
            raise ValueError(f"Commit '{at}' does not exist")
        if not self.store.cas(branch_key, to_bytes(target), expected=None):
            raise ValueError(f"Branch '{name}' already exists")
        return VersionedKV(self.store, commit_hash=target, branch=name)

    def delete_branch(self, name: str) -> None:
        """Delete a branch and clean up orphaned commits."""
        if name == self._branch:
            raise ValueError("Cannot delete the current branch")
        branch_key = BRANCH_HEAD % name
        if self.store.get(branch_key) is None:
            raise ValueError(f"Branch '{name}' does not exist")
        self.store.remove(branch_key)
        self.clean_orphans()

    def switch_branch(self, name: str) -> None:
        """Switch this instance to a different branch in-place."""
        commit_hash = _resolve_head(self.store, name)
        if commit_hash is None:
            if self.store.get(BRANCH_HEAD % name) is not None:
                raise ValueError(f"Branch '{name}' HEAD is corrupt and unrecoverable")
            raise ValueError(f"Branch '{name}' does not exist")
        self._branch = name
        self._load_commit(commit_hash, update_base=True)

    def peek(self, key: str, *, branch: str) -> bytes | None:
        """Read a key from another branch's HEAD without switching."""
        commit_hash = _resolve_head(self.store, branch)
        if commit_hash is None:
            return None
        root = _load_root(self.store, commit_hash)
        if root is None:
            return None
        ks = Keyset(self.store, root=root)
        entry = ks.get(key)
        if entry is None:
            return None
        return self.store.get(entry.blob)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_ROOT % commit_hash) is None:
            return False
        branch_key = BRANCH_HEAD % self._branch
        prev_key = BRANCH_HEAD_PREV % self._branch
        # Save current HEAD as prev before overwriting
        current = self.store.get(branch_key)
        if current is not None:
            self.store.set(prev_key, current)
        self.store.set(branch_key, to_bytes(commit_hash))
        self._load_commit(commit_hash, update_base=True)
        return True

    @staticmethod
    def branches(store: KVStore) -> list[str]:
        """List all branch names in the store."""
        prefix = BRANCH_HEAD.replace("%s", "")
        result = []
        for key in store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                branch_name = key[len(prefix) :]
                if branch_name:
                    result.append(branch_name)
        return sorted(result)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return VersionedKV.branches(self.store)

    def commit_info(self, commit_hash: str | None = None) -> dict | None:
        """Retrieve the info dict for a commit, or None if none was stored."""
        target = commit_hash or self._current_commit
        info_bytes = self.store.get(INFO_KEY % target)
        if info_bytes is None:
            return None
        return from_bytes(info_bytes)

    # -- Orphan cleanup --

    def clean_orphans(self, min_age: float = 3600) -> int:
        """Remove orphaned commits unreachable from any branch HEAD.

        Traces all reachable commits from live branch HEADs, then
        deletes commit metadata, blobs, and HAMT nodes that are not
        reachable from any reachable commit.

        The ``min_age`` guard (default 1 hour) prevents recently
        created commits from being falsely swept during concurrent
        writes.

        Returns:
            Number of orphaned commits removed.
        """
        gc_logger = logging.getLogger("kvgit.orphans")

        # Mark phase: walk every branch's history, collecting reachable
        # commits, blob keys, and HAMT node hashes.
        reachable_commits: set[str] = set()
        reachable_blobs: set[str] = set()
        reachable_nodes: set[str] = set()

        branch_prefix = BRANCH_HEAD.replace("%s", "")
        for key in self.store.keys():
            if not (isinstance(key, str) and key.startswith(branch_prefix)):
                continue
            branch_name = key[len(branch_prefix) :]
            branch_head = _resolve_head(self.store, branch_name)
            if branch_head is None:
                continue
            for commit in self.history(commit_hash=branch_head, all_parents=True):
                if commit in reachable_commits:
                    continue
                reachable_commits.add(commit)
                root = _load_root(self.store, commit)
                if root is None:
                    continue
                ks = Keyset(self.store, root=root)
                for _, entry in ks.items():
                    reachable_blobs.add(entry.blob)
                for node_hash in ks.reachable_nodes():
                    reachable_nodes.add(node_hash)

        # Sweep phase: find orphaned commits via __commit_root__ scan.
        cutoff_time = time.time() - min_age
        orphans: list[str] = []
        root_prefix = COMMIT_ROOT.replace("%s", "")

        for key in self.store.keys():
            if not (isinstance(key, str) and key.startswith(root_prefix)):
                continue
            commit_hash = key[len(root_prefix) :]
            if not commit_hash or commit_hash in reachable_commits:
                continue
            time_bytes = self.store.get(COMMIT_TIME % commit_hash)
            if time_bytes is None:
                # No timestamp recorded — be conservative, leave it alone.
                continue
            try:
                ts_val = _safe_from_bytes(time_bytes)
                if not isinstance(ts_val, (int, float)):
                    continue
                if float(ts_val) < cutoff_time:
                    orphans.append(commit_hash)
            except (TypeError, ValueError):
                continue

        # Collect everything to delete in one batch so the sweep is atomic
        # at the store level (defends against partial sweeps under crash).
        all_removals: list[str] = []

        for orphan_hash in orphans:
            orphan_root = _load_root(self.store, orphan_hash)
            if orphan_root is not None and orphan_root != EMPTY_HASH:
                try:
                    orphan_ks = Keyset(self.store, root=orphan_root)
                    for _, entry in orphan_ks.items():
                        if entry.blob not in reachable_blobs:
                            all_removals.append(entry.blob)
                except Exception:
                    pass
            all_removals.extend(
                [
                    COMMIT_ROOT % orphan_hash,
                    PARENT_COMMIT % orphan_hash,
                    COMMIT_TIME % orphan_hash,
                    INFO_KEY % orphan_hash,
                ]
            )

        # Orphan HAMT nodes: any keyset node not reachable from a live commit
        keyset_prefix = Keyset.DEFAULT_PREFIX
        for key in self.store.keys():
            if not (isinstance(key, str) and key.startswith(keyset_prefix)):
                continue
            node_hash = key[len(keyset_prefix) :]
            if node_hash and node_hash not in reachable_nodes:
                all_removals.append(key)

        if all_removals:
            self.store.remove_many(*all_removals)

        if orphans:
            gc_logger.debug("Cleaned %d orphaned commit(s)", len(orphans))

        return len(orphans)

    # -- Internal --

    def _touch(self, key: str) -> None:
        """Update last_touch for a key (in-memory only).

        Note: as of storage v2 this no longer persists across commits.
        Persisting touch counts would require rewriting every leaf on
        every commit, which would obliterate structural sharing.
        ``last_touch`` is now an in-session counter that resets when a
        commit is reloaded.
        """
        if key in self._meta:
            self._touch_counter += 1
            entry = self._meta[key]
            self._meta[key] = MetaEntry(
                last_touch=self._touch_counter,
                size=entry.size,
                created_at=entry.created_at,
            )

    def _load_commit(self, commit_hash: str, *, update_base: bool) -> None:
        """Load a commit's state into memory."""
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
        self._populate_state(commit_hash)
