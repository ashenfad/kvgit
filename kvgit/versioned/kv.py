"""KVStore-backed versioned state."""

import hashlib
import json
import logging
import time

from ..encoding import MetaEntry, from_bytes, meta_from_bytes, meta_to_bytes, to_bytes
from ..kv.base import KVStore
from ..kv.memory import Memory
from .base import VersionedBase
from .merge import MergeResolution

PARENT_COMMIT = "__parent_commit__%s"
COMMIT_KEYSET = "__commit_keyset__%s"
BRANCH_HEAD = "__branch_head__%s"
META_KEY = "__meta__%s"
TOTAL_VAR_SIZE_KEY = "__total_var_size__%s"
INFO_KEY = "__info__%s"


def content_hash(
    parents: tuple[str, ...],
    keyset: dict[str, str],
    updates: dict[str, bytes],
    info: dict | None = None,
) -> str:
    """Compute a content-addressable commit hash.

    Hashes the parent pointers, keyset, update blob digests, and
    optional info to produce a deterministic 40-hex-char commit hash.
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

        if commit_hash is None:
            head_bytes = store.get(BRANCH_HEAD % branch)
            if head_bytes is not None:
                commit_hash = from_bytes(head_bytes)
            else:
                # Create initial empty commit
                commit_hash = content_hash((), {}, {})
                initial = {
                    COMMIT_KEYSET % commit_hash: to_bytes({}),
                    PARENT_COMMIT % commit_hash: to_bytes([]),
                    BRANCH_HEAD % branch: to_bytes(commit_hash),
                    META_KEY % commit_hash: meta_to_bytes({}),
                    TOTAL_VAR_SIZE_KEY % commit_hash: to_bytes(0),
                }
                store.set_many(**initial)

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )

        super().__init__(branch=branch, commit_hash=commit_hash)

        # Load commit keyset
        keyset_bytes = self.store.get(COMMIT_KEYSET % self._current_commit)
        if keyset_bytes is not None:
            self._commit_keys = from_bytes(keyset_bytes)

        # Load metadata for GC
        self._meta: dict[str, MetaEntry] = {}
        meta_bytes = self.store.get(META_KEY % self._current_commit)
        if meta_bytes is not None:
            try:
                self._meta = meta_from_bytes(meta_bytes)
            except Exception:
                self._meta = {}
        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is not None:
            return from_bytes(head_bytes)
        return None

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

        # Build new keyset: carry forward, apply removals, apply updates
        new_commit_keys: dict[str, str] = {}
        new_meta: dict[str, MetaEntry] = {}

        for key, versioned_key in self._commit_keys.items():
            if key in removals:
                continue
            new_commit_keys[key] = versioned_key
            if key in self._meta:
                new_meta[key] = self._meta[key]

        # Compute content-addressable hash
        # (includes parent, new keyset preview, and update blobs)
        preview_keys = dict(new_commit_keys)
        for key in updates:
            preview_keys[key] = f"<pending:{key}>"
        new_hash = content_hash(
            (self._current_commit,), preview_keys, updates, info=info
        )

        # Store update blobs with versioned keys
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

        # Store commit metadata
        diffs[COMMIT_KEYSET % new_hash] = to_bytes(new_commit_keys)
        diffs[PARENT_COMMIT % new_hash] = to_bytes([self._current_commit])
        diffs[META_KEY % new_hash] = meta_to_bytes(new_meta)
        total_size = sum(e.size for e in new_meta.values() if e.size is not None)
        diffs[TOTAL_VAR_SIZE_KEY % new_hash] = to_bytes(total_size)
        if info is not None:
            diffs[INFO_KEY % new_hash] = to_bytes(info)

        # Write everything
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

        # Build meta for merge commit
        our_meta_bytes = self.store.get(META_KEY % self._current_commit)
        their_meta_bytes = self.store.get(META_KEY % parents[0])
        our_meta = meta_from_bytes(our_meta_bytes) if our_meta_bytes else {}
        their_meta = meta_from_bytes(their_meta_bytes) if their_meta_bytes else {}

        merged_meta: dict[str, MetaEntry] = {}
        for key in merged_keyset:
            if key in merged_values:
                self._touch_counter += 1
                merged_meta[key] = MetaEntry(
                    last_touch=self._touch_counter,
                    size=len(merged_values[key]),
                    created_at=time.time(),
                )
            elif key in our_meta:
                merged_meta[key] = our_meta[key]
            elif key in their_meta:
                merged_meta[key] = their_meta[key]

        diffs[COMMIT_KEYSET % merge_hash] = to_bytes(merged_keyset)
        diffs[PARENT_COMMIT % merge_hash] = to_bytes(list(parents))
        diffs[META_KEY % merge_hash] = meta_to_bytes(merged_meta)
        total_size = sum(e.size for e in merged_meta.values() if e.size is not None)
        diffs[TOTAL_VAR_SIZE_KEY % merge_hash] = to_bytes(total_size)
        if info is not None:
            diffs[INFO_KEY % merge_hash] = to_bytes(info)

        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = merged_keyset
        self._current_commit = merge_hash
        self._meta = merged_meta

        return merge_hash

    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD via KVStore CAS."""
        branch_key = BRANCH_HEAD % self._branch
        return self.store.cas(
            branch_key, to_bytes(new_head), expected=to_bytes(expected)
        )

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        """Load just the keyset for a commit (key -> versioned_key mapping)."""
        keyset_bytes = self.store.get(COMMIT_KEYSET % commit_hash)
        if keyset_bytes is None:
            return {}
        return from_bytes(keyset_bytes)

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
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is None:
            raise ValueError("No HEAD commit found for branch %s" % self._branch)
        self._load_commit(from_bytes(head_bytes), update_base=True)

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "VersionedKV | None":
        """Return a new VersionedKV at a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
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
        if at is not None and self.store.get(COMMIT_KEYSET % at) is None:
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
        self.clean_orphans(min_age=0)

    def switch_branch(self, name: str) -> None:
        """Switch this instance to a different branch in-place."""
        branch_key = BRANCH_HEAD % name
        head_bytes = self.store.get(branch_key)
        if head_bytes is None:
            raise ValueError(f"Branch '{name}' does not exist")
        self._branch = name
        self._load_commit(from_bytes(head_bytes), update_base=True)

    def peek(self, key: str, *, branch: str) -> bytes | None:
        """Read a key from another branch's HEAD without switching."""
        head_bytes = self.store.get(BRANCH_HEAD % branch)
        if head_bytes is None:
            return None
        commit_hash = from_bytes(head_bytes)
        keyset_bytes = self.store.get(COMMIT_KEYSET % commit_hash)
        if keyset_bytes is None:
            return None
        keyset = from_bytes(keyset_bytes)
        content_hash_ = keyset.get(key)
        if content_hash_ is None:
            return None
        return self.store.get(content_hash_)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return False
        self.store.set(BRANCH_HEAD % self._branch, to_bytes(commit_hash))
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
        deletes commit metadata and versioned keys for any commit
        not in the reachable set. Only deletes blobs that are not
        referenced by any reachable commit.

        The ``min_age`` guard (default 1 hour) prevents recently
        created commits from being falsely swept during concurrent
        writes. Use ``min_age=0`` when concurrency is not a concern
        (e.g. single-user browser environments).

        Returns:
            Number of orphaned commits removed.
        """
        logger = logging.getLogger("kvgit.orphans")

        # Mark phase: find all reachable commits and their blob keys
        reachable: set[str] = set()
        reachable_blobs: set[str] = set()
        prefix = BRANCH_HEAD.replace("%s", "")
        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                head_bytes = self.store.get(key)
                if head_bytes is None:
                    continue
                branch_head = from_bytes(head_bytes)
                for commit in self.history(commit_hash=branch_head, all_parents=True):
                    if commit not in reachable:
                        reachable.add(commit)
                        ks_bytes = self.store.get(COMMIT_KEYSET % commit)
                        if ks_bytes:
                            try:
                                for vk in from_bytes(ks_bytes).values():
                                    reachable_blobs.add(vk)
                            except Exception:
                                pass

        # Sweep phase: find orphaned commits by scanning for meta keys
        meta_prefix = META_KEY.replace("%s", "")
        cutoff_time = time.time() - min_age
        orphans: list[str] = []

        for key in self.store.keys():
            if not isinstance(key, str) or not key.startswith(meta_prefix):
                continue
            commit_hash = key[len(meta_prefix) :]
            if not commit_hash or commit_hash in reachable:
                continue
            # Check age
            meta_bytes = self.store.get(key)
            if meta_bytes is None:
                continue
            try:
                meta = meta_from_bytes(meta_bytes)
                if meta:
                    newest = max(e.created_at for e in meta.values())
                    if newest < cutoff_time:
                        orphans.append(commit_hash)
                else:
                    orphans.append(commit_hash)
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # Delete orphaned commits — only remove blobs not used by reachable commits
        for orphan_hash in orphans:
            keyset_bytes = self.store.get(COMMIT_KEYSET % orphan_hash)
            if keyset_bytes:
                try:
                    keyset = from_bytes(keyset_bytes)
                    orphan_only = [
                        vk for vk in keyset.values() if vk not in reachable_blobs
                    ]
                    if orphan_only:
                        self.store.remove_many(*orphan_only)
                except Exception:
                    pass
            self.store.remove_many(
                META_KEY % orphan_hash,
                COMMIT_KEYSET % orphan_hash,
                PARENT_COMMIT % orphan_hash,
                TOTAL_VAR_SIZE_KEY % orphan_hash,
                INFO_KEY % orphan_hash,
            )

        if orphans:
            logger.debug("Cleaned %d orphaned commit(s)", len(orphans))

        return len(orphans)

    # -- Internal --

    def _touch(self, key: str) -> None:
        """Update last_touch for a key (in-memory only, persisted on commit)."""
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

        keyset_bytes = self.store.get(COMMIT_KEYSET % commit_hash)
        self._commit_keys = from_bytes(keyset_bytes) if keyset_bytes else {}

        meta_bytes = self.store.get(META_KEY % commit_hash)
        if meta_bytes is not None:
            try:
                self._meta = meta_from_bytes(meta_bytes)
            except Exception:
                self._meta = {}
        else:
            self._meta = {}

        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )
