"""Versioned state: a commit log over a KV store."""

import hashlib
import json
import time
from dataclasses import asdict, dataclass
from collections import deque
from typing import Callable, Iterable

from .errors import ConcurrencyError, MergeConflict
from .kv.base import KVStore
from .kv.memory import Memory

PARENT_COMMIT = "__parent_commit__%s"
COMMIT_KEYSET = "__commit_keyset__%s"
BRANCH_HEAD = "__branch_head__%s"
META_KEY = "__meta__%s"
TOTAL_VAR_SIZE_KEY = "__total_var_size__%s"
INFO_KEY = "__info__%s"


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


def _content_hash(
    parents: tuple[str, ...],
    keyset: dict[str, str],
    updates: dict[str, bytes],
    info: dict | None = None,
) -> str:
    """Compute a content-addressable commit hash.

    Hashes the parent pointers, keyset, update blob digests, and
    optional info to produce a deterministic 16-hex-char commit hash.
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


class Versioned:
    """A commit log over a KV store.

    The caller owns the working state. Versioned provides:
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
        self._branch = branch

        if commit_hash is None:
            head_bytes = store.get(BRANCH_HEAD % branch)
            if head_bytes is not None:
                commit_hash = _from_bytes(head_bytes)
            else:
                # Create initial empty commit
                commit_hash = _content_hash((), {}, {})
                initial = {
                    COMMIT_KEYSET % commit_hash: _to_bytes({}),
                    PARENT_COMMIT % commit_hash: _to_bytes([]),
                    BRANCH_HEAD % branch: _to_bytes(commit_hash),
                    META_KEY % commit_hash: _meta_to_bytes({}),
                    TOTAL_VAR_SIZE_KEY % commit_hash: _to_bytes(0),
                }
                store.set_many(**initial)

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )
        self._current_commit: str = commit_hash
        self._base_commit: str = commit_hash

        # Load commit keyset
        self._commit_keys: dict[str, str] = {}
        keyset_bytes = self.store.get(COMMIT_KEYSET % self._current_commit)
        if keyset_bytes is not None:
            self._commit_keys = _from_bytes(keyset_bytes)

        # Load metadata for GC
        self._meta: dict[str, MetaEntry] = {}
        meta_bytes = self.store.get(META_KEY % self._current_commit)
        if meta_bytes is not None:
            try:
                self._meta = _meta_from_bytes(meta_bytes)
            except Exception:
                self._meta = {}
        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )

        # Merge function registry
        self._merge_fns: dict[str, BytesMergeFn] = {}
        self._default_merge: BytesMergeFn | None = None
        self.last_merge_result: MergeResult | None = None

    @property
    def current_commit(self) -> str:
        return self._current_commit

    @property
    def base_commit(self) -> str:
        return self._base_commit

    @property
    def current_branch(self) -> str:
        """The name of the current branch."""
        return self._branch

    def __repr__(self) -> str:
        n_keys = len(self._commit_keys)
        short_hash = self._current_commit[:8]
        return (
            f"Versioned(branch={self._branch!r}, commit={short_hash}..., keys={n_keys})"
        )

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is not None:
            return _from_bytes(head_bytes)
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

    def keys(self) -> Iterable[str]:
        """All keys in the current commit."""
        return self._commit_keys.keys()

    def __contains__(self, key: str) -> bool:
        return key in self._commit_keys

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: BytesMergeFn) -> None:
        """Register a merge function for a specific key."""
        self._merge_fns[key] = fn

    def set_default_merge(self, fn: BytesMergeFn) -> None:
        """Register a default merge function for unregistered keys."""
        self._default_merge = fn

    # -- Write operations --

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
        new_hash = _content_hash(
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
        diffs[COMMIT_KEYSET % new_hash] = _to_bytes(new_commit_keys)
        diffs[PARENT_COMMIT % new_hash] = _to_bytes([self._current_commit])
        diffs[META_KEY % new_hash] = _meta_to_bytes(new_meta)
        total_size = sum(e.size for e in new_meta.values() if e.size is not None)
        diffs[TOTAL_VAR_SIZE_KEY % new_hash] = _to_bytes(total_size)
        if info is not None:
            diffs[INFO_KEY % new_hash] = _to_bytes(info)

        # Write everything
        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    # -- Branching / concurrency --

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, BytesMergeFn] | None = None,
        default_merge: BytesMergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Commit changes and atomically advance HEAD.

        Creates a new commit with the given changes and advances the
        branch HEAD. If HEAD has diverged, performs a three-way merge.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.
            on_conflict: 'raise' (default) or 'abandon' for CAS failures.
            merge_fns: Per-key merge functions (override instance-level).
            default_merge: Default merge function (override instance-level).
            info: Optional metadata dict for the commit.

        Returns:
            A MergeResult (truthy when committed, falsy if abandoned).

        Raises:
            ConcurrencyError: If on_conflict='raise' and CAS fails.
            MergeConflict: If keys conflict and no merge function
                resolves them.
        """
        # No-op if no changes
        if not updates and not removals and info is None:
            result = MergeResult(
                merged=True,
                commit=self._current_commit,
                strategy="no_op",
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result

        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )

        branch_key = BRANCH_HEAD % self._branch
        current_head = self.latest_head

        if current_head == self._base_commit:
            # Fast-forward path: create commit with info, CAS HEAD
            saved = self._snapshot_state()
            self._create_commit(updates, removals, info=info)

            expected = _to_bytes(self._base_commit)
            new_head = _to_bytes(self._current_commit)
            if self.store.cas(branch_key, new_head, expected=expected):
                self._base_commit = self._current_commit
                result = MergeResult(
                    merged=True,
                    commit=self._current_commit,
                    strategy="fast_forward",
                    auto_merged_keys=(),
                    carried_keys=tuple(self._commit_keys.keys()),
                )
                self.last_merge_result = result
                return result
            self._restore_state(saved)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="fast_forward",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError(
                f"HEAD changed from {self._base_commit}. Refresh and retry."
            )

        # Three-way path: create commit without info, merge with info
        if current_head is None:
            raise ValueError(f"Branch '{self._branch}' has no HEAD")
        saved = self._snapshot_state()
        self._create_commit(updates, removals)
        return self._three_way_merge(
            current_head,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            default_merge=default_merge,
            info=info,
            saved_state=saved,
        )

    def _three_way_merge(
        self,
        their_head: str,
        *,
        on_conflict: str,
        merge_fns: dict[str, BytesMergeFn] | None,
        default_merge: BytesMergeFn | None,
        info: dict | None,
        saved_state: tuple | None = None,
    ) -> MergeResult:
        """Perform a three-way merge between our branch and their HEAD."""
        branch_key = BRANCH_HEAD % self._branch
        lca = self._find_lca(self._current_commit, their_head)
        if lca is None:
            if saved_state is not None:
                self._restore_state(saved_state)
            if on_conflict == "abandon":
                result = MergeResult(
                    merged=False,
                    commit=None,
                    strategy="three_way",
                    auto_merged_keys=(),
                    carried_keys=(),
                )
                self.last_merge_result = result
                return result
            raise ConcurrencyError(
                "No common ancestor found between current commit and HEAD."
            )

        our_diff = self.diff(lca, self._current_commit)
        their_diff = self.diff(lca, their_head)

        # Build effective merge function lookup
        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_default = default_merge or self._default_merge

        # Load keysets
        lca_keyset = self._load_keyset(lca)
        our_keyset = self._load_keyset(self._current_commit)
        their_keyset = self._load_keyset(their_head)

        our_changed = our_diff.added | our_diff.removed | our_diff.modified
        their_changed = their_diff.added | their_diff.removed | their_diff.modified
        all_changed = our_changed | their_changed

        merged_keyset: dict[str, str] = {}
        merged_values: dict[str, bytes] = {}
        auto_merged: list[str] = []
        conflicts: set[str] = set()
        merge_errors: dict[str, Exception] = {}

        # Keys unchanged by either side: carry from their keyset (HEAD)
        all_keys = set(our_keyset.keys()) | set(their_keyset.keys())
        for key in all_keys - all_changed:
            if key in their_keyset:
                merged_keyset[key] = their_keyset[key]
            elif key in our_keyset:
                merged_keyset[key] = our_keyset[key]

        # Keys changed only by us
        for key in our_changed - their_changed:
            if key in our_diff.removed:
                pass  # removed by us, not touched by them
            else:
                merged_keyset[key] = our_keyset[key]
                auto_merged.append(key)

        # Keys changed only by them
        for key in their_changed - our_changed:
            if key in their_diff.removed:
                pass  # removed by them, not touched by us
            else:
                merged_keyset[key] = their_keyset[key]

        # Contested keys: changed by both sides
        contested = our_changed & their_changed
        for key in contested:
            our_removed = key in our_diff.removed
            their_removed = key in their_diff.removed

            if our_removed and their_removed:
                continue  # both removed, key is gone

            # Check if both sides made the same change
            if (
                not our_removed
                and not their_removed
                and our_keyset.get(key) == their_keyset.get(key)
            ):
                merged_keyset[key] = their_keyset[key]
                continue

            # Try merge function
            fn = effective_fns.get(key, effective_default)
            if fn is None:
                conflicts.add(key)
                continue

            old_val = self.store.get(lca_keyset[key]) if key in lca_keyset else None
            our_val = None if our_removed else self.store.get(our_keyset[key])
            their_val = None if their_removed else self.store.get(their_keyset[key])
            try:
                result_val = fn(old_val, our_val, their_val)
                merged_values[key] = result_val
                auto_merged.append(key)
            except Exception as e:
                conflicts.add(key)
                merge_errors[key] = e

        if conflicts:
            raise MergeConflict(conflicts, merge_errors)

        # Build merge commit
        parents = (their_head, self._current_commit)

        preview_keys = dict(merged_keyset)
        for key in merged_values:
            preview_keys[key] = f"<pending:{key}>"

        merge_hash = _content_hash(parents, preview_keys, merged_values, info)

        # Build write batch
        diffs: dict[str, bytes] = {}
        for key, value in merged_values.items():
            vk = f"{merge_hash}:{key}"
            merged_keyset[key] = vk
            diffs[vk] = value

        # Build meta for merge commit
        our_meta_bytes = self.store.get(META_KEY % self._current_commit)
        their_meta_bytes = self.store.get(META_KEY % their_head)
        our_meta = _meta_from_bytes(our_meta_bytes) if our_meta_bytes else {}
        their_meta = _meta_from_bytes(their_meta_bytes) if their_meta_bytes else {}

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

        diffs[COMMIT_KEYSET % merge_hash] = _to_bytes(merged_keyset)
        diffs[PARENT_COMMIT % merge_hash] = _to_bytes(list(parents))
        diffs[META_KEY % merge_hash] = _meta_to_bytes(merged_meta)
        total_size = sum(e.size for e in merged_meta.values() if e.size is not None)
        diffs[TOTAL_VAR_SIZE_KEY % merge_hash] = _to_bytes(total_size)
        if info is not None:
            diffs[INFO_KEY % merge_hash] = _to_bytes(info)

        self.store.set_many(**diffs)

        # CAS HEAD from their_head to merge_hash
        expected = _to_bytes(their_head)
        new_head_bytes = _to_bytes(merge_hash)
        if self.store.cas(branch_key, new_head_bytes, expected=expected):
            self._commit_keys = merged_keyset
            self._current_commit = merge_hash
            self._base_commit = merge_hash
            self._meta = merged_meta
            result = MergeResult(
                merged=True,
                commit=merge_hash,
                strategy="three_way",
                auto_merged_keys=tuple(auto_merged),
                carried_keys=tuple(
                    k
                    for k in merged_keyset
                    if k not in auto_merged and k not in merged_values
                ),
            )
            self.last_merge_result = result
            return result

        if saved_state is not None:
            self._restore_state(saved_state)
        if on_conflict == "abandon":
            result = MergeResult(
                merged=False,
                commit=None,
                strategy="three_way",
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result
        raise ConcurrencyError(
            "HEAD changed during three-way merge. Refresh and retry."
        )

    def refresh(self) -> None:
        """Reload state from HEAD."""
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is None:
            raise ValueError("No HEAD commit found for branch %s" % self._branch)
        self._load_commit(_from_bytes(head_bytes), update_base=True)

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "Versioned | None":
        """Return a new Versioned at a specific commit.

        Args:
            commit_hash: The commit to check out.
            branch: Branch for the new instance (default: same as self).
        """
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return None
        return Versioned(
            self.store,
            commit_hash=commit_hash,
            branch=branch or self._branch,
        )

    def create_branch(self, name: str, *, at: str | None = None) -> "Versioned":
        """Fork a commit onto a new branch.

        Args:
            name: Branch name.
            at: Commit hash to fork from. Defaults to current commit.

        Returns a new Versioned instance on the new branch.

        Raises ValueError if the branch already exists or the commit
        does not exist.
        """
        branch_key = BRANCH_HEAD % name
        target = at or self._current_commit
        if at is not None and self.store.get(COMMIT_KEYSET % at) is None:
            raise ValueError(f"Commit '{at}' does not exist")
        if not self.store.cas(branch_key, _to_bytes(target), expected=None):
            raise ValueError(f"Branch '{name}' already exists")
        return Versioned(self.store, commit_hash=target, branch=name)

    def delete_branch(self, name: str) -> None:
        """Delete a branch by name.

        Removes the branch HEAD pointer. Commits are not removed
        and may be collected by GC if unreachable.

        Raises ValueError if the branch is the current branch or
        does not exist.
        """
        if name == self._branch:
            raise ValueError("Cannot delete the current branch")
        branch_key = BRANCH_HEAD % name
        if self.store.get(branch_key) is None:
            raise ValueError(f"Branch '{name}' does not exist")
        self.store.remove(branch_key)

    def switch_branch(self, name: str) -> None:
        """Switch this instance to a different branch in-place.

        Loads the HEAD commit of the target branch.

        Raises ValueError if the branch does not exist.
        """
        branch_key = BRANCH_HEAD % name
        head_bytes = self.store.get(branch_key)
        if head_bytes is None:
            raise ValueError(f"Branch '{name}' does not exist")
        self._branch = name
        self._load_commit(_from_bytes(head_bytes), update_base=True)

    def peek(self, key: str, *, branch: str) -> bytes | None:
        """Read a key from another branch's HEAD without switching.

        Returns None if the branch doesn't exist or the key isn't present.
        """
        head_bytes = self.store.get(BRANCH_HEAD % branch)
        if head_bytes is None:
            return None
        commit_hash = _from_bytes(head_bytes)
        keyset_bytes = self.store.get(COMMIT_KEYSET % commit_hash)
        if keyset_bytes is None:
            return None
        keyset = _from_bytes(keyset_bytes)
        content_hash = keyset.get(key)
        if content_hash is None:
            return None
        return self.store.get(content_hash)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return False
        self.store.set(BRANCH_HEAD % self._branch, _to_bytes(commit_hash))
        self._load_commit(commit_hash, update_base=True)
        return True

    # -- History --

    def history(
        self,
        commit_hash: str | None = None,
        *,
        all_parents: bool = False,
    ) -> Iterable[str]:
        """Yield the commit chain from newest to oldest.

        Args:
            commit_hash: Starting commit (default: current).
            all_parents: If True, BFS over all parents (full DAG).
                If False, follow first parent only (linear).
        """
        start = commit_hash or self._current_commit
        if not all_parents:
            current = start
            while current is not None:
                yield current
                parents = self._load_parents(current)
                current = parents[0] if parents else None
        else:
            visited: set[str] = set()
            queue: deque[str] = deque([start])
            while queue:
                current = queue.popleft()
                if current in visited:
                    continue
                visited.add(current)
                yield current
                for p in self._load_parents(current):
                    if p not in visited:
                        queue.append(p)

    @property
    def initial_commit(self) -> str:
        """The root commit hash."""
        commits = list(self.history())
        return commits[-1]

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
        return Versioned.branches(self.store)

    def commit_info(self, commit_hash: str | None = None) -> dict | None:
        """Retrieve the info dict for a commit, or None if none was stored."""
        target = commit_hash or self._current_commit
        info_bytes = self.store.get(INFO_KEY % target)
        if info_bytes is None:
            return None
        return _from_bytes(info_bytes)

    def diff(self, commit_a: str, commit_b: str) -> DiffResult:
        """Compute key-level differences between two commits.

        Returns which keys were added, removed, or modified going
        from commit_a to commit_b.
        """
        keyset_a = self._load_keyset(commit_a)
        keyset_b = self._load_keyset(commit_b)

        keys_a = set(keyset_a.keys())
        keys_b = set(keyset_b.keys())

        added = keys_b - keys_a
        removed = keys_a - keys_b
        common = keys_a & keys_b
        modified = frozenset(k for k in common if keyset_a[k] != keyset_b[k])

        return DiffResult(
            added=frozenset(added),
            removed=frozenset(removed),
            modified=modified,
        )

    def parents(self, commit_hash: str | None = None) -> tuple[str, ...]:
        """Get the direct parent commit(s) of a commit.

        Args:
            commit_hash: The commit to query (default: current).

        Returns:
            Tuple of parent commit hashes (empty for the initial commit,
            one element for normal commits, two for merge commits).
        """
        target = commit_hash or self._current_commit
        return self._load_parents(target)

    # -- Internal --

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits.

        Uses interleaved BFS from both commits. Returns the first
        commit reachable from both, or None if no common ancestor.
        """
        if commit_a == commit_b:
            return commit_a

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

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        """Load just the keyset for a commit (key -> versioned_key mapping)."""
        keyset_bytes = self.store.get(COMMIT_KEYSET % commit_hash)
        if keyset_bytes is None:
            return {}
        return _from_bytes(keyset_bytes)

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""
        parent_bytes = self.store.get(PARENT_COMMIT % commit_hash)
        if parent_bytes is None:
            return ()
        raw = _from_bytes(parent_bytes)
        if raw is None:
            return ()
        if isinstance(raw, str):
            return (raw,)
        return tuple(raw)

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
        self._commit_keys = _from_bytes(keyset_bytes) if keyset_bytes else {}

        meta_bytes = self.store.get(META_KEY % commit_hash)
        if meta_bytes is not None:
            try:
                self._meta = _meta_from_bytes(meta_bytes)
            except Exception:
                self._meta = {}
        else:
            self._meta = {}

        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )
