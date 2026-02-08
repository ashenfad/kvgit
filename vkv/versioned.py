"""Versioned state: a commit log over a KV store."""

import hashlib
import pickle
import time
from dataclasses import dataclass
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


MergeFn = Callable[
    [bytes | None, bytes | None, bytes | None], bytes
]
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
    h.update(pickle.dumps(parents))
    h.update(pickle.dumps(sorted(keyset.items())))
    for key in sorted(updates):
        h.update(key.encode())
        h.update(updates[key])
    if info is not None:
        h.update(pickle.dumps(sorted(info.items())))
    return h.hexdigest()[:16]


class Versioned:
    """A commit log over a KV store.

    The caller owns the working state. Versioned provides:
    - ``get()`` / ``get_many()`` to read from the current commit
    - ``snapshot()`` to commit a batch of changes (bytes-only)
    - ``merge()`` / ``reset()`` for CAS-based concurrency
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
                commit_hash = pickle.loads(head_bytes)
            else:
                # Create initial empty commit
                commit_hash = _content_hash((), {}, {})
                initial = {
                    COMMIT_KEYSET % commit_hash: pickle.dumps({}),
                    PARENT_COMMIT % commit_hash: pickle.dumps(()),
                    BRANCH_HEAD % branch: pickle.dumps(commit_hash),
                    META_KEY % commit_hash: pickle.dumps({}),
                    TOTAL_VAR_SIZE_KEY % commit_hash: pickle.dumps(0),
                }
                store.set_many(**initial)

        self._current_commit = commit_hash
        self._base_commit = commit_hash

        # Load commit keyset
        self._commit_keys: dict[str, str] = {}
        keyset_bytes = self.store.get(COMMIT_KEYSET % self._current_commit)
        if keyset_bytes is not None:
            self._commit_keys = pickle.loads(keyset_bytes)

        # Load metadata for GC
        self._meta: dict[str, MetaEntry] = {}
        meta_bytes = self.store.get(META_KEY % self._current_commit)
        if meta_bytes is not None:
            try:
                self._meta = pickle.loads(meta_bytes)
            except Exception:
                self._meta = {}
        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )

        # Merge function registry
        self._merge_fns: dict[str, MergeFn] = {}
        self._default_merge: MergeFn | None = None
        self.last_merge_result: MergeResult | None = None

    @property
    def current_commit(self) -> str:
        return self._current_commit

    @property
    def base_commit(self) -> str:
        return self._base_commit

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is not None:
            return pickle.loads(head_bytes)
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
        result: dict[str, bytes] = {}
        for key in keys:
            value = self.get(key)
            if value is not None:
                result[key] = value
        return result

    def keys(self) -> Iterable[str]:
        """All keys in the current commit."""
        return self._commit_keys.keys()

    def __contains__(self, key: str) -> bool:
        return key in self._commit_keys

    # -- Merge function registry --

    def set_merge_fn(self, key: str, fn: MergeFn) -> None:
        """Register a merge function for a specific key."""
        self._merge_fns[key] = fn

    def set_default_merge(self, fn: MergeFn) -> None:
        """Register a default merge function for unregistered keys."""
        self._default_merge = fn

    def set_content_type(self, key: str, ct) -> None:
        """Register a ContentType for a key (sets its merge function).

        Args:
            ct: A ContentType instance (from vkv.content_types).
        """
        self.set_merge_fn(key, ct.as_merge_fn())

    # -- Write operations --

    def snapshot(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
    ) -> str:
        """Create a new commit with the given changes.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.
            info: Optional metadata dict (e.g. author, message).
                Included in content hash.

        Returns:
            The new commit hash.
        """
        if not updates and not removals:
            return self._current_commit

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
        diffs[COMMIT_KEYSET % new_hash] = pickle.dumps(new_commit_keys)
        diffs[PARENT_COMMIT % new_hash] = pickle.dumps((self._current_commit,))
        diffs[META_KEY % new_hash] = pickle.dumps(new_meta)
        total_size = sum(
            e.size for e in new_meta.values() if e.size is not None
        )
        diffs[TOTAL_VAR_SIZE_KEY % new_hash] = pickle.dumps(total_size)
        if info is not None:
            diffs[INFO_KEY % new_hash] = pickle.dumps(info)

        # Write everything
        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    # -- Branching / concurrency --

    def merge(
        self,
        on_conflict: str = "raise",
        *,
        merge_fns: dict[str, MergeFn] | None = None,
        default_merge: MergeFn | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Atomically update HEAD to this branch's tip.

        If HEAD has diverged, attempts a three-way merge using the
        lowest common ancestor.

        Args:
            on_conflict: 'raise' (default) or 'abandon' for CAS failures.
            merge_fns: Per-key merge functions (override instance-level).
            default_merge: Default merge function (override instance-level).
            info: Optional info dict for the merge commit.

        Returns:
            A MergeResult (truthy when merged, falsy otherwise).

        Raises:
            ConcurrencyError: If on_conflict='raise' and CAS fails.
            MergeConflict: If keys conflict and no merge function
                resolves them.
        """
        branch_key = BRANCH_HEAD % self._branch

        # Case 1: No local changes
        if self._current_commit == self._base_commit:
            result = MergeResult(
                merged=True,
                commit=self._current_commit,
                strategy="no_op",
                auto_merged_keys=(),
                carried_keys=(),
            )
            self.last_merge_result = result
            return result

        current_head = self.latest_head

        # Case 2: Fast-forward (HEAD hasn't moved since we branched)
        if current_head == self._base_commit:
            expected = pickle.dumps(self._base_commit)
            new_head = pickle.dumps(self._current_commit)
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
                f"HEAD changed from {self._base_commit}. Reset and retry."
            )

        # Case 3: Three-way merge (HEAD has diverged)
        return self._three_way_merge(
            current_head,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            default_merge=default_merge,
            info=info,
        )

    def _three_way_merge(
        self,
        their_head: str,
        *,
        on_conflict: str,
        merge_fns: dict[str, MergeFn] | None,
        default_merge: MergeFn | None,
        info: dict | None,
    ) -> MergeResult:
        """Perform a three-way merge between our branch and their HEAD."""
        branch_key = BRANCH_HEAD % self._branch
        lca = self._find_lca(self._current_commit, their_head)
        if lca is None:
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
        their_changed = (
            their_diff.added | their_diff.removed | their_diff.modified
        )
        all_changed = our_changed | their_changed

        merged_keyset: dict[str, str] = {}
        merged_values: dict[str, bytes] = {}
        auto_merged: list[str] = []
        conflicts: set[str] = set()

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

            old_val = (
                self.store.get(lca_keyset[key])
                if key in lca_keyset
                else None
            )
            our_val = (
                None
                if our_removed
                else self.store.get(our_keyset[key])
            )
            their_val = (
                None
                if their_removed
                else self.store.get(their_keyset[key])
            )
            try:
                result_val = fn(old_val, our_val, their_val)
                merged_values[key] = result_val
                auto_merged.append(key)
            except Exception:
                conflicts.add(key)

        if conflicts:
            raise MergeConflict(conflicts)

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
        our_meta = pickle.loads(our_meta_bytes) if our_meta_bytes else {}
        their_meta = pickle.loads(their_meta_bytes) if their_meta_bytes else {}

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

        diffs[COMMIT_KEYSET % merge_hash] = pickle.dumps(merged_keyset)
        diffs[PARENT_COMMIT % merge_hash] = pickle.dumps(parents)
        diffs[META_KEY % merge_hash] = pickle.dumps(merged_meta)
        total_size = sum(
            e.size for e in merged_meta.values() if e.size is not None
        )
        diffs[TOTAL_VAR_SIZE_KEY % merge_hash] = pickle.dumps(total_size)
        if info is not None:
            diffs[INFO_KEY % merge_hash] = pickle.dumps(info)

        self.store.set_many(**diffs)

        # CAS HEAD from their_head to merge_hash
        expected = pickle.dumps(their_head)
        new_head_bytes = pickle.dumps(merge_hash)
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
            "HEAD changed during three-way merge. Reset and retry."
        )

    def reset(self) -> None:
        """Abandon local branch and reload from HEAD."""
        head_bytes = self.store.get(BRANCH_HEAD % self._branch)
        if head_bytes is None:
            raise ValueError(
                "No HEAD commit found for branch %s" % self._branch
            )
        self._load_commit(pickle.loads(head_bytes), update_base=True)

    def checkout(self, commit_hash: str) -> "Versioned | None":
        """Return a new Versioned at a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return None
        return Versioned(
            self.store, commit_hash=commit_hash, branch=self._branch
        )

    def create_branch(self, name: str) -> "Versioned":
        """Fork the current commit onto a new branch.

        Returns a new Versioned instance on the new branch, pointing
        at the same commit as self.

        Raises ValueError if the branch already exists.
        """
        branch_key = BRANCH_HEAD % name
        if self.store.get(branch_key) is not None:
            raise ValueError(f"Branch '{name}' already exists")
        self.store.set(branch_key, pickle.dumps(self._current_commit))
        return Versioned(
            self.store, commit_hash=self._current_commit, branch=name
        )

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return False
        self.store.set(BRANCH_HEAD % self._branch, pickle.dumps(commit_hash))
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
                branch_name = key[len(prefix):]
                if branch_name:
                    result.append(branch_name)
        return sorted(result)

    def commit_info(self, commit_hash: str | None = None) -> dict | None:
        """Retrieve the info dict for a commit, or None if none was stored."""
        target = commit_hash or self._current_commit
        info_bytes = self.store.get(INFO_KEY % target)
        if info_bytes is None:
            return None
        return pickle.loads(info_bytes)

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
        modified = frozenset(
            k for k in common if keyset_a[k] != keyset_b[k]
        )

        return DiffResult(
            added=frozenset(added),
            removed=frozenset(removed),
            modified=modified,
        )

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
        return pickle.loads(keyset_bytes)

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""
        parent_bytes = self.store.get(PARENT_COMMIT % commit_hash)
        if parent_bytes is None:
            return ()
        raw = pickle.loads(parent_bytes)
        if raw is None:
            return ()
        if isinstance(raw, str):
            return (raw,)
        return tuple(raw)

    def _touch(self, key: str) -> None:
        """Update last_touch for a key (in-memory only, persisted on snapshot)."""
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
        self._commit_keys = pickle.loads(keyset_bytes) if keyset_bytes else {}

        meta_bytes = self.store.get(META_KEY % commit_hash)
        if meta_bytes is not None:
            try:
                self._meta = pickle.loads(meta_bytes)
            except Exception:
                self._meta = {}
        else:
            self._meta = {}

        self._touch_counter = (
            max((e.last_touch for e in self._meta.values()), default=0)
            if self._meta
            else 0
        )
