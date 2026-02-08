"""Versioned state: a commit log over a KV store."""

import hashlib
import pickle
import time
from dataclasses import dataclass
from typing import Iterable

from .errors import ConcurrencyError
from .kv.base import KVStore
from .kv.memory import Memory

PARENT_COMMIT = "__parent_commit__%s"
COMMIT_KEYSET = "__commit_keyset__%s"
HEAD_COMMIT = "__head_commit__"
META_KEY = "__meta__%s"
TOTAL_VAR_SIZE_KEY = "__total_var_size__%s"


@dataclass
class MetaEntry:
    """Metadata for a single key in versioned state."""

    last_touch: int
    size: int | None
    created_at: float


def _content_hash(
    parent: str | None,
    keyset: dict[str, str],
    updates: dict[str, bytes],
) -> str:
    """Compute a content-addressable commit hash.

    Hashes the parent pointer, keyset, and update blob digests
    to produce a deterministic 16-hex-char commit hash.
    """
    h = hashlib.sha256()
    h.update(pickle.dumps(parent))
    h.update(pickle.dumps(sorted(keyset.items())))
    for key in sorted(updates):
        h.update(key.encode())
        h.update(updates[key])
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
    ) -> None:
        if store is None:
            store = Memory()
        self.store = store

        if commit_hash is None:
            head_bytes = store.get(HEAD_COMMIT)
            if head_bytes is not None:
                commit_hash = pickle.loads(head_bytes)
            else:
                # Create initial empty commit
                commit_hash = _content_hash(None, {}, {})
                initial = {
                    COMMIT_KEYSET % commit_hash: pickle.dumps({}),
                    PARENT_COMMIT % commit_hash: pickle.dumps(None),
                    HEAD_COMMIT: pickle.dumps(commit_hash),
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

    @property
    def current_commit(self) -> str:
        return self._current_commit

    @property
    def base_commit(self) -> str:
        return self._base_commit

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        head_bytes = self.store.get(HEAD_COMMIT)
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

    # -- Write operations --

    def snapshot(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
    ) -> str:
        """Create a new commit with the given changes.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.

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
        new_hash = _content_hash(self._current_commit, preview_keys, updates)

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
        diffs[PARENT_COMMIT % new_hash] = pickle.dumps(self._current_commit)
        diffs[META_KEY % new_hash] = pickle.dumps(new_meta)
        total_size = sum(
            e.size for e in new_meta.values() if e.size is not None
        )
        diffs[TOTAL_VAR_SIZE_KEY % new_hash] = pickle.dumps(total_size)

        # Write everything
        self.store.set_many(**diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    # -- Branching / concurrency --

    def merge(self, on_conflict: str = "raise") -> bool:
        """Atomically update HEAD to this branch's tip via CAS.

        Args:
            on_conflict: 'raise' (default) or 'abandon'.

        Returns:
            True if HEAD was updated. False if on_conflict='abandon'
            and HEAD had diverged.

        Raises:
            ConcurrencyError: If on_conflict='raise' and HEAD diverged.
        """
        if self._current_commit == self._base_commit:
            return True

        expected = pickle.dumps(self._base_commit)
        new_head = pickle.dumps(self._current_commit)

        if self.store.cas(HEAD_COMMIT, new_head, expected=expected):
            self._base_commit = self._current_commit
            return True

        if on_conflict == "abandon":
            return False

        raise ConcurrencyError(
            f"HEAD changed from {self._base_commit}. Reset and retry."
        )

    def reset(self) -> None:
        """Abandon local branch and reload from HEAD."""
        head_bytes = self.store.get(HEAD_COMMIT)
        if head_bytes is None:
            raise ValueError("No HEAD commit found in store")
        self._load_commit(pickle.loads(head_bytes), update_base=True)

    def checkout(self, commit_hash: str) -> "Versioned | None":
        """Return a new Versioned at a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return None
        return Versioned(self.store, commit_hash=commit_hash)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_KEYSET % commit_hash) is None:
            return False
        self.store.set(HEAD_COMMIT, pickle.dumps(commit_hash))
        self._load_commit(commit_hash, update_base=True)
        return True

    # -- History --

    def history(self, commit_hash: str | None = None) -> Iterable[str]:
        """Yield the commit chain from newest to oldest."""
        current = commit_hash or self._current_commit
        while current is not None:
            yield current
            parent_bytes = self.store.get(PARENT_COMMIT % current)
            if parent_bytes is not None:
                current = pickle.loads(parent_bytes)
            else:
                current = None

    @property
    def initial_commit(self) -> str:
        """The root commit hash."""
        commits = list(self.history())
        return commits[-1]

    # -- Internal --

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
