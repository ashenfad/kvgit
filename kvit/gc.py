"""GCVersioned: Versioned state with automatic garbage collection."""

import json
import time
from dataclasses import dataclass
from typing import Callable

from .errors import ConcurrencyError
from .kv.base import KVStore
from .versioned import (
    BRANCH_HEAD,
    COMMIT_KEYSET,
    INFO_KEY,
    META_KEY,
    PARENT_COMMIT,
    TOTAL_VAR_SIZE_KEY,
    MergeResult,
    MetaEntry,
    Versioned,
    _from_bytes,
    _meta_from_bytes,
    _meta_to_bytes,
    _to_bytes,
)


def _is_system_key(key: str) -> bool:
    """Check if a key is a system/protected key (starts with ``__``).

    Handles both direct keys (``"__foo__"``) and namespaced keys
    (``"ns/__foo__"``) by extracting the base key name.

    This is the default ``is_protected`` policy for ``GCVersioned``.
    """
    base_key = key.split("/")[-1] if "/" in key else key
    return base_key.startswith("__")


@dataclass(frozen=True)
class RebaseResult:
    """Result of a rebase/GC operation."""

    performed: bool
    new_commit: str | None
    dropped_keys: tuple[str, ...]
    kept_keys: tuple[str, ...]
    total_size_before: int
    total_size_after: int
    orphans_cleaned: int = 0


class GCVersioned(Versioned):
    """Versioned state with built-in garbage collection via rebase.

    Rebase strategy (high/low water):
    - Track total persisted user-var size from commit metadata.
    - If total <= high_water_bytes: no-op.
    - If total > high_water_bytes: drop coldest user keys (oldest touch,
      then largest) until total <= low_water_bytes (default 80% of high).
    - Protected keys (as determined by ``is_protected``) are always retained.
    - Write a fresh root commit with only retained keys, then delete
      dropped blobs and orphaned commits.

    Every ``commit()`` auto-runs the high/low check.
    """

    def __init__(
        self,
        store: KVStore | None = None,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
        high_water_bytes: int,
        low_water_bytes: int | None = None,
        is_protected: Callable[[str], bool] = _is_system_key,
    ) -> None:
        super().__init__(store, commit_hash=commit_hash, branch=branch)
        if high_water_bytes <= 0:
            raise ValueError("high_water_bytes must be > 0")
        self.high_water = high_water_bytes
        self.low_water = (
            low_water_bytes
            if low_water_bytes is not None
            else int(high_water_bytes * 0.8)
        )
        if self.low_water <= 0 or self.low_water > self.high_water:
            self.low_water = int(high_water_bytes * 0.8)
        self._is_protected = is_protected
        self.last_rebase_result: RebaseResult | None = None

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        merge_fns=None,
        default_merge=None,
        info: dict | None = None,
    ) -> "MergeResult":
        """Commit changes, then run GC if above high water mark."""

        result = super().commit(
            updates,
            removals,
            on_conflict=on_conflict,
            merge_fns=merge_fns,
            default_merge=default_merge,
            info=info,
        )
        if result.merged:
            rebase_result = self.maybe_rebase()
            self.last_rebase_result = rebase_result
        return result

    def maybe_rebase(self) -> RebaseResult:
        """Run rebase only if total size exceeds high water mark."""
        total = self._load_total_size()
        if total <= self.high_water:
            return RebaseResult(
                performed=False,
                new_commit=None,
                dropped_keys=(),
                kept_keys=tuple(self._commit_keys.keys()),
                total_size_before=total,
                total_size_after=total,
            )
        return self.rebase()

    def rebase(
        self,
        keep_keys: set[str] | None = None,
        *,
        info: dict | None = None,
    ) -> RebaseResult:
        """Rebase: create a fresh root commit, dropping cold keys.

        Args:
            keep_keys: If provided, retain exactly these keys (plus protected
                keys). Otherwise, use the high/low water strategy.
            info: Optional metadata for the rebase commit.
        """
        meta = self._meta
        total_before = self._load_total_size(
            default=sum(e.size or 0 for e in meta.values())
        )

        # Identify protected and user keys
        protected_keys = {
            k: v for k, v in self._commit_keys.items() if self._is_protected(k)
        }
        user_meta = {k: v for k, v in meta.items() if not self._is_protected(k)}

        retained_keys = set(protected_keys.keys()) | set(user_meta.keys())
        total = sum(e.size or 0 for e in user_meta.values())
        dropped: list[str] = []

        if keep_keys is not None:
            # Explicit keep set — drop everything not in it (except protected keys)
            for key in list(retained_keys):
                if self._is_protected(key):
                    continue
                if key not in keep_keys:
                    retained_keys.discard(key)
                    dropped.append(key)
                    total -= (user_meta.get(key) and user_meta[key].size) or 0
        else:
            # High/low water strategy: drop coldest until under low water
            candidates: list[tuple[str, MetaEntry]] = sorted(
                user_meta.items(),
                key=lambda kv: (kv[1].last_touch, -(kv[1].size or 0)),
            )
            for key, entry in candidates:
                if total <= self.low_water:
                    break
                retained_keys.discard(key)
                dropped.append(key)
                total -= entry.size or 0

        # Build new commit with retained keys
        from .versioned import _content_hash

        # Collect retained data
        new_commit_keys: dict[str, str] = {}
        new_meta: dict[str, MetaEntry] = {}
        retained_data: dict[str, bytes] = {}

        for key in retained_keys:
            versioned_key = self._commit_keys.get(key)
            if not versioned_key:
                continue
            value = self.store.get(versioned_key)
            if value is None:
                continue
            if not self._is_protected(key):
                retained_data[key] = value
                if key in meta:
                    new_meta[key] = meta[key]

        # Content-addressable hash for the rebase commit (parent=None, fresh root)
        preview_keys: dict[str, str] = {}
        for key in protected_keys:
            preview_keys[key] = protected_keys[key]
        for key in retained_data:
            preview_keys[key] = f"<pending:{key}>"
        new_hash = _content_hash((), preview_keys, retained_data, info=info)

        # Build the write batch
        diffs: dict[str, bytes] = {}

        # Protected keys — copy blobs with new versioned keys
        for key, old_vk in protected_keys.items():
            value = self.store.get(old_vk)
            if value is None:
                continue
            new_vk = f"{new_hash}:{key}"
            new_commit_keys[key] = new_vk
            diffs[new_vk] = value

        # Retained user keys
        for key, value in retained_data.items():
            new_vk = f"{new_hash}:{key}"
            new_commit_keys[key] = new_vk
            diffs[new_vk] = value

        # Commit metadata
        diffs[COMMIT_KEYSET % new_hash] = _to_bytes(new_commit_keys)
        diffs[PARENT_COMMIT % new_hash] = _to_bytes([])
        diffs[META_KEY % new_hash] = _meta_to_bytes(new_meta)
        total_after = sum(e.size or 0 for e in new_meta.values())
        diffs[TOTAL_VAR_SIZE_KEY % new_hash] = _to_bytes(total_after)
        if info is not None:
            diffs[INFO_KEY % new_hash] = _to_bytes(info)

        self.store.set_many(**diffs)

        # CAS HEAD to the new rebase commit
        branch_key = BRANCH_HEAD % self._branch
        expected = _to_bytes(self._base_commit)
        if not self.store.cas(
            branch_key, _to_bytes(new_hash), expected=expected
        ):
            raise ConcurrencyError("HEAD changed during rebase.")

        # Delete dropped blobs
        to_delete = []
        for key in dropped:
            vk = self._commit_keys.get(key)
            if vk:
                to_delete.append(vk)
        if to_delete:
            self.store.remove_many(*to_delete)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._base_commit = new_hash
        self._meta = new_meta

        # Clean orphaned commits
        orphans_cleaned = self.clean_orphans()

        return RebaseResult(
            performed=True,
            new_commit=new_hash,
            dropped_keys=tuple(dropped),
            kept_keys=tuple(retained_keys),
            total_size_before=total_before,
            total_size_after=total_after,
            orphans_cleaned=orphans_cleaned,
        )

    def clean_orphans(self, min_age: float = 3600) -> int:
        """Remove orphaned commits unreachable from HEAD.

        Args:
            min_age: Only delete orphans older than this many seconds
                (default 1 hour).

        Returns:
            Number of orphaned commits cleaned.
        """
        # Mark phase: find all reachable commits across ALL branches
        reachable: set[str] = set()
        prefix = BRANCH_HEAD.replace("%s", "")
        for key in self.store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                head_bytes = self.store.get(key)
                if head_bytes is None:
                    continue
                branch_head = _from_bytes(head_bytes)
                for commit in self.history(
                    commit_hash=branch_head, all_parents=True
                ):
                    reachable.add(commit)

        # Sweep phase: find orphaned commits by scanning for meta keys
        meta_prefix = META_KEY.replace("%s", "")
        cutoff_time = time.time() - min_age
        orphans: list[str] = []

        for key in self.store.keys():
            if not isinstance(key, str) or not key.startswith(meta_prefix):
                continue
            commit_hash = key[len(meta_prefix):]
            if not commit_hash or commit_hash in reachable:
                continue
            # Check age
            meta_bytes = self.store.get(key)
            if meta_bytes is None:
                continue
            try:
                meta = _meta_from_bytes(meta_bytes)
                if meta:
                    first_entry = next(iter(meta.values()), None)
                    if (
                        first_entry
                        and first_entry.created_at < cutoff_time
                    ):
                        orphans.append(commit_hash)
            except (json.JSONDecodeError, TypeError, KeyError):
                continue

        # Delete orphaned commits and their data
        for orphan_hash in orphans:
            keyset_bytes = self.store.get(COMMIT_KEYSET % orphan_hash)
            if keyset_bytes:
                try:
                    keyset = _from_bytes(keyset_bytes)
                    blob_keys = list(keyset.values())
                    if blob_keys:
                        self.store.remove_many(*blob_keys)
                except Exception:
                    pass
            self.store.remove_many(
                META_KEY % orphan_hash,
                COMMIT_KEYSET % orphan_hash,
                PARENT_COMMIT % orphan_hash,
                TOTAL_VAR_SIZE_KEY % orphan_hash,
                INFO_KEY % orphan_hash,
            )

        return len(orphans)

    def _load_total_size(self, default: int = 0) -> int:
        """Load the total variable size for the current commit."""
        total_bytes = self.store.get(
            TOTAL_VAR_SIZE_KEY % self._current_commit
        )
        if total_bytes is None:
            return default
        try:
            return _from_bytes(total_bytes)
        except Exception:
            return default
