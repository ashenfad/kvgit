"""A worktree's engine: the commit its branch is checked out at, read
through the core, and the commits, merges and resets that move it."""

import time
from typing import Any

from . import _core
from ._drive import drive
from ._store import (
    BRANCH_HEAD,
    CHUNK_PREFIX,
    EMPTY_HASH,
    CorruptHeadRecoverer,
    _call,
    _on_tree,
    _reject_reserved_branch,
    _resolve_head,
    blob_key,
)
from ._types import MergeChoice, MergePolicy, MergeResult, PostCheck
from .errors import CorruptHeadError, UnknownBranchError
from .kv.base import KVStore

# How many commits' roots and generations an engine remembers before it
# starts over. Neither ever changes once its commit exists, so the
# bound is on memory alone.
_KNOWN_LIMIT = 10_000


class Engine:
    """The state a worktree reads and the operations that move it.

    It holds the keyset root of the commit its branch is checked out
    at (its head), and the key index the core builds on the first
    listing. Commits, merges and applies run in the core and move it to
    what they land; a refresh or reset moves it to another commit.
    """

    def __init__(
        self,
        store: KVStore,
        branch: str,
        *,
        recover: CorruptHeadRecoverer | None = None,
    ) -> None:
        # A name the branch API refuses never reaches the store: the
        # empty name, or one under the tag namespace.
        _reject_reserved_branch(branch)
        self.store = store
        self.branch = branch
        self._recover = recover
        # Whether the store is known stamped for content-keyed blobs.
        # Stamps only rise, so once seen it is never read again.
        self._stamped = False
        self._roots: dict[str, str] = {}
        self._generations: dict[str, int] = {}
        self._tree = _core.WorktreeState(EMPTY_HASH)
        self.head = self._resolve()
        self._move_to(self.head)

    def _resolve(self) -> str:
        """The commit the branch's head resolves to.

        Raises:
            UnknownBranchError: if the branch has no head.
            CorruptHeadError: if its head is damaged beyond recovery.
        """
        found = _resolve_head(
            self.store, self.branch, recover_from_corrupt_head=self._recover
        )
        if found is not None:
            return found
        if self.store.get(BRANCH_HEAD % self.branch) is not None:
            raise CorruptHeadError(
                f"Branch '{self.branch}' HEAD is corrupt and unrecoverable"
            )
        raise UnknownBranchError(f"Branch '{self.branch}' does not exist")

    def _move_to(self, commit: str) -> None:
        """Move to a commit's keyset root, reading nothing of the tree
        under it; a key index already built follows the move. The read
        of the root brings the commit's records with it, so the engine
        knows the generation its next commit builds on."""
        found = self._on_tree("wt_checkout", commit=commit)
        if found["root"] is not None:
            self._remember({commit: found["root"]}, {})
        if found["generation"] is not None:
            self._remember({}, {commit: found["generation"]})
        self.head = commit

    def _on_tree(self, op: str, **args: Any) -> Any:
        return _on_tree(self._tree, op, self.store, self._recover, **args)

    def _remember(self, roots: dict[str, str], generations: dict[str, int]) -> None:
        if len(self._roots) + len(roots) > _KNOWN_LIMIT:
            self._roots.clear()
        if len(self._generations) + len(generations) > _KNOWN_LIMIT:
            self._generations.clear()
        self._roots.update(roots)
        self._generations.update(generations)

    # -- Reads at the head --

    def get_many(self, keys: list[str]) -> dict[str, bytes]:
        """The stored bytes of each of ``keys`` the head holds."""
        # Keys holding equal bytes share one blob, so a blob answers for
        # every key that points at it.
        keys_by_blob: dict[str, list[str]] = {}
        for key, (blob, _size, _chunks) in self._on_tree(
            "wt_lookup", keys=list(dict.fromkeys(keys))
        ):
            keys_by_blob.setdefault(blob, []).append(key)
        if not keys_by_blob:
            return {}
        raw = self.store.get_many(keys_by_blob.keys())
        return {key: value for blob, value in raw.items() for key in keys_by_blob[blob]}

    def keys(self) -> list[str]:
        """Every key at the head. The first call builds the key index
        with one walk of the tree; later ones read it."""
        return self._on_tree("wt_keys")

    def __contains__(self, key: str) -> bool:
        return self._on_tree("wt_contains", key=key)

    # -- Moving the branch --

    def commit(
        self,
        updates: dict[str, bytes],
        removals: set[str],
        *,
        on_conflict: str,
        on_moved: str,
        info: dict | None,
        chunks: dict[str, bytes],
        chunk_refs: dict[str, list[str]],
        merge_fns: dict[str, MergePolicy] | None,
        merge_prefixes: dict[str, MergePolicy] | None,
        default_merge: MergePolicy | None,
    ) -> MergeResult:
        """Commit ``updates`` and ``removals`` on the head, fast-forward
        or merged onto a branch that moved; see :meth:`Worktree.commit`."""
        if on_moved not in ("merge", "raise"):
            raise ValueError(f"on_moved must be 'merge' or 'raise', got {on_moved!r}")
        if (updates or removals or info is not None) and on_conflict not in (
            "raise",
            "abandon",
        ):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )
        # The bytes the commit writes stay here; the core names them by
        # key. Every blob and chunk is written, even one already stored,
        # so the batch holds everything its commit needs whatever a sweep
        # took before it landed.
        held: dict[str, bytes] = {}
        entries = []
        for key, value in updates.items():
            pointer = blob_key(value)
            held[pointer] = value
            refs = chunk_refs.get(key)
            meta = _core.MetaEntry(size=len(value), chunks=list(refs) if refs else None)
            entries.append((key, _core.KeysetEntry(blob=pointer, meta=meta)))
        chunk_keys = []
        for chunk_hash, chunk_bytes in chunks.items():
            held[CHUNK_PREFIX + chunk_hash] = chunk_bytes
            chunk_keys.append(CHUNK_PREFIX + chunk_hash)
        hosted: list[MergePolicy] = []
        op = _core.Op.commit(
            self.branch,
            self.head,
            self.head,
            entries,
            sorted(removals),
            chunk_keys,
            info,
            on_moved,
            on_conflict,
            *_lowered(hosted, merge_fns, merge_prefixes, default_merge),
            self._recover is not None,
            self._stamped,
            *self._known(self.head),
        )
        return self._land(op, held, hosted)

    def merge(
        self,
        theirs: str,
        *,
        base: str | None,
        fast_forward: bool,
        on_conflict: str,
        info: dict | None,
        post_check: PostCheck | None,
        merge_fns: dict[str, MergePolicy] | None,
        merge_prefixes: dict[str, MergePolicy] | None,
        default_merge: MergePolicy | None,
    ) -> MergeResult:
        """Merge ``theirs`` into the branch, or with ``base``, apply the
        change from ``base`` to ``theirs``; see :meth:`Worktree.merge`
        and :meth:`Worktree.apply`."""
        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )
        hosted: list[MergePolicy] = []
        op = _core.Op.merge(
            self.branch,
            self.head,
            theirs,
            base,
            fast_forward,
            info,
            on_conflict,
            *_lowered(hosted, merge_fns, merge_prefixes, default_merge),
            post_check is not None,
            self._recover is not None,
            self._stamped,
            *self._known(self.head),
        )
        return self._land(op, {}, hosted, post_check)

    def refresh(self) -> None:
        """Move to the branch's head as the store holds it now.

        Raises:
            UnknownBranchError: if the branch has no head.
            CorruptHeadError: if its head is damaged beyond recovery.
        """
        self._move_to(self._resolve())

    def reset_to(self, commit: str) -> bool:
        """Move the branch to ``commit``, the head it held becoming the
        backup in the same write; False if the store lacks ``commit``.

        Raises:
            UnknownBranchError: if the branch has no head.
        """
        if not _call("reset_branch", self.store, name=self.branch, target=commit):
            return False
        self._move_to(commit)
        return True

    def _known(
        self, commit: str
    ) -> tuple[list[tuple[str, str]], list[tuple[str, int]]]:
        """The root and generation the engine knows of ``commit``."""
        root = self._roots.get(commit)
        generation = self._generations.get(commit)
        return (
            [] if root is None else [(commit, root)],
            [] if generation is None else [(commit, generation)],
        )

    def _land(
        self,
        op: Any,
        held: dict[str, bytes],
        hosted: list[MergePolicy],
        post_check: PostCheck | None = None,
    ) -> MergeResult:
        """Run a commit or merge, and move to what it landed."""
        op.use_tree(self._tree)
        done = drive(
            op,
            self.store,
            held=held,
            merge_fns=hosted,
            post_check=post_check,
            recoverer=self._recover,
            clock=time.time,
            sleep=time.sleep,
        )
        result = MergeResult(
            merged=done["merged"],
            commit=done["commit"],
            strategy=done["strategy"],
            auto_merged_keys=done["auto_merged_keys"],
            carried_keys=done["carried_keys"],
        )
        if not result.merged or result.strategy == "no_op":
            return result
        # Landed: the core moved the state with it.
        self._remember(dict(done["roots"]), dict(done["generations"]))
        self._stamped = done["stamped"]
        self.head = result.commit
        return result


def _lowered(
    hosted: list[MergePolicy],
    merge_fns: dict[str, MergePolicy] | None,
    merge_prefixes: dict[str, MergePolicy] | None,
    default_merge: MergePolicy | None,
) -> tuple[dict[str, str | int], list[tuple[str, str | int]], str | int | None]:
    """The merge rules as the core names them: a side, or the number of
    a function in ``hosted``, which gains each function named."""

    def lower(policy: MergePolicy) -> str | int:
        if isinstance(policy, MergeChoice):
            return policy.value
        hosted.append(policy)
        return len(hosted) - 1

    return (
        {k: lower(p) for k, p in (merge_fns or {}).items()},
        [(k, lower(p)) for k, p in (merge_prefixes or {}).items()],
        lower(default_merge) if default_merge is not None else None,
    )
