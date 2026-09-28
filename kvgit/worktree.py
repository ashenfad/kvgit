"""Worktree: a branch checked out for work."""

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .content_types import MergeFn
from .errors import UnknownCommitError
from .versioned.kv import ROOT_COMMIT, VersionedKV
from .versioned.protocol import BytesMergeFn, MergeChoice, MergeResult, PostCheck

if TYPE_CHECKING:
    from .repo import Repo

MergeRule = MergeFn | MergeChoice


@dataclass(frozen=True)
class Status:
    """A worktree's pending changes: keys written and keys deleted since
    its last commit. Falsy when there are none."""

    updated: frozenset[str]
    removed: frozenset[str]

    def __bool__(self) -> bool:
        return bool(self.updated or self.removed)


class Worktree(MutableMapping[str, Any]):
    """A branch checked out for work: a dict whose writes stay pending
    until :meth:`commit`.

    Bound to one branch for its whole life. Reads see pending changes on
    top of the commit the worktree is based on (:attr:`head`). Several
    worktrees — in one process or many — may hold the same branch; a
    commit that loses the race to another merges automatically, and
    raises only when the two changed the same key in ways no merge rule
    resolves.

    Obtain one from :meth:`Repo.worktree`, or :func:`kvgit.store` for
    the one-line case.
    """

    def __init__(self, repo: Repo, engine: VersionedKV) -> None:
        self._repo = repo
        self._engine = engine
        self._codec = repo._codec
        self._updates: dict[str, Any] = {}
        self._removals: set[str] = set()
        self._cache: dict[str, Any] = {}
        self._merge_fns: dict[str, MergeRule] = {}
        self._merge_prefixes: dict[str, MergeRule] = {}
        self._default_merge: MergeRule | None = None

    def __repr__(self) -> str:
        pending = len(self._updates) + len(self._removals)
        return (
            f"Worktree(branch={self.branch!r}, head={self.head[:8]}..., "
            f"pending={pending})"
        )

    # -- Identity --

    @property
    def repo(self) -> Repo:
        """The repository this worktree belongs to."""
        return self._repo

    @property
    def branch(self) -> str:
        """The branch this worktree is bound to."""
        return self._engine.current_branch

    @property
    def head(self) -> str:
        """The commit this worktree is based on.

        The branch itself may have moved on since, if another worktree
        committed to it; ``repo.head(wt.branch)`` reads the branch's tip.
        """
        return self._engine.current_commit

    # -- Reads --

    def get(self, key: str, default: Any = None) -> Any:
        """A value, pending changes included; ``default`` if absent."""
        if key in self._removals:
            return default
        if key in self._updates:
            return self._updates[key]
        if key in self._cache:
            return self._cache[key]
        raw = self._engine.get(key)
        if raw is None:
            return default
        value = self._codec.decode(raw)
        self._cache[key] = value
        return value

    def get_many(self, *keys: str) -> dict[str, Any]:
        """The values of several keys that exist, pending changes included."""
        result: dict[str, Any] = {}
        fetch: list[str] = []
        for key in keys:
            if key in self._removals:
                continue
            if key in self._updates:
                result[key] = self._updates[key]
            elif key in self._cache:
                result[key] = self._cache[key]
            else:
                fetch.append(key)
        if fetch:
            for key, raw in self._engine.get_many(*fetch).items():
                value = self._codec.decode(raw)
                self._cache[key] = value
                result[key] = value
        return result

    def keys(self) -> set[str]:  # type: ignore[override]
        """Every key visible here: committed, plus pending, minus deleted."""
        seen = {key for key in self._engine.keys() if key not in self._removals}
        seen.update(self._updates.keys())
        return seen

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str) or key in self._removals:
            return False
        if key in self._updates:
            return True
        return key in self._engine

    def __getitem__(self, key: str) -> Any:
        if key not in self:
            raise KeyError(key)
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        self._removals.discard(key)
        self._updates[key] = value

    def __delitem__(self, key: str) -> None:
        if key not in self:
            raise KeyError(key)
        self._updates.pop(key, None)
        # A key written only in pending changes has nothing committed to
        # remove: dropping the pending write is the whole deletion.
        if key in self._engine:
            self._removals.add(key)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return len(self.keys())

    def status(self) -> Status:
        """The changes pending since this worktree's last commit."""
        return Status(frozenset(self._updates), frozenset(self._removals))

    # -- Merge rules --

    def set_merge_fn(self, key: str, fn: MergeRule) -> None:
        """Register a merge function, or a ``MergeChoice``, for one key.

        This worktree's registrations sit over the repository's defaults
        and under the rules a single call passes.
        """
        self._merge_fns[key] = fn

    def set_merge_prefix(self, prefix: str, fn: MergeRule) -> None:
        """Register a merge function, or a ``MergeChoice``, under a prefix.

        Prefixes cover keys whose names are not known when the policy is
        set (``"runs/"`` for ``runs/<id>``). A key takes the most
        specific registration: its exact key, else the longest registered
        prefix it starts with, else the default. A merge function is
        consulted only where both sides changed a key; a ``MergeChoice``
        is a standing policy over the whole prefix, so ``OURS`` also
        drops a key the other side added and keeps one it removed.
        """
        self._merge_prefixes[prefix] = fn

    def set_default_merge(self, fn: MergeRule) -> None:
        """Register the merge function for keys no other rule covers."""
        self._default_merge = fn

    def _rules(
        self,
        merge_fns: dict[str, MergeRule] | None,
        merge_prefixes: dict[str, MergeRule] | None,
        default_merge: MergeRule | None,
    ) -> dict[str, Any]:
        """The effective rules, lowered to bytes: the repository's
        defaults, then this worktree's registrations, then the call's."""
        repo = self._repo
        fns = {**repo._merge_fns, **self._merge_fns, **(merge_fns or {})}
        prefixes = {
            **repo._merge_prefixes,
            **self._merge_prefixes,
            **(merge_prefixes or {}),
        }
        default = default_merge or self._default_merge or repo._default_merge
        return {
            "merge_fns": {k: self._lower(r) for k, r in fns.items()} or None,
            "merge_prefixes": {k: self._lower(r) for k, r in prefixes.items()} or None,
            "default_merge": self._lower(default) if default is not None else None,
        }

    def _lower(self, rule: MergeRule) -> BytesMergeFn | MergeChoice:
        """A merge rule, as the bytes-level merge takes it.

        A ``MergeChoice`` names a side rather than computing a value, so
        it goes down untouched. A merge function is wrapped: each side is
        decoded with the repository's codec, and the result encoded back
        with it.
        """
        if isinstance(rule, MergeChoice):
            return rule
        decode = self._codec.decode
        encode = self._codec.encode_merged

        def lowered(
            old: bytes | None, ours: bytes | None, theirs: bytes | None
        ) -> bytes | MergeChoice:
            result = rule(
                decode(old) if old is not None else None,
                decode(ours) if ours is not None else None,
                decode(theirs) if theirs is not None else None,
            )
            if isinstance(result, MergeChoice):
                return result
            return encode(result)

        return lowered

    # -- Commit --

    def commit(
        self,
        *,
        keys: set[str] | None = None,
        info: dict | None = None,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
    ) -> MergeResult:
        """Commit the pending changes to the branch.

        Fast-forwards when the branch has not moved since this worktree's
        head; otherwise three-way merges onto its tip with the effective
        merge rules. ``keys`` commits only those pending keys and leaves
        the rest pending. ``on_conflict="abandon"`` returns a falsy result
        instead of raising :class:`MergeConflict`.

        Raises:
            MergeConflict: keys both sides changed that no rule resolves.
            ConcurrencyError: the branch moved again mid-merge.
            UnknownBranchError: the branch was deleted.
        """
        sink = self._codec.new_sink()
        if keys is not None:
            updates = {k: self._updates[k] for k in keys if k in self._updates}
            removals = self._removals.intersection(keys)
        else:
            updates = self._updates
            removals = self._removals
        encoded = {k: self._codec.encode(k, v, sink) for k, v in updates.items()}
        result = self._engine.commit(
            encoded or None,
            set(removals) or None,
            on_conflict=on_conflict,
            info=info,
            chunks=(sink.chunks or None) if sink is not None else None,
            chunk_refs=(sink.refs_by_key or None) if sink is not None else None,
            **self._rules(merge_fns, merge_prefixes, default_merge),
        )
        if result.merged:
            if keys is not None:
                for key in keys:
                    self._updates.pop(key, None)
                    self._removals.discard(key)
            else:
                self._updates.clear()
                self._removals.clear()
            # The head moved, and a merge may have changed other keys.
            self._cache.clear()
        return result

    # -- Merge and apply --

    def _require_clean(self, verb: str) -> None:
        if self._updates or self._removals:
            raise ValueError(
                f"cannot {verb} with pending changes; commit or discard them first"
            )

    def merge(
        self,
        *,
        commit: str | None = None,
        branch: str | None = None,
        tag: str | None = None,
        info: dict | None = None,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
        post_check: PostCheck | None = None,
    ) -> MergeResult:
        """Merge a commit, branch or tag into this worktree's branch.

        Name exactly one. The lowest common ancestor, a three-way
        resolve, and a two-parent merge commit published on this
        worktree's head. ``post_check(key, merged_bytes)`` may refuse a
        merge-produced value, filing it as a conflict.

        Raises:
            ValueError: with pending changes — commit or discard first.
            MergeConflict: keys both sides changed that no rule resolves.
            ConcurrencyError: the branch moved during the merge, or the two
                share no history.
        """
        self._require_clean("merge")
        their_head = self._repo._resolve_ref(commit=commit, branch=branch, tag=tag)
        result = self._engine.merge_heads(
            their_head,
            on_conflict=on_conflict,
            post_check=post_check,
            info=info,
            **self._rules(merge_fns, merge_prefixes, default_merge),
        )
        if result.merged:
            self._cache.clear()
        return result

    def apply(
        self,
        base: str,
        target: str,
        *,
        info: dict | None = None,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergeRule] | None = None,
        merge_prefixes: dict[str, MergeRule] | None = None,
        default_merge: MergeRule | None = None,
        post_check: PostCheck | None = None,
    ) -> MergeResult:
        """Commit the change from ``base`` to ``target`` onto this branch.

        A three-way merge with ``base`` standing in for the common
        ancestor: what ``target`` changed relative to ``base`` lands on
        this worktree's head as one ordinary commit, and changes made
        here since are kept. A change already present commits nothing
        (strategy ``"no_op"``). :meth:`cherry_pick` and :meth:`revert`
        are the common cases.

        Raises:
            ValueError: with pending changes — commit or discard first.
            UnknownCommitError: ``base`` or ``target`` is not in the store.
            MergeConflict: keys both changed that no rule resolves.
            ConcurrencyError: the branch moved during the apply.
        """
        self._require_clean("apply a change")
        self._repo.get_commit(base)
        self._repo.get_commit(target)
        result = self._engine.apply_change(
            base,
            target,
            on_conflict=on_conflict,
            post_check=post_check,
            info=info,
            **self._rules(merge_fns, merge_prefixes, default_merge),
        )
        if result.merged:
            self._cache.clear()
        return result

    def cherry_pick(self, commit: str, **options: Any) -> MergeResult:
        """Commit the change ``commit`` made (relative to its first parent)
        onto this branch. Takes :meth:`apply`'s options."""
        parents = self._repo.get_commit(commit).parents
        return self.apply(parents[0] if parents else ROOT_COMMIT, commit, **options)

    def revert(self, commit: str, **options: Any) -> MergeResult:
        """Commit the undoing of the change ``commit`` made (relative to its
        first parent) onto this branch. Takes :meth:`apply`'s options."""
        parents = self._repo.get_commit(commit).parents
        return self.apply(commit, parents[0] if parents else ROOT_COMMIT, **options)

    # -- Moving --

    def discard(self) -> None:
        """Drop every pending change (like ``git restore .``)."""
        self._updates.clear()
        self._removals.clear()
        self._cache.clear()

    def reset(self, commit: str) -> None:
        """Move the branch to ``commit`` and drop pending changes (like
        ``git reset --hard``). The commit the branch left stays in its
        history only if something else reaches it.

        Raises:
            UnknownCommitError: ``commit`` is not in the store.
            UnknownBranchError: the branch was deleted.
        """
        if not self._engine.reset_to(commit):
            raise UnknownCommitError(f"Commit '{commit}' does not exist")
        self.discard()

    def refresh(self) -> None:
        """Move to the branch's current tip, dropping pending changes.

        Raises:
            UnknownBranchError: the branch was deleted.
            CorruptHeadError: its HEAD is damaged beyond recovery.
        """
        self._engine.refresh()
        self.discard()
