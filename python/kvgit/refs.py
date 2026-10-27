"""A repository's branches and tags, as collections."""

from __future__ import annotations

from collections.abc import ItemsView, Iterator, Mapping
from typing import TYPE_CHECKING

from . import _store as _kv
from ._store import ROOT_COMMIT
from ._types import TagInfo
from .errors import UnknownTagError

if TYPE_CHECKING:
    from .repo import Repo


class Branches(Mapping[str, str]):
    """Every branch of a repository, mapped to the commit it points at.

    A live view: each read goes to the store, so it sees branches other
    processes create, move and delete. ``name in repo.branches`` and
    iteration never write; iteration is by name, sorted, and never
    includes tags.

    A missing branch raises :class:`UnknownBranchError`, a ``KeyError``,
    so ``repo.branches.get(name)`` answers None. A branch whose HEAD is
    damaged beyond recovery raises :class:`CorruptHeadError` instead,
    from ``get`` too: damage is not absence.
    """

    def __init__(self, repo: Repo) -> None:
        self._repo = repo

    def __repr__(self) -> str:
        return f"Branches({list(self)!r})"

    def __getitem__(self, name: str) -> str:
        return self._resolve(name)

    def _resolve(self, name: str, roots: dict[str, str] | None = None) -> str:
        """The branch's head, recording its root in ``roots`` when given
        (see :meth:`Repo._resolve_ref`)."""
        recover = self._repo._recover
        found = _kv._call(
            "head_of",
            self._repo._store,
            recover,
            branch=name,
            recover=recover is not None,
        )
        if roots is not None and found["root"] is not None:
            roots[found["commit"]] = found["root"]
        return found["commit"]

    def __contains__(self, name: object) -> bool:
        # A damaged head still counts: opening it raises either way.
        return isinstance(name, str) and _kv._call(
            "branch_exists", self._repo._store, name=name
        )

    def __iter__(self) -> Iterator[str]:
        return iter(_kv._call("branches", self._repo._store))

    def __len__(self) -> int:
        return len(_kv._call("branches", self._repo._store))

    def create(self, name: str, at: str | None = None) -> str:
        """Create branch ``name`` at commit ``at`` (default: the empty root
        commit); return the commit it points at.

        Raises:
            BranchExistsError: if the name is taken.
            UnknownCommitError: if ``at`` is not in the store.
        """
        target = at or ROOT_COMMIT
        _kv.create_branch(self._repo._store, name, target)
        return target

    def delete(self, name: str) -> None:
        """Delete a branch. Its commits become collectable at the next
        ``gc``; worktrees still holding it read from their heads and raise
        :class:`UnknownBranchError` on commit.

        The removal is conditional on the head this call read: if another
        writer moves the branch, or deletes and recreates it, in between,
        nothing is removed and the call raises rather than retrying, so a
        newer head is never deleted on the strength of an older one. A
        branch made again at the same commit is indistinguishable from
        the one read, and is deleted, as with git's
        ``update-ref -d <ref> <old-oid>``.

        Raises:
            UnknownBranchError: if there is no such branch.
            ConcurrencyError: if the branch changed while being deleted.
            ValueError: if ``name`` is empty or in the reserved tag
                namespace.
        """
        _kv.delete_branch(self._repo._store, name)


class Tags(Mapping[str, str]):
    """Every tag of a repository, mapped to the commit it names.

    A live view, like :class:`Branches`. A tag never moves: creating one
    over a taken name raises, and pointing a name elsewhere is ``delete``
    then ``create``. A missing tag raises :class:`UnknownTagError`, a
    ``KeyError``. A tag whose commit is no longer in the store still
    maps to it; :meth:`info` says whether it is dangling.
    """

    def __init__(self, repo: Repo) -> None:
        self._repo = repo

    def __repr__(self) -> str:
        return f"Tags({list(self)!r})"

    def __getitem__(self, name: str) -> str:
        commit = _kv._resolve_tag(self._repo._store, name)
        if commit is None:
            raise UnknownTagError(f"Tag '{name}' does not exist")
        return commit

    def __contains__(self, name: object) -> bool:
        return (
            isinstance(name, str)
            and _kv._resolve_tag(self._repo._store, name) is not None
        )

    def __iter__(self) -> Iterator[str]:
        return iter(_kv.tags(self._repo._store))

    def __len__(self) -> int:
        return len(_kv.tags(self._repo._store))

    def items(self) -> ItemsView[str, str]:
        """Every tag and its commit: a live view, like the mapping, that
        reads all of them in one pass each time it is iterated."""
        return _TagItems(self)

    def create(self, name: str, commit: str, *, info: dict | None = None) -> None:
        """Name ``commit`` permanently. A tag keeps its commit, and
        everything that commit descends from, alive.

        Raises:
            TagExistsError: if the name is taken.
            UnknownCommitError: if ``commit`` is not in the store.
        """
        _kv.create_tag(self._repo._store, name, commit, info)

    def delete(self, name: str) -> None:
        """Delete a tag. A commit it alone kept alive becomes collectable
        at the next ``gc``.

        The removal is conditional on the head and info record this call
        read: a tag deleted and made again by another writer in between,
        even at the same commit, keeps its new commit and info, and the
        call raises rather than retrying.

        Raises:
            UnknownTagError: if there is no such tag.
            ConcurrencyError: if the tag changed while being deleted.
        """
        _kv.delete_tag(self._repo._store, name)

    def info(self, name: str) -> TagInfo:
        """A tag's commit, creation time, info, and whether its commit is
        missing from the store.

        Raises:
            UnknownTagError: if there is no such tag.
        """
        found = _kv.tag_info(self._repo._store, name)
        if found is None:
            raise UnknownTagError(f"Tag '{name}' does not exist")
        return found


class _TagItems(ItemsView[str, str]):
    """A live items view of :class:`Tags` that iterates in one pass,
    rather than listing the names and then reading each tag again."""

    _mapping: Tags

    def __iter__(self) -> Iterator[tuple[str, str]]:
        return iter(_kv.tags(self._mapping._repo.store).items())
