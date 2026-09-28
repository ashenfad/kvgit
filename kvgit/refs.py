"""A repository's branches and tags, as collections."""

from __future__ import annotations

from collections.abc import ItemsView, Iterator, Mapping
from typing import TYPE_CHECKING

from .errors import CorruptHeadError, UnknownBranchError, UnknownTagError
from .versioned import kv as _kv
from .versioned.kv import BRANCH_HEAD, ROOT_COMMIT, VersionedKV
from .versioned.protocol import TagInfo

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
        _kv._reject_reserved_branch(name)
        store = self._repo.store
        commit = _kv._resolve_head(
            store, name, recover_from_corrupt_head=self._repo._recover
        )
        if commit is not None:
            return commit
        if store.get(BRANCH_HEAD % name) is not None:
            raise CorruptHeadError(f"Branch '{name}' HEAD is corrupt and unrecoverable")
        raise UnknownBranchError(f"Branch '{name}' does not exist")

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and VersionedKV.exists(self._repo.store, name)

    def __iter__(self) -> Iterator[str]:
        return iter(VersionedKV.branches(self._repo.store))

    def __len__(self) -> int:
        return len(VersionedKV.branches(self._repo.store))

    def create(self, name: str, at: str | None = None) -> str:
        """Create branch ``name`` at commit ``at`` (default: the empty root
        commit); return the commit it points at.

        Raises:
            BranchExistsError: if the name is taken.
            UnknownCommitError: if ``at`` is not in the store.
        """
        target = at or ROOT_COMMIT
        _kv.create_branch(self._repo.store, name, target)
        return target

    def delete(self, name: str) -> None:
        """Delete a branch. Its commits become collectable at the next
        ``gc``; worktrees still holding it read from their heads and raise
        :class:`UnknownBranchError` on commit.

        Raises:
            UnknownBranchError: if there is no such branch.
        """
        _kv.delete_branch(self._repo.store, name)


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
        commit = _kv._resolve_tag(self._repo.store, name)
        if commit is None:
            raise UnknownTagError(f"Tag '{name}' does not exist")
        return commit

    def __contains__(self, name: object) -> bool:
        return (
            isinstance(name, str)
            and _kv._resolve_tag(self._repo.store, name) is not None
        )

    def __iter__(self) -> Iterator[str]:
        return iter(_kv.tags(self._repo.store))

    def __len__(self) -> int:
        return len(_kv.tags(self._repo.store))

    def items(self) -> ItemsView[str, str]:
        """Every tag and its commit, read in one pass over the store."""
        return _kv.tags(self._repo.store).items()

    def create(self, name: str, commit: str, *, info: dict | None = None) -> None:
        """Name ``commit`` permanently. A tag keeps its commit, and
        everything that commit descends from, alive.

        Raises:
            TagExistsError: if the name is taken.
            UnknownCommitError: if ``commit`` is not in the store.
        """
        _kv.create_tag(self._repo.store, name, commit, info)

    def delete(self, name: str) -> None:
        """Delete a tag. A commit it alone kept alive becomes collectable
        at the next ``gc``.

        Raises:
            UnknownTagError: if there is no such tag.
        """
        _kv.delete_tag(self._repo.store, name)

    def info(self, name: str) -> TagInfo:
        """A tag's commit, creation time, info, and whether its commit is
        missing from the store.

        Raises:
            UnknownTagError: if there is no such tag.
        """
        found = _kv.tag_info(self._repo.store, name)
        if found is None:
            raise UnknownTagError(f"Tag '{name}' does not exist")
        return found
