"""Shared diff and history helpers."""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable, Mapping
from typing import TYPE_CHECKING

from .protocol import DiffResult

if TYPE_CHECKING:
    from .merge import Change


def changes_as_diff(changes: Mapping[str, Change]) -> DiffResult:
    """Group per-key changes into keys added, removed and modified."""
    added = frozenset(k for k, c in changes.items() if c.old is None)
    removed = frozenset(k for k, c in changes.items() if c.new is None)
    modified = frozenset(changes.keys() - added - removed)
    return DiffResult(added=added, removed=removed, modified=modified)


def walk_history(
    start: str,
    parent_loader: Callable[[str], tuple[str, ...]],
    *,
    all_parents: bool = False,
) -> Iterable[str]:
    """Yield commit hashes from newest to oldest.

    Args:
        start: The commit hash to begin walking from.
        parent_loader: A callable that takes a commit hash and returns
            its parent hashes as a tuple.
        all_parents: If False (default), follow only the first parent
            (linear history).  If True, BFS across all parents.
    """
    if not all_parents:
        current: str | None = start
        while current is not None:
            yield current
            parents = parent_loader(current)
            current = parents[0] if parents else None
    else:
        visited: set[str] = set()
        queue: deque[str] = deque([start])
        while queue:
            current_hash = queue.popleft()
            if current_hash in visited:
                continue
            visited.add(current_hash)
            yield current_hash
            for p in parent_loader(current_hash):
                if p not in visited:
                    queue.append(p)
