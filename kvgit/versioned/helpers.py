"""Shared diff and history helpers."""

from collections import deque
from typing import Callable, Iterable

from .protocol import DiffResult


def diff_keysets(
    keyset_a: dict[str, str],
    keyset_b: dict[str, str],
) -> DiffResult:
    """Compute key-level differences between two keysets.

    Each keyset maps user-facing key names to opaque content identifiers
    (versioned keys in KV, blob hex-SHAs in GP).  Two keys are considered
    "modified" when present in both keysets but mapped to different
    identifiers.
    """
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
