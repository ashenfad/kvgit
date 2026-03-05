"""GitPython-backed versioned store."""

import binascii
import io
import json
import os
import urllib.parse
from collections import deque
from typing import Iterable

from .errors import ConcurrencyError, MergeConflict
from .protocol import BytesMergeFn, DiffResult, MergeResult

try:
    import git
    from git import Blob, Commit, Repo, Tree
    from git.exc import BadObject, GitCommandError
    from gitdb import IStream
except ImportError:
    pass


def _quote(key: str) -> str:
    return urllib.parse.quote_plus(key)


def _unquote(quoted: str) -> str:
    return urllib.parse.unquote_plus(quoted)


class VersionedGP:
    """A commit log backed by a Git repository."""

    def __init__(
        self,
        repo_path: str,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
    ) -> None:
        self.repo_path = repo_path
        self._branch = branch

        if not os.path.exists(repo_path):
            self.repo = Repo.init(repo_path, bare=True)
        else:
            self.repo = Repo(repo_path)

        if commit_hash is None:
            # Try to read the branch HEAD
            try:
                commit_hash = self.repo.heads[branch].commit.hexsha
            except (IndexError, AttributeError):
                # Branch doesn't exist, create an initial empty commit
                commit_hash = self._create_empty_commit(branch)

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )
        self._current_commit: str = commit_hash
        self._base_commit: str = commit_hash

        # Load commit keyset
        self._commit_keys: dict[str, str] = {}
        self._load_commit(commit_hash, update_base=True)

        # Merge function registry
        self._merge_fns: dict[str, BytesMergeFn] = {}
        self._default_merge: BytesMergeFn | None = None
        self.last_merge_result: MergeResult | None = None

    def _create_empty_commit(self, branch: str) -> str:
        # Create an empty tree

        tree_data = bytearray()
        istream = self.repo.odb.store(
            IStream(Tree.type, len(tree_data), io.BytesIO(tree_data))
        )
        tree_sha = istream.binsha
        tree_obj = Tree(self.repo, tree_sha)

        commit = Commit.create_from_tree(
            self.repo, tree_obj, "Initial commit", parent_commits=[]
        )
        self.repo.create_head(branch, commit)
        return commit.hexsha

    @property
    def current_commit(self) -> str:
        return self._current_commit

    @property
    def base_commit(self) -> str:
        return self._base_commit

    @property
    def current_branch(self) -> str:
        return self._branch

    def __repr__(self) -> str:
        n_keys = len(self._commit_keys)
        short_hash = self._current_commit[:8]
        return f"VersionedGP(branch={self._branch!r}, commit={short_hash}..., keys={n_keys})"

    @property
    def latest_head(self) -> str | None:
        try:
            return self.repo.heads[self._branch].commit.hexsha
        except (IndexError, AttributeError):
            return None

    @property
    def initial_commit(self) -> str:
        """The root commit hash (cached after first access)."""
        if not hasattr(self, "_initial_commit"):
            commits = list(self.history())
            self._initial_commit = commits[-1]
        return self._initial_commit

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the current commit."""
        hexsha = self._commit_keys.get(key)
        if hexsha is None:
            return None
        return self._read_blob(hexsha)

    def _read_blob(self, hexsha: str) -> bytes:
        blob = Blob(self.repo, binascii.unhexlify(hexsha))
        return blob.data_stream.read()

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the current commit."""
        result = {}
        for key in keys:
            val = self.get(key)
            if val is not None:
                result[key] = val
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
        return (self._current_commit, dict(self._commit_keys))

    def _restore_state(self, saved: tuple) -> None:
        self._current_commit, self._commit_keys = saved

    def _write_blob(self, data: bytes) -> str:
        istream = self.repo.odb.store(IStream(Blob.type, len(data), io.BytesIO(data)))
        return istream.hexsha

    def _create_tree(self, keyset: dict[str, str]) -> str:
        # Git tree objects are a sequence of entries sorted by name.
        # Each entry is: "<mode> <name>\0<20-byte binary SHA>"
        # We use mode 100644 (regular file) for all entries.
        tree_data = bytearray()
        entries = []
        for key, hexsha in keyset.items():
            entries.append((0o100644, _quote(key), hexsha))

        for mode, name, hexsha in sorted(entries, key=lambda e: e[1]):
            tree_data.extend(f"{mode:o} {name}".encode("utf-8"))
            tree_data.append(0)
            tree_data.extend(binascii.unhexlify(hexsha))

        istream = self.repo.odb.store(
            IStream(Tree.type, len(tree_data), io.BytesIO(tree_data))
        )
        return istream.hexsha

    def _create_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
        parents: list[str] | None = None,
    ) -> str:
        updates = updates or {}
        removals = removals or set()

        new_commit_keys = dict(self._commit_keys)
        for key in removals:
            new_commit_keys.pop(key, None)

        for key, value in updates.items():
            new_commit_keys[key] = self._write_blob(value)

        tree_hexsha = self._create_tree(new_commit_keys)
        tree_obj = Tree(self.repo, binascii.unhexlify(tree_hexsha))

        info_str = json.dumps(info, separators=(",", ":")) if info else ""
        msg = f"kvgit commit\n\n{info_str}" if info_str else "kvgit commit"

        if parents is None:
            parent_objs = [Commit(self.repo, binascii.unhexlify(self._current_commit))]
        else:
            parent_objs = [Commit(self.repo, binascii.unhexlify(p)) for p in parents]

        commit = Commit.create_from_tree(
            self.repo, tree_obj, msg, parent_commits=parent_objs
        )

        self._commit_keys = new_commit_keys
        self._current_commit = commit.hexsha
        return commit.hexsha

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

        If HEAD has diverged, performs a three-way merge.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.
            on_conflict: ``'raise'`` (default) or ``'abandon'``.
            merge_fns: Per-key merge functions (override instance-level).
            default_merge: Default merge function (override instance-level).
            info: Optional metadata dict for the commit.

        Returns:
            A ``MergeResult`` (truthy when committed, falsy if abandoned).

        Raises:
            ConcurrencyError: If ``on_conflict='raise'`` and CAS fails.
            MergeConflict: If keys conflict with no merge function.
        """
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

        current_head = self.latest_head

        if current_head == self._base_commit:
            saved = self._snapshot_state()
            new_hash = self._create_commit(updates, removals, info=info)

            try:
                self.repo.git.update_ref(
                    f"refs/heads/{self._branch}", new_hash, self._base_commit
                )
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
            except GitCommandError:
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

        effective_fns = dict(self._merge_fns)
        if merge_fns:
            effective_fns.update(merge_fns)
        effective_default = default_merge or self._default_merge

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

        all_keys = set(our_keyset.keys()) | set(their_keyset.keys())
        for key in all_keys - all_changed:
            if key in their_keyset:
                merged_keyset[key] = their_keyset[key]
            elif key in our_keyset:
                merged_keyset[key] = our_keyset[key]

        for key in our_changed - their_changed:
            if key in our_diff.removed:
                pass
            else:
                merged_keyset[key] = our_keyset[key]
                auto_merged.append(key)

        for key in their_changed - our_changed:
            if key in their_diff.removed:
                pass
            else:
                merged_keyset[key] = their_keyset[key]

        contested = our_changed & their_changed
        for key in contested:
            our_removed = key in our_diff.removed
            their_removed = key in their_diff.removed

            if our_removed and their_removed:
                continue

            if (
                not our_removed
                and not their_removed
                and our_keyset.get(key) == their_keyset.get(key)
            ):
                merged_keyset[key] = their_keyset[key]
                continue

            fn = effective_fns.get(key, effective_default)
            if fn is None:
                conflicts.add(key)
                continue

            old_val = self._read_blob(lca_keyset[key]) if key in lca_keyset else None
            our_val = None if our_removed else self._read_blob(our_keyset[key])
            their_val = None if their_removed else self._read_blob(their_keyset[key])
            try:
                result_val = fn(old_val, our_val, their_val)
                merged_values[key] = result_val
                auto_merged.append(key)
            except Exception as e:
                conflicts.add(key)
                merge_errors[key] = e

        if conflicts:
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
            raise MergeConflict(conflicts, merge_errors)

        parents = [their_head, self._current_commit]

        updates = merged_values
        removals = (
            set(self._commit_keys.keys())
            - set(merged_keyset.keys())
            - set(merged_values.keys())
        )
        self._commit_keys = merged_keyset
        merge_hash = self._create_commit(updates, removals, info=info, parents=parents)

        try:
            self.repo.git.update_ref(
                f"refs/heads/{self._branch}", merge_hash, their_head
            )
            self._base_commit = merge_hash
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
        except GitCommandError:
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
        head = self.latest_head
        if head is None:
            raise ValueError(f"No HEAD commit found for branch {self._branch}")
        self._load_commit(head, update_base=True)

    def checkout(
        self, commit_hash: str, *, branch: str | None = None
    ) -> "VersionedGP | None":
        """Return a new VersionedGP at a specific commit, or None if missing."""
        try:
            Commit(self.repo, binascii.unhexlify(commit_hash))
            return VersionedGP(
                self.repo_path, commit_hash=commit_hash, branch=branch or self._branch
            )
        except (BadObject, ValueError):
            return None

    def create_branch(self, name: str, *, at: str | None = None) -> "VersionedGP":
        """Fork a commit onto a new branch.

        Returns a new ``VersionedGP`` on the new branch.
        """
        target = at or self._current_commit
        if at is not None:
            try:
                c = Commit(self.repo, binascii.unhexlify(at))
                c.tree  # force load to verify commit exists
            except (BadObject, ValueError):
                raise ValueError(f"Commit '{at}' does not exist")

        if name in self.repo.heads:
            raise ValueError(f"Branch '{name}' already exists")

        self.repo.create_head(name, Commit(self.repo, binascii.unhexlify(target)))
        return VersionedGP(self.repo_path, commit_hash=target, branch=name)

    def delete_branch(self, name: str) -> None:
        """Delete a branch by name."""
        if name == self._branch:
            raise ValueError("Cannot delete the current branch")
        if name not in self.repo.heads:
            raise ValueError(f"Branch '{name}' does not exist")

        git.Head.delete(self.repo, self.repo.heads[name], force=True)

    def switch_branch(self, name: str) -> None:
        """Switch this instance to a different branch in-place."""
        if name not in self.repo.heads:
            raise ValueError(f"Branch '{name}' does not exist")
        self._branch = name
        self._load_commit(self.repo.heads[name].commit.hexsha, update_base=True)

    def peek(self, key: str, *, branch: str) -> bytes | None:
        """Read a key from another branch's HEAD without switching."""
        if branch not in self.repo.heads:
            return None
        commit_hash = self.repo.heads[branch].commit.hexsha
        keyset = self._load_keyset(commit_hash)
        hexsha = keyset.get(key)
        if not hexsha:
            return None
        return self._read_blob(hexsha)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit."""
        try:
            c = Commit(self.repo, binascii.unhexlify(commit_hash))
            c.tree  # force load to verify commit exists
        except (BadObject, ValueError):
            return False
        self.repo.git.update_ref(f"refs/heads/{self._branch}", commit_hash)
        self._load_commit(commit_hash, update_base=True)
        return True

    def history(
        self, commit_hash: str | None = None, *, all_parents: bool = False
    ) -> Iterable[str]:
        """Yield the commit chain from newest to oldest."""
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

    def list_branches(self) -> list[str]:
        """List all branch names."""
        return [h.name for h in self.repo.heads]

    def commit_info(self, commit_hash: str | None = None) -> dict | None:
        """Retrieve the info dict for a commit, or None if none was stored."""
        target = commit_hash or self._current_commit
        try:
            c = Commit(self.repo, binascii.unhexlify(target))
            msg = c.message
        except (BadObject, ValueError):
            return None
        if "\n\n" not in msg:
            return None
        info_part = msg.split("\n\n", 1)[1].strip()
        if not info_part:
            return None
        return json.loads(info_part)

    def diff(self, commit_a: str, commit_b: str) -> DiffResult:
        """Compute key-level differences between two commits."""
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
        """Get the direct parent commit(s) of a commit."""
        target = commit_hash or self._current_commit
        return self._load_parents(target)

    # -- Internal --

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        try:
            bases = self.repo.merge_base(commit_a, commit_b)
        except GitCommandError:
            return None
        if bases:
            return bases[0].hexsha
        return None

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        c = Commit(self.repo, binascii.unhexlify(commit_hash))
        return {_unquote(blob.name): blob.hexsha for blob in c.tree.blobs}

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        c = Commit(self.repo, binascii.unhexlify(commit_hash))
        return tuple(p.hexsha for p in c.parents)

    def _load_commit(self, commit_hash: str, *, update_base: bool) -> None:
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
        self._commit_keys = self._load_keyset(commit_hash)
