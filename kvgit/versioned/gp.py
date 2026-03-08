"""GitPython-backed versioned store."""

import binascii
import io
import json
import os
import urllib.parse

from .base import VersionedBase
from .merge import MergeResolution

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


class VersionedGP(VersionedBase):
    """A commit log backed by a Git repository."""

    def __init__(
        self,
        repo_path: str,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
    ) -> None:
        self.repo_path = repo_path

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

        super().__init__(branch=branch, commit_hash=commit_hash)

        # Load commit keyset
        self._load_commit(commit_hash, update_base=True)

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
    def latest_head(self) -> str | None:
        try:
            return self.repo.heads[self._branch].commit.hexsha
        except (IndexError, AttributeError):
            return None

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the current commit."""
        hexsha = self._commit_keys.get(key)
        if hexsha is None:
            return None
        return self._read_blob(hexsha)

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the current commit."""
        result = {}
        for key in keys:
            val = self.get(key)
            if val is not None:
                result[key] = val
        return result

    # -- Abstract method implementations --

    def _snapshot_state(self) -> tuple:
        return (self._current_commit, dict(self._commit_keys))

    def _restore_state(self, saved: tuple) -> None:
        self._current_commit, self._commit_keys = saved

    def _read_blob(self, content_id: str) -> bytes | None:
        """Read a blob by its git hex SHA."""
        blob = Blob(self.repo, binascii.unhexlify(content_id))
        return blob.data_stream.read()

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

    def _create_git_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
        parents: list[str] | None = None,
    ) -> str:
        """Create a git commit object.

        Internal method that supports explicit parents (for merge commits).
        """
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

    def _create_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
    ) -> str:
        """Create a single-parent commit (parent = current commit)."""
        return self._create_git_commit(updates, removals, info=info)

    def _create_merge_commit(
        self,
        resolution: MergeResolution,
        parents: tuple[str, ...],
        info: dict | None,
    ) -> str:
        """Create a multi-parent merge commit from a resolved merge."""
        merged_keyset = resolution.merged_keyset
        merged_values = resolution.merged_values

        removals = (
            set(self._commit_keys.keys())
            - set(merged_keyset.keys())
            - set(merged_values.keys())
        )
        self._commit_keys = merged_keyset
        return self._create_git_commit(
            merged_values, removals, info=info, parents=list(parents)
        )

    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD via git update-ref."""
        try:
            self.repo.git.update_ref(f"refs/heads/{self._branch}", new_head, expected)
            return True
        except GitCommandError:
            return False

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        c = Commit(self.repo, binascii.unhexlify(commit_hash))
        return {_unquote(blob.name): blob.hexsha for blob in c.tree.blobs}

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        c = Commit(self.repo, binascii.unhexlify(commit_hash))
        return tuple(p.hexsha for p in c.parents)

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        try:
            bases = self.repo.merge_base(commit_a, commit_b)
        except GitCommandError:
            return None
        if bases:
            return bases[0].hexsha
        return None

    # -- Navigation --

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

    # -- Internal --

    def _load_commit(self, commit_hash: str, *, update_base: bool) -> None:
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
        self._commit_keys = self._load_keyset(commit_hash)
