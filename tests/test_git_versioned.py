"""Tests for GitVersioned."""

import pytest
import os
import shutil

from kvgit.git_versioned import GitVersioned
from kvgit.errors import MergeConflict


@pytest.fixture
def repo_path(tmp_path):
    path = str(tmp_path / "test_repo.git")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


def test_empty_init(repo_path):
    v = GitVersioned(repo_path)
    assert v.current_commit is not None
    assert v.base_commit == v.current_commit
    assert list(v.keys()) == []


def test_commit_and_get(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"greeting": b"hello"})
    assert v.get("greeting") == b"hello"


def test_commit_and_get_many(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1", "b": b"2", "c": b"3"})
    result = v.get_many("a", "c", "missing")
    assert result == {"a": b"1", "c": b"3"}


def test_branching(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"main_key": b"1"})
    v2 = v.create_branch("dev")
    v2.commit({"dev_key": b"2"})

    assert v.get("dev_key") is None
    assert v2.get("main_key") == b"1"
    assert v2.get("dev_key") == b"2"

    assert v2.peek("main_key", branch="main") == b"1"
    assert v2.peek("dev_key", branch="main") is None


def test_merge_fast_forward(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v2 = GitVersioned(repo_path)
    v2.commit({"b": b"2"})

    assert v.latest_head == v2.current_commit

    v.refresh()
    assert v.get("b") == b"2"


def test_merge_three_way(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"b": b"1"})
    v2.commit({"c": b"1"})  # This will trigger a three-way merge internally

    assert v2.get("a") == b"1"
    assert v2.get("b") == b"1"
    assert v2.get("c") == b"1"


def test_merge_conflict(repo_path):
    v = GitVersioned(repo_path)
    v.commit({"a": b"1"})

    v1 = GitVersioned(repo_path)
    v2 = GitVersioned(repo_path)

    v1.commit({"a": b"2"})
    with pytest.raises(MergeConflict):
        v2.commit({"a": b"3"})


def test_staged_wrapper(repo_path):
    from kvgit.staged import Staged

    v = GitVersioned(repo_path)
    s = Staged(v)

    s["user"] = "alice"
    s["score"] = 100
    s.commit()

    assert s["user"] == "alice"
    assert s["score"] == 100

    s2 = Staged(GitVersioned(repo_path))
    assert s2["user"] == "alice"
    assert s2["score"] == 100
