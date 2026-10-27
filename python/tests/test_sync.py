"""Sync between stores: a round trip through a reference remote, and
what a transfer refuses."""

import dataclasses

import pytest

from kvgit import IntegrityError, Repo, UnknownBranchError, _sync as sync
from kvgit._store import ROOT_COMMIT
from kvgit.kv.memory import Memory


def device(**values: bytes) -> Memory:
    """A store with ``main`` holding ``values``, one commit per value."""
    store = Memory()
    wt = Repo(store, codec="bytes").worktree("main", create=True)
    for key, value in values.items():
        wt[key] = value
        wt.commit(info={"set": key})
    return store


def test_a_branch_round_trips_through_a_remote():
    a = device(x=b"one", y=b"two")
    remote = sync.MemoryRemote(Memory(), id="r")
    pushed = sync.push_branch(a, remote, "main")
    assert pushed.status == "created"
    assert pushed.transferred == 3  # the root commit too

    b = Memory()
    pulled = sync.pull_branch(b, remote, "main")
    assert pulled.status == "created"
    assert pulled.local_head == pushed.local_head
    assert dict(Repo(b, codec="bytes").snapshot(branch="main").items()) == {
        "x": b"one",
        "y": b"two",
    }
    assert sync.get_sync_head(b, "main", remote) == pushed.local_head
    assert sync.sync_branch(b, remote, "main").status == "up-to-date"


def test_a_tampered_value_is_refused_unwritten():
    a = device(x=b"one")
    wire = list(sync.walk_delta(a, _head(a)))
    last = wire[-1]
    tampered = [*wire[:-1], dataclasses.replace(last, updates={"x": b"evil"})]
    b = Memory()
    with pytest.raises(IntegrityError, match="integrity check failed"):
        sync.apply_wire(b, tampered)
    assert b.get(f"__commit_root__{last.hash}") is None


def test_a_child_before_its_parent_is_refused():
    a = device(x=b"one", y=b"two")
    wire = list(sync.walk_delta(a, _head(a)))
    with pytest.raises(IntegrityError, match="sends parents first"):
        sync.apply_wire(Memory(), wire[::-1])


def test_a_hollow_fetch_moves_no_ref():
    a = device(x=b"one")
    remote = sync.MemoryRemote(Memory())
    sync.push_branch(a, remote, "main")

    class Hollow(sync.MemoryRemote):
        def fetch(self, want, have):
            return iter(())

    b = Memory()
    with pytest.raises(IntegrityError, match="not present after the fetch"):
        sync.pull_branch(b, Hollow(remote.store), "main")
    assert b.get("__branch_head__main") is None


def test_pushing_a_branch_that_does_not_exist_raises():
    with pytest.raises(UnknownBranchError):
        sync.push_branch(Memory(), sync.MemoryRemote(Memory()), "main")


def test_a_synced_branch_is_kept_from_deletion():
    a = device(x=b"one")
    assert not sync.has_sync_head(a, "main")
    sync.push_branch(a, sync.MemoryRemote(Memory()), "main")
    assert sync.has_sync_head(a, "main")
    assert sync.delete_branch_unless_synced(a, "main") is False
    sync.clear_sync_head(a, "main")
    assert sync.delete_branch_unless_synced(a, "main") is True


def _head(store: Memory) -> str:
    return sync._text(store.get("__branch_head__main"))


def test_a_lost_pull_race_reports_the_head_that_won():
    a = device(x=b"one")
    remote = sync.MemoryRemote(Memory())
    head = sync.push_branch(a, remote, "main").local_head
    b = Memory()
    applied = sync.apply_wire(b, remote.fetch(head, []))
    # another writer creates the branch between the plan and the finish
    assert sync.publish_head(b, "main", None, ROOT_COMMIT)
    done = sync._call(
        "pull_finish",
        b,
        branch="main",
        remote={"id": None},
        remote_head=head,
        local_head=None,
        in_flight=list(applied.in_flight),
    )
    assert done == {"status": "diverged", "local_head": ROOT_COMMIT}


def test_a_push_streams_what_it_sends():
    a = device(x=b"one", y=b"two")
    received = []

    class Streaming(sync.MemoryRemote):
        def push(self, branch, expected, head, commits):
            assert not isinstance(commits, (list, tuple))
            received.extend(commits)
            return super().push(branch, expected, head, received)

    result = sync.push_branch(a, Streaming(Memory()), "main")
    assert result.transferred == len(received) == 3


def test_a_remote_that_cannot_delete_branches_says_so():
    class Keeper:
        id = None

    with pytest.raises(TypeError, match="can't delete branches"):
        sync.delete_remote_branch(Memory(), Keeper(), "main", expected=ROOT_COMMIT)


def test_a_retried_push_counts_what_every_attempt_sent():
    a = device(x=b"one", y=b"two")

    class LosesOnce(sync.MemoryRemote):
        lost = False

        def push(self, branch, expected, head, commits):
            if not self.lost:
                self.lost = True
                list(commits)
                return False
            return super().push(branch, expected, head, commits)

    result = sync.push_branch(a, LosesOnce(Memory()), "main")
    assert result.status == "created"
    assert result.transferred == 6  # three commits, sent twice


def test_a_tag_is_not_synced_as_a_branch():
    a = device(x=b"one")
    repo = Repo(a, codec="bytes")
    repo.tags.create("v1", repo.branches["main"])
    remote = sync.MemoryRemote(Memory())
    for move in (sync.push_branch, sync.pull_branch):
        with pytest.raises(ValueError, match="reserved for tags"):
            move(a, remote, "refs/tags/v1")
    assert remote.list_refs() == []


def test_a_remote_tag_is_not_deleted_as_a_branch():
    remote = sync.MemoryRemote(device(x=b"one"))
    repo = Repo(remote.store, codec="bytes")
    repo.tags.create("v1", repo.branches["main"])
    with pytest.raises(ValueError, match="reserved for tags"):
        sync.delete_remote_branch(
            Memory(), remote, "refs/tags/v1", expected=repo.tags["v1"]
        )
    assert "v1" in repo.tags


def test_a_pull_fetches_a_shared_commit_whose_root_is_gone():
    a = device(x=b"one")
    remote = sync.MemoryRemote(Memory())
    shared = sync.push_branch(a, remote, "main").local_head
    b = Memory()
    sync.pull_branch(b, remote, "main")
    wt = Repo(b, codec="bytes").worktree("main")
    wt["y"] = b"two"
    wt.commit()
    b.remove(f"__commit_root__{shared}")
    sync.pull_branch(b, remote, "main")
    assert b.get(f"__commit_root__{shared}") == a.get(f"__commit_root__{shared}")
