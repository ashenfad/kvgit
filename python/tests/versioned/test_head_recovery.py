"""HEAD backup, recovery, and repair semantics.

Three contracts live here.

**prev-HEAD names a real, immediately-prior HEAD.**
``__branch_head_prev__<branch>`` is the input to ``_resolve_head``'s
recovery fallback, so what it names decides what a damaged branch
recovers *to*. It must only ever name a value the
``__branch_head__<branch>`` key actually held, and specifically the one
it held immediately before its current value. Naming an older real HEAD
silently drops commits; naming a commit that was never HEAD hands the
branch a lineage it never had — the resurrection a deleted branch's
leftover backup would cause.

**Reading never writes.** Recovery on a read path is in-memory only.
Persisting it is either an explicit ``repair_head`` call or a side
effect of a write that has to move HEAD anyway.

**A lost CAS leaves its writes alone.** Its commit is garbage, but the
nodes and chunks under it may be shared with the winner, so nothing is
deleted inline; ordinary GC reclaims it.

Every race here is a seam, not a sleep. ``HookStore`` runs a one-shot
callback at a chosen point in a chosen store operation, so "the winner
lands between the loser's commit write and its CAS" is expressed
exactly and repeats identically.
"""

from __future__ import annotations

import threading
import time

import pytest
from support import clean_orphans, fork, race_publish, root_of

from kvgit import ConcurrencyError, MergeConflict, Repo, Worktree, _core
from kvgit._store import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    COMMIT_TIME,
    PARENT_COMMIT,
    _resolve_head,
    blob_key,
    recover_by_commit_scan,
    repair_head,
)
from kvgit._types import MergeResult
from kvgit.encoding import dumps, loads
from kvgit.errors import CorruptHeadError, UnknownBranchError
from kvgit.kv.memory import Memory

HEAD_PREFIX = BRANCH_HEAD.replace("%s", "")
ROOT_PREFIX = COMMIT_ROOT.replace("%s", "")
NODE_PREFIX = "kvgit:keyset:"


class HookStore(Memory):
    """Memory store that records HEAD history and arms one-shot seams.

    ``head_history[branch]`` is the ground truth for "was this commit
    ever HEAD of that branch": every successful write to a
    ``__branch_head__`` key is appended, whichever store method made
    it. Assertions about prev-HEAD compare against this rather than
    against what the code under test believes.

    Three seams, all one-shot:

    * ``arm_commit_batch`` fires when a commit's write batch is written
      — after its writer has read HEAD, before it reaches its CAS.
    * ``arm_get`` fires after a chosen key's value has been read but
      before the caller sees it, on the *nth* read of that key.
    * ``arm_set`` fires before a chosen key is written — alone, or as
      part of an atomic batch — so a writer can be paused while another
      completes.

    All are points every version of the code passes through, so a test
    written against them means the same thing before and after a fix.
    """

    def __init__(self) -> None:
        super().__init__()
        self.head_history: dict[str, list[str]] = {}
        self._on_commit_batch = None
        self._on_get: dict[str, object] = {}
        self._on_set: dict[str, object] = {}

    # -- seams --

    def arm_commit_batch(self, fn) -> None:
        """Fire ``fn`` once, on the next commit write batch."""
        self._on_commit_batch = fn

    def arm_get(self, key: str, fn, *, nth: int = 1) -> None:
        """Fire ``fn`` once, after the ``nth`` read of ``key``."""
        self._on_get[key] = [fn, nth]

    def arm_set(self, key: str, fn) -> None:
        """Fire ``fn`` once, immediately before ``key`` is written."""
        self._on_set[key] = fn

    # -- recording --

    def _record(self, key: str, value: bytes) -> None:
        if not key.startswith(HEAD_PREFIX):
            return
        commit = loads(value) if value else None
        if isinstance(commit, str):
            self.head_history.setdefault(key[len(HEAD_PREFIX) :], []).append(commit)

    # -- KVStore overrides --

    def _read(self, key: str) -> None:
        armed = self._on_get.get(key)
        if armed is not None:
            armed[1] -= 1
            if armed[1] <= 0:
                del self._on_get[key]
                armed[0]()

    def get(self, key: str) -> bytes | None:
        value = super().get(key)
        self._read(key)
        return value

    def get_many(self, *args):
        found = super().get_many(*args)
        for key in self._normalize_keys(args):
            self._read(key)
        return found

    def set(self, key: str, value: bytes) -> None:
        fn = self._on_set.pop(key, None)
        if fn is not None:
            fn()
        super().set(key, value)
        self._record(key, value)

    def set_many(self, items=None, /, **kwargs) -> None:
        items = self._normalize_items(items, kwargs)
        fn = self._on_commit_batch
        if fn is not None and any(k.startswith(ROOT_PREFIX) for k in items):
            self._on_commit_batch = None
            fn()
        super().set_many(items)
        for key, value in items.items():
            self._record(key, value)

    def cas_many(self, expected, writes, removes=()) -> bool:
        fn = self._on_commit_batch
        if fn is not None and any(k.startswith(ROOT_PREFIX) for k in writes):
            self._on_commit_batch = None
            fn()
        for key in writes:
            armed = self._on_set.pop(key, None)
            if armed is not None:
                armed()
        applied = super().cas_many(expected, writes, removes)
        if applied:
            for key, value in writes.items():
                self._record(key, value)
        return applied


def open_repo(store, **options) -> Repo:
    """A repository over ``store`` that stores values as given."""
    return Repo(store, codec="bytes", **options)


def open_main(store, **options) -> Worktree:
    """A worktree on ``main``, created if the store has none."""
    return open_repo(store, **options).worktree("main", create=True)


def commit(wt: Worktree, updates: dict[str, bytes], **options) -> MergeResult:
    """Write ``updates`` into ``wt`` and commit them."""
    wt.update(updates)
    return wt.commit(**options)


def history(repo: Repo, branch: str = "main") -> list[str]:
    """Every commit reachable from ``branch``, merge parents included."""
    return [c.hash for c in repo.log(branch=branch)]


def age_commits(store, seconds: float) -> None:
    """Backdate every ``__commit_time__`` so orphans clear ``min_age``."""
    now = time.time()
    prefix = COMMIT_TIME.replace("%s", "")
    for key in list(store.keys()):
        if key.startswith(prefix):
            store.set(key, dumps(now - seconds))


def node_hashes(store, commit_hash: str) -> set[str]:
    """Every HAMT node hash reachable from a commit's keyset root."""
    root = root_of(store, commit_hash)
    if root is None:
        return set()
    op = _core.TreeOp.walk(NODE_PREFIX, 8, root, set(), False)
    kind, payload = op.resume()
    while kind == "get":
        kind, payload = op.resume(dict(store.get_many(payload)))
    return set(payload[1])


class TestPrevHeadInvariant:
    """``__branch_head_prev__`` must name a real, immediately-prior HEAD."""

    def test_prev_head_never_names_a_commit_that_was_never_head(self):
        """A losing CAS must not plant a foreign lineage in prev-HEAD.

        Main's HEAD is damaged and its backup is gone, so head
        resolution falls through to the commit scan, which picks the
        newest unclaimed tip — here the surviving tip of a deleted
        branch. That value is a *recovery candidate*, not a HEAD:
        nothing has written it to ``__branch_head__main``. Writing it
        into main's prev-HEAD on the way into a CAS that then fails
        makes the candidate durable, and every later read short-circuits
        the scan and 'recovers' main onto a branch it never had.

        The scan is opt-in, so the repository asks for it. Whatever a
        recoverer returns is still a candidate, and prev-HEAD must not
        be where it becomes durable.
        """
        store = HookStore()
        v = open_main(store, recover_from_corrupt_head=recover_by_commit_scan)
        commit(v, {"a": b"1"})

        # A commit that is never main's HEAD: a branch tip that outlives
        # its branch (young orphans survive the min_age guard).
        tmp = fork(v, "tmp")
        commit(tmp, {"t": b"tmp-only"})
        tmp_tip = tmp.head
        v.repo.branches.delete("tmp")

        # The damage the recovery path exists for: unreadable HEAD, and
        # no backup to fall back on.
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")

        try:
            commit(v, {"a": b"2"})
        except ConcurrencyError:
            pass

        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        history = store.head_history["main"]
        assert prev in history, (
            f"prev-HEAD names {prev}, which was never main's HEAD "
            f"(history: {history}). It is the deleted branch's tip "
            f"({tmp_tip}), so recovery would resurrect that branch onto "
            f"main."
        )
        assert prev == history[-2], (
            f"prev-HEAD is {prev}, not the immediately-previous HEAD "
            f"{history[-2]} (history: {history})"
        )

    def test_prev_head_is_not_overwritten_by_a_losing_writer(self):
        """A loser's stale backup must not clobber the winner's.

        The loser reads HEAD, builds its commit, and only then reaches
        the CAS. Two commits land in that window. Writing the backup on
        the way *into* the CAS makes the loser's stale value the last
        one written, so prev-HEAD ends up two commits behind HEAD
        instead of one — recovery from here silently drops the winner's
        first commit as well as its second.
        """
        store = HookStore()
        v = open_main(store)
        commit(v, {"a": b"1"})

        loser = v.repo.worktree("main")
        winner = v.repo.worktree("main")

        def winner_lands_twice() -> None:
            commit(winner, {"w": b"1"})
            commit(winner, {"w": b"2"})

        store.arm_commit_batch(winner_lands_twice)

        # A lost fast-forward race merges instead of raising, so the
        # loser lands a merge commit on top. Whatever wins the final CAS
        # writes the backup, so prev-HEAD is still the immediately-prior
        # HEAD rather than the loser's stale value.
        commit(loser, {"a": b"2"})

        history = store.head_history["main"]
        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        assert prev == history[-2], (
            f"prev-HEAD is {prev}, the loser's stale value; the "
            f"immediately-previous HEAD is {history[-2]} "
            f"(history: {history})"
        )

    def test_a_crash_cannot_separate_head_from_its_backup(self):
        """HEAD and its backup move in one write, or neither moves.

        A process that dies at the publishing write leaves the branch
        exactly as it was: HEAD on the previous commit, the backup on
        the one before that. There is no state with HEAD advanced and
        the backup stale.
        """
        store = HookStore()
        v = open_main(store)
        first = commit(v, {"a": b"1"}).commit
        second = commit(v, {"a": b"2"}).commit

        class Died(Exception):
            """Stands in for the process dying mid-commit."""

        def die():
            raise Died

        store.arm_set(BRANCH_HEAD_PREV % "main", die)
        with pytest.raises(Died):
            commit(v, {"a": b"3"})

        assert loads(store.get(BRANCH_HEAD % "main")) == second
        assert loads(store.get(BRANCH_HEAD_PREV % "main")) == first


class TestReadsDoNotWrite:
    """Head resolution on a read path recovers without persisting."""

    def test_opening_a_damaged_branch_does_not_mutate_the_store(self):
        """Opening a worktree is a read, even on a damaged branch."""
        store = Memory()
        v = open_main(store)
        commit(v, {"x": b"1"})
        commit(v, {"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        before = dict(store.items())
        recovered = open_repo(store).worktree("main")
        assert recovered.get("x") == b"1", "recovery must still happen"
        assert dict(store.items()) == before, (
            "a read repaired the store in place; a read-only consumer "
            "cannot do that, and two concurrent readers race each other"
        )

    def test_snapshot_and_worktree_do_not_mutate_the_store(self):
        """The other read entry points hold the same line."""
        store = Memory()
        v = open_main(store)
        commit(v, {"x": b"1"})
        dev = fork(v, "dev")
        commit(dev, {"d": b"1"})
        commit(dev, {"d": b"2"})
        store.set(BRANCH_HEAD % "dev", b"")

        repo = open_repo(store)
        before = dict(store.items())
        assert repo.snapshot(branch="dev")["d"] == b"1"
        assert dict(store.items()) == before, "snapshot wrote to the store"

        assert repo.worktree("dev").get("d") == b"1"
        assert dict(store.items()) == before, "worktree wrote to the store"

    def test_repair_head_is_the_explicit_persisting_call(self):
        """``repair_head`` is how a damaged HEAD is made good on disk."""
        store = Memory()
        v = open_main(store)
        good = commit(v, {"x": b"1"}).commit
        commit(v, {"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        assert repair_head(store, "main") == good
        assert loads(store.get(BRANCH_HEAD % "main")) == good
        # Idempotent, and a no-op on an already-healthy branch.
        assert repair_head(store, "main") == good
        assert open_repo(store).repair_head("main") == good

    def test_repair_head_reports_an_unrecoverable_branch(self):
        """Nothing to recover, and no branch at all, both read as None."""
        store = Memory()
        open_main(store)
        store.set(BRANCH_HEAD % "main", b"")
        for key in [k for k in list(store.keys()) if k.startswith(ROOT_PREFIX)]:
            store.remove(key)
        assert repair_head(store, "main") is None
        assert repair_head(store, "no-such-branch") is None

    def test_a_write_still_heals_a_damaged_head(self):
        """Dropping repair from reads must not strand the write path.

        A corrupt HEAD makes every CAS against it fail, so if nothing
        ever heals it the branch becomes permanently unwritable. The
        heal moves to the writer, where it belongs, and is itself a CAS
        against the exact corrupt bytes — two writers racing it cannot
        both win.
        """
        store = HookStore()
        v = open_main(store)
        good = commit(v, {"x": b"1"}).commit
        commit(v, {"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        writer = open_repo(store).worktree("main")
        assert writer.head == good, "the read should recover"
        assert store.get(BRANCH_HEAD % "main") == b"", "and not persist it"

        result = commit(writer, {"x": b"3"})
        assert result.merged, "a damaged HEAD must not make a branch read-only"
        assert loads(store.get(BRANCH_HEAD % "main")) == result.commit
        history = store.head_history["main"]
        assert history[-1] == result.commit
        assert history[-2] == good, "the heal is itself a recorded HEAD write"
        assert loads(store.get(BRANCH_HEAD_PREV % "main")) == good


class TestLostCasGarbage:
    """A lost CAS leaves its writes for GC, and must not delete them."""

    def test_lost_cas_leaves_collectable_garbage(self):
        """The loser's commits are garbage the sweeps reclaim.

        Nothing is deleted inline. The loser's blobs and HAMT nodes are
        keyed by content, so the winner may legitimately share them.
        The sweep collects the orphan commit, and whatever content only
        it held, once it ages past ``min_age``.

        A lost fast-forward race is retried through the merge path, so
        a conflicting loser surfaces ``MergeConflict`` rather than
        ``ConcurrencyError``. The retry merges from the commit it has
        already built, so there is exactly one orphan for the sweep.
        """
        store = HookStore()
        v = open_main(store)
        commit(v, {"a": b"1"})

        loser = v.repo.worktree("main")
        winner = v.repo.worktree("main")
        store.arm_commit_batch(lambda: commit(winner, {"a": b"winner"}))

        with pytest.raises(MergeConflict):
            commit(loser, {"a": b"loser"})

        # The loser's writes are still there, untouched.
        live_history = set(history(winner.repo))
        orphans = [
            key[len(ROOT_PREFIX) :]
            for key in store.keys()
            if key.startswith(ROOT_PREFIX)
            and key[len(ROOT_PREFIX) :] not in live_history
        ]
        assert len(orphans) == 1, (
            "the retry rewrites the same commit objects, leaving one orphan"
        )
        orphan = orphans[0]
        orphan_nodes = node_hashes(store, orphan)
        assert store.get(blob_key(b"loser")) == b"loser"
        assert store.get(PARENT_COMMIT % orphan) is not None

        # The winner is unaffected, and ordinary GC reclaims the orphan.
        assert open_repo(store).worktree("main").get("a") == b"winner"
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % orphan) is None
        assert store.get(blob_key(b"loser")) is None

        live = open_repo(store).worktree("main")
        unshared = orphan_nodes - node_hashes(store, live.head)
        assert unshared, "the orphan should own at least one node of its own"
        assert not [n for n in unshared if store.get(NODE_PREFIX + n) is not None], (
            "the orphan's own HAMT nodes were not reclaimed"
        )
        assert live.get("a") == b"winner"


class TestRetryNodeAccounting:
    def test_raced_new_key_merge_strands_no_nodes(self):
        """A retry after a lost CAS must keep its first attempt as our side.

        Rebuilding the commit after a lost CAS would mint a second
        commit for the same change (the hash covers the commit time)
        and leave the first behind. Merging from the already built
        commit leaves every written node reachable from live history.
        """
        store = Memory()
        v1 = open_main(store)
        commit(v1, {"base": b"0"})
        v2 = v1.repo.worktree("main")
        race_publish(store, "main", lambda: commit(v1, {"other": b"1"}))
        result = commit(v2, {"mine": b"2"})
        assert result.merged

        reachable: set[str] = set()
        for h in history(v2.repo):
            reachable |= node_hashes(store, h)
        present = {
            key[len(NODE_PREFIX) :]
            for key in store.keys()
            if key.startswith(NODE_PREFIX)
        }
        assert present - reachable == set()


class TestAbsentHeadCannotRecover:
    """A branch with no HEAD is deleted, not damaged.

    Recovery tiers exist for a HEAD that is *present and unusable*.
    Deleting a branch removes the key, so an absent HEAD means the
    branch is gone — and a backup that outlives it must not bring it
    back. Writing the backup after the CAS would open a route to exactly
    that: a writer descheduled between the two, resuming after a
    concurrent delete, recreating only the backup.
    """

    def test_a_writer_racing_a_delete_cannot_bring_the_branch_back(self):
        """A publish that loses to a delete writes nothing at all.

        HEAD and its backup land in one conditional write, so a writer
        whose branch is deleted just before it publishes cannot leave a
        backup behind to resurrect it.
        """
        store = HookStore()
        repo = open_repo(store)
        commit(repo.worktree("main", create=True), {"anchor": b"1"})
        doomed = repo.worktree("doomed", create=True)
        commit(doomed, {"secret": b"classified"})

        def delete_it_mid_write():
            open_repo(store).branches.delete("doomed")

        # Delete the branch just before the writer's publishing write.
        store.arm_set(BRANCH_HEAD_PREV % "doomed", delete_it_mid_write)
        with pytest.raises(UnknownBranchError, match="no HEAD"):
            commit(doomed, {"secret": b"classified-v2"})

        assert store.get(BRANCH_HEAD % "doomed") is None, "the delete should have won"
        assert store.get(BRANCH_HEAD_PREV % "doomed") is None
        assert _resolve_head(store, "doomed") is None

    def test_a_corrupt_but_present_head_still_recovers(self):
        """The gate must not cost the tier its actual purpose."""
        store = HookStore()
        v = open_main(store)
        first = commit(v, {"k": b"1"}).commit
        commit(v, {"k": b"2"})
        store.set(BRANCH_HEAD % "main", b"")
        assert _resolve_head(store, "main") == first

    def test_a_healthy_branch_is_unaffected(self):
        store = HookStore()
        head = commit(open_main(store), {"k": b"1"}).commit
        assert _resolve_head(store, "main") == head

    def test_creating_a_branch_drops_any_stale_backup(self):
        """Creating a branch means that name has no previous HEAD.

        A backup can outlive a branch deletion (the delayed write above).
        While the name is unclaimed the gate makes it harmless — but
        creating the branch again would make it reachable again, since
        the prev-HEAD tier only requires HEAD to *exist*, and a fresh
        branch's HEAD can be corrupted before its first successful CAS.
        """
        store = HookStore()
        vk = open_main(store)
        commit(vk, {"anchor": b"1"})
        store.set(BRANCH_HEAD_PREV % "revived", dumps("deadbeef" * 5))

        vk.repo.branches.create("revived", at=vk.head)
        assert store.get(BRANCH_HEAD_PREV % "revived") is None, (
            "creating the branch left a stale backup it could recover onto"
        )

    def test_fresh_initialization_drops_any_stale_backup(self):
        """The other path that creates a branch for an unclaimed name."""
        store = HookStore()
        commit(open_main(store), {"anchor": b"1"})
        store.set(BRANCH_HEAD_PREV % "revived", dumps("deadbeef" * 5))

        open_repo(store).worktree("revived", create=True)
        assert store.get(BRANCH_HEAD_PREV % "revived") is None


class TestTheBackupIsExact:
    """The backup names the commit HEAD held immediately before.

    HEAD and ``__branch_head_prev__`` are written in one atomic write
    conditioned on HEAD, so however writers interleave, the backup is
    always HEAD's immediate predecessor on this branch.
    """

    def test_a_paused_writer_publishes_with_the_right_backup(self):
        store = HookStore()
        writer = open_main(store)
        commit(writer, {"k": b"1"})

        landed = {}

        def other_writer_advances():
            other = open_repo(store).worktree("main")
            landed["third"] = commit(other, {"k2": b"3"}).commit

        # Pause the writer just before its publishing write; another
        # writer publishes first, so this one merges and publishes after.
        store.arm_set(BRANCH_HEAD_PREV % "main", other_writer_advances)
        result = commit(writer, {"k": b"2"})

        head = loads(store.get(BRANCH_HEAD % "main"))
        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        history = store.head_history["main"]

        assert result.strategy == "three_way"
        assert head == result.commit
        assert prev == landed["third"] == history[-2]


class TestRepairHeadReturnValue:
    """``repair_head`` reports the store, not its own attempt."""

    def test_returns_what_head_names_when_another_process_wins(self):
        store = HookStore()
        v = open_main(store)
        commit(v, {"k": b"1"})
        second = commit(v, {"k": b"2"}).commit
        store.set(BRANCH_HEAD % "main", b"")  # corrupt -> resolves to `first`

        def another_process_repairs_it_differently():
            store.set(BRANCH_HEAD % "main", dumps(second))

        # Fire between the resolve's read of HEAD and the heal's, so the
        # heal CAS finds a HEAD it must not touch and declines.
        store.arm_get(
            BRANCH_HEAD % "main", another_process_repairs_it_differently, nth=2
        )

        returned = repair_head(store, "main")
        actual = loads(store.get(BRANCH_HEAD % "main"))

        assert actual == second, "the other process should hold HEAD"
        assert returned == actual, (
            f"repair_head returned {returned}, but HEAD names {actual}; its "
            f"contract is 'the commit HEAD now names', and returning the "
            f"stale candidate hands the caller an older commit than the store has"
        )

    def test_healthy_branch_is_a_no_op_that_still_reports_head(self):
        store = HookStore()
        v = open_main(store)
        commit(v, {"k": b"1"})
        head = commit(v, {"k": b"2"}).commit
        assert repair_head(store, "main") == head

    def test_missing_branch_returns_none(self):
        store = HookStore()
        commit(open_main(store), {"k": b"1"})
        store.remove(BRANCH_HEAD % "main")
        store.remove(BRANCH_HEAD_PREV % "main")
        assert repair_head(store, "nonexistent") is None


class TestRedoMintsANewCommit:
    """Rolling a branch back and redoing a change mints a new commit.

    The commit hash covers the commit time, so a redo is never the
    commit it repeats. A sweep deleting the old one — even a sweep that
    has already judged it old garbage when the redo lands — deletes
    nothing the redo needs: the redo's metadata lives under its own
    hash, and its blob and nodes are content, which the incremental
    sweep leaves alone.
    """

    def test_a_redo_is_a_new_commit_with_the_same_root(self):
        store = Memory()
        v = open_main(store)
        first = commit(v, {"a": b"1"}).commit
        second = commit(v, {"a": b"2"}).commit

        v.reset(first)
        commit(v, {"a": b"2"})
        assert v.head != second
        assert root_of(store, v.head) == root_of(store, second)

    def test_a_redo_under_a_sweep_loses_nothing(self):
        """A writer that starts mid-sweep waits for it, then lands whole."""
        store = HookStore()
        v = open_main(store)
        first = commit(v, {"a": b"1"}).commit
        second = commit(v, {"a": b"2"}).commit
        v.reset(first)
        age_commits(store, 10_000)

        # Once the sweep has read the orphan's age and believes it old,
        # another thread redoes the same change onto the branch.
        redone: list[str] = []
        writer = threading.Thread(
            target=lambda: redone.append(commit(v, {"a": b"2"}).commit)
        )
        store.arm_get(COMMIT_TIME % second, writer.start)
        assert clean_orphans(store, min_age=3600) == 1
        writer.join(timeout=10)

        assert store.get(COMMIT_ROOT % second) is None
        assert redone and redone[0] != second
        assert loads(store.get(BRANCH_HEAD % "main")) == redone[0]
        assert _resolve_head(store, "main") == redone[0]
        assert open_repo(store).worktree("main").get("a") == b"2"


class TestScanRecoveryIsOptIn:
    """The commit scan is a capability, not a default.

    When HEAD is unresolvable *and* the prev-HEAD backup is gone, the
    information needed to answer "what did this branch point at" is not
    in the store. A scan picks the newest tip no healthy branch claims —
    and a deleted branch's commits are unclaimed by definition until a
    sweep collects them. So the scan could serve one branch another
    branch's deleted data, with nothing but a ``logger.warning`` to say
    so.

    ``None`` — "this branch is unrecoverable" — is honest and
    actionable. The scan is still available to anyone who wants it,
    passed in explicitly.
    """

    @staticmethod
    def _leaky_store():
        """A deleted branch's tip, and an unrelated doubly-damaged HEAD.

        No race and no seam: an ordinary branch delete, then main's HEAD
        corrupted with its backup removed. ``tmp``'s commits are young
        orphans, unclaimed, and therefore scan candidates.
        """
        store = Memory()
        v = open_main(store)
        commit(v, {"anchor": b"1"})

        tmp = fork(v, "tmp")
        commit(tmp, {"classified": b"top-secret-payload"})
        tmp_tip = tmp.head
        v.repo.branches.delete("tmp")

        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        return store, tmp_tip

    def test_a_doubly_damaged_branch_does_not_inherit_deleted_data(self):
        """The leak: main resolving onto a deleted branch's tip."""
        store, tmp_tip = self._leaky_store()

        resolved = _resolve_head(store, "main")
        assert resolved is None, (
            f"main resolved to {resolved}, which is the deleted branch "
            f"'tmp''s tip ({tmp_tip}) — a branch main never had. The "
            f"commit scan served another branch's deleted data."
            if resolved == tmp_tip
            else f"main resolved to {resolved}; an unresolvable HEAD with "
            f"no backup must report None"
        )

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            open_repo(store).worktree("main")
        assert repair_head(store, "main") is None

    def test_the_scan_still_recovers_when_it_is_asked_for(self):
        """Relocation, not removal: the capability is one argument away."""
        store, tmp_tip = self._leaky_store()

        assert (
            _resolve_head(
                store, "main", recover_from_corrupt_head=recover_by_commit_scan
            )
            == tmp_tip
        )

        repo = open_repo(store, recover_from_corrupt_head=recover_by_commit_scan)
        assert repo.worktree("main").get("anchor") == b"1"
        assert (
            repair_head(store, "main", recover_from_corrupt_head=recover_by_commit_scan)
            == tmp_tip
        )
        assert loads(store.get(BRANCH_HEAD % "main")) == tmp_tip

    def test_a_healthy_branch_resolves_without_a_recoverer(self):
        """The first tier, HEAD itself, needs no recoverer."""
        store = Memory()
        head = commit(open_main(store), {"k": b"1"}).commit
        assert _resolve_head(store, "main") == head
        assert open_repo(store).worktree("main").head == head

    def test_a_corrupt_head_still_recovers_from_its_backup(self):
        """The backup tier needs no recoverer — the backup is real
        information."""
        store = Memory()
        v = open_main(store)
        first = commit(v, {"k": b"1"}).commit
        commit(v, {"k": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        assert store.get(BRANCH_HEAD_PREV % "main") is not None
        assert _resolve_head(store, "main") == first
        assert open_repo(store).worktree("main").get("k") == b"1"


class TestRecovererThreading:
    """A repository's recoverer applies to every resolve it makes."""

    @staticmethod
    def _spy():
        """A recoverer that records its calls and defers to the scan."""
        calls: list[str] = []

        def recoverer(store, branch):
            calls.append(branch)
            return recover_by_commit_scan(store, branch)

        return recoverer, calls

    @staticmethod
    def _healthy(branch: str):
        """A store with a healthy ``main`` and, if named, ``branch``
        forked from it with a commit of its own; and ``branch``'s tip."""
        store = Memory()
        v = open_main(store)
        commit(v, {"anchor": b"1"})
        if branch != "main":
            other = fork(v, branch)
            commit(other, {"k": b"payload"})
            return store, other.head
        return store, v.head

    @staticmethod
    def _damage(store, branch: str) -> None:
        """Corrupt ``branch``'s HEAD and remove its backup."""
        store.set(BRANCH_HEAD % branch, b"")
        store.remove(BRANCH_HEAD_PREV % branch)

    def _damaged(self, branch: str):
        """A store with a healthy ``main`` and ``branch`` doubly damaged."""
        store, tip = self._healthy(branch)
        self._damage(store, branch)
        return store, tip

    def test_a_commit_takes_a_recoverer_that_cannot_say(self):
        # A recoverer answering None leaves the branch unrecoverable; the
        # commit then reports the branch, not a failure of its own.
        store, tip = self._healthy("main")
        v = open_repo(store, recover_from_corrupt_head=lambda s, b: None).worktree(
            "main"
        )
        assert v.head == tip
        self._damage(store, "main")
        with pytest.raises(UnknownBranchError):
            commit(v, {"k": b"2"})

    def test_branch_lookup_uses_the_recoverer(self):
        store, tip = self._damaged("main")
        recoverer, calls = self._spy()

        repo = open_repo(store, recover_from_corrupt_head=recoverer)
        assert repo.branches["main"] == tip
        assert calls == ["main"]

        with pytest.raises(CorruptHeadError):
            open_repo(store).branches["main"]

    def test_refresh_uses_the_recoverer(self):
        store, tip = self._healthy("main")
        recoverer, calls = self._spy()
        v = open_repo(store, recover_from_corrupt_head=recoverer).worktree("main")
        plain = open_repo(store).worktree("main")
        self._damage(store, "main")

        v.refresh()
        assert v.head == tip
        assert calls == ["main"]

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            plain.refresh()

    def test_opening_a_worktree_uses_the_recoverer(self):
        store, _ = self._damaged("dev")
        recoverer, calls = self._spy()

        repo = open_repo(store, recover_from_corrupt_head=recoverer)
        assert repo.worktree("dev").get("k") == b"payload"
        assert calls == ["dev"]

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            open_repo(store).worktree("dev")

    def test_snapshot_uses_the_recoverer(self):
        store, _ = self._damaged("dev")
        recoverer, calls = self._spy()

        repo = open_repo(store, recover_from_corrupt_head=recoverer)
        assert repo.snapshot(branch="dev")["k"] == b"payload"
        assert calls == ["dev"]

        with pytest.raises(CorruptHeadError):
            open_repo(store).snapshot(branch="dev")

    def test_every_worktree_inherits_it(self):
        """Each worktree a repository hands out carries its recoverer."""
        store, tip = self._damaged("main")
        recoverer, _ = self._spy()

        repo = open_repo(store, recover_from_corrupt_head=recoverer)

        # A second worktree stays on the damaged branch, so this is
        # behaviour: only an inherited recoverer resolves it.
        assert repo.worktree("main").head == tip
        with pytest.raises(CorruptHeadError):
            open_repo(store).worktree("main")

        repo.branches.create("forked", at=tip)
        assert repo.worktree("forked")._engine._recover is recoverer

    def test_gc_never_uses_it(self):
        """GC must not decide reachability from a guess.

        Deliberate, not an oversight: a wrong answer here marks the
        wrong commits live, so real garbage survives and a guessed tip
        gets walked as though it were the branch's own history. The
        sweep sees only what the store actually claims, even when the
        repository carries a recoverer.
        """
        store, dev_tip = self._damaged("dev")
        recoverer, calls = self._spy()
        age_commits(store, 7200)

        repo = open_repo(store, recover_from_corrupt_head=recoverer)
        repo.worktree("main")
        assert repo.gc() == 1
        assert calls == [], (
            f"gc consulted the recoverer for {calls} — the mark "
            f"phase must not resolve a branch by guessing"
        )
        assert store.get(COMMIT_ROOT % dev_tip) is None, (
            "the unresolvable branch's tip was marked live off a guess"
        )

    def test_the_documented_entry_point_can_opt_in(self):
        """A seam unreachable through ``Repo`` is not a seam.

        ``Repo`` is how the docs tell people to open a store with
        options, so the opt-in has to be expressible there.
        """
        repo = Repo(Memory(), recover_from_corrupt_head=recover_by_commit_scan)
        wt = repo.worktree("main", create=True)
        wt["k"] = 1
        wt.commit()
        assert wt["k"] == 1
        assert wt._engine._recover is recover_by_commit_scan

        plain = Repo(Memory()).worktree("main", create=True)
        assert plain._engine._recover is None

    def test_a_recoverer_returning_a_dangling_hash_is_rejected(self):
        """A pluggable tier is not a trusted tier.

        HEAD and its backup are validated when read: a string naming a
        commit whose root exists. The scan satisfies that by
        construction — its answer comes straight off a
        ``__commit_root__`` key — but a caller-supplied recoverer does
        not, and ``_resolve_head`` promises a *valid* commit or None.

        An unchecked answer is worse than no answer: ``repair_head``
        makes it durable, replacing obviously-corrupt HEAD bytes with a
        plausible hash naming nothing — harder to diagnose than the
        damage it replaced.
        """
        store = HookStore()
        commit(open_main(store), {"k": b"1"})
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        dangling = "deadbeef" * 5

        assert (
            _resolve_head(
                store, "main", recover_from_corrupt_head=lambda s, b: dangling
            )
            is None
        )
        assert (
            repair_head(store, "main", recover_from_corrupt_head=lambda s, b: dangling)
            is None
        )
        assert store.get(BRANCH_HEAD % "main") == b"", (
            "a rejected candidate was written into HEAD, replacing visible "
            "damage with a plausible hash that names nothing"
        )

    def test_a_recoverer_returning_a_non_string_is_rejected(self):
        store = HookStore()
        commit(open_main(store), {"k": b"1"})
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        assert (
            _resolve_head(store, "main", recover_from_corrupt_head=lambda s, b: 12345)
            is None
        )

    def test_a_valid_recoverer_is_still_honoured(self):
        """The check must not cost the tier its purpose."""
        store = HookStore()
        head = commit(open_main(store), {"k": b"1"}).commit
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        assert (
            _resolve_head(store, "main", recover_from_corrupt_head=lambda s, b: head)
            == head
        )
