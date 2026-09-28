"""HEAD backup, recovery, and repair semantics.

Three contracts live here.

**prev-HEAD names a real, immediately-prior HEAD.**
``__branch_head_prev__<branch>`` is the input to ``_resolve_head``'s
recovery fallback, so what it names decides what a damaged branch
recovers *to*. It must only ever name a value the
``__branch_head__<branch>`` key actually held, and specifically the one
it held immediately before its current value. Naming an older real HEAD
silently drops commits; naming a commit that was never HEAD hands the
branch a lineage it never had — the resurrection class that
``delete_branch`` leaving its prev-HEAD behind produced in v0.3.1.

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

from kvgit import ConcurrencyError, MergeConflict
from kvgit.encoding import dumps, loads
from kvgit.errors import CorruptHeadError, UnknownBranchError
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    COMMIT_TIME,
    PARENT_COMMIT,
    VersionedKV,
    _load_root,
    _resolve_head,
    blob_key,
    clean_orphans,
    recover_by_commit_scan,
    repair_head,
)

HEAD_PREFIX = BRANCH_HEAD.replace("%s", "")
ROOT_PREFIX = COMMIT_ROOT.replace("%s", "")


class HookStore(Memory):
    """Memory store that records HEAD history and arms one-shot seams.

    ``head_history[branch]`` is the ground truth for "was this commit
    ever HEAD of that branch": every successful write to a
    ``__branch_head__`` key is appended, whichever store method made
    it. Assertions about prev-HEAD compare against this rather than
    against what the code under test believes.

    Two seams, both one-shot:

    * ``arm_commit_batch`` fires when a commit's write batch is written
      — after its writer has read HEAD, before it reaches its CAS.
    * ``arm_get`` fires after a chosen key's value has been read but
      before the caller sees it, on the *nth* read of that key.
    * ``arm_set`` fires before a chosen key is written — alone, or as
      part of an atomic batch — so a writer can be paused while another
      completes.

    Both are points every version of the code passes through, so a test
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

    def get(self, key: str) -> bytes | None:
        value = super().get(key)
        armed = self._on_get.get(key)
        if armed is not None:
            armed[1] -= 1
            if armed[1] <= 0:
                del self._on_get[key]
                armed[0]()
        return value

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


def age_commits(store, seconds: float) -> None:
    """Backdate every ``__commit_time__`` so orphans clear ``min_age``."""
    now = time.time()
    prefix = COMMIT_TIME.replace("%s", "")
    for key in list(store.keys()):
        if key.startswith(prefix):
            store.set(key, dumps(now - seconds))


def node_hashes(store, commit_hash: str) -> set[str]:
    """Every HAMT node hash reachable from a commit's keyset root."""
    root = _load_root(store, commit_hash)
    if root is None:
        return set()
    _, nodes = Keyset(store, root=root).walk()
    return set(nodes)


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

        The scan is opt-in since v0.3.4, so the handle asks for it. The
        invariant is unchanged: whatever a recoverer returns is still a
        candidate, and prev-HEAD must not be where it becomes durable.
        """
        store = HookStore()
        v = VersionedKV(store, recover_from_corrupt_head=recover_by_commit_scan)
        v.commit({"a": b"1"})

        # A commit that is never main's HEAD: a branch tip that outlives
        # its branch (young orphans survive the min_age guard).
        v.create_branch("tmp")
        tmp = VersionedKV(store, branch="tmp")
        tmp.commit({"t": b"tmp-only"})
        tmp_tip = tmp.current_commit
        v.delete_branch("tmp")

        # The damage the recovery path exists for: unreadable HEAD, and
        # no backup to fall back on.
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")

        try:
            v.commit({"a": b"2"})
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
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit

        loser = VersionedKV(store, commit_hash=first)
        winner = VersionedKV(store, commit_hash=first)

        def winner_lands_twice() -> None:
            winner.commit({"w": b"1"})
            winner.commit({"w": b"2"})

        store.arm_commit_batch(winner_lands_twice)

        # Issue #39: the lost fast-forward race merges internally instead
        # of raising, so the loser lands a merge commit on top. The
        # invariant under test is unchanged: whatever wins the final CAS
        # writes the backup, so prev-HEAD is still the immediately-prior
        # HEAD rather than the loser's stale value.
        loser.commit({"a": b"2"})

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
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit

        class Died(Exception):
            """Stands in for the process dying mid-commit."""

        def die():
            raise Died

        store.arm_set(BRANCH_HEAD_PREV % "main", die)
        with pytest.raises(Died):
            v.commit({"a": b"3"})

        assert loads(store.get(BRANCH_HEAD % "main")) == second
        assert loads(store.get(BRANCH_HEAD_PREV % "main")) == first


class TestReadsDoNotWrite:
    """Head resolution on a read path recovers without persisting."""

    def test_opening_a_damaged_branch_does_not_mutate_the_store(self):
        """Constructing a handle is a read, even on a damaged branch."""
        store = Memory()
        v = VersionedKV(store)
        v.commit({"x": b"1"})
        v.commit({"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        before = dict(store.items())
        recovered = VersionedKV(store)
        assert recovered.get("x") == b"1", "recovery must still happen"
        assert dict(store.items()) == before, (
            "a read repaired the store in place; a read-only consumer "
            "cannot do that, and two concurrent readers race each other"
        )

    def test_peek_and_switch_do_not_mutate_the_store(self):
        """The other read entry points hold the same line."""
        store = Memory()
        v = VersionedKV(store)
        v.commit({"x": b"1"})
        v.create_branch("dev")
        dev = VersionedKV(store, branch="dev")
        dev.commit({"d": b"1"})
        dev.commit({"d": b"2"})
        store.set(BRANCH_HEAD % "dev", b"")

        before = dict(store.items())
        assert v.peek("d", branch="dev") == b"1"
        assert dict(store.items()) == before, "peek wrote to the store"

        v.switch_branch("dev")
        assert v.get("d") == b"1"
        assert dict(store.items()) == before, "switch_branch wrote to the store"

    def test_repair_head_is_the_explicit_persisting_call(self):
        """``repair_head`` is how a damaged HEAD is made good on disk."""
        store = Memory()
        v = VersionedKV(store)
        v.commit({"x": b"1"})
        good = v.current_commit
        v.commit({"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        assert repair_head(store, "main") == good
        assert loads(store.get(BRANCH_HEAD % "main")) == good
        # Idempotent, and a no-op on an already-healthy branch.
        assert repair_head(store, "main") == good
        assert VersionedKV(store).repair_head() == good

    def test_repair_head_reports_an_unrecoverable_branch(self):
        """Nothing to recover, and no branch at all, both read as None."""
        store = Memory()
        VersionedKV(store)
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
        v = VersionedKV(store)
        v.commit({"x": b"1"})
        good = v.current_commit
        v.commit({"x": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        writer = VersionedKV(store)
        assert writer.current_commit == good, "the read should recover"
        assert store.get(BRANCH_HEAD % "main") == b"", "and not persist it"

        result = writer.commit({"x": b"3"})
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
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit

        loser = VersionedKV(store, commit_hash=first)
        winner = VersionedKV(store, commit_hash=first)
        store.arm_commit_batch(lambda: winner.commit({"a": b"winner"}))

        with pytest.raises(MergeConflict):
            loser.commit({"a": b"loser"})

        # The loser's writes are still there, untouched.
        live_history = set(winner.history())
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
        assert VersionedKV(store).get("a") == b"winner"
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % orphan) is None
        assert store.get(blob_key(b"loser")) is None

        live = VersionedKV(store)
        unshared = orphan_nodes - node_hashes(store, live.current_commit)
        assert unshared, "the orphan should own at least one node of its own"
        assert not [
            n for n in unshared if store.get(Keyset.DEFAULT_PREFIX + n) is not None
        ], "the orphan's own HAMT nodes were not reclaimed"
        assert live.get("a") == b"winner"


class TestRetryNodeAccounting:
    def test_raced_new_key_merge_strands_no_nodes(self):
        """The #39 retry must keep its first attempt as our side.

        Rebuilding the commit after a lost CAS would mint a second
        commit for the same change (the hash covers the commit time)
        and leave the first behind. Merging from the already built
        commit leaves every written node reachable from live history.
        """
        store = Memory()
        v1 = VersionedKV(store)
        v1.commit({"base": b"0"})
        v2 = VersionedKV(store)
        real_cas = v2._cas_head
        raced = False

        def cas(expected, new_head):
            nonlocal raced
            if not raced:
                raced = True
                v1.commit({"other": b"1"})
            return real_cas(expected, new_head)

        v2._cas_head = cas
        result = v2.commit({"mine": b"2"})
        assert result.merged

        reachable: set[str] = set()
        for h in v2.history(all_parents=True):
            reachable |= node_hashes(store, h)
        present = {
            key[len(Keyset.DEFAULT_PREFIX) :]
            for key in store.keys()
            if key.startswith(Keyset.DEFAULT_PREFIX)
        }
        assert present - reachable == set()


class TestAbsentHeadCannotRecover:
    """A branch with no HEAD is deleted, not damaged.

    Recovery tiers exist for a HEAD that is *present and unusable*.
    ``delete_branch`` removes the key, so an absent HEAD means the
    branch is gone — and a backup that outlives it must not bring it
    back. Writing the backup after the CAS once opened a route to
    exactly that: a writer descheduled between the two, resuming after a
    concurrent delete, recreated only the backup.

    Reviving a branch from a lone backup is the v0.3.1 failure class,
    reached from a new direction.
    """

    def test_a_writer_racing_a_delete_cannot_bring_the_branch_back(self):
        """A publish that loses to a delete writes nothing at all.

        HEAD and its backup land in one conditional write, so a writer
        whose branch is deleted just before it publishes cannot leave a
        backup behind to resurrect it.
        """
        store = HookStore()
        VersionedKV(store).commit({"anchor": b"1"})
        doomed = VersionedKV(store, branch="doomed")
        doomed.commit({"secret": b"classified"})

        def delete_it_mid_write():
            VersionedKV(store).delete_branch("doomed")

        # Delete the branch just before the writer's publishing write.
        store.arm_set(BRANCH_HEAD_PREV % "doomed", delete_it_mid_write)
        with pytest.raises(UnknownBranchError, match="no HEAD"):
            doomed.commit({"secret": b"classified-v2"})

        assert store.get(BRANCH_HEAD % "doomed") is None, "the delete should have won"
        assert store.get(BRANCH_HEAD_PREV % "doomed") is None
        assert _resolve_head(store, "doomed") is None

    def test_a_corrupt_but_present_head_still_recovers(self):
        """The gate must not cost the tier its actual purpose."""
        store = HookStore()
        v = VersionedKV(store)
        first = v.commit({"k": b"1"}).commit
        v.commit({"k": b"2"})
        store.set(BRANCH_HEAD % "main", b"")
        assert _resolve_head(store, "main") == first

    def test_a_healthy_branch_is_unaffected(self):
        store = HookStore()
        head = VersionedKV(store).commit({"k": b"1"}).commit
        assert _resolve_head(store, "main") == head

    def test_creating_a_branch_drops_any_stale_backup(self):
        """Installing an anchor means that name has no previous HEAD.

        A backup can outlive ``delete_branch`` (the delayed write above).
        While the name is unclaimed the gate makes it harmless — but
        re-installing an anchor would make it reachable again, since the
        prev-HEAD tier only requires HEAD to *exist*, and a fresh branch's
        HEAD can be corrupted before its first successful CAS.
        """
        store = HookStore()
        vk = VersionedKV(store)
        vk.commit({"anchor": b"1"})
        store.set(BRANCH_HEAD_PREV % "revived", dumps("deadbeef" * 5))

        vk.create_branch("revived")
        assert store.get(BRANCH_HEAD_PREV % "revived") is None, (
            "create_branch left a stale backup the new branch could recover onto"
        )

    def test_fresh_initialization_drops_any_stale_backup(self):
        """The other path that installs an anchor for an unclaimed name."""
        store = HookStore()
        VersionedKV(store).commit({"anchor": b"1"})
        store.set(BRANCH_HEAD_PREV % "revived", dumps("deadbeef" * 5))

        VersionedKV(store, branch="revived")
        assert store.get(BRANCH_HEAD_PREV % "revived") is None


class TestTheBackupIsExact:
    """The backup names the commit HEAD held immediately before.

    HEAD and ``__branch_head_prev__`` are written in one atomic write
    conditioned on HEAD, so however writers interleave, the backup is
    always HEAD's immediate predecessor on this branch.
    """

    def test_a_paused_writer_publishes_with_the_right_backup(self):
        store = HookStore()
        writer = VersionedKV(store)
        writer.commit({"k": b"1"})

        landed = {}

        def other_writer_advances():
            other = VersionedKV(store)
            landed["third"] = other.commit({"k2": b"3"}).commit

        # Pause the writer just before its publishing write; another
        # writer publishes first, so this one merges and publishes after.
        store.arm_set(BRANCH_HEAD_PREV % "main", other_writer_advances)
        result = writer.commit({"k": b"2"})

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
        v = VersionedKV(store)
        v.commit({"k": b"1"})
        second = v.commit({"k": b"2"}).commit
        store.set(BRANCH_HEAD % "main", b"")  # corrupt -> resolves to `first`

        def another_process_repairs_it_differently():
            store.set(BRANCH_HEAD % "main", dumps(second))

        # Fire between _resolve_head's read and _heal_head's, so the heal
        # CAS finds a HEAD it must not touch and declines.
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
        v = VersionedKV(store)
        v.commit({"k": b"1"})
        head = v.commit({"k": b"2"}).commit
        assert repair_head(store, "main") == head

    def test_missing_branch_returns_none(self):
        store = HookStore()
        VersionedKV(store).commit({"k": b"1"})
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
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit

        v.reset_to(first)
        v.commit({"a": b"2"})
        assert v.current_commit != second
        assert _load_root(store, v.current_commit) == _load_root(store, second)

    def test_a_redo_under_a_sweep_loses_nothing(self):
        """A writer that starts mid-sweep waits for it, then lands whole."""
        store = HookStore()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit
        v.reset_to(first)
        age_commits(store, 10_000)

        # Once the sweep has read the orphan's age and believes it old,
        # another thread redoes the same change onto the branch.
        redone: list[str] = []
        writer = threading.Thread(
            target=lambda: redone.append(v.commit({"a": b"2"}).commit)
        )
        store.arm_get(COMMIT_TIME % second, writer.start)
        assert clean_orphans(store, min_age=3600) == 1
        writer.join(timeout=10)

        assert store.get(COMMIT_ROOT % second) is None
        assert redone and redone[0] != second
        assert loads(store.get(BRANCH_HEAD % "main")) == redone[0]
        assert _resolve_head(store, "main") == redone[0]
        assert VersionedKV(store).get("a") == b"2"


class TestScanRecoveryIsOptIn:
    """The commit scan is a capability, not a default.

    When HEAD is unresolvable *and* the prev-HEAD backup is gone, the
    information needed to answer "what did this branch point at" is not
    in the store. The scan tier answered anyway, by picking the newest
    tip no healthy branch claims — and a deleted branch's commits are
    unclaimed by definition until ``clean_orphans`` collects them. So
    the scan could serve one branch another branch's deleted data, with
    nothing but a ``logger.warning`` to say so.

    ``None`` — "this branch is unrecoverable" — is honest and
    actionable. The scan is still available to anyone who wants it,
    passed in explicitly.
    """

    @staticmethod
    def _leaky_store():
        """A deleted branch's tip, and an unrelated doubly-damaged HEAD.

        No race and no seam: an ordinary ``delete_branch``, then main's
        HEAD corrupted with its backup removed. ``tmp``'s commits are
        young orphans, so the sweep inside ``delete_branch`` leaves them
        in place — unclaimed, and therefore scan candidates.
        """
        store = Memory()
        v = VersionedKV(store)
        v.commit({"anchor": b"1"})

        v.create_branch("tmp")
        tmp = VersionedKV(store, branch="tmp")
        tmp.commit({"classified": b"top-secret-payload"})
        tmp_tip = tmp.current_commit
        v.delete_branch("tmp")

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
            VersionedKV(store)
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

        v = VersionedKV(store, recover_from_corrupt_head=recover_by_commit_scan)
        assert v.get("anchor") == b"1"
        assert (
            repair_head(store, "main", recover_from_corrupt_head=recover_by_commit_scan)
            == tmp_tip
        )
        assert loads(store.get(BRANCH_HEAD % "main")) == tmp_tip

    def test_a_healthy_branch_resolves_without_a_recoverer(self):
        """Tier 1 is untouched."""
        store = Memory()
        head = VersionedKV(store).commit({"k": b"1"}).commit
        assert _resolve_head(store, "main") == head
        assert VersionedKV(store).current_commit == head

    def test_a_corrupt_head_still_recovers_from_its_backup(self):
        """Tier 2 is untouched — the backup is real information."""
        store = Memory()
        v = VersionedKV(store)
        first = v.commit({"k": b"1"}).commit
        v.commit({"k": b"2"})
        store.set(BRANCH_HEAD % "main", b"")

        assert store.get(BRANCH_HEAD_PREV % "main") is not None
        assert _resolve_head(store, "main") == first
        assert VersionedKV(store).get("k") == b"1"


class TestRecovererThreading:
    """A handle's recoverer applies to every resolve the handle makes."""

    @staticmethod
    def _spy():
        """A recoverer that records its calls and defers to the scan."""
        calls: list[tuple[str, ...]] = []

        def recoverer(store, branch):
            calls.append(branch)
            return recover_by_commit_scan(store, branch)

        return recoverer, calls

    @staticmethod
    def _damaged(branch: str):
        """A store with a healthy ``main`` and ``branch`` doubly damaged."""
        store = Memory()
        v = VersionedKV(store)
        v.commit({"anchor": b"1"})
        if branch != "main":
            v.create_branch(branch)
            other = VersionedKV(store, branch=branch)
            other.commit({"k": b"payload"})
            tip = other.current_commit
        else:
            tip = v.current_commit
        store.set(BRANCH_HEAD % branch, b"")
        store.remove(BRANCH_HEAD_PREV % branch)
        return store, tip

    def test_latest_head_uses_the_recoverer(self):
        store, tip = self._damaged("main")
        recoverer, calls = self._spy()

        v = VersionedKV(store, commit_hash=tip, recover_from_corrupt_head=recoverer)
        assert v.latest_head == tip
        assert calls == ["main"]

        assert VersionedKV(store, commit_hash=tip).latest_head is None

    def test_refresh_uses_the_recoverer(self):
        store, tip = self._damaged("main")
        recoverer, calls = self._spy()

        v = VersionedKV(store, commit_hash=tip, recover_from_corrupt_head=recoverer)
        v.refresh()
        assert v.current_commit == tip
        assert calls == ["main"]

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            VersionedKV(store, commit_hash=tip).refresh()

    def test_switch_branch_uses_the_recoverer(self):
        store, _ = self._damaged("dev")
        recoverer, calls = self._spy()

        v = VersionedKV(store, recover_from_corrupt_head=recoverer)
        v.switch_branch("dev")
        assert v.get("k") == b"payload"
        assert calls == ["dev"]

        with pytest.raises(CorruptHeadError, match="corrupt and unrecoverable"):
            VersionedKV(store).switch_branch("dev")

    def test_peek_uses_the_recoverer(self):
        store, _ = self._damaged("dev")
        recoverer, calls = self._spy()

        v = VersionedKV(store, recover_from_corrupt_head=recoverer)
        assert v.peek("k", branch="dev") == b"payload"
        assert calls == ["dev"]

        assert VersionedKV(store).peek("k", branch="dev") is None

    def test_derived_handles_inherit_it(self):
        """``checkout`` and ``create_branch`` hand back the same setting."""
        store, tip = self._damaged("main")
        recoverer, _ = self._spy()

        v = VersionedKV(store, commit_hash=tip, recover_from_corrupt_head=recoverer)

        # ``checkout`` stays on the damaged branch, so this is behaviour:
        # only an inherited recoverer resolves it.
        assert v.checkout(tip).latest_head == tip
        assert VersionedKV(store, commit_hash=tip).checkout(tip).latest_head is None

        assert v.create_branch("forked")._recover_from_corrupt_head is recoverer

    def test_clean_orphans_never_uses_it(self):
        """GC must not decide reachability from a guess.

        Deliberate, not an oversight: a wrong answer here marks the
        wrong commits live, so real garbage survives and a guessed tip
        gets walked as though it were the branch's own history. The
        sweep sees only what the store actually claims, even when the
        caller's handle carries a recoverer.
        """
        store, dev_tip = self._damaged("dev")
        recoverer, calls = self._spy()
        age_commits(store, 7200)

        v = VersionedKV(store, recover_from_corrupt_head=recoverer)
        assert v.clean_orphans() == 1
        assert calls == [], (
            f"clean_orphans consulted the recoverer for {calls} — the mark "
            f"phase must not resolve a branch by guessing"
        )
        assert store.get(COMMIT_ROOT % dev_tip) is None, (
            "the unresolvable branch's tip was marked live off a guess"
        )

    def test_the_documented_entry_point_can_opt_in(self):
        """A seam unreachable through ``Repo`` is not a seam.

        ``Repo`` is how the docs tell people to open a store with
        options, so the opt-in has to be expressible there and not only
        by constructing a ``VersionedKV`` by hand.
        """
        from kvgit import Repo
        from kvgit.versioned.kv import recover_by_commit_scan

        repo = Repo(Memory(), recover_from_corrupt_head=recover_by_commit_scan)
        wt = repo.worktree("main", create=True)
        wt["k"] = 1
        wt.commit()
        assert wt["k"] == 1
        assert wt._engine._recover_from_corrupt_head is recover_by_commit_scan

        plain = Repo(Memory()).worktree("main", create=True)
        assert plain._engine._recover_from_corrupt_head is None

    def test_a_recoverer_returning_a_dangling_hash_is_rejected(self):
        """A pluggable tier is not a trusted tier.

        Tiers 1 and 2 validate what they read: a string naming a commit
        whose root exists. The scan used to satisfy that by construction
        — its answer came straight off a ``__commit_root__`` key — but a
        caller-supplied recoverer does not, and ``_resolve_head``
        promises a *valid* commit or None.

        An unchecked answer is worse than no answer: ``repair_head``
        makes it durable, replacing obviously-corrupt HEAD bytes with a
        plausible hash naming nothing — harder to diagnose than the
        damage it replaced.
        """
        store = HookStore()
        VersionedKV(store).commit({"k": b"1"})
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
        VersionedKV(store).commit({"k": b"1"})
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        assert (
            _resolve_head(store, "main", recover_from_corrupt_head=lambda s, b: 12345)
            is None
        )

    def test_a_valid_recoverer_is_still_honoured(self):
        """The check must not cost the tier its purpose."""
        store = HookStore()
        v = VersionedKV(store)
        head = v.commit({"k": b"1"}).commit
        store.set(BRANCH_HEAD % "main", b"")
        store.remove(BRANCH_HEAD_PREV % "main")
        assert (
            _resolve_head(store, "main", recover_from_corrupt_head=lambda s, b: head)
            == head
        )
