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

import time

import pytest

from kvgit import ConcurrencyError, VersionedKV
from kvgit.encoding import dumps, loads
from kvgit.kv.memory import Memory
from kvgit.versioned.keyset import Keyset
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
    COMMIT_TIME,
    PARENT_COMMIT,
    _load_root,
    _resolve_head,
    clean_orphans,
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
    * ``arm_set`` fires before a chosen key is written, so a writer can
      be paused mid-sequence while another completes.

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

    def cas(self, key: str, value: bytes, expected: bytes | None) -> bool:
        won = super().cas(key, value, expected)
        if won:
            self._record(key, value)
        return won


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
        """
        store = HookStore()
        v = VersionedKV(store)
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

        with pytest.raises(ConcurrencyError):
            loser.commit({"a": b"2"})

        history = store.head_history["main"]
        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        assert prev == history[-2], (
            f"prev-HEAD is {prev}, the loser's stale value; the "
            f"immediately-previous HEAD is {history[-2]} "
            f"(history: {history})"
        )

    def test_crash_between_cas_and_backup_degrades_to_an_older_head(self):
        """The crash window costs a commit, never an invented lineage.

        Writing the backup after the CAS opens a window where HEAD has
        advanced but the backup has not. Recovery from that state lands
        on the *previous* previous HEAD — one commit further back than
        ideal, but a commit that really was HEAD, with real ancestry.
        That is the trade this ordering buys, and the store offers no
        multi-key atomicity that would avoid the trade.
        """
        store = HookStore()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit

        class Died(Exception):
            """Stands in for the process dying mid-commit."""

        prev_key = BRANCH_HEAD_PREV % "main"
        real_set = store.set

        def dying_set(key, value):
            if key == prev_key:
                raise Died
            real_set(key, value)

        store.set = dying_set  # type: ignore[method-assign]
        with pytest.raises(Died):
            v.commit({"a": b"3"})
        store.set = real_set  # type: ignore[method-assign]

        third = loads(store.get(BRANCH_HEAD % "main"))
        assert third not in (first, second), "the CAS should have landed"
        assert loads(store.get(prev_key)) == first, (
            "the backup should still hold its pre-crash value"
        )

        # HEAD is then damaged; recovery lands on a real, older HEAD.
        store.set(BRANCH_HEAD % "main", b"")
        recovered = _resolve_head(store, "main")
        assert recovered == first
        assert recovered in store.head_history["main"]


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
        """The loser's commit is garbage the ordinary sweep reclaims.

        Nothing is deleted inline. The loser's HAMT nodes are keyed by
        content, so the winner may legitimately share them, and blowing
        them away is the resurrection hazard the scoped sweep exists to
        avoid. The orphan is collected on the normal path once it ages
        past ``min_age``; chunks wait for ``deep_clean`` (see
        ``clean_orphans``).
        """
        store = HookStore()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit

        loser = VersionedKV(store, commit_hash=first)
        winner = VersionedKV(store, commit_hash=first)
        store.arm_commit_batch(lambda: winner.commit({"a": b"winner"}))

        with pytest.raises(ConcurrencyError):
            loser.commit({"a": b"loser"})

        # The loser's writes are still there, untouched.
        live_history = set(winner.history())
        orphan = next(
            key[len(ROOT_PREFIX) :]
            for key in store.keys()
            if key.startswith(ROOT_PREFIX)
            and key[len(ROOT_PREFIX) :] not in live_history
        )
        orphan_nodes = node_hashes(store, orphan)
        assert store.get(f"{orphan}:a") == b"loser"
        assert store.get(PARENT_COMMIT % orphan) is not None

        # The winner is unaffected, and ordinary GC reclaims the orphan.
        assert VersionedKV(store).get("a") == b"winner"
        age_commits(store, 10_000)
        assert clean_orphans(store, min_age=3600) == 1
        assert store.get(COMMIT_ROOT % orphan) is None
        assert store.get(f"{orphan}:a") is None

        live = VersionedKV(store)
        unshared = orphan_nodes - node_hashes(store, live.current_commit)
        assert unshared, "the orphan should own at least one node of its own"
        assert not [
            n for n in unshared if store.get(Keyset.DEFAULT_PREFIX + n) is not None
        ], "the orphan's own HAMT nodes were not reclaimed"
        assert live.get("a") == b"winner"


class TestAbsentHeadCannotRecover:
    """A branch with no HEAD is deleted, not damaged.

    Recovery tiers exist for a HEAD that is *present and unusable*.
    ``delete_branch`` removes the key, so an absent HEAD means the
    branch is gone — and a backup that outlives it must not bring it
    back. Writing the backup after the CAS opened a route to exactly
    that: a writer descheduled between the two, resuming after a
    concurrent delete, recreates only the backup.

    Reviving a branch from a lone backup is the v0.3.1 failure class,
    reached from a new direction.
    """

    def test_a_delayed_backup_does_not_resurrect_a_deleted_branch(self):
        store = HookStore()
        VersionedKV(store).commit({"anchor": b"1"})
        doomed = VersionedKV(store, branch="doomed")
        doomed.commit({"secret": b"classified"})

        def delete_it_mid_write():
            VersionedKV(store).delete_branch("doomed")

        # Pause the winner between its CAS and its backup write.
        store.arm_set(BRANCH_HEAD_PREV % "doomed", delete_it_mid_write)
        doomed.commit({"secret": b"classified-v2"})

        assert store.get(BRANCH_HEAD % "doomed") is None, "the delete should have won"
        assert store.get(BRANCH_HEAD_PREV % "doomed") is not None, (
            "this test is only meaningful while the delayed write recreates "
            "the backup; if that stops happening, the seam has drifted"
        )
        assert _resolve_head(store, "doomed") is None, (
            "a backup outliving its branch resurrected it — the deleted "
            "branch's state is readable again"
        )

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


class TestConcurrencyLimitsOfTheBackup:
    """What writing the backup after the CAS does *not* buy.

    The swap and the backup write are two steps. Anything that separates
    them — a crash, or simply losing the CPU while another writer
    completes both of its own — lets the older writer's backup land
    last. The invariant that survives is the narrower one: the backup
    names a commit HEAD really held. "Exactly one commit back" is not a
    guarantee, and these pin that so the docs are not quietly re-widened.
    """

    def test_a_paused_winner_can_clobber_a_newer_backup(self):
        store = HookStore()
        writer = VersionedKV(store)
        first = writer.commit({"k": b"1"}).commit

        landed = {}

        def other_writer_advances():
            other = VersionedKV(store)
            landed["third"] = other.commit({"k2": b"3"}).commit

        # Pause the winner between its successful CAS and its backup write.
        store.arm_set(BRANCH_HEAD_PREV % "main", other_writer_advances)
        second = writer.commit({"k": b"2"}).commit

        head = loads(store.get(BRANCH_HEAD % "main"))
        prev = loads(store.get(BRANCH_HEAD_PREV % "main"))
        history = store.head_history["main"]

        assert head == landed["third"], "the later writer should hold HEAD"
        assert prev != second, (
            "this pins the limitation, not the ideal: the paused winner's "
            "backup landed last"
        )
        assert prev == first
        # The guarantee that does survive.
        assert prev in history, (
            f"prev-HEAD {prev} was never HEAD (history: {history}) — the "
            f"invariant this ordering exists to protect is broken"
        )
        assert history.index(prev) < history.index(head), (
            "the backup must name a commit older than HEAD, not a sibling"
        )


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


class TestAgedOutOrphanResurrection:
    """A recreated orphan can be swept out from under a live HEAD.

    ``content_hash`` has no nonce, so rolling a branch back and redoing
    the same change byte-for-byte mints the *same* commit hash. If that
    happens while a sweep is in flight — after the sweep has read the
    orphan's ``__commit_time__`` and decided it is old, before it
    deletes — the sweep deletes commit metadata that is now live.

    Closing that properly needs a lock, which is out of scope here.
    What is pinned is the fallout: HEAD names a commit whose metadata is
    gone, and head resolution lands on the immediately-previous HEAD.
    Both fixes in this module make that outcome *better*. The backup is
    a commit that really was HEAD rather than a losing writer's stale
    guess, and a read no longer makes the loss durable behind the
    operator's back. It lands one commit back here because this scenario
    has a single writer; under concurrent writers the backup can sit
    further behind (see ``test_a_paused_winner_can_clobber_a_newer_backup``).
    """

    def _resurrect_under_a_sweep(self):
        store = HookStore()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit

        v.reset_to(first)
        age_commits(store, 10_000)

        # Fire once the sweep has read the orphan's age and believes it
        # old: the writer then recreates it byte-identically and CASes
        # HEAD onto it.
        store.arm_get(COMMIT_TIME % second, lambda: v.commit({"a": b"2"}))
        cleaned = clean_orphans(store, min_age=3600)
        return store, first, second, cleaned

    def test_the_hash_collision_is_real(self):
        """Rollback-then-redo mints the same commit hash."""
        store = Memory()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit

        v.reset_to(first)
        v.commit({"a": b"2"})
        assert v.current_commit == second

        # Differing info breaks it, which is why commits carrying
        # distinct metadata never collide this way.
        v.reset_to(first)
        v.commit({"a": b"2"}, info={"who": "someone else"})
        assert v.current_commit != second

    def test_ordinary_interleavings_do_not_corrupt(self):
        """A resurrection that lands before the age check is safe.

        Recreating the commit rewrites ``__commit_time__`` under the
        same key, so the orphan reads as young and ``min_age`` protects
        it. Only a writer landing between the age read and the delete
        gets through.
        """
        store = HookStore()
        v = VersionedKV(store)
        v.commit({"a": b"1"})
        first = v.current_commit
        v.commit({"a": b"2"})
        second = v.current_commit
        v.reset_to(first)
        age_commits(store, 10_000)
        v.commit({"a": b"2"})  # resurrects before the sweep starts

        assert clean_orphans(store, min_age=3600) == 0
        assert store.get(COMMIT_ROOT % second) is not None
        assert VersionedKV(store).get("a") == b"2"

    def test_resurrection_under_a_sweep_degrades_to_a_lost_commit(self):
        """The narrow window costs the newest commit, not readability."""
        store, first, second, cleaned = self._resurrect_under_a_sweep()

        assert cleaned == 1
        assert loads(store.get(BRANCH_HEAD % "main")) == second
        assert store.get(COMMIT_ROOT % second) is None, (
            "the sweep should have deleted the resurrected commit"
        )

        # Head resolution falls back — one commit back, this being a
        # single-writer scenario.
        recovered = _resolve_head(store, "main")
        assert recovered == first
        assert recovered == store.head_history["main"][-2]
        assert VersionedKV(store).get("a") == b"1"

    def test_the_fallback_does_not_make_the_loss_durable(self):
        """Reading the damaged branch leaves the evidence in place."""
        store, first, second, _ = self._resurrect_under_a_sweep()

        before = dict(store.items())
        assert VersionedKV(store).get("a") == b"1"
        assert dict(store.items()) == before
        assert loads(store.get(BRANCH_HEAD % "main")) == second, (
            "a read overwrote the damaged HEAD, discarding the only "
            "record of which commit went missing"
        )

        # The operator decides when to make it durable.
        assert repair_head(store, "main") == first
        assert loads(store.get(BRANCH_HEAD % "main")) == first
