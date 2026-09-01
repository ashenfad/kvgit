"""HEAD backup, recovery, and repair semantics.

**Reading never writes.** Recovery on a read path is in-memory only.
Persisting it is either an explicit ``repair_head`` call or a side
effect of a write that has to move HEAD anyway.

Every race here is a seam, not a sleep. ``HookStore`` runs a one-shot
callback at a chosen point in a chosen store operation, so "the winner
lands between the loser's commit write and its CAS" is expressed
exactly and repeats identically.
"""

from __future__ import annotations

from kvgit import VersionedKV
from kvgit.encoding import loads
from kvgit.kv.memory import Memory
from kvgit.versioned.kv import (
    BRANCH_HEAD,
    BRANCH_HEAD_PREV,
    COMMIT_ROOT,
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
      before the caller sees it.

    Both are points every version of the code passes through, so a test
    written against them means the same thing before and after a fix.
    """

    def __init__(self) -> None:
        super().__init__()
        self.head_history: dict[str, list[str]] = {}
        self._on_commit_batch = None
        self._on_get: dict[str, object] = {}

    # -- seams --

    def arm_commit_batch(self, fn) -> None:
        """Fire ``fn`` once, on the next commit write batch."""
        self._on_commit_batch = fn

    def arm_get(self, key: str, fn) -> None:
        """Fire ``fn`` once, after ``key`` has been read."""
        self._on_get[key] = fn

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
        fn = self._on_get.pop(key, None)
        if fn is not None:
            fn()
        return value

    def set(self, key: str, value: bytes) -> None:
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
