"""Sync: commits moving between stores, and branches kept in step with
a remote.

Internal: these are the primitives a transport builds on, as
TypeScript's ``PackRemote`` does. The core walks, checks and replays
the commits and decides each pull and push; this calls the remote
between its steps.

A remote is a far peer that stores history and has refs, not a
:class:`KVStore`: it lists its refs, sends the commits one head has and
a receiver lacks, and takes commits with a compare-and-swap of a ref.
Merging is never the remote's: a branch both sides moved is diverged,
and reconciling it is a local merge, after which it syncs again.
"""

import json
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from . import _core
from .errors import BranchExistsError
from .kv.base import KVStore
from .versioned._drive import drive
from .versioned.kv import _call, _new_lease_owner, _stream, blob_key

SyncStatus = Literal[
    "up-to-date", "fast-forwarded", "pushed", "created", "diverged", "remote-gone"
]
DeleteStatus = Literal["deleted", "moved", "gone"]


@dataclass(frozen=True)
class WireCommit:
    """A commit as it travels: its parents and records, and its keyset's
    changes from its first parent's. Tree nodes never travel; the
    receiver rebuilds them, and the commit hash checks the result.
    """

    hash: str
    parents: tuple[str, ...]
    #: Each parent's generation as recorded; ``None`` for the root commit.
    parent_gens: tuple[int, ...] | None
    #: Commit time in seconds, as recorded.
    time: float
    #: The commit's info as recorded, or ``None``.
    info: Any
    #: Each key whose entry differs from the first parent's, and its value.
    updates: dict[str, bytes] = field(default_factory=dict)
    #: Each key in the first parent's keyset and not in this one's.
    removals: frozenset[str] = frozenset()


@dataclass(frozen=True)
class RemoteRef:
    branch: str
    head: str


class Remote(Protocol):
    #: Who this remote is, the same from one run to the next: the head
    #: each branch last synced to here is kept under it. Remotes without
    #: one share a single such record per branch.
    id: str | None

    def list_refs(self) -> list[RemoteRef]: ...

    def fetch(self, want: str, have: Iterable[str]) -> Iterable[WireCommit]:
        """The commits reachable from ``want`` and from none of ``have``,
        parents first."""
        ...

    def push(
        self,
        branch: str,
        expected: str | None,
        head: str,
        commits: Iterable[WireCommit],
    ) -> bool:
        """Store ``commits`` and move ``branch`` to ``head`` if it is at
        ``expected`` (``None``: if it doesn't exist); False, the ref
        unmoved, otherwise."""
        ...

    # Optional: ``delete_branch(branch, expected) -> bool`` deletes the
    # ref only while it is at ``expected``. A remote without it keeps
    # its branches.


@dataclass(frozen=True)
class ApplyWireResult:
    #: Commits written by the call.
    applied: int
    #: Commits that were already in the store.
    skipped: int
    #: The last commit of the stream, or ``None`` for an empty one.
    head: str | None
    #: Commits marked in flight until a head that names them is
    #: published: pass them to :func:`publish_head`.
    in_flight: tuple[str, ...]


@dataclass(frozen=True)
class SyncResult:
    status: SyncStatus
    branch: str
    #: A pull's local head after it, ``None`` for no branch; a push's,
    #: the head it read and sent, whatever a local writer did since.
    local_head: str | None
    #: The remote's head as last seen, or ``None`` for no ref.
    remote_head: str | None
    #: Commits applied by a pull, or sent by a push.
    transferred: int


@dataclass(frozen=True)
class SyncOutcome:
    #: The push's status, or the pull's when the push changed nothing.
    status: SyncStatus
    pull: SyncResult
    #: ``None`` when a diverged or vanished remote stopped the sync.
    push: SyncResult | None


@dataclass(frozen=True)
class DeleteResult:
    status: DeleteStatus
    branch: str
    remote_head: str | None


def _remote(remote: Remote | None) -> dict[str, str | None] | None:
    return None if remote is None else {"id": remote.id}


def walk_delta(
    store: KVStore, want: str, have: Iterable[str] = ()
) -> Iterator[WireCommit]:
    """The commits reachable from ``want`` and from none of ``have``,
    parents first; of those ready to go, the oldest first.

    Raises:
        UnknownCommitError: if the store lacks a commit on ``want``'s
            side, since a partial history can't be sent.
    """
    for walked in _stream("wire_walk", store, want=want, have=list(have)):
        blobs = dict(store.get_many([blob for _, blob in walked["updates"]]))
        updates = {}
        for key, blob in walked["updates"]:
            if blob not in blobs:
                raise KeyError(f"walk_delta: blob missing for {blob}")
            updates[key] = blobs[blob]
        recorded = walked["time"]
        info = walked["info"]
        yield WireCommit(
            hash=walked["hash"],
            parents=tuple(walked["parents"]),
            parent_gens=None
            if walked["parent_gens"] is None
            else tuple(walked["parent_gens"]),
            time=float(json.loads(recorded)) if recorded is not None else 0.0,
            info=None if info is None else json.loads(info),
            updates=updates,
            removals=frozenset(walked["removals"]),
        )


def apply_wire(
    store: KVStore,
    commits: Iterable[WireCommit],
    *,
    create_branch: str | None = None,
) -> ApplyWireResult:
    """Write a stream of commits, parents first, checking each before
    anything of it lands; each is marked in flight until a head that
    names it is published. With ``create_branch``, create that branch at
    the stream's last commit.

    Raises:
        IntegrityError: for a commit whose hash or parent generations
            don't match what it carries, or whose parent never came.
            Commits before it stay written.
        BranchExistsError: if ``create_branch`` exists.
    """
    _call("check_version", store, stamp=True)
    applied = skipped = 0
    head = None
    in_flight: list[str] = []
    for wc in commits:
        head = wc.hash
        held = {blob_key(value): value for value in wc.updates.values()}
        op = _core.Op(
            "wire_replay",
            {
                "hash": wc.hash,
                "parents": list(wc.parents),
                "parent_gens": None if wc.parent_gens is None else list(wc.parent_gens),
                "time": wc.time,
                "info": wc.info,
                "updates": [
                    [key, blob_key(value), len(value)]
                    for key, value in wc.updates.items()
                ],
                "removals": sorted(wc.removals),
            },
        )
        replayed = drive(
            op,
            store,
            held=held,
            owner=_new_lease_owner,
            clock=time.time,
            sleep=time.sleep,
        )
        if replayed == "present":
            skipped += 1
        else:
            applied += 1
        in_flight.append(wc.hash)
    if create_branch is not None and head is not None:
        if not publish_head(store, create_branch, None, head, in_flight=in_flight):
            raise BranchExistsError(f"Branch '{create_branch}' already exists")
        in_flight = []
    return ApplyWireResult(applied, skipped, head, tuple(in_flight))


def publish_head(
    store: KVStore,
    branch: str,
    expected: str | None,
    head: str,
    *,
    in_flight: Iterable[str] = (),
) -> bool:
    """Move ``branch`` from ``expected`` (``None``: create it) to
    ``head``, with its backup, while no sweep runs, dropping the
    in-flight markers of the commits it publishes in the same write.
    False if the branch isn't at ``expected``."""
    return _call(
        "publish_head",
        store,
        branch=branch,
        expected=expected,
        head=head,
        in_flight=list(in_flight),
    )


def get_sync_head(
    store: KVStore, branch: str, remote: Remote | None = None
) -> str | None:
    """The last commit ``branch`` is known to share with ``remote``; with
    no remote, its one sync head, whichever remote it is with."""
    return _call("sync_head", store, branch=branch, remote=_remote(remote))


def set_sync_head(
    store: KVStore, branch: str, commit: str, remote: Remote | None = None
) -> None:
    _call("set_sync_head", store, branch=branch, commit=commit, remote=_remote(remote))


def clear_sync_head(store: KVStore, branch: str, remote: Remote | None = None) -> None:
    """Forget what ``branch`` last synced to with ``remote``; with no
    remote, with every remote."""
    _call("clear_sync_head", store, branch=branch, remote=_remote(remote))


def has_sync_head(store: KVStore, branch: str) -> bool:
    return _call("has_sync_head", store, branch=branch)


def delete_branch_unless_synced(store: KVStore, branch: str) -> bool:
    """Delete ``branch`` unless it syncs with some remote; whether it
    went."""
    return _call("delete_branch_unless_synced", store, name=branch)


def _remote_head(remote: Remote, branch: str) -> str | None:
    return next((r.head for r in remote.list_refs() if r.branch == branch), None)


def pull_branch(store: KVStore, remote: Remote, branch: str) -> SyncResult:
    """Fetch what ``remote`` has of ``branch`` and fast-forward (or
    create) the local branch to it."""
    remote_head = _remote_head(remote, branch)
    plan = _call(
        "pull_plan",
        store,
        branch=branch,
        remote=_remote(remote),
        remote_head=remote_head,
    )
    local_head = plan["local_head"]
    if plan["status"] is not None or remote_head is None:
        status = plan["status"] or "up-to-date"
        return SyncResult(status, branch, local_head, remote_head, 0)
    applied = apply_wire(store, remote.fetch(remote_head, plan["have"]))
    done = _call(
        "pull_finish",
        store,
        branch=branch,
        remote=_remote(remote),
        remote_head=remote_head,
        local_head=local_head,
        in_flight=list(applied.in_flight),
    )
    return SyncResult(
        done["status"], branch, done["local_head"], remote_head, applied.applied
    )


def push_branch(store: KVStore, remote: Remote, branch: str) -> SyncResult:
    """Send ``remote`` what it lacks of ``branch`` and move its ref
    along, by compare-and-swap.

    Raises:
        UnknownBranchError: if there is no such local branch.
    """
    start = _call("push_start", store, branch=branch, remote=_remote(remote))
    local_head = start["local_head"]
    remote_head = None
    # Commits sent, over every attempt: a remote may keep what a push
    # that lost its compare-and-swap sent.
    sent = [0]
    # A lost compare-and-swap means the remote moved meanwhile: look
    # again once before calling the branch diverged.
    for _ in range(2):
        remote_head = _remote_head(remote, branch)
        plan = _call(
            "push_plan",
            store,
            branch=branch,
            remote=_remote(remote),
            local_head=local_head,
            sync_head=start["sync_head"],
            remote_head=remote_head,
        )
        if plan["status"] is not None:
            return SyncResult(plan["status"], branch, local_head, remote_head, sent[0])
        commits = _counted(walk_delta(store, local_head, plan["have"]), sent)
        if remote.push(branch, plan["expected"], local_head, commits):
            set_sync_head(store, branch, local_head, remote)
            status = "created" if plan["expected"] is None else "pushed"
            return SyncResult(status, branch, local_head, remote_head, sent[0])
    return SyncResult("diverged", branch, local_head, remote_head, sent[0])


def _counted(commits: Iterable[WireCommit], sent: list[int]) -> Iterator[WireCommit]:
    """``commits`` as they stream, counted into ``sent[0]``."""
    for wc in commits:
        sent[0] += 1
        yield wc


def sync_branch(store: KVStore, remote: Remote, branch: str) -> SyncOutcome:
    """Pull, then push. A diverged or vanished remote stops it after
    the pull."""
    pull = pull_branch(store, remote, branch)
    if pull.status in ("diverged", "remote-gone"):
        return SyncOutcome(pull.status, pull, None)
    push = push_branch(store, remote, branch)
    status = pull.status if push.status == "up-to-date" else push.status
    return SyncOutcome(status, pull, push)


def delete_remote_branch(
    store: KVStore, remote: Remote, branch: str, *, expected: str | None = None
) -> DeleteResult:
    """Delete ``branch`` on ``remote`` only if nobody pushed to it since
    this store last synced it: its ref must be at the branch's sync head
    there, or at ``expected``. Once the ref is gone, the sync head goes
    too, so pushing the name again creates it afresh.

    Raises:
        ValueError: if ``branch`` can't be a branch's name.
        TypeError: if ``remote`` can't delete branches.
        ValueError: if the branch never synced with ``remote`` and no
            ``expected`` is given.
    """
    _core.check_branch_name(branch)
    delete = getattr(remote, "delete_branch", None)
    if delete is None:
        raise TypeError("this remote can't delete branches")
    if expected is None:
        expected = _call(
            "claim_sync_head", store, branch=branch, remote=_remote(remote)
        )
    if expected is None:
        raise ValueError(
            f"'{branch}' never synced with this remote; pass the head to expect"
        )
    # The ref can move without its head moving, so one retry.
    remote_head: str | None = expected
    for _ in range(2):
        if remote_head != expected:
            break
        if delete(branch, expected):
            clear_sync_head(store, branch, remote)
            return DeleteResult("deleted", branch, None)
        remote_head = _remote_head(remote, branch)
    if remote_head is None:
        clear_sync_head(store, branch, remote)
        return DeleteResult("gone", branch, None)
    return DeleteResult("moved", branch, remote_head)


class MemoryRemote:
    """A remote that is a kvgit store with branch refs, on any
    :class:`KVStore`. It checks what is pushed to it as a receiver
    does, by replaying it."""

    def __init__(self, store: KVStore, *, id: str | None = None):
        self.store = store
        self.id = id

    def list_refs(self) -> list[RemoteRef]:
        branches = _call("branches", self.store)
        heads = dict(self.store.get_many([f"__branch_head__{b}" for b in branches]))
        refs = []
        for branch in branches:
            raw = heads.get(f"__branch_head__{branch}")
            head = None if raw is None else _text(raw)
            if head is not None:
                refs.append(RemoteRef(branch, head))
        return refs

    def fetch(self, want: str, have: Iterable[str]) -> Iterator[WireCommit]:
        return walk_delta(self.store, want, have)

    def push(
        self,
        branch: str,
        expected: str | None,
        head: str,
        commits: Iterable[WireCommit],
    ) -> bool:
        applied = apply_wire(self.store, commits)
        return publish_head(
            self.store, branch, expected, head, in_flight=applied.in_flight
        )

    def delete_branch(self, branch: str, expected: str) -> bool:
        key = f"__branch_head__{branch}"
        current = self.store.get(key)
        if current is None or _text(current) != expected:
            return False
        # its objects stay, for a sweep to collect
        return self.store.cas_many(
            {key: current}, {}, [key, f"__branch_head_prev__{branch}"]
        )


def _text(raw: bytes) -> str | None:
    try:
        value = json.loads(raw)
    except ValueError:
        return None
    return value if isinstance(value, str) else None
