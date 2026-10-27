"""The store's layout, and the operations on it that need no worktree.

Storage layout (v4):

- ``__kvgit_version__``                — storage version sentinel
- ``__branch_head__<branch>``          — current HEAD commit hash
- ``__branch_head__refs/tags/<tag>``   — commit a tag names
- ``__branch_head_prev__<branch>``     — previous HEAD (recovery backup)
- ``__commit_root__<commit>``          — keyset HAMT root hash
- ``__parent_commit__<commit>``        — list of parent commit hashes
- ``__commit_time__<commit>``          — wall time the commit was created
- ``__info__<commit>``                 — optional caller-supplied info dict
- ``__tag_info__<tag>``                — tag creation time + info
- ``kvgit:keyset:<node_hash>``         — HAMT node bytes
- ``kvgit:chunk:<chunk_hash>``         — content-addressed chunk bytes (v3)
- ``__gc_lease__``                     — lease every sweep runs under
- ``__inflight__<commit>``             — a written commit not yet published
- ``kvgit:blob:<sha256>``              — blob value bytes, keyed by content
- ``<commit_hash>:<user_key>``         — blob value bytes written before v4

A tag is deliberately not a key kind of its own. It is a branch head
under a reserved name, hidden from the branch API, so that reachability
— which every kvgit version decides by walking branch heads — keeps a
tagged commit alive with no knowledge of tags at all. The record beside
it holds only creation time and caller info, which nothing collects.

``__branch_head_prev__`` is written only after a HEAD swap succeeds, so
it always names a commit ``__branch_head__`` really held. Recovery reads
it, so a value that was never HEAD would graft onto the branch a lineage
it never had.

The keyset (key -> blob_pointer + meta) is stored as a content-addressable
HAMT, so unchanged subtrees are shared across commits by hash equality. A
single-key change writes O(log N) new nodes instead of rewriting a full
keyset snapshot per commit.

Chunks (v3) are content-addressed bytes referenced by per-key
``MetaEntry.chunks``. They let chunked codecs (numpy, pandas, ...) share
large buffers across keys, commits, and branches.

Everything below a commit is keyed by content. A blob's key is the
SHA-256 of its bytes; a keyset entry holds only what follows from those
bytes (the pointer, the size, the chunk references), so a HAMT node is
named by the entries it holds; a chunk is named by its bytes. The
commit hash is computed last, over the parents, the keyset root, the
time and the info, so one commit hash names one root. Equal bytes are
stored once across keys, commits and branches, and the two sides of a
merge agree about a key exactly when they point at the same blob.

Content two commits share is one key, so a sweep may delete an orphan's
content only if no live commit uses it — including a commit a writer is
in the middle of making. Every sweep therefore runs under the
``__gc_lease__`` key, and every commit batch is a ``cas_many`` expecting
the lease record its writer read after waiting any live lease out: no
batch lands while a sweep runs. A batch that landed earlier carries an
``__inflight__`` marker for its commit, removed by the write that
publishes it, and a sweep marks from those markers as well as from the
branch heads. So every live commit — published, in flight, or younger
than ``min_age`` — is marked, and everything an orphan alone held goes
with it.

This code reads one layout, v4, and refuses a store stamped with any
other, or with branches and no stamp; a fresh store is stamped v4 when
it is opened:

* v3 added chunks.
* v4 changed how new blobs, entries and commits are keyed. Nothing
  already stored is rewritten: existing commit hashes, branch heads
  and tags stay valid, and one keyset may hold blobs of both kinds. A
  store an older kvgit wrote reads once its stamp is raised to v4,
  which is what the 0.4.x backfill does.
* A stamp locks out older code, deliberately. An older sweep deletes
  by rules that are wrong for content it did not write, so it must not
  run; every sweep and every handle refuses a store of another layout.

The pre-v2 layout, from before stamps, is **not** supported: such a
store raises on open and needs to be rebuilt fresh.
"""

import hashlib
import logging
import os
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

from . import _core
from ._drive import drive, steps
from ._types import TagInfo
from .kv.base import KVStore

PARENT_COMMIT = "__parent_commit__%s"
PARENT_GENS = "__parent_gens__%s"
"""Each parent's generation, in the order of ``__parent_commit__``.

A commit's generation is one more than its highest parent's, and 0 for
a commit with no parents, so every commit's generation is above all of
its ancestors'. Storing the parents' generations with the child, rather
than each commit's own, lets a search that pops a commit learn where
its parents fall from the one record it reads. Commits written before
generations were stored have no such key; a search that reaches one
falls back to walking whole histories.
"""
COMMIT_ROOT = "__commit_root__%s"
COMMIT_TIME = "__commit_time__%s"
BRANCH_HEAD = "__branch_head__%s"
BRANCH_HEAD_PREV = "__branch_head_prev__%s"
INFO_KEY = "__info__%s"
TAG_INFO_KEY = "__tag_info__%s"

TAG_BRANCH_PREFIX = "refs/tags/"
"""Reserved branch-name prefix a tag's commit pointer lives under.

A tag is stored as ``__branch_head__refs/tags/<name>`` — a branch head
in every respect except that this code hides it from the branch API.
That is what keeps a tagged commit alive across kvgit versions: reachability
is decided by walking branch heads, so *any* version, including ones
written before tags existed, marks a tag's commit as a root without
being taught anything. A separate key kind would have been invisible to
them, and no version stamp can retrofit the rule into code that already
shipped.
"""

CHUNK_PREFIX = "kvgit:chunk:"

GC_LEASE_KEY = "__gc_lease__"
"""Reserved key holding the store-wide lease every sweep runs under.

The value is ``{"owner": <opaque id>, "expires": <unix time>}`` encoded
with :func:`kvgit.encoding.dumps`. A record whose ``expires`` is in the
past — or whose bytes do not decode — is not a lease: it may be taken
over by CAS against those exact bytes. Absent, unreadable and expired
all mean "no live lease", so a holder that dies mid-sweep blocks the
store only until its expiry passes.
"""

GC_LEASE_TTL = 600.0
"""Default seconds a sweep's GC lease stays live.

A sweep that crashes holding it blocks writers for at most this long.
"""

IN_FLIGHT_KEY = "__inflight__%s"
"""Marker for a commit whose batch has landed but whose head has not.

Written in the commit's own batch and removed by the write that
publishes it, so a sweep that starts in between marks the commit live:
it is unreachable from every head, and without the marker it would be
indistinguishable from abandoned work. The value is the unix time the
protection lapses, :data:`IN_FLIGHT_TTL` after the batch, which is what
eventually releases the commit of a writer that died before publishing.
"""

IN_FLIGHT_TTL = 600.0
"""Seconds an in-flight marker protects an unpublished commit."""

GC_WAIT_POLL = 0.05
"""Seconds a writer sleeps between checks while a GC lease is live.

The store offers no wait primitive, so waiting is polling. Short enough
that release is noticed promptly, long enough that a writer blocked on a
ten-minute sweep is not hammering the backend.
"""

STORAGE_VERSION_KEY = "__kvgit_version__"
STORAGE_VERSION = 4
"""Highest layout this code knows how to write.

Tags did not raise it. They are branch heads under a reserved name, so
every kvgit that walks branch heads already treats them correctly; a
version bump would have locked those readers out of a store they can
serve perfectly well, and would have protected nothing that the naming
does not.
"""

CHUNK_STORAGE_VERSION = 3
"""Lowest layout that can read a store containing chunks.

Named separately from :data:`STORAGE_VERSION` because it is a rule about
chunks, not about whatever the newest layout happens to be.
"""

BLOB_STORAGE_VERSION = 4
"""Lowest layout that can read a store holding content-addressed blobs.

Stamped before a handle's first commit batch lands, so no v4 object is
ever visible to code that would sweep it by the older rules.
"""

BLOB_PREFIX = "kvgit:blob:"

EMPTY_HASH: str = _core.empty_hash()
"""The root of an empty keyset."""

ROOT_COMMIT = "821bf06b4dcb406ea508a4a992eadc22f29850cd"
"""Hash of every branch's initial empty commit, in every layout.

Fixed rather than derived from the commit hash scheme, so any two
branches share it as an ancestor whichever kvgit minted them.
"""


def blob_key(value: bytes) -> str:
    """Storage key of a blob: the SHA-256 of its bytes."""
    return BLOB_PREFIX + hashlib.sha256(value).hexdigest()


logger = logging.getLogger("kvgit")


def _on_tree(
    tree: Any,
    op: str,
    store: KVStore,
    recoverer: Callable[[KVStore, str], Any] | None = None,
    /,
    **args: Any,
) -> Any:
    """Run the core's operation ``op`` on worktree state ``tree``."""
    run = _core.Op(op, args)
    run.use_tree(tree)
    return drive(
        run,
        store,
        recoverer=recoverer,
        owner=lambda: _new_lease_owner(),
        clock=time.time,
        sleep=time.sleep,
    )


Clock = Callable[[], float]
"""Seconds since the epoch, as a float: where commit, tag and sweep
times are read from. Leases always expire by the wall clock."""


def _check_storage_version(store: KVStore) -> None:
    """Refuse a store stamped with another storage layout, or one with
    branches and no stamp, from before stamps; stamp a fresh one."""
    _call("check_version", store, stamp=True)


def _call(
    op: str,
    store: KVStore,
    recoverer: Callable[[KVStore, str], Any] | None = None,
    now: Clock | None = None,
    /,
    **args: Any,
) -> Any:
    """Run the core's operation ``op`` on ``args`` against ``store``."""
    return drive(
        _core.Op(op, args),
        store,
        recoverer=recoverer,
        owner=lambda: _new_lease_owner(),
        clock=time.time,
        sleep=time.sleep,
        now=now,
    )


def _stream(
    op: str,
    store: KVStore,
    recoverer: Callable[[KVStore, str], Any] | None = None,
    /,
    **args: Any,
) -> Iterator[Any]:
    """Run the core's operation ``op``, yielding each item of the parts
    it emits."""
    run = steps(
        _core.Op(op, args),
        store,
        recoverer=recoverer,
        clock=time.time,
        sleep=time.sleep,
    )
    for part in run:
        yield from part


CorruptHeadRecoverer = Callable[[KVStore, str], "str | None"]
"""Last-resort recovery for a HEAD that is present but unresolvable.

Called with the store and the branch name; returns a commit hash to
treat as that branch's HEAD, or ``None`` if it cannot say. Mirrors the
TypeScript port's ``CorruptHeadRecoverer`` so the two implementations
read the same.

There is **no default**. When HEAD is unresolvable and the prev-HEAD
backup is missing or equally broken, the information needed is not in
the store, so no implementation can be correct — only lucky. kvgit
reports ``None`` and leaves the guess to a caller who has decided the
trade is worth it. :func:`recover_by_commit_scan` is the implementation
to hand in if that caller is you.
"""


def _resolve_head(
    store: KVStore,
    branch: str,
    *,
    recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
    roots: dict[str, str] | None = None,
) -> str | None:
    """Resolve a branch HEAD, falling back to prev HEAD then an injected recoverer.

    **Never writes.** Every read path in the library goes through here,
    so healing the damage in place would make an ordinary ``get`` a
    mutation — impossible for a read-only consumer, and a race between
    two readers repairing the same branch to different answers. The
    recovery is returned to the caller and forgotten; :func:`repair_head`
    is the explicit call that makes it durable, and the write path heals
    HEAD itself as part of the CAS that has to move it anyway.

    Args:
        recover_from_corrupt_head: Optional third tier, fired only when
            HEAD exists, is unusable, and the backup did not save it.
            Unset — the default — means such a branch resolves to None.
            See :data:`CorruptHeadRecoverer`.
        roots: Where to record the root of the commit this resolves to,
            read while checking it.

    Returns a valid commit hash, or None if unrecoverable.
    """
    found = _call(
        "resolve_head",
        store,
        recover_from_corrupt_head,
        branch=branch,
        recover=recover_from_corrupt_head is not None,
    )
    if found is None:
        return None
    if roots is not None and found["root"] is not None:
        roots[found["commit"]] = found["root"]
    return found["commit"]


def recover_by_commit_scan(store: KVStore, branch: str) -> str | None:
    """Guess a corrupt branch's HEAD by scanning every commit. Opt-in.

    Finds all valid commits, excludes those reachable from healthy
    branches, and returns the most recent remaining tip (by
    ``__commit_time__``). A :data:`CorruptHeadRecoverer`, so it is
    passed in rather than reached for::

        import kvgit

        repo = kvgit.Repo(
            backend, recover_from_corrupt_head=kvgit.recover_by_commit_scan
        )

    Off unless passed in: without a recoverer, a branch whose HEAD and
    backup are both unusable is unrecoverable rather than guessed at.
    The name says what it does rather than what it is for, because what
    it is for is the part that cannot be guaranteed: this is a heuristic
    over a store that has already lost the answer.

    Two things to weigh before wiring it in.

    **It can serve another branch's deleted data.** "Not claimed by a
    healthy branch" is the only signal it has for whose commit a commit
    is, and a deleted branch's commits are unclaimed by definition until
    garbage collection (``Repo.gc``) takes them. Delete a branch, damage an
    unrelated branch's HEAD, lose its backup, and this returns the
    deleted branch's tip — grafting onto the survivor a lineage it never
    had, behind a ``logger.warning``. No race, no concurrency, no
    unusual store required.

    **It is O(store).** Every ``__commit_root__`` and every branch
    ancestry, walked per unresolved read until someone calls
    ``Repo.repair_head``.

    It is worth it when losing the branch outright is worse than
    recovering it to a plausible commit — a single-branch store, or one
    where branches are never deleted, has neither hazard in play. That
    judgement belongs to whoever owns the data.
    """
    return _call("recover_by_commit_scan", store, branch=branch)


def repair_head(
    store: KVStore,
    branch: str = "main",
    *,
    recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
) -> str | None:
    """Persist a recovered HEAD for a damaged branch.

    Read paths recover a corrupt ``__branch_head__`` in memory and leave
    the store untouched, so the damage stays visible until someone
    decides what to do about it. This is that decision: resolve the
    branch the way a read would, and write the answer back.

    It takes a raw ``KVStore`` and touches nothing else, so it works
    with or without a worktree open on the branch.

    Idempotent, and a no-op on a healthy branch. The write is a CAS
    against the damaged bytes, so it cannot overwrite a HEAD another
    process fixed, or advanced, in the meantime.

    When that CAS does not win — another process repaired the branch,
    advanced it, or deleted it between resolving and healing — the
    recovery candidate is stale, and returning it would name an older
    commit than HEAD actually holds. The branch is re-resolved instead,
    so the answer describes the store rather than the attempt.

    Args:
        recover_from_corrupt_head: Optional last-resort recovery, used
            only for a HEAD that is present, unusable, and has no usable
            backup. Unset — the default — means such a branch is
            reported unrecoverable rather than guessed at. See
            :data:`CorruptHeadRecoverer` and
            :func:`recover_by_commit_scan`.

    Returns:
        The commit HEAD now names, or None if the branch does not exist
        or nothing recoverable was found.
    """
    return _call(
        "repair_head",
        store,
        recover_from_corrupt_head,
        branch=branch,
        recover=recover_from_corrupt_head is not None,
    )


def _reject_reserved_branch(name: str) -> None:
    """Refuse the empty branch name, and names in the reserved tag namespace.

    An empty name would write the bare head key: a branch that exists,
    takes commits and roots a sweep, but that no branch listing shows.

    The branch API does not hand out names under ``refs/tags/``, because
    a branch there would be indistinguishable from a tag: it would show
    up in ``tags()``, and moving it would silently move the tag. Tags
    are created, listed and deleted through the tag API instead.
    """
    if isinstance(name, str):
        _core.check_branch_name(name)


def _resolve_tag(store: KVStore, name: str) -> str | None:
    """Read the commit a tag names, or None if it does not resolve.

    Reads the tag's head key and nothing else. The prev-HEAD recovery
    tiers a branch gets do not apply: a tag is written once and never
    moved, so this code never creates a backup for one, and a backup
    left by something that treated the tag as an ordinary branch
    describes a move that is not ours to honour.
    """
    return _call("resolve_tag", store, name=name)


def tags(store: KVStore) -> dict[str, str]:
    """Map every tag in the store to the commit it names.

    Dangling tags — ones whose commit is no longer in the store — are
    included, because leaving them out would make a damaged tag look
    deleted. :func:`tag_info` says which is which.
    """
    return dict(_call("tags", store))


def tag_info(store: KVStore, name: str) -> TagInfo | None:
    """Describe one tag, or None if the store has no such tag."""
    found = _call("tag_info", store, name=name)
    return None if found is None else TagInfo(**found)


def _new_lease_owner() -> str:
    """A name for a lease this process takes, unique among sweepers."""
    return f"{os.getpid()}-{uuid.uuid4().hex[:12]}"


def create_branch(
    store: KVStore, name: str, target: str, *, now: Clock | None = None
) -> None:
    """Create branch ``name`` pointing at commit ``target``.

    ``target`` may be :data:`ROOT_COMMIT`, which the store holds as soon
    as any branch has been created: the first branch in a store writes
    it, in the same write as its head.

    "The commit is here" and "the head names it" are decided against one
    lease record: a sweep that starts between the check and the write
    makes the write fail, and the check runs again. So the outcomes are
    a branch on a commit that loads, or a refusal — never a head
    pointing at nothing. The write also drops any backup under this
    name: a branch just created has no previous HEAD, and a stale backup
    left by an earlier branch of the same name would otherwise be what
    head recovery serves if this HEAD were ever damaged.

    Raises:
        BranchExistsError: if the name is taken.
        UnknownCommitError: if ``target`` is not in the store.
    """
    _call("create_branch", store, None, now, name=name, target=target)


def delete_branch(store: KVStore, name: str) -> None:
    """Delete a branch: its head and its backup, in one removal.

    Its commits become collectable at the next :func:`gc`. The removal
    expects the exact head bytes this call read, so a branch that moved,
    or was deleted and made again elsewhere, in between is left alone and
    the call raises. It is not retried: the decision to delete was made
    about the head that was read, and a retry would delete a newer one. A
    caller that wants the newer head gone too reads it again and deletes
    again.

    A head holds only the commit it names, so a branch deleted and made
    again at the *same* commit is indistinguishable from the one read,
    and the delete removes it, as git's ``update-ref -d <ref> <old-oid>``
    would. The state removed is exactly the state the caller decided to
    delete.

    A delete does not wait out a sweep. Removing a root from under one
    only means the sweep keeps what that root reached until the next.

    Raises:
        UnknownBranchError: if there is no such branch.
        ConcurrencyError: if the head changed between the read and the
            removal; nothing was removed.
        ValueError: if ``name`` is empty or in the reserved tag namespace.
    """
    _call("delete_branch", store, name=name)


def create_tag(
    store: KVStore,
    name: str,
    target: str,
    info: dict | None = None,
    *,
    now: Clock | None = None,
) -> None:
    """Name commit ``target`` permanently, keeping it and its ancestry alive.

    A tag is immutable: creating one over an existing name raises, and
    there is no move. It is a GC root, so it must not be planted under a
    sweep that has already decided what is reachable, and the existence
    check must still hold when it lands: both are decided against one
    lease record, and a sweep in between sends them round again. The
    head is claimed against absence, so two writers racing the same name
    cannot both win; the info record lands with it, and any backup left
    under the name by an earlier tag goes in the same write.

    Raises:
        TagExistsError: if the name is taken.
        UnknownCommitError: if ``target`` is not in the store.
    """
    _call("create_tag", store, None, now, name=name, target=target, info=info)


def delete_tag(store: KVStore, name: str) -> None:
    """Delete a tag: its reserved head, that head's backup and its record.

    A commit the tag was the last root for becomes collectable at the
    next :func:`gc`. This code never writes a backup for a tag, since it
    never moves one; the removal is for a backup written by something
    that treated the tag as an ordinary branch, which would otherwise
    let head resolution serve the deleted tag's commit under a later
    branch of the same reserved name.

    The removal expects the exact head and info-record bytes this call
    read, so a tag deleted and made again in between keeps its new head
    and record, and the call raises. That holds even for a tag made again
    at the same commit: its record carries the time it landed, so the
    record differs where the head does not. A tag with no record is
    compared by its head alone. The call is not retried, since a retry
    would delete the newer tag. Nor does it wait out a sweep: removing a
    root from under one only means the sweep keeps what that root reached
    until the next.

    Raises:
        UnknownTagError: if there is no such tag.
        ConcurrencyError: if the tag's head or record changed between the
            read and the removal; nothing was removed.
        ValueError: if ``name`` is not a valid tag name.
    """
    _call("delete_tag", store, name=name)


def gc(
    store: KVStore,
    min_age: float = 3600,
    *,
    deep: bool = False,
    wait: bool = True,
    lease_ttl: float = GC_LEASE_TTL,
    now: Clock | None = None,
) -> int:
    """Sweep what no live commit reaches; return how many commits went.

    A commit is live if a branch head or a tag reaches it, if it is in
    flight (written, not yet published), or if it is younger than
    ``min_age`` seconds — which is policy alone: how long abandoned work
    lingers. Everything an orphan alone held goes with it: its commit
    metadata, blobs, HAMT nodes and chunks. ``deep=True`` adds a scan of
    the whole ``kvgit:blob:``, ``kvgit:keyset:`` and ``kvgit:chunk:``
    namespaces for content no commit references at all — leftovers from
    a crash or an interrupted write, or from a store swept by an earlier
    kvgit, which no orphan keyset points at.

    Runs under the store's GC lease. No commit batch can land while it
    is held, and every commit written but not yet published carries an
    in-flight marker the sweep marks from, so this is safe beside
    concurrent writers at any ``min_age``, 0 included. If another sweep
    holds the lease, ``wait=True`` waits it out and ``wait=False``
    raises :class:`GcBusy`. ``lease_ttl`` bounds what a sweep that
    crashes holding the lease costs writers; a sweep that outlives it
    finishes and logs a warning rather than extending it.

    Raises:
        GcBusy: with ``wait=False``, if another sweep holds the lease.
        StorageVersionError: if the store is stamped with another layout
            than the one this code reads. Nothing is written, the lease
            key included.
    """
    removed = _call(
        "gc",
        store,
        None,
        now,
        min_age=min_age,
        deep=deep,
        wait=wait,
        lease_ttl=lease_ttl,
    )
    if removed:
        logging.getLogger("kvgit.orphans").debug(
            "Cleaned %d orphaned commit(s)", removed
        )
    return removed
