"""KVStore-backed versioned state.

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

Every layout reads the ones before it, and a store is stamped up only
when something newer is actually written:

* v3 added chunks. The first chunked write stamps v3.
* v4 changed how new blobs, entries and commits are keyed. The first
  commit written by this code stamps v4. Nothing already stored is
  rewritten: existing commit hashes, branch heads and tags stay valid,
  and one keyset may hold blobs of both kinds.
* A stamp locks out older code, deliberately. An older sweep deletes
  by rules that are wrong for content it did not write, so it must not
  run; every sweep and every handle refuses a store stamped above what
  it reads.

The pre-v2 layout is **not** supported. Stores written by an earlier
version raise on open and need to be rebuilt fresh.
"""

import hashlib
import logging
import os
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, KeysView
from typing import Any

from .. import _core
from ..encoding import dumps, loads, safe_loads
from ..errors import (
    BranchExistsError,
    CorruptHeadError,
    StorageVersionError,
    UnknownBranchError,
)
from ..hamt import EMPTY_HASH
from ..kv.base import KVStore
from ..kv.memory import Memory
from ._drive import drive, steps
from .base import VersionedBase
from .keyset import Keyset, KeysetEntry, MetaEntry
from .merge import Change
from .protocol import MergeChoice, MergePolicy, MergeResult, PostCheck, TagInfo

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

# Lower versions accepted as input, read as they are; the stamp moves
# only when a write needs a newer layout.
SUPPORTED_READ_VERSIONS = frozenset({2, 3, 4})

BLOB_PREFIX = "kvgit:blob:"

ROOT_COMMIT = "821bf06b4dcb406ea508a4a992eadc22f29850cd"
"""Hash of every branch's initial empty commit, in every layout.

Fixed rather than derived from the commit hash scheme, so any two
branches share it as an ancestor whichever kvgit minted them.
"""


def blob_key(value: bytes) -> str:
    """Storage key of a blob: the SHA-256 of its bytes."""
    return BLOB_PREFIX + hashlib.sha256(value).hexdigest()


def commit_hash(
    parents: tuple[str, ...],
    root: str,
    created: float,
    info: dict | None = None,
) -> str:
    """The 40-hex hash naming a commit.

    Covers the parents, the keyset root, the commit time and the info,
    and is computed after the keyset is built, so a hash names exactly
    one root, and every ``__commit_*__<hash>`` key is written once and
    never rewritten. Two writers making the same change mint two
    commits, and merging them is clean because both point at the same
    blobs; making it at the same clock instant, they mint the very same
    commit, byte for byte.
    """
    return _core.commit_hash(list(parents), root, created, info)


logger = logging.getLogger("kvgit")


def _assert_supported_version(store: KVStore) -> None:
    """Raise if the store's version stamp is one this code cannot read.

    Read-only, and silent on an unstamped store — it answers "may this
    code touch what is here", not "is this store initialized". Every
    entry point that mutates a store it did not open through
    ``VersionedKV`` calls it first, the sweep above all: a store stamped
    higher than this code understands may hold roots this code cannot
    see, and sweeping it would delete live data.
    """
    raw = store.get(STORAGE_VERSION_KEY)
    if raw is None:
        return
    version = safe_loads(raw)
    if version not in SUPPORTED_READ_VERSIONS:
        raise StorageVersionError(
            f"Store has kvgit storage version {version!r}, "
            f"this code supports {sorted(SUPPORTED_READ_VERSIONS)}. "
            "Use a fresh store."
        )


def _check_storage_version(store: KVStore) -> None:
    """Verify the store's kvgit version is compatible.

    Stamps the version on a fresh store. Accepts any version listed in
    :data:`SUPPORTED_READ_VERSIONS`; the on-disk stamp is left
    untouched on open so that opening a v2 store with v3 code does not
    silently upgrade it to v3 (which would lock out older readers).
    The upgrade happens lazily, the first time a chunked write or a tag
    write actually occurs.
    """
    raw = store.get(STORAGE_VERSION_KEY)
    if raw is not None:
        _assert_supported_version(store)
        return

    # No version sentinel. Either fresh, or pre-v2.
    branch_prefix = BRANCH_HEAD.replace("%s", "")
    has_existing = any(True for _ in store.keys(branch_prefix))
    if has_existing:
        raise StorageVersionError(
            "Store appears to use an older kvgit storage format. "
            f"This version requires storage v{min(SUPPORTED_READ_VERSIONS)} "
            "or higher. Use a fresh store."
        )
    store.set(STORAGE_VERSION_KEY, dumps(STORAGE_VERSION))


def _call(
    op: str,
    store: KVStore,
    recoverer: Callable[[KVStore, str], Any] | None = None,
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


def _load_root(store: KVStore, commit_hash: str) -> str | None:
    """Load the keyset HAMT root hash for a commit, or None if missing."""
    raw = store.get(COMMIT_ROOT % commit_hash)
    if raw is None:
        return None
    val = safe_loads(raw)
    return val if isinstance(val, str) else None


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

    Handle-independent, like :func:`clean_orphans` — it takes a raw
    ``KVStore`` and touches nothing else, so it works with or without a
    ``VersionedKV`` anchored on the branch.

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
    takes commits and roots a sweep, but that :meth:`VersionedKV.branches`
    never lists.

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


def create_branch(store: KVStore, name: str, target: str) -> None:
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
    _call("create_branch", store, name=name, target=target)


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
    store: KVStore, name: str, target: str, info: dict | None = None
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
    _call("create_tag", store, name=name, target=target, info=info)


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


def commit_changes(
    store: KVStore, commit_a: str | None, commit_b: str
) -> dict[str, Change]:
    """Each key whose value differs from commit ``a`` to commit ``b``.

    A structural diff of the two keysets: subtrees the commits share are
    skipped by hash, one batched read per tree level, so the cost follows
    the size of the change rather than of the keysets. A key whose entry
    differs only in metadata, with the same blob, is not a change. A
    ``None`` or missing ``a`` reads as empty.
    """
    wanted = [COMMIT_ROOT % c for c in (commit_a, commit_b) if c is not None]
    found = store.get_many(*wanted)

    def root_of(commit: str | None) -> str:
        if commit is None:
            return EMPTY_HASH
        raw = found.get(COMMIT_ROOT % commit)
        root = safe_loads(raw) if raw is not None else None
        return root if isinstance(root, str) else EMPTY_HASH

    diff = Keyset(store, root=root_of(commit_a)).diff(
        Keyset(store, root=root_of(commit_b))
    )
    changes = {key: Change(None, entry) for key, entry in diff.added.items()}
    changes.update({key: Change(entry, None) for key, entry in diff.removed.items()})
    for key, (old, new) in diff.modified.items():
        if old.blob != new.blob:
            changes[key] = Change(old, new)
    return changes


def _decode_parents(raw: bytes | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    value = loads(raw)
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def load_parents(store: KVStore, commit_hash: str) -> tuple[str, ...]:
    """The parent commits of a commit, in order; () for a root or unknown commit."""
    return _decode_parents(store.get(PARENT_COMMIT % commit_hash))


_Record = tuple[tuple[str, ...], "tuple[int, ...] | None"]
"""A commit's parents, and their generations if the commit stores them."""


def _load_records(store: KVStore, commits: Iterable[str]) -> dict[str, _Record]:
    """Parents and parent generations of several commits, in one read."""
    commits = list(dict.fromkeys(commits))
    keys = [key % c for c in commits for key in (PARENT_COMMIT, PARENT_GENS)]
    found = store.get_many(*keys)
    return {
        c: _decode_record(found.get(PARENT_COMMIT % c), found.get(PARENT_GENS % c))
        for c in commits
    }


def _decode_record(parents_raw: bytes | None, gens_raw: bytes | None) -> _Record:
    parents = _decode_parents(parents_raw)
    gens = safe_loads(gens_raw) if gens_raw is not None else None
    if (
        isinstance(gens, list)
        and len(gens) == len(parents)
        and all(type(g) is int and g >= 0 for g in gens)
    ):
        return parents, tuple(gens)
    return parents, None


def _generation(record: _Record) -> int | None:
    """A commit's generation from its record; None if it stores none."""
    parents, gens = record
    if not parents:
        return 0
    if gens is None:
        return None
    return 1 + max(gens)


def _generations(
    store: KVStore,
    commits: Iterable[str],
    known: dict[str, int] | None = None,
) -> dict[str, int]:
    """The generation of each commit, reading as little as it can.

    A commit that stores its parents' generations costs its own record.
    One written before generations were stored costs a walk down its
    history to commits that do store them (or to roots), one read per
    level of the walk. ``known`` supplies and collects generations
    already worked out.
    """
    memo: dict[str, int] = known if known is not None else {}
    commits = list(commits)
    records: dict[str, _Record] = {}
    frontier = [c for c in commits if c not in memo]
    while frontier:
        records.update(_load_records(store, frontier))
        next_frontier: list[str] = []
        for c in frontier:
            g = _generation(records[c])
            if g is not None:
                memo[c] = g
                continue
            for parent in records[c][0]:
                if parent not in memo and parent not in records:
                    next_frontier.append(parent)
        frontier = list(dict.fromkeys(next_frontier))
    # What is left is history without stored generations: fill it in
    # from the parents up, deepest first.
    for c in commits:
        stack = [c]
        while stack:
            node = stack[-1]
            if node in memo:
                stack.pop()
                continue
            parents = records[node][0]
            missing = [p for p in parents if p not in memo]
            if missing:
                stack.extend(missing)
                continue
            memo[node] = 1 + max(memo[p] for p in parents) if parents else 0
            stack.pop()
    return {c: memo[c] for c in commits}


def merge_base(
    store: KVStore,
    commit_a: str,
    commit_b: str,
    *,
    known: dict[str, int] | None = None,
) -> str | None:
    """Find the lowest common ancestor of two commits, or None.

    When several commits tie for lowest — criss-cross histories — the
    smallest hash wins: deterministic, but arbitrary, so criss-cross
    merges resolve cleanly rather than raising. ``known`` collects the
    generations the search works out, for a caller that writes a commit
    over these two next.
    """
    found = _call("merge_base", store, a=commit_a, b=commit_b, check=False)
    if known is not None:
        known.update(found["generations"])
    return found["base"]


def gc(
    store: KVStore,
    min_age: float = 3600,
    *,
    deep: bool = False,
    wait: bool = True,
    lease_ttl: float = GC_LEASE_TTL,
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
        StorageVersionError: if the store is stamped above the layout
            this code reads. Nothing is written, the lease key included.
    """
    # Before the lease, not after: a store this code must not touch has
    # to come out of the call untouched, and an acquire-then-fail would
    # leave an expired lease record behind in a store kvgit had no
    # business writing to.
    _assert_supported_version(store)
    removed = _call(
        "gc", store, min_age=min_age, deep=deep, wait=wait, lease_ttl=lease_ttl
    )
    if removed:
        logging.getLogger("kvgit.orphans").debug(
            "Cleaned %d orphaned commit(s)", removed
        )
    return removed


def clean_orphans(store: KVStore, min_age: float = 3600) -> int:
    """:func:`gc` without the namespace scan, waiting on a busy lease."""
    return gc(store, min_age)


def deep_clean(
    store: KVStore, min_age: float = 3600, *, lease_ttl: float = GC_LEASE_TTL
) -> int:
    """:func:`gc` with the namespace scan, raising on a busy lease."""
    return gc(store, min_age, deep=True, wait=False, lease_ttl=lease_ttl)


class VersionedKV(VersionedBase):
    """A commit log over a KV store.

    The caller owns the working state. VersionedKV provides:
    - ``get()`` / ``get_many()`` to read from the current commit
    - ``commit()`` to atomically write changes and advance HEAD
    - ``refresh()`` to reload from HEAD
    - ``checkout()`` / ``history()`` for navigating commits

    ``recover_from_corrupt_head`` is the optional last-resort tier of
    HEAD resolution, for a HEAD that is present, unusable, and has no
    usable backup. Unset by default, which makes such a branch
    unrecoverable rather than guessed at; pass
    :func:`recover_by_commit_scan` to guess from a scan of every commit
    instead. See :data:`CorruptHeadRecoverer`.
    """

    def __init__(
        self,
        store: KVStore | None = None,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
        create: bool = True,
        recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
        check_version: bool = True,
    ) -> None:
        if store is None:
            store = Memory()
        self.store = store
        _reject_reserved_branch(branch)
        # Applies to every resolve this handle makes — opening, reading
        # HEAD, refreshing, switching, peeking, repairing — and is
        # inherited by the handles ``checkout`` and ``create_branch``
        # hand back, so a caller opts in once rather than per call.
        self._recover_from_corrupt_head = recover_from_corrupt_head

        # A Repo checks once for all the handles it opens.
        if check_version:
            _check_storage_version(store)

        if commit_hash is None:
            commit_hash = _resolve_head(
                store, branch, recover_from_corrupt_head=recover_from_corrupt_head
            )
            if commit_hash is None and store.get(BRANCH_HEAD % branch) is not None:
                raise CorruptHeadError(
                    f"Branch '{branch}' HEAD is corrupt and unrecoverable"
                )
            if commit_hash is None:
                if not create:
                    raise UnknownBranchError(
                        f"Branch '{branch}' does not exist "
                        "(open with create=True to create it)"
                    )
                # A new branch starts at the empty root commit. Another
                # opener may create it first; either way it now exists.
                try:
                    create_branch(store, branch, ROOT_COMMIT)
                except BranchExistsError:
                    pass
                commit_hash = _resolve_head(
                    store, branch, recover_from_corrupt_head=recover_from_corrupt_head
                )
                if commit_hash is None:
                    raise CorruptHeadError(
                        f"Branch '{branch}' HEAD is corrupt and unrecoverable"
                    )

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )

        super().__init__(branch=branch, commit_hash=commit_hash)
        # Stamps only ever rise, so once this handle has seen the store
        # at v4 it never needs to read the stamp again.
        self._blob_version_stamped = False
        # Generations of commits this handle has written, loaded or
        # searched through. A generation never changes once its commit
        # exists, so nothing here goes stale.
        self._generations: dict[str, int] = {}
        # Keyset roots of commits this handle has written, loaded or
        # resolved a head to. A commit's root never changes, so nothing
        # here goes stale either.
        self._roots: dict[str, str] = {}

        # The committed state is the keyset's root, held by the core,
        # with the key index the first listing builds.
        self._tree: _core.WorktreeState | None = None
        self._populate_state(commit_hash)

    def _populate_state(self, commit_hash: str) -> None:
        """Move to a commit's keyset root: read it, and nothing of the
        tree under it. A key index already built follows the move."""
        # The commit's parent record rides along with its root, so the
        # handle knows the generation its next commit builds on without
        # another read.
        found = self.store.get_many(
            COMMIT_ROOT % commit_hash,
            PARENT_COMMIT % commit_hash,
            PARENT_GENS % commit_hash,
        )
        generation = _generation(
            _decode_record(
                found.get(PARENT_COMMIT % commit_hash),
                found.get(PARENT_GENS % commit_hash),
            )
        )
        if generation is not None and COMMIT_ROOT % commit_hash in found:
            self._remember_generations({commit_hash: generation})
        root_raw = found.get(COMMIT_ROOT % commit_hash)
        root = safe_loads(root_raw) if root_raw is not None else None
        if not isinstance(root, str):
            root = EMPTY_HASH
        else:
            self._remember_root(commit_hash, root)
        if self._tree is None:
            self._tree = _core.WorktreeState(root)
        else:
            self._on_tree("wt_move", root=root)

    def _on_tree(self, op: str, **args: Any) -> Any:
        """Run the core's operation ``op`` on this handle's state."""
        run = _core.Op(op, args)
        run.use_tree(self._tree)
        return drive(
            run,
            self.store,
            recoverer=self._recover_from_corrupt_head,
            owner=lambda: _new_lease_owner(),
            clock=time.time,
            sleep=time.sleep,
        )

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers).

        One round trip in the common case, where HEAD is still the
        commit this handle is based on. The read that fetches HEAD also
        fetches that commit's root, so the check that HEAD names a
        commit whose root is present costs nothing extra. Any other
        answer — HEAD moved, or its root is gone — is resolved the full
        way, recovery tiers and all.
        """
        branch_key = BRANCH_HEAD % self._branch
        base = self._base_commit
        base_root = COMMIT_ROOT % base
        found = self.store.get_many(branch_key, base_root)
        head_bytes = found.get(branch_key)
        if (
            head_bytes is not None
            and safe_loads(head_bytes) == base
            and found.get(base_root) is not None
        ):
            return base
        return _resolve_head(
            self.store,
            self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
            roots=self._roots,
        )

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the current commit."""
        return self.get_many(key).get(key)

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the current commit."""
        # Keys holding equal bytes share one blob, so a blob answers for
        # every key that points at it. Missing keys are skipped.
        keys_by_blob: dict[str, list[str]] = {}
        for key, (blob, _size, _chunks) in self._on_tree(
            "wt_lookup", keys=list(dict.fromkeys(keys))
        ):
            keys_by_blob.setdefault(blob, []).append(key)

        if not keys_by_blob:
            return {}

        raw = self.store.get_many(keys_by_blob.keys())
        return {key: value for blob, value in raw.items() for key in keys_by_blob[blob]}

    # -- Commit --

    def commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        on_conflict: str = "raise",
        on_moved: str = "merge",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        info: dict | None = None,
        chunks: dict[str, bytes] | None = None,
        chunk_refs: dict[str, list[str]] | None = None,
    ) -> MergeResult:
        """Commit changes and atomically advance HEAD.

        Creates a new commit with the given changes and advances the
        branch HEAD.  If HEAD has diverged, performs a three-way merge.
        A HEAD move that lands inside the fast-forward CAS window is
        retried once against the new HEAD through that same merge path,
        so a single well-behaved writer never sees ``ConcurrencyError``
        for a race it can merge past.

        With ``on_moved="raise"`` a moved HEAD raises
        :class:`BranchMovedError` instead of merging, whether it moved
        before the commit started or inside the CAS window: the only
        commit that can land is the fast-forward, published by the one
        CAS that expects HEAD at the base. A commit that loses withdraws
        its in-flight marker, leaving nothing reachable.

        Args:
            updates: Key-value pairs to add or update (bytes values).
            removals: Keys to remove.
            on_conflict: ``'raise'`` (default) or ``'abandon'`` for CAS failures.
            on_moved: ``'merge'`` (default) three-way merges onto a HEAD
                that moved since this handle's base; ``'raise'`` raises
                :class:`BranchMovedError` and writes nothing.
            merge_fns: Per-key registrations (override instance-level).
                A merge function, or a ``MergeChoice`` giving one side
                every key it covers.
            merge_prefixes: Registrations by key prefix, layered over
                the instance-level prefix registrations.
            default_merge: Default merge function (override instance-level).
            info: Optional metadata dict for the commit.
            chunks: Optional content-addressed chunks to write under
                ``kvgit:chunk:<hash>``. Keyed by chunk hash. Backends
                that don't understand chunks ignore this argument.
            chunk_refs: Optional per-key list of chunk hashes referenced
                by that key's encoded blob. Stored on the keyset
                ``MetaEntry.chunks`` so GC can trace reachability.

        Returns:
            A ``MergeResult`` (truthy when committed, falsy if abandoned).

        Raises:
            BranchMovedError: If ``on_moved='raise'`` and HEAD moved.
            ConcurrencyError: If ``on_conflict='raise'`` and CAS fails.
            MergeConflict: If keys conflict and no merge function
                resolves them.
        """
        if on_moved not in ("merge", "raise"):
            raise ValueError(f"on_moved must be 'merge' or 'raise', got {on_moved!r}")
        if (updates or removals or info is not None) and on_conflict not in (
            "raise",
            "abandon",
        ):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )

        # The bytes the commit writes stay here; the core names them by
        # key. Every blob and chunk is written, even one already stored,
        # so the batch holds everything its commit needs whatever a sweep
        # took before it landed.
        held: dict[str, bytes] = {}
        entries = []
        refs_by_key = chunk_refs or {}
        for key, value in (updates or {}).items():
            pointer = blob_key(value)
            held[pointer] = value
            refs = refs_by_key.get(key)
            meta = MetaEntry(size=len(value), chunks=list(refs) if refs else None)
            entries.append((key, KeysetEntry(blob=pointer, meta=meta)))
        chunk_keys = []
        for chunk_hash, chunk_bytes in (chunks or {}).items():
            held[CHUNK_PREFIX + chunk_hash] = chunk_bytes
            chunk_keys.append(CHUNK_PREFIX + chunk_hash)

        hosted: list[MergePolicy] = []
        current = self._current_commit
        op = _core.Op.commit(
            self._branch,
            self._base_commit,
            current,
            entries,
            sorted(removals or ()),
            chunk_keys,
            info,
            on_moved,
            on_conflict,
            *self._lowered(hosted, merge_fns, merge_prefixes, default_merge),
            self._recover_from_corrupt_head is not None,
            self._blob_version_stamped,
            *self._known(current),
        )
        return self._land(op, held, hosted)

    def merge_heads(
        self,
        their_head: str,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        post_check: PostCheck | None = None,
        info: dict | None = None,
        fast_forward: bool = True,
    ) -> MergeResult:
        """Merge another head into this branch.

        ``their_head`` is any commit in the store — usually another
        branch's HEAD. Finds the lowest common ancestor with our head,
        three-way resolves, and creates a two-parent merge commit,
        CAS-guarded on our own head (a race raises ``ConcurrencyError``
        and changes nothing). No common ancestor raises
        ``ConcurrencyError`` likewise without changing anything; retrying
        cannot help that case, unlike a race. A ``their_head`` not in the
        store raises ``UnknownCommitError`` before anything else is read.

        A head our history already contains merges as a no-op, provided
        the branch is still at our head. When ours is an ancestor of
        theirs, the branch fast-forwards — HEAD moves to ``their_head``
        and no commit is written, so ``info`` is not recorded — unless
        ``fast_forward=False``, which writes the merge commit anyway.

        ``post_check`` runs over each merge-function-produced value;
        a False files that key as conflicted (handled per
        ``on_conflict`` like any other conflict).
        """
        return self._merge(
            their_head,
            None,
            fast_forward,
            on_conflict,
            merge_fns,
            merge_prefixes,
            default_merge,
            post_check,
            info,
        )

    def apply_change(
        self,
        base: str,
        target: str,
        *,
        on_conflict: str = "raise",
        merge_fns: dict[str, MergePolicy] | None = None,
        merge_prefixes: dict[str, MergePolicy] | None = None,
        default_merge: MergePolicy | None = None,
        post_check: PostCheck | None = None,
        info: dict | None = None,
    ) -> MergeResult:
        """Apply the change from ``base`` to ``target`` as one commit.

        A three-way merge with ``base`` in place of the common ancestor:
        what ``target`` changed relative to ``base`` lands on our head,
        and our own changes since then are kept, conflicts resolved (or
        raised) as in a merge. The result is an ordinary single-parent
        commit, CAS-guarded on our head — a cherry-pick is
        ``apply_change(parent, commit)`` and a revert
        ``apply_change(commit, parent)``.

        A change already present commits nothing (``no_op``), provided the
        branch is still at our head: if it moved, the answer may no longer
        hold, and this raises ``ConcurrencyError`` (or abandons, with
        ``on_conflict='abandon'``) as a lost publish would.

        Raises:
            UnknownCommitError: if ``base`` or ``target`` is not in the
                store, checked before anything else is read.
            UnknownBranchError: if the branch was deleted.
            ConcurrencyError: if the branch moved during the apply, or
                away from our head when the change is already present.
            MergeConflict: if keys conflict and no merge function
                resolves them.
        """
        return self._merge(
            target,
            base,
            False,
            on_conflict,
            merge_fns,
            merge_prefixes,
            default_merge,
            post_check,
            info,
        )

    def _merge(
        self,
        theirs: str,
        base: str | None,
        fast_forward: bool,
        on_conflict: str,
        merge_fns: dict[str, MergePolicy] | None,
        merge_prefixes: dict[str, MergePolicy] | None,
        default_merge: MergePolicy | None,
        post_check: PostCheck | None,
        info: dict | None,
    ) -> MergeResult:
        """Merge ``theirs``, or with ``base`` apply the change from it."""
        if on_conflict not in ("raise", "abandon"):
            raise ValueError(
                f"on_conflict must be 'raise' or 'abandon', got {on_conflict!r}"
            )
        hosted: list[MergePolicy] = []
        current = self._current_commit
        op = _core.Op.merge(
            self._branch,
            current,
            theirs,
            base,
            fast_forward,
            info,
            on_conflict,
            *self._lowered(hosted, merge_fns, merge_prefixes, default_merge),
            post_check is not None,
            self._recover_from_corrupt_head is not None,
            self._blob_version_stamped,
            *self._known(current),
        )
        return self._land(op, {}, hosted, post_check)

    def _lowered(
        self,
        hosted: list[MergePolicy],
        merge_fns: dict[str, MergePolicy] | None,
        merge_prefixes: dict[str, MergePolicy] | None,
        default_merge: MergePolicy | None,
    ) -> tuple[dict[str, str | int], list[tuple[str, str | int]], str | int | None]:
        """The merge rules in effect, layered over this handle's, as the
        core names them: a side, or the number of a function in
        ``hosted``, which gains each function named."""

        def lower(policy: MergePolicy) -> str | int:
            if isinstance(policy, MergeChoice):
                return policy.value
            hosted.append(policy)
            return len(hosted) - 1

        fns = {**self._merge_fns, **(merge_fns or {})}
        prefixes = {**self._merge_prefixes, **(merge_prefixes or {})}
        default = default_merge or self._default_merge
        return (
            {k: lower(p) for k, p in fns.items()},
            [(k, lower(p)) for k, p in prefixes.items()],
            lower(default) if default is not None else None,
        )

    def _known(
        self, commit: str
    ) -> tuple[list[tuple[str, str]], list[tuple[str, int]]]:
        """The root and generation this handle knows of ``commit``."""
        root = self._roots.get(commit)
        generation = self._generations.get(commit)
        return (
            [] if root is None else [(commit, root)],
            [] if generation is None else [(commit, generation)],
        )

    def _land(
        self,
        op: Any,
        held: dict[str, bytes],
        hosted: list[MergePolicy],
        post_check: PostCheck | None = None,
    ) -> MergeResult:
        """Run a commit or merge, and move this handle to what it landed."""
        op.use_tree(self._tree)
        done = drive(
            op,
            self.store,
            held=held,
            merge_fns=hosted,
            post_check=post_check,
            recoverer=self._recover_from_corrupt_head,
            clock=time.time,
            sleep=time.sleep,
        )
        result = MergeResult(
            merged=done["merged"],
            commit=done["commit"],
            strategy=done["strategy"],
            auto_merged_keys=done["auto_merged_keys"],
            carried_keys=done["carried_keys"],
        )
        self.last_merge_result = result
        if not result.merged or result.strategy == "no_op":
            return result
        # Landed: the core moved this handle's state with it.
        for commit, commit_root in done["roots"]:
            self._remember_root(commit, commit_root)
        self._remember_generations(dict(done["generations"]))
        self._blob_version_stamped = done["stamped"]
        self._current_commit = self._base_commit = result.commit
        return result

    # -- Abstract method implementations --

    def keys(self) -> KeysView[str]:
        """All keys in the current commit. The first call builds the key
        index with one walk of the tree; later ones read it."""
        return dict.fromkeys(self._on_tree("wt_keys")).keys()

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and self._on_tree("wt_contains", key=key)

    _GENERATION_CACHE_LIMIT = 10_000

    def _remember_root(self, commit_hash: str, root: str) -> None:
        if len(self._roots) >= self._GENERATION_CACHE_LIMIT:
            self._roots.clear()
        self._roots[commit_hash] = root

    def _remember_generations(self, generations: dict[str, int]) -> None:
        if len(self._generations) + len(generations) > self._GENERATION_CACHE_LIMIT:
            self._generations.clear()
        self._generations.update(generations)

    def _changes(self, commit_a: str | None, commit_b: str) -> dict[str, Change]:
        """Each key whose value differs from commit ``a`` to ``b``; see
        :func:`commit_changes`."""
        return commit_changes(self.store, commit_a, commit_b)

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""
        return load_parents(self.store, commit_hash)

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Lowest common ancestor of two commits; see :func:`merge_base`."""
        found: dict[str, int] = {}
        base = merge_base(self.store, commit_a, commit_b, known=found)
        self._remember_generations(found)
        return base

    # -- Navigation --

    def refresh(self) -> None:
        """Reload state from HEAD."""
        commit_hash = _resolve_head(
            self.store,
            self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )
        if commit_hash is None:
            if self.store.get(BRANCH_HEAD % self._branch) is not None:
                raise CorruptHeadError(
                    f"Branch '{self._branch}' HEAD is corrupt and unrecoverable"
                )
            raise UnknownBranchError(f"Branch '{self._branch}' does not exist")
        self._load_commit(commit_hash, update_base=True)

    def checkout(
        self,
        commit_hash: str | None = None,
        *,
        branch: str | None = None,
        tag: str | None = None,
    ) -> "VersionedKV | None":
        """Return a new VersionedKV at a specific commit or tag.

        Name the commit positionally or name a ``tag``, not both.

        The handle comes back on this handle's branch (or ``branch``),
        not on a branch of its own — there is no read-only mode. A
        commit made from it goes through the ordinary HEAD CAS, so it
        fast-forwards when the branch has not moved since, and conflicts
        exactly as any other stale handle does when it has.

        Returns None when the commit, or the tag, is not in the store.
        """
        if (commit_hash is None) == (tag is None):
            raise ValueError("Pass exactly one of commit_hash or tag")
        if tag is not None:
            tagged = _resolve_tag(self.store, tag)
            if tagged is None or self.store.get(COMMIT_ROOT % tagged) is None:
                return None
            commit_hash = tagged
        elif self.store.get(COMMIT_ROOT % commit_hash) is None:
            return None
        return VersionedKV(
            self.store,
            commit_hash=commit_hash,
            branch=branch or self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )

    def create_branch(self, name: str, *, at: str | None = None) -> "VersionedKV":
        """Fork a commit onto a new branch.

        Returns a new VersionedKV instance on the new branch.
        """
        target = at or self._current_commit
        create_branch(self.store, name, target)
        return VersionedKV(
            self.store,
            commit_hash=target,
            branch=name,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )

    def delete_branch(self, name: str) -> None:
        """Delete a branch and clean up orphaned commits.

        Raises:
            UnknownBranchError: if there is no such branch.
            ConcurrencyError: if the branch's head changed while it was
                being deleted; nothing was removed.
            ValueError: if ``name`` is the current branch, empty, or in
                the reserved tag namespace.
        """
        _reject_reserved_branch(name)
        if name == self._branch:
            raise ValueError("Cannot delete the current branch")
        delete_branch(self.store, name)
        self.clean_orphans()

    def switch_branch(self, name: str) -> None:
        """Switch this instance to a different branch in-place."""
        _reject_reserved_branch(name)
        commit_hash = _resolve_head(
            self.store,
            name,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )
        if commit_hash is None:
            if self.store.get(BRANCH_HEAD % name) is not None:
                raise CorruptHeadError(
                    f"Branch '{name}' HEAD is corrupt and unrecoverable"
                )
            raise UnknownBranchError(f"Branch '{name}' does not exist")
        self._branch = name
        self._load_commit(commit_hash, update_base=True)

    def peek(
        self, key: str, *, branch: str | None = None, tag: str | None = None
    ) -> bytes | None:
        """Read a key from another branch's HEAD, or from a tag.

        Pass exactly one of ``branch`` or ``tag``. Returns None when the
        key, the branch, or the tag is not there.
        """
        if (branch is None) == (tag is None):
            raise ValueError("Pass exactly one of branch or tag")
        commit_hash: str | None
        if branch is not None:
            _reject_reserved_branch(branch)
            commit_hash = _resolve_head(
                self.store,
                branch,
                recover_from_corrupt_head=self._recover_from_corrupt_head,
            )
        else:
            tagged = _resolve_tag(self.store, tag or "")
            commit_hash = (
                tagged
                if tagged is not None
                and self.store.get(COMMIT_ROOT % tagged) is not None
                else None
            )
        if commit_hash is None:
            return None
        root = _load_root(self.store, commit_hash)
        if root is None:
            return None
        ks = Keyset(self.store, root=root)
        entry = ks.get(key)
        if entry is None:
            return None
        return self.store.get(entry.blob)

    def reset_to(self, commit_hash: str) -> bool:
        """Reset HEAD to a specific commit.

        Whatever HEAD held becomes the prev-HEAD backup in the same
        write. A concurrent writer moving HEAD in between does not stop
        the reset; it is retried against the new HEAD. A deleted branch
        is not recreated: the write expects the HEAD bytes it read, so a
        delete that lands first makes it fail and the retry raises.

        Raises:
            UnknownBranchError: the branch has no HEAD.
        """
        if not _call("reset_branch", self.store, name=self._branch, target=commit_hash):
            return False
        self._load_commit(commit_hash, update_base=True)
        return True

    @staticmethod
    def branches(store: KVStore) -> list[str]:
        """List all branch names in the store.

        Tags live under the reserved ``refs/tags/`` branch name and are
        excluded here: callers of this list treat what it returns as
        branches — switching to them, deleting them, showing them to a
        user — and a tag is none of those things. :meth:`tags` lists
        those.
        """
        return _call("branches", store)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return VersionedKV.branches(self.store)

    @staticmethod
    def exists(store: KVStore, name: str) -> bool:
        """Whether a branch exists — openable without creating.

        The same predicate :meth:`branches` lists by: the branch has a
        HEAD entry. The empty name and reserved ``refs/tags/`` names are
        never branches, so they read as missing here as they are
        excluded there. A damaged HEAD still counts as existing; opening
        it raises either way. Never writes, so checking cannot resurrect
        a deleted branch.
        """
        return isinstance(name, str) and _call("branch_exists", store, name=name)

    def branch_exists(self, name: str) -> bool:
        """Whether a branch exists in this handle's store."""
        return VersionedKV.exists(self.store, name)

    # -- Tags --

    def tag(self, name: str, *, at: str | None = None, info: dict | None = None) -> str:
        """Name a commit permanently, and keep it (and its ancestry) alive.

        A tag is immutable: creating one over an existing name raises,
        and there is no move. Point a name somewhere else by deleting
        the tag and creating it again, so that the history of the name
        is at least visible in the calling code. Tags and branches are
        separate namespaces — ``v1`` can be both, and they are unrelated.

        The tagged commit and everything it descends from survive
        garbage collection for as long as the tag exists, exactly as a
        branch head's ancestry does — because a tag *is* a branch head,
        stored under the reserved name ``refs/tags/<name>`` and hidden
        from the branch API.

        That storage choice is the compatibility contract, and it is
        worth stating plainly. Any kvgit that walks branch heads reads a
        tag's commit and keeps it alive, including versions written
        before tags existed and including their anchor-free admin sweep;
        no version stamp could have taught them a new key kind. What
        such a version does see is a branch named ``refs/tags/<name>``.
        It can delete that branch by name, or switch to it and commit,
        which deletes or moves the tag — both deliberate acts naming a
        path that says what it is.

        Args:
            at: Commit to tag. Defaults to this handle's current commit,
                the last *committed* state — pending changes are not part
                of any commit yet and are not tagged.
            info: Optional caller metadata, stored beside the tag. Must
                be JSON-serializable, like commit info.

        Returns:
            The commit hash the tag names.

        Raises:
            ValueError: if the name is unusable, already taken, or the
                commit does not exist.

        Tagging a commit that is *already* an orphan older than
        ``min_age`` can still lose to a concurrent sweep, which was free
        to collect that commit before the tag existed — but it loses
        cleanly. The lookup and the tag are decided against one lease
        record, so it either tags a commit that is really there or
        raises; it never leaves a tag naming a commit the sweep took.
        Callers tag a commit they are holding — a head, or something a
        head descends from — and a commit a branch reaches is never a
        sweep candidate.
        """
        target = at or self._current_commit
        create_tag(self.store, name, target, info)
        return target

    def tags(self) -> dict[str, str]:
        """Map every tag in the store to the commit it names."""
        return tags(self.store)

    def tag_info(self, name: str) -> TagInfo | None:
        """Describe one tag, or None if the store has no such tag."""
        return tag_info(self.store, name)

    def delete_tag(self, name: str) -> None:
        """Remove a tag, then sweep the commits nothing else reaches.

        Three keys go — the tag's reserved head, that head's prev-HEAD
        backup, and the info record — and then :meth:`clean_orphans`
        runs, matching :meth:`delete_branch`. A commit the tag was the
        last root for becomes collectable at that point, subject to the
        sweep's ``min_age`` guard.

        This code never writes a backup for a tag, since it never moves
        one. The removal is for a backup written by something that
        treated the tag as an ordinary branch — switching to it and
        committing — where leaving it behind would let head resolution
        serve the deleted tag's commit under a later branch of the same
        reserved name.

        Raises:
            UnknownTagError: if there is no such tag.
            ConcurrencyError: if the tag's head changed while it was being
                deleted; nothing was removed, and no sweep ran.
        """
        delete_tag(self.store, name)
        self.clean_orphans()

    def commit_info(self, commit_hash: str | None = None) -> dict | None:
        """Retrieve the info dict for a commit, or None if none was stored."""
        target = commit_hash or self._current_commit
        info_bytes = self.store.get(INFO_KEY % target)
        if info_bytes is None:
            return None
        return loads(info_bytes)

    # -- Recovery --

    def repair_head(self) -> str | None:
        """Persist a recovered HEAD for this branch.

        Thin instance wrapper over :func:`repair_head`. Reads recover a
        damaged HEAD without writing it back; this is the explicit call
        that makes the recovery durable.

        Returns:
            The commit HEAD now names, or None if nothing was
            recoverable.
        """
        return repair_head(
            self.store,
            self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )

    # -- Orphan cleanup --

    def clean_orphans(self, min_age: float = 3600) -> int:
        """Remove orphaned commits unreachable from any branch HEAD.

        Thin instance wrapper over :func:`clean_orphans`, which does the
        mark-and-sweep against ``self.store`` under the GC lease. Kept as
        a method so existing callers (and ``delete_branch``) read
        naturally.

        Returns:
            Number of orphaned commits removed.
        """
        return clean_orphans(self.store, min_age)

    def deep_clean(
        self, min_age: float = 3600, *, lease_ttl: float = GC_LEASE_TTL
    ) -> int:
        """Orphan sweep plus a scan for content nothing references.

        Thin instance wrapper over :func:`deep_clean`. Use
        :meth:`clean_orphans` for routine cleanup and this one as an
        occasional maintenance pass, for leftovers no orphan keyset
        points at.

        Returns:
            Number of orphaned commits removed.

        Raises:
            GcBusy: if another sweep holds a live lease.
            ValueError: if the store is stamped above the layout this
                code reads. Nothing is written, the lease key included.
        """
        return deep_clean(self.store, min_age, lease_ttl=lease_ttl)

    # -- Internal --

    def _load_commit(self, commit_hash: str, *, update_base: bool) -> None:
        """Load a commit's state into memory."""
        self._populate_state(commit_hash)
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
