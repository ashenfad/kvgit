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
import json
import logging
import os
import time
import uuid
from collections import deque
from collections.abc import Callable

from ..encoding import dumps, loads, safe_loads
from ..errors import GcBusy, UnknownBranchError
from ..hamt import EMPTY_HASH
from ..kv.base import KVStore
from ..kv.memory import Memory
from .base import VersionedBase
from .helpers import walk_history
from .keyset import Keyset, KeysetEntry, MetaEntry
from .merge import MergeResolution
from .protocol import TagInfo

PARENT_COMMIT = "__parent_commit__%s"
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
    payload = json.dumps(
        ["kvgit/4", list(parents), root, created, info],
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:40]


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
        raise ValueError(
            f"Store has kvgit storage version {version!r}, "
            f"this code supports {sorted(SUPPORTED_READ_VERSIONS)}. "
            "Use a fresh store."
        )


def _stamp_version_at_least(store: KVStore, version: int) -> None:
    """Raise the store's version stamp to ``version`` if it is lower.

    Called from any path that writes an artifact older layouts cannot
    handle — currently a chunk, which needs v3.

    The write is a CAS against the exact bytes the decision was made
    from, not a plain ``set``: read-then-set lets two writers stamping
    different versions interleave so that the *lower* one lands last,
    leaving a store whose stamp under-describes what is in it, which is
    precisely the state older readers are willing to open. A lost CAS
    means someone else moved the stamp, so the loop re-reads and decides
    again. It terminates because a stamp is only ever raised: every
    iteration either returns or observes a strictly higher value.
    """
    while True:
        raw = store.get(STORAGE_VERSION_KEY)
        current = safe_loads(raw) if raw is not None else None
        if (
            isinstance(current, int)
            and not isinstance(current, bool)
            and current >= version
        ):
            return
        if store.cas(STORAGE_VERSION_KEY, dumps(version), expected=raw):
            return


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
        raise ValueError(
            "Store appears to use an older kvgit storage format. "
            f"This version requires storage v{min(SUPPORTED_READ_VERSIONS)} "
            "or higher. Use a fresh store."
        )
    store.set(STORAGE_VERSION_KEY, dumps(STORAGE_VERSION))


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
) -> str | None:
    """Resolve a branch HEAD, falling back to prev HEAD then an injected recoverer.

    **Never writes.** Every read path in the library goes through here,
    so healing the damage in place would make an ordinary ``get`` a
    mutation — impossible for a read-only consumer, and a race between
    two readers repairing the same branch to different answers. The
    recovery is returned to the caller and forgotten; :func:`repair_head`
    is the explicit call that makes it durable, and the write path heals
    HEAD itself as part of the CAS that has to move it anyway.

    The cost of not persisting is paid per read on a damaged store: two
    extra ``get`` calls for the prev-HEAD tier, plus whatever the
    injected recoverer costs below it. A store sitting on a corrupt HEAD
    has a bigger problem than read latency.

    Args:
        recover_from_corrupt_head: Optional third tier, fired only when
            HEAD exists, is unusable, and the backup did not save it.
            Unset — the default — means such a branch resolves to None.
            See :data:`CorruptHeadRecoverer`.

    Returns a valid commit hash, or None if unrecoverable.
    """
    # 1. Try current HEAD
    head_bytes = store.get(BRANCH_HEAD % branch)
    if head_bytes is not None:
        commit_hash = safe_loads(head_bytes)
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            return commit_hash

    # 2. HEAD is present but unusable — try the backup.
    #
    # Only reached when HEAD exists. An absent HEAD does not mean
    # damage, it means the branch is gone — ``delete_branch`` removes
    # the key — and a backup that outlives its branch must not bring
    # the branch back. This code writes a backup only in the same atomic
    # write as its HEAD and removes the two together, but a store an
    # older kvgit wrote, which did each separately, can hold a backup
    # with no HEAD.
    prev_bytes = (
        store.get(BRANCH_HEAD_PREV % branch) if head_bytes is not None else None
    )
    if prev_bytes is not None:
        commit_hash = safe_loads(prev_bytes)
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            logger.warning(
                "Branch '%s': HEAD corrupt, recovered from prev HEAD", branch
            )
            return commit_hash

    # 3. HEAD existed, is corrupt, and the backup did not save it. The
    # store no longer holds the answer, so there is nothing left to
    # read — only to guess. Guessing is the caller's call, not ours.
    if recover_from_corrupt_head is not None and head_bytes is not None:
        commit_hash = recover_from_corrupt_head(store, branch)
        # A recoverer is caller-supplied, so its answer is checked the
        # same way anything else read out of the store is: a string
        # naming a commit whose root is present. This function promises
        # a valid commit or None, and an unchecked answer is worse than
        # no answer — ``repair_head`` makes it durable, replacing
        # obviously-corrupt HEAD bytes with a plausible hash naming
        # nothing, which is harder to diagnose than the damage it
        # replaced.
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            logger.warning(
                "Branch '%s': HEAD corrupt, recovered via injected recoverer",
                branch,
            )
            return commit_hash
        if commit_hash is not None:
            logger.warning(
                "Branch '%s': recoverer returned %r, which is not a commit in "
                "this store; treating the branch as unrecoverable",
                branch,
                commit_hash,
            )

    return None


def recover_by_commit_scan(store: KVStore, branch: str) -> str | None:
    """Guess a corrupt branch's HEAD by scanning every commit. Opt-in.

    Finds all valid commits, excludes those reachable from healthy
    branches, and returns the most recent remaining tip (by
    ``__commit_time__``). A :data:`CorruptHeadRecoverer`, so it is
    passed in rather than reached for::

        from kvgit.versioned.kv import recover_by_commit_scan

        v = VersionedKV(store, recover_from_corrupt_head=recover_by_commit_scan)

    kvgit's default through v0.3.3, and **not** the default any more.
    The name says what it does rather than what it is for, because what
    it is for is the part that cannot be guaranteed: this is a heuristic
    over a store that has already lost the answer.

    Two things to weigh before wiring it in.

    **It can serve another branch's deleted data.** "Not claimed by a
    healthy branch" is the only signal it has for whose commit a commit
    is, and a deleted branch's commits are unclaimed by definition until
    :func:`clean_orphans` collects them. Delete a branch, damage an
    unrelated branch's HEAD, lose its backup, and this returns the
    deleted branch's tip — grafting onto the survivor a lineage it never
    had, behind a ``logger.warning``. No race, no concurrency, no
    unusual store required.

    **It is O(store).** Every ``__commit_root__`` and every branch
    ancestry, walked per unresolved read until someone calls
    :func:`repair_head`.

    It is worth it when losing the branch outright is worse than
    recovering it to a plausible commit — a single-branch store, or one
    where branches are never deleted, has neither hazard in play. That
    judgement belongs to whoever owns the data.
    """
    root_prefix = COMMIT_ROOT.replace("%s", "")
    all_commits: dict[str, float] = {}
    for key in store.keys(root_prefix):
        if not isinstance(key, str) or not key.startswith(root_prefix):
            continue
        h = key[len(root_prefix) :]
        if not h:
            continue
        time_bytes = store.get(COMMIT_TIME % h)
        ts = 0.0
        if time_bytes is not None:
            try:
                val = safe_loads(time_bytes)
                if isinstance(val, (int, float)):
                    ts = float(val)
            except Exception:  # noqa: BLE001 — recovery scan over a store
                # already known to be damaged; an unreadable timestamp
                # must degrade to 0.0, never abort the scan.
                pass
        all_commits[h] = ts

    if not all_commits:
        return None

    # Exclude commits reachable from healthy branches
    claimed: set[str] = set()
    head_prefix = BRANCH_HEAD.replace("%s", "")
    for key in store.keys(head_prefix):
        if not isinstance(key, str) or not key.startswith(head_prefix):
            continue
        other = key[len(head_prefix) :]
        if other == branch or not other:
            continue
        hb = store.get(key)
        if hb is None:
            continue
        h = safe_loads(hb)
        if not isinstance(h, str) or store.get(COMMIT_ROOT % h) is None:
            continue
        # Walk parent chain
        stack = [h]
        while stack:
            c = stack.pop()
            if c in claimed:
                continue
            claimed.add(c)
            pb = store.get(PARENT_COMMIT % c)
            if pb is not None:
                parsed = safe_loads(pb)
                if isinstance(parsed, str):
                    stack.append(parsed)
                elif isinstance(parsed, list):
                    stack.extend(p for p in parsed if isinstance(p, str))

    candidates = {h for h in all_commits if h not in claimed}
    if not candidates:
        candidates = set(all_commits)

    # Find tips (not a parent of any other candidate)
    all_parents: set[str] = set()
    for h in candidates:
        pb = store.get(PARENT_COMMIT % h)
        if pb is not None:
            parsed = safe_loads(pb)
            if isinstance(parsed, str):
                all_parents.add(parsed)
            elif isinstance(parsed, list):
                all_parents.update(p for p in parsed if isinstance(p, str))
    tips = candidates - all_parents
    if not tips:
        tips = candidates

    return max(tips, key=lambda h: all_commits.get(h, 0))


def _heal_head(store: KVStore, branch: str, recovered: bytes) -> bool:
    """Atomically replace an unresolvable HEAD with a recovered value.

    Returns True only when HEAD was damaged *and* this call is the one
    that replaced it. Three cases are deliberately left alone:

    * HEAD resolves fine — it did not break, it *moved*, and another
      writer won a legitimate race. Overwriting it would destroy a good
      commit to make a losing CAS succeed.
    * HEAD already holds ``recovered`` — nothing to do.
    * HEAD is absent — the branch was deleted. Re-creating the key is
      exactly the resurrection ``delete_branch`` drops the prev-HEAD
      backup to prevent.

    The replacement is a CAS against the exact damaged bytes, so two
    processes healing the same branch cannot both win, and a HEAD that
    someone else repaired (or advanced) in the meantime is never
    clobbered.
    """
    branch_key = BRANCH_HEAD % branch
    # Installing a recovered HEAD makes a commit reachable that the
    # store did not claim a moment ago — a root appearing under a sweep
    # that has already decided what is reachable. So the checks and the
    # write happen against one lease record, and a sweep in between
    # sends the whole decision round again.
    while True:
        lease = _wait_for_gc(store)
        raw = store.get(branch_key)
        if raw is None or raw == recovered:
            return False
        commit_hash = safe_loads(raw)
        if (
            isinstance(commit_hash, str)
            and store.get(COMMIT_ROOT % commit_hash) is not None
        ):
            return False
        landed = _try_land(store, lease, {branch_key: raw}, {branch_key: recovered})
        if landed is None:
            continue
        if not landed:
            return False
        logger.warning(
            "Branch '%s': corrupt HEAD replaced with recovered commit", branch
        )
        return True


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
    commit_hash = _resolve_head(
        store, branch, recover_from_corrupt_head=recover_from_corrupt_head
    )
    if commit_hash is None:
        return None
    if _heal_head(store, branch, dumps(commit_hash)):
        return commit_hash
    return _resolve_head(
        store, branch, recover_from_corrupt_head=recover_from_corrupt_head
    )


def _validate_tag_name(name: str) -> None:
    """Reject tag names that cannot be stored, or cannot be read back.

    Branch names are unvalidated, so this is deliberately close to
    unvalidated too: any non-empty string, ``/`` included, so embedders
    can namespace their own tags (``pub/v1``). ``%`` is the one
    exclusion — tag keys are built with ``%``-formatting, and a name
    carrying its own format specifier turns the key template into
    something other than a template.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Tag name must be a non-empty string")
    if "%" in name:
        raise ValueError(f"Tag name must not contain '%': {name!r}")


def _tag_branch(name: str) -> str:
    """The reserved branch name a tag's commit pointer lives under."""
    return TAG_BRANCH_PREFIX + name


def _reject_reserved_branch(name: str) -> None:
    """Refuse a branch name inside the reserved tag namespace.

    The branch API does not hand out names under ``refs/tags/``, because
    a branch there would be indistinguishable from a tag: it would show
    up in ``tags()``, and moving it would silently move the tag. Tags
    are created, listed and deleted through the tag API instead.
    """
    if isinstance(name, str) and name.startswith(TAG_BRANCH_PREFIX):
        raise ValueError(
            f"Branch name {name!r} is reserved for tags "
            f"(the '{TAG_BRANCH_PREFIX}' namespace). "
            "Use tag() / tags() / delete_tag() instead."
        )


def _resolve_tag(store: KVStore, name: str) -> str | None:
    """Read the commit a tag names, or None if it does not resolve.

    Reads the tag's head key and nothing else. The prev-HEAD recovery
    tiers a branch gets do not apply: a tag is written once and never
    moved, so this code never creates a backup for one, and a backup
    left by something that treated the tag as an ordinary branch
    describes a move that is not ours to honour.
    """
    raw = store.get(BRANCH_HEAD % _tag_branch(name))
    if raw is None:
        return None
    commit_hash = safe_loads(raw)
    return commit_hash if isinstance(commit_hash, str) else None


def tags(store: KVStore) -> dict[str, str]:
    """Map every tag in the store to the commit it names.

    Dangling tags — ones whose commit is no longer in the store — are
    included, because leaving them out would make a damaged tag look
    deleted. :func:`tag_info` says which is which.
    """
    prefix = BRANCH_HEAD % TAG_BRANCH_PREFIX
    found: dict[str, str] = {}
    for key in store.keys(prefix):
        if not (isinstance(key, str) and key.startswith(prefix)):
            continue
        name = key[len(prefix) :]
        if not name:
            continue
        raw = store.get(key)
        if raw is None:
            continue
        commit_hash = safe_loads(raw)
        if isinstance(commit_hash, str):
            found[name] = commit_hash
    return dict(sorted(found.items()))


def tag_info(store: KVStore, name: str) -> TagInfo | None:
    """Describe one tag, or None if the store has no such tag."""
    commit_hash = _resolve_tag(store, name)
    if commit_hash is None:
        return None

    created: float | None = None
    info: dict | None = None
    record_bytes = store.get(TAG_INFO_KEY % name)
    if record_bytes is not None:
        record = safe_loads(record_bytes)
        if isinstance(record, dict):
            ts = record.get("time")
            if isinstance(ts, (int, float)) and not isinstance(ts, bool):
                created = float(ts)
            stored_info = record.get("info")
            if isinstance(stored_info, dict):
                info = stored_info

    return TagInfo(
        name=name,
        commit=commit_hash,
        time=created,
        info=info,
        dangling=store.get(COMMIT_ROOT % commit_hash) is None,
    )


def _lease_expiry(raw: bytes | None) -> float:
    """Unix time the lease in ``raw`` runs out; 0.0 if there is no lease.

    Bytes that do not decode to a record with a numeric ``expires`` are
    treated as already expired rather than as an error. A lease is a
    hint about who is sweeping right now, so garbage under the key must
    not wedge the store forever — CAS against those exact bytes takes it
    over.
    """
    if raw is None:
        return 0.0
    record = safe_loads(raw)
    if not isinstance(record, dict):
        return 0.0
    expires = record.get("expires")
    if isinstance(expires, (int, float)) and not isinstance(expires, bool):
        return float(expires)
    return 0.0


def _wait_for_gc(store: KVStore) -> bytes | None:
    """Block until no live GC lease is held; return the lease record then.

    The bytes returned are what a write that must not land under a
    sweep expects the lease key to still hold: absent, or the expired
    record the last sweep left. A sweep that starts afterwards replaces
    them with a record of its own — every acquisition writes a fresh
    owner id — so a ``cas_many`` expecting them fails for as long as that
    sweep runs, and afterwards too; the writer comes back here and waits.

    Costs one ``get`` when no lease is live, which is the common case.
    While a lease is live this polls, sleeping at most until that
    lease's own expiry, so a holder that died without releasing delays a
    writer by the remainder of its term and no longer. A fresh lease
    taken by a different sweep is waited out in turn.
    """
    raw = store.get(GC_LEASE_KEY)
    while raw is not None:
        remaining = _lease_expiry(raw) - time.time()
        if remaining <= 0:
            return raw
        time.sleep(min(GC_WAIT_POLL, remaining))
        raw = store.get(GC_LEASE_KEY)
    return None


def _try_land(
    store: KVStore,
    lease: bytes | None,
    expected: dict[str, bytes | None],
    writes: dict[str, bytes],
    removes: tuple[str, ...] = (),
) -> bool | None:
    """Apply a batch unless a sweep has started since ``lease`` was read.

    Returns True when the batch landed, False when one of the caller's
    own expectations failed, and None when the lease moved — a sweep
    began (or ran) since, so whatever the caller checked before writing
    may no longer hold; it waits again, re-checks, and retries.
    """
    if store.cas_many({GC_LEASE_KEY: lease, **expected}, writes, removes):
        return True
    if store.get(GC_LEASE_KEY) != lease:
        return None
    return False


def _acquire_gc_lease(store: KVStore, lease_ttl: float) -> tuple[bytes, float, float]:
    """Take the GC lease by CAS, or raise :class:`GcBusy`.

    The CAS expects exactly the bytes the liveness decision was made
    from: absent, or an expired/undecodable record. Two sweeps racing
    the same expired lease therefore cannot both win, and a live lease
    held by someone else is never overwritten.

    Returns:
        The lease bytes written, the unix time the lease was taken, and
        the unix time it expires at.
    """
    raw = store.get(GC_LEASE_KEY)
    now = time.time()
    expiry = _lease_expiry(raw)
    if expiry > now:
        holder = safe_loads(raw)
        owner = holder.get("owner") if isinstance(holder, dict) else None
        raise GcBusy(
            f"A sweep holds the GC lease (owner {owner!r}, "
            f"expires in {expiry - now:.1f}s)"
        )
    expires = now + lease_ttl
    owner = f"{os.getpid()}-{uuid.uuid4().hex[:12]}"
    ours = dumps({"owner": owner, "expires": expires})
    if not store.cas(GC_LEASE_KEY, ours, expected=raw):
        raise GcBusy("Another sweep took the GC lease first")
    return ours, now, expires


def _release_gc_lease(
    store: KVStore, ours: bytes, expires: float, lease_ttl: float
) -> None:
    """Give up a lease this call took, if the store still holds it.

    Release is a CAS overwriting our own bytes with the same record
    expired (``expires: 0``), never a delete: writers that read the lease
    before this sweep began expect the bytes they read, and a record
    carrying this sweep's owner id can never match them again. A failed
    CAS means the lease under the key is no longer ours — it ran out and
    another sweep claimed it — and that sweep's lease must not be
    cleared, so the failure is ignored.
    """
    overrun = time.time() - expires
    if overrun > 0:
        logging.getLogger("kvgit.orphans").warning(
            "a sweep ran %.1fs past its %.1fs GC lease; writers were free to "
            "write during that window. Raise lease_ttl.",
            overrun,
            lease_ttl,
        )
    record = safe_loads(ours)
    owner = record.get("owner") if isinstance(record, dict) else None
    store.cas(GC_LEASE_KEY, dumps({"owner": owner, "expires": 0}), expected=ours)


def clean_orphans(store: KVStore, min_age: float = 3600) -> int:
    """Remove orphaned commits unreachable from any branch HEAD.

    Traces all reachable commits from live branch HEADs, then deletes
    the orphaned commits' metadata and everything in their keysets —
    blobs, HAMT nodes, chunks — that nothing live shares. Tags need no
    special handling: a tag is a branch head under a reserved name, so
    it keeps its commit's whole ancestry alive by being walked with
    everything else.

    Handle-independent by design: it marks from ALL live branch HEADs
    and touches nothing but ``store``, so it works with or without a
    ``VersionedKV`` anchored on it. :meth:`VersionedKV.clean_orphans`
    and the anchor-free admin paths (:func:`kvgit.delete_branches`,
    :func:`kvgit.delete_tags`) share this one implementation.

    Runs under the store's GC lease, waiting out another sweep's lease
    first rather than failing. While it holds the lease no commit batch
    can land, and every commit written but not yet published carries an
    in-flight marker the sweep marks from, so it is safe beside
    concurrent writers at any ``min_age`` — including 0. ``min_age`` is
    policy alone: how long abandoned work lingers before it is taken.

    Returns:
        Number of orphaned commits removed.

    Raises:
        ValueError: if the store is stamped above the layout this code
            reads. Nothing is written, the lease key included.
    """
    _assert_supported_version(store)
    while True:
        _wait_for_gc(store)
        try:
            ours, _acquired, expires = _acquire_gc_lease(store, GC_LEASE_TTL)
        except GcBusy:
            continue
        break
    try:
        return _sweep(store, min_age, deep=False)
    finally:
        _release_gc_lease(store, ours, expires, GC_LEASE_TTL)


def deep_clean(
    store: KVStore,
    min_age: float = 3600,
    *,
    lease_ttl: float = GC_LEASE_TTL,
) -> int:
    """:func:`clean_orphans`, plus a scan for content nothing references.

    Sweeps orphans the way :func:`clean_orphans` does, then scans the
    whole ``kvgit:blob:``, ``kvgit:keyset:`` and ``kvgit:chunk:``
    namespaces and deletes anything no live branch head, young orphan or
    in-flight commit reaches. That scan is the only way to reclaim
    content no commit references any more — leftovers from a crash,
    from an interrupted write, or from a store swept by an earlier
    kvgit — because no orphan keyset points at them. Run it as an
    occasional maintenance pass; :func:`clean_orphans` is the routine
    one.

    1. It refuses a store stamped above the layout this code reads,
       before touching the lease key, so such a store comes out of the
       call with nothing written to it at all.
    2. It takes the ``__gc_lease__`` key by CAS, raising :class:`GcBusy`
       if another sweep holds an unexpired one.
    3. It marks and sweeps. Every commit batch is written conditionally
       on the lease record, so none can land while the lease is held;
       batches that landed before carry in-flight markers the mark
       phase reads, so their commits survive until published.
    4. It releases the lease, in a ``finally``.

    A ``lease_ttl`` shorter than the sweep takes is not extended
    silently: the sweep finishes and logs a warning naming the overrun,
    during which writers are free to write. Set it above the longest
    sweep this store has taken.

    Args:
        min_age: Unreachable commits younger than this many seconds are
            kept, along with everything they reference.
        lease_ttl: Seconds the lease stays live. A holder that crashes
            blocks writers for at most this long.

    Returns:
        Number of orphaned commits removed.

    Raises:
        GcBusy: if another sweep holds a live lease.
        ValueError: if the store is stamped above the layout this code
            reads. Nothing is written, the lease key included.
    """
    # Before the lease, not after: a store this code must not touch has
    # to come out of the call untouched, and an acquire-then-fail would
    # leave an expired lease record behind in a store kvgit had no
    # business writing to.
    _assert_supported_version(store)
    ours, _acquired, expires = _acquire_gc_lease(store, lease_ttl)
    try:
        return _sweep(store, min_age, deep=True)
    finally:
        _release_gc_lease(store, ours, expires, lease_ttl)


def _sweep(store: KVStore, min_age: float, *, deep: bool) -> int:
    """Shared mark-and-sweep behind ``clean_orphans`` / ``deep_clean``.

    The caller holds the GC lease, so no commit batch lands while this
    runs and nothing this deletes can be rewritten underneath it.
    """
    gc_logger = logging.getLogger("kvgit.orphans")
    now = time.time()
    cutoff_time = now - min_age

    def _parent_loader(commit_hash: str) -> tuple[str, ...]:
        parent_bytes = store.get(PARENT_COMMIT % commit_hash)
        if parent_bytes is None:
            return ()
        raw = loads(parent_bytes)
        if raw is None:
            return ()
        if isinstance(raw, str):
            return (raw,)
        return tuple(raw)

    # Mark phase: collect reachable commits, blob keys, HAMT node hashes
    # and chunk references.
    reachable_commits: set[str] = set()
    reachable_blobs: set[str] = set()
    reachable_nodes: set[str] = set()
    reachable_chunks: set[str] = set()

    def _walk_commit_for_marks(commit_hash: str) -> None:
        """Walk one commit's keyset, accumulating reachable refs."""
        root = _load_root(store, commit_hash)
        if root is None:
            return
        # Single batched walk per commit collects HAMT node hashes
        # and the entries (each carrying blob + optional chunks).
        # ``skip_nodes`` lets us skip subtrees already seen via
        # structural sharing — the blobs under those subtrees are
        # already accounted for.
        entries, new_nodes = Keyset(store, root=root).walk(skip_nodes=reachable_nodes)
        for entry in entries.values():
            reachable_blobs.add(entry.blob)
            if entry.meta.chunks:
                reachable_chunks.update(entry.meta.chunks)
        reachable_nodes.update(new_nodes)

    def _mark_from(tip: str) -> None:
        for commit in walk_history(tip, _parent_loader, all_parents=True):
            if commit in reachable_commits:
                continue
            reachable_commits.add(commit)
            _walk_commit_for_marks(commit)

    # In-flight markers first, branch heads second. Publishing a commit
    # installs its head and drops its marker in one atomic write, so a
    # commit published after its marker was read is under a head read
    # later, and one published before has no marker to miss — read the
    # other way round, a publish landing between the two reads would
    # leave the commit under neither.
    stale_markers: list[str] = []
    in_flight_prefix = IN_FLIGHT_KEY.replace("%s", "")
    for key in list(store.keys(in_flight_prefix)):
        commit_hash = key[len(in_flight_prefix) :]
        expires = safe_loads(store.get(key) or b"null")
        if isinstance(expires, (int, float)) and expires > now:
            _mark_from(commit_hash)
        else:
            # The writer never published or withdrew it; its commit is
            # an ordinary orphan now, governed by ``min_age``.
            stale_markers.append(key)

    # Every root is a branch head, tags included: a tag is a head under
    # the reserved ``refs/tags/`` name, so it is marked here without the
    # sweep knowing tags exist. That is the whole compatibility
    # property — a kvgit that predates tags runs this same loop and
    # keeps tagged commits alive for the same reason.
    branch_prefix = BRANCH_HEAD.replace("%s", "")
    for key in list(store.keys(branch_prefix)):
        branch_name = key[len(branch_prefix) :]
        # No ``recover_from_corrupt_head`` here, deliberately, even when
        # the caller has one wired into their handles. GC must not
        # decide reachability from a guess. A wrong answer from a
        # last-resort recoverer marks the wrong commits live — real
        # garbage survives forever, and a guessed tip gets walked as
        # though it were this branch's own history, so another branch's
        # ancestry can be pinned into this one's mark set. The sweep
        # should see only what the store actually claims: a branch whose
        # HEAD resolves is marked from its real HEAD, and one whose HEAD
        # does not resolve marks nothing and keeps its commits as young
        # orphans until ``min_age`` and an explicit ``repair_head``
        # settle what it points at. The inconsistency with the read
        # paths is the point, not an oversight.
        branch_head = _resolve_head(store, branch_name)
        if branch_head is not None:
            _mark_from(branch_head)

    # Sweep phase: find orphaned commits via the __commit_root__ scan,
    # and set aside the young ones — unreachable, but inside the
    # ``min_age`` window the caller gave abandoned work to linger. They
    # are kept whole, so everything they reference is marked too.
    orphans: list[str] = []
    young_orphan_commits: list[str] = []
    root_prefix = COMMIT_ROOT.replace("%s", "")

    for key in list(store.keys(root_prefix)):
        commit_hash = key[len(root_prefix) :]
        if not commit_hash or commit_hash in reachable_commits:
            continue
        time_bytes = store.get(COMMIT_TIME % commit_hash)
        if time_bytes is None:
            # No timestamp recorded — be conservative, leave it alone.
            continue
        ts_val = safe_loads(time_bytes)
        if not isinstance(ts_val, (int, float)) or isinstance(ts_val, bool):
            continue
        if float(ts_val) < cutoff_time:
            orphans.append(commit_hash)
        else:
            young_orphan_commits.append(commit_hash)

    for young in young_orphan_commits:
        _walk_commit_for_marks(young)

    # Collect everything to delete in one batch so the sweep is atomic
    # at the store level (defends against partial sweeps under crash).
    all_removals: list[str] = list(stale_markers)
    keyset_prefix = Keyset.DEFAULT_PREFIX

    # Every deletion candidate on this path comes from walking an
    # orphan's own keyset. Content is keyed by what it holds, so an
    # orphan's blob or node may be the very key a live commit uses; the
    # mark phase saw every live commit — published, in flight, or young
    # — so "unmarked" is what makes a key safe to take, and no batch can
    # land to change that while the lease is held.
    #
    # ``skip_nodes=reachable_nodes`` prunes subtrees shared with a live
    # commit: nothing under them is deletable, and shared structure is
    # walked once, not per orphan. Two orphans sharing a subtree may each
    # name the same key; ``remove_many`` tolerates duplicates.
    for orphan_hash in orphans:
        orphan_root = _load_root(store, orphan_hash)
        if orphan_root is not None and orphan_root != EMPTY_HASH:
            try:
                orphan_entries, orphan_nodes = Keyset(store, root=orphan_root).walk(
                    skip_nodes=reachable_nodes
                )
            except Exception:  # noqa: BLE001 — deliberate: a damaged
                # orphan must not stall the sweep; drop its payload and
                # still reclaim its commit metadata. Narrowing this would
                # let one corrupt keyset block GC for the whole store.
                orphan_entries, orphan_nodes = {}, set()
            for entry in orphan_entries.values():
                if entry.blob not in reachable_blobs:
                    all_removals.append(entry.blob)
                for chunk in entry.meta.chunks or ():
                    if chunk not in reachable_chunks:
                        all_removals.append(CHUNK_PREFIX + chunk)
            all_removals.extend(keyset_prefix + node for node in orphan_nodes)
        all_removals.extend(
            [
                COMMIT_ROOT % orphan_hash,
                PARENT_COMMIT % orphan_hash,
                COMMIT_TIME % orphan_hash,
                INFO_KEY % orphan_hash,
            ]
        )

    if deep:
        # Namespace scans: the way content no orphan keyset points at (a
        # crash's leftovers, an interrupted write) comes back.
        for key in store.keys(keyset_prefix):
            node_hash = key[len(keyset_prefix) :]
            if node_hash and node_hash not in reachable_nodes:
                all_removals.append(key)
        for key in store.keys(CHUNK_PREFIX):
            chunk_hash = key[len(CHUNK_PREFIX) :]
            if chunk_hash and chunk_hash not in reachable_chunks:
                all_removals.append(key)
        for key in store.keys(BLOB_PREFIX):
            if key not in reachable_blobs:
                all_removals.append(key)

    if all_removals:
        store.remove_many(all_removals)

    if orphans:
        gc_logger.debug("Cleaned %d orphaned commit(s)", len(orphans))

    return len(orphans)


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
    :func:`recover_by_commit_scan` to restore kvgit's pre-0.3.4
    behaviour. See :data:`CorruptHeadRecoverer`.
    """

    def __init__(
        self,
        store: KVStore | None = None,
        *,
        commit_hash: str | None = None,
        branch: str = "main",
        create: bool = True,
        recover_from_corrupt_head: CorruptHeadRecoverer | None = None,
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

        _check_storage_version(store)

        if commit_hash is None:
            commit_hash = _resolve_head(
                store, branch, recover_from_corrupt_head=recover_from_corrupt_head
            )
            if commit_hash is None and store.get(BRANCH_HEAD % branch) is not None:
                raise ValueError(f"Branch '{branch}' HEAD is corrupt and unrecoverable")
            if commit_hash is None:
                if not create:
                    raise UnknownBranchError(
                        f"Branch '{branch}' does not exist "
                        "(open with create=True to create it)"
                    )
                # Create initial empty commit
                commit_hash = ROOT_COMMIT
                initial = {
                    COMMIT_ROOT % commit_hash: dumps(EMPTY_HASH),
                    PARENT_COMMIT % commit_hash: dumps([]),
                    COMMIT_TIME % commit_hash: dumps(time.time()),
                    BRANCH_HEAD % branch: dumps(commit_hash),
                }
                store.set_many(initial)
                # Same reasoning as ``create_branch``: this name had no HEAD
                # a moment ago, so it has no previous HEAD either, and a
                # backup that outlived a delete must not become reachable
                # again through the anchor we just installed.
                store.remove(BRANCH_HEAD_PREV % branch)

        if not isinstance(commit_hash, str):
            raise TypeError(
                f"commit_hash must be str, got {type(commit_hash).__name__}"
            )

        super().__init__(branch=branch, commit_hash=commit_hash)
        # Stamps only ever rise, so once this handle has seen the store
        # at v4 it never needs to read the stamp again.
        self._blob_version_stamped = False
        # Commits this handle has written and not yet published, with the
        # bytes of each one's in-flight marker: the marker stays in the
        # store until the publishing write removes it, or a failed
        # attempt withdraws it.
        self._in_flight: dict[str, bytes] = {}
        # The lease record this handle's latest batch landed against: a
        # publish expecting it cannot land while a sweep runs.
        self._lease_seen: bytes | None = None

        # Materialize keyset + meta from the HAMT
        self._meta: dict[str, MetaEntry] = {}
        self._populate_state(commit_hash)

    def _populate_state(self, commit_hash: str) -> None:
        """Walk the commit's HAMT and populate ``_commit_keys`` / ``_meta``.

        Uses ``Keyset.materialize`` (batched BFS, one ``get_many`` per
        tree level) so cold loads against high-latency stores like
        Redis or IndexedDB are O(log_branching N) round-trips, not
        O(N).
        """
        root = _load_root(self.store, commit_hash)
        if root is None:
            self._commit_keys = {}
            self._meta = {}
            return

        materialized = Keyset(self.store, root=root).materialize()
        self._commit_keys = {k: e.blob for k, e in materialized.items()}
        self._meta = {k: e.meta for k, e in materialized.items()}

    @property
    def latest_head(self) -> str | None:
        """Read HEAD directly from the KV store (reflects other writers)."""
        return _resolve_head(
            self.store,
            self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )

    # -- Read operations --

    def get(self, key: str) -> bytes | None:
        """Get a value from the current commit."""
        versioned_key = self._commit_keys.get(key)
        if versioned_key is None:
            return None
        return self.store.get(versioned_key)

    def get_many(self, *keys: str) -> dict[str, bytes]:
        """Get multiple values from the current commit."""
        # Keys holding equal bytes share one blob, so a blob answers for
        # every key that points at it. Missing keys are skipped.
        keys_by_blob: dict[str, list[str]] = {}
        for key in keys:
            blob = self._commit_keys.get(key)
            if blob is not None:
                keys_by_blob.setdefault(blob, []).append(key)

        if not keys_by_blob:
            return {}

        raw = self.store.get_many(keys_by_blob.keys())
        return {key: value for blob, value in raw.items() for key in keys_by_blob[blob]}

    # -- Abstract method implementations --

    def _snapshot_state(self) -> tuple:
        """Capture in-memory state before a commit attempt."""
        return (
            self._current_commit,
            dict(self._commit_keys),
            dict(self._meta),
        )

    def _restore_state(self, saved: tuple) -> None:
        """Restore in-memory state after a failed commit attempt.

        The attempt's commits are abandoned, so their in-flight markers
        are withdrawn and they become ordinary orphans. Best effort: a
        marker left behind lapses after :data:`IN_FLIGHT_TTL`.
        """
        self._current_commit, self._commit_keys, self._meta = saved
        if self._in_flight:
            markers = [IN_FLIGHT_KEY % commit for commit in self._in_flight]
            self._in_flight = {}
            self.store.remove_many(markers)

    def _land_batch(self, commit: str, diffs: dict[str, bytes]) -> None:
        """Write a commit's batch, marked in flight, while no sweep runs.

        The batch is conditional on the lease record read after waiting
        any live sweep out, so it lands before a sweep starts or after
        one ends, never during. Landing before is safe because the batch
        carries the commit's in-flight marker, which every later sweep
        marks from until the commit is published.
        """
        marker = dumps(time.time() + IN_FLIGHT_TTL)
        diffs[IN_FLIGHT_KEY % commit] = marker
        while True:
            lease = _wait_for_gc(self.store)
            if self.store.cas_many({GC_LEASE_KEY: lease}, diffs):
                break
        self._in_flight[commit] = marker
        self._lease_seen = lease

    def _create_commit(
        self,
        updates: dict[str, bytes] | None = None,
        removals: set[str] | None = None,
        *,
        info: dict | None = None,
        chunks: dict[str, bytes] | None = None,
        chunk_refs: dict[str, list[str]] | None = None,
    ) -> str:
        """Create a new local commit with the given changes.

        Does not advance HEAD. Use ``commit()`` for the public API.

        Returns:
            The new commit hash.
        """
        updates = updates or {}
        removals = removals or set()
        chunks = chunks or {}
        chunk_refs = chunk_refs or {}

        # Build new in-memory dicts: carry forward, apply removals, apply updates
        new_commit_keys: dict[str, str] = {}
        new_meta: dict[str, MetaEntry] = {}

        for key, versioned_key in self._commit_keys.items():
            if key in removals:
                continue
            new_commit_keys[key] = versioned_key
            if key in self._meta:
                new_meta[key] = self._meta[key]

        # Every blob is written, even one whose key is already stored,
        # so the batch holds everything its commit needs whatever a sweep
        # took before it landed.
        diffs: dict[str, bytes] = {}
        for key, value in updates.items():
            pointer = blob_key(value)
            diffs[pointer] = value
            new_commit_keys[key] = pointer
            refs = chunk_refs.get(key)
            new_meta[key] = MetaEntry(
                size=len(value), chunks=list(refs) if refs else None
            )

        # Stage chunk writes under their content-addressed namespace.
        # Like blobs, every chunk is written even when its key is
        # already stored; the key is the hash, so the rewrite is a no-op
        # for the data and a guarantee against an earlier sweep.
        if chunks:
            _stamp_version_at_least(self.store, CHUNK_STORAGE_VERSION)
            for chunk_hash, chunk_bytes in chunks.items():
                diffs[CHUNK_PREFIX + chunk_hash] = chunk_bytes

        # Build the new keyset by applying changes to the parent's HAMT.
        # Only the explicitly changed keys generate new entries; structural
        # sharing reuses unchanged subtrees from the parent commit.
        parent_root = _load_root(self.store, self._current_commit) or EMPTY_HASH
        parent_ks = Keyset(self.store, root=parent_root)
        keyset_updates = {
            key: KeysetEntry(blob=new_commit_keys[key], meta=new_meta[key])
            for key in updates
        }
        new_ks, pending = parent_ks.updated(updates=keyset_updates, removals=removals)
        diffs.update(pending)

        created = time.time()
        new_hash = commit_hash((self._current_commit,), new_ks.root, created, info)
        diffs[COMMIT_ROOT % new_hash] = dumps(new_ks.root)
        diffs[PARENT_COMMIT % new_hash] = dumps([self._current_commit])
        diffs[COMMIT_TIME % new_hash] = dumps(created)
        if info is not None:
            diffs[INFO_KEY % new_hash] = dumps(info)

        self._stamp_blob_version()

        # Everything this commit writes that a sweep could delete —
        # chunks, HAMT nodes, blobs, commit metadata — lands in this one
        # batch. (The version stamp above is not in it and needs no
        # cover: no sweep deletes it.)
        self._land_batch(new_hash, diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    def _stamp_blob_version(self) -> None:
        """Stamp the store v4 before this handle's first commit batch."""
        if not self._blob_version_stamped:
            _stamp_version_at_least(self.store, BLOB_STORAGE_VERSION)
            self._blob_version_stamped = True

    def _create_merge_commit(
        self,
        resolution: MergeResolution,
        parents: tuple[str, ...],
        info: dict | None,
    ) -> str:
        """Create a merge commit from a resolved three-way merge."""
        merged_keyset = resolution.merged_keyset
        merged_values = resolution.merged_values

        # Build write batch
        diffs: dict[str, bytes] = {}
        for key, value in merged_values.items():
            pointer = blob_key(value)
            merged_keyset[key] = pointer
            diffs[pointer] = value

        # Build merged meta from the parents' meta, indexed by blob
        # pointer. Metadata describes the blob, not the key: size is that
        # blob's length and ``chunks`` lists the chunk references garbage
        # collection traces from it. A merge that keeps one side's
        # pointer must keep that side's meta with it, or the entry
        # describes a blob it no longer points at — and a stale chunk
        # list makes garbage collection trace the wrong chunks. Indexing
        # by key instead cannot express that, since the two sides
        # disagree about the key. A pointer names either the bytes it
        # holds or the commit that wrote it, so one pointer has one meta
        # and first-seen wins.
        meta_by_blob: dict[str, MetaEntry] = {}
        meta_by_key: dict[str, MetaEntry] = {}
        for parent in parents:
            parent_root = _load_root(self.store, parent)
            if parent_root is None:
                continue
            for key, entry in Keyset(self.store, root=parent_root).items():
                meta_by_blob.setdefault(entry.blob, entry.meta)
                meta_by_key.setdefault(key, entry.meta)

        merged_meta: dict[str, MetaEntry] = {}
        for key, blob in merged_keyset.items():
            if key in merged_values:
                # A value the merge itself produced: new blob, new meta.
                # Merge output is never chunked, so it lists no chunks.
                merged_meta[key] = MetaEntry(size=len(merged_values[key]))
                continue
            meta = meta_by_blob.get(blob)
            if meta is None and blob == self._commit_keys.get(key):
                meta = self._meta.get(key)
            if meta is None:
                # A parent whose keyset would not load: fall back to
                # whatever the key had rather than dropping the entry.
                meta = meta_by_key.get(key) or self._meta.get(key)
            if meta is not None:
                merged_meta[key] = meta

        # Apply the merge result on top of our parent's HAMT. We compute
        # the minimal updates and removals so structural sharing kicks in
        # for unchanged subtrees.
        our_root = _load_root(self.store, self._current_commit) or EMPTY_HASH
        parent_ks = Keyset(self.store, root=our_root)

        keyset_updates: dict[str, KeysetEntry] = {}
        for key, blob in merged_keyset.items():
            new_entry = KeysetEntry(blob=blob, meta=merged_meta[key])
            old_blob = self._commit_keys.get(key)
            old_meta = self._meta.get(key)
            if old_blob != new_entry.blob or old_meta != new_entry.meta:
                keyset_updates[key] = new_entry

        keyset_removals = {key for key in self._commit_keys if key not in merged_keyset}

        new_ks, pending = parent_ks.updated(
            updates=keyset_updates, removals=keyset_removals
        )
        diffs.update(pending)

        created = time.time()
        merge_hash = commit_hash(parents, new_ks.root, created, info)
        diffs[COMMIT_ROOT % merge_hash] = dumps(new_ks.root)
        diffs[PARENT_COMMIT % merge_hash] = dumps(list(parents))
        diffs[COMMIT_TIME % merge_hash] = dumps(created)
        if info is not None:
            diffs[INFO_KEY % merge_hash] = dumps(info)

        self._stamp_blob_version()
        self._land_batch(merge_hash, diffs)

        # Update in-memory state
        self._commit_keys = merged_keyset
        self._current_commit = merge_hash
        self._meta = merged_meta

        return merge_hash

    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Publish ``new_head`` as this branch's HEAD if HEAD is ``expected``.

        One atomic write moves HEAD, records ``expected`` as the
        prev-HEAD backup, and removes the in-flight markers of the
        commits being published. The backup therefore always names the
        commit HEAD held immediately before, and a published commit
        never keeps a marker, nor loses it before its head lands.

        The write also expects the GC lease record, so it cannot land
        while a sweep runs: a sweep decides what to delete from the
        heads and markers it read at its start, and a head installed
        before it deletes would name a commit it may still take. The
        record the latest batch landed against is expected first — no
        extra read in the common case — and after a sweep the publish
        waits it out and tries again.

        And it expects each marker to hold the bytes this handle wrote.
        A marker only disappears early if it lapsed — the writer took
        longer than :data:`IN_FLIGHT_TTL` to publish — and a sweep reaped
        it, possibly with the commit; then the publish fails, and the
        commit surfaces an error rather than a head over a commit that
        may be gone.

        A CAS that fails against a *damaged* HEAD is retried through
        :func:`_heal_head`, which repairs it atomically. That is the only
        place a corrupt HEAD is written back, now that reads do not.
        """
        branch_key = BRANCH_HEAD % self._branch
        expected_bytes = dumps(expected)
        writes = {
            branch_key: dumps(new_head),
            BRANCH_HEAD_PREV % self._branch: expected_bytes,
        }
        markers = {IN_FLIGHT_KEY % c: marker for c, marker in self._in_flight.items()}
        lease = self._lease_seen if self._in_flight else _wait_for_gc(self.store)
        while True:
            expect = {GC_LEASE_KEY: lease, branch_key: expected_bytes, **markers}
            if self.store.cas_many(expect, writes, tuple(markers)):
                break
            if self.store.get(GC_LEASE_KEY) != lease:
                # A sweep ran, or is running: wait it out and try again.
                # A marker it reaped fails the next attempt on its own.
                lease = _wait_for_gc(self.store)
                continue
            present = self.store.get_many(markers.keys())
            if any(present.get(key) != marker for key, marker in markers.items()):
                return False
            if not _heal_head(self.store, self._branch, expected_bytes):
                return False
        self._in_flight = {}
        return True

    def _load_keyset(self, commit_hash: str) -> dict[str, str]:
        """Load just the keyset for a commit (key -> versioned_key mapping).

        Used by the merge layer; returns a flat dict, dropping meta.
        """
        root = _load_root(self.store, commit_hash)
        if root is None:
            return {}
        ks = Keyset(self.store, root=root)
        return {key: entry.blob for key, entry in ks.items()}

    def _load_parents(self, commit_hash: str) -> tuple[str, ...]:
        """Load the parent tuple for a commit."""
        parent_bytes = self.store.get(PARENT_COMMIT % commit_hash)
        if parent_bytes is None:
            return ()
        raw = loads(parent_bytes)
        if raw is None:
            return ()
        if isinstance(raw, str):
            return (raw,)
        return tuple(raw)

    def _find_lca(self, commit_a: str, commit_b: str) -> str | None:
        """Find the lowest common ancestor of two commits.

        Ancestor-set intersection with non-minimal candidates dropped
        (a candidate that is itself an ancestor of another candidate is
        not lowest). When several commits tie for lowest — criss-cross
        histories — the smallest hash wins: deterministic, but
        arbitrary, so criss-cross merges resolve cleanly rather than
        raising.
        """
        if commit_a == commit_b:
            return commit_a

        parents: dict[str, tuple[str, ...]] = {}
        ancestors_a = self._walk_ancestors(commit_a, parents)
        # Fast path: b inside a's history (or vice versa) names the
        # lowest directly — every other common ancestor sits above it.
        if commit_b in ancestors_a:
            return commit_b
        ancestors_b = self._walk_ancestors(commit_b, parents)
        if commit_a in ancestors_b:
            return commit_a

        common = ancestors_a & ancestors_b
        if not common:
            return None
        # Minimality in one bottom-up pass: a candidate is lowest when
        # no other candidate sits below it. Propagate "a candidate is
        # at-or-below here" from tips to roots over the in-memory
        # parent map — no further store reads, linear in the history.
        children: dict[str, list[str]] = {node: [] for node in parents}
        for node, node_parents in parents.items():
            for parent in node_parents:
                children[parent].append(node)
        below: dict[str, bool] = dict.fromkeys(parents, False)
        remaining = {node: len(kids) for node, kids in children.items()}
        queue = deque(node for node, kids in children.items() if not kids)
        while queue:
            node = queue.popleft()
            for parent in parents[node]:
                if node in common or below[node]:
                    below[parent] = True
                remaining[parent] -= 1
                if remaining[parent] == 0:
                    queue.append(parent)
        best = {c for c in common if not below[c]}
        return min(best) if best else None

    def _walk_ancestors(
        self, start: str, parents: dict[str, tuple[str, ...]]
    ) -> set[str]:
        """All ancestors of ``start`` (itself included), recording each
        visited commit's parents in ``parents`` for later passes."""
        ancestors = {start}
        stack = [start]
        while stack:
            current = stack.pop()
            if current in parents:
                continue
            node_parents = self._load_parents(current)
            parents[current] = node_parents
            for parent in node_parents:
                if parent not in ancestors:
                    ancestors.add(parent)
                    stack.append(parent)
        return ancestors

    def _read_blob(self, content_id: str) -> bytes | None:
        """Read a blob by its versioned key."""
        return self.store.get(content_id)

    # -- Navigation --

    def refresh(self) -> None:
        """Reload state from HEAD."""
        commit_hash = _resolve_head(
            self.store,
            self._branch,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )
        if commit_hash is None:
            raise ValueError(f"No HEAD commit found for branch {self._branch}")
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
        _reject_reserved_branch(name)
        branch_key = BRANCH_HEAD % name
        target = at or self._current_commit
        # "The commit is here" and "the head names it" are decided
        # against one lease record: a sweep that starts between the
        # check and the write makes the write fail, and the check runs
        # again. So the outcomes are a branch on a commit that loads, or
        # a refusal — never a head pointing at nothing.
        #
        # The write also drops any backup under this name. A branch that
        # has just been created has no previous HEAD, and a stale backup
        # left by an earlier branch of the same name would otherwise be
        # what head recovery serves if this HEAD were ever damaged.
        while True:
            lease = _wait_for_gc(self.store)
            if at is not None and self.store.get(COMMIT_ROOT % at) is None:
                raise ValueError(f"Commit '{at}' does not exist")
            landed = _try_land(
                self.store,
                lease,
                {branch_key: None},
                {branch_key: dumps(target)},
                (BRANCH_HEAD_PREV % name,),
            )
            if landed is None:
                continue
            if not landed:
                raise ValueError(f"Branch '{name}' already exists")
            break
        return VersionedKV(
            self.store,
            commit_hash=target,
            branch=name,
            recover_from_corrupt_head=self._recover_from_corrupt_head,
        )

    def delete_branch(self, name: str) -> None:
        """Delete a branch and clean up orphaned commits."""
        _reject_reserved_branch(name)
        if name == self._branch:
            raise ValueError("Cannot delete the current branch")
        branch_key = BRANCH_HEAD % name
        if self.store.get(branch_key) is None:
            raise ValueError(f"Branch '{name}' does not exist")
        # The prev-HEAD recovery backup goes in the same removal: left
        # behind, it would be a lone backup naming the deleted state.
        # Both go before clean_orphans, so commits only they referenced
        # are collectable.
        self.store.remove_many([branch_key, BRANCH_HEAD_PREV % name])
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
                raise ValueError(f"Branch '{name}' HEAD is corrupt and unrecoverable")
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
        the reset; it is retried against the new HEAD.
        """
        branch_key = BRANCH_HEAD % self._branch
        prev_key = BRANCH_HEAD_PREV % self._branch
        # The existence check and the write are decided against one
        # lease record, as in ``create_branch``: this either resets onto
        # a commit that loads or reports the commit gone.
        while True:
            lease = _wait_for_gc(self.store)
            if self.store.get(COMMIT_ROOT % commit_hash) is None:
                return False
            current = self.store.get(branch_key)
            writes = {branch_key: dumps(commit_hash)}
            if current is not None:
                writes[prev_key] = current
            if _try_land(self.store, lease, {branch_key: current}, writes):
                break
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
        prefix = BRANCH_HEAD.replace("%s", "")
        result = []
        for key in store.keys(prefix):
            if isinstance(key, str) and key.startswith(prefix):
                branch_name = key[len(prefix) :]
                if branch_name and not branch_name.startswith(TAG_BRANCH_PREFIX):
                    result.append(branch_name)
        return sorted(result)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return VersionedKV.branches(self.store)

    @staticmethod
    def exists(store: KVStore, name: str) -> bool:
        """Whether a branch exists — openable without creating.

        The same predicate :meth:`branches` lists by: the branch has a
        HEAD entry. Reserved ``refs/tags/`` names are never branches,
        so they read as missing here as they are excluded there. A
        damaged HEAD still counts as existing; opening it raises either
        way. Never writes, so checking cannot resurrect a deleted
        branch.
        """
        if not isinstance(name, str) or name.startswith(TAG_BRANCH_PREFIX):
            return False
        return store.get(BRANCH_HEAD % name) is not None

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
                which for a ``Staged`` wrapper means the last *committed*
                state — staged changes are not part of any commit yet and
                are not tagged.
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
        _validate_tag_name(name)
        target = at or self._current_commit
        # Encoded before anything is written, so info that cannot be
        # serialized raises without leaving a tag behind. The record
        # itself is built after the wait below, so its time is when the
        # tag lands, not when the call began waiting on a sweep.
        dumps(info)
        head_key = BRANCH_HEAD % _tag_branch(name)
        # A tag is a GC root, so it must not be planted under a sweep
        # that has already decided what is reachable, and the existence
        # check must still hold when it lands: both are decided against
        # one lease record, and a sweep in between sends them round
        # again. The head is claimed against absence, so two writers
        # racing the same name cannot both win and an existing tag is
        # never overwritten; the record lands with it, and any backup
        # left under the name by an earlier tag goes in the same write.
        while True:
            lease = _wait_for_gc(self.store)
            if self.store.get(COMMIT_ROOT % target) is None:
                raise ValueError(f"Commit '{target}' does not exist")
            record = dumps({"time": time.time(), "info": info})
            landed = _try_land(
                self.store,
                lease,
                {head_key: None},
                {head_key: dumps(target), TAG_INFO_KEY % name: record},
                (BRANCH_HEAD_PREV % _tag_branch(name),),
            )
            if landed is None:
                continue
            if not landed:
                raise ValueError(f"Tag '{name}' already exists")
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
        """
        _validate_tag_name(name)
        head_key = BRANCH_HEAD % _tag_branch(name)
        if self.store.get(head_key) is None:
            raise ValueError(f"Tag '{name}' does not exist")
        self.store.remove_many(
            [head_key, BRANCH_HEAD_PREV % _tag_branch(name), TAG_INFO_KEY % name]
        )
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
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
        self._populate_state(commit_hash)
