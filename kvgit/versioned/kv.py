"""KVStore-backed versioned state.

Storage layout (v3):

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
- ``<commit_hash>:<user_key>``         — blob value bytes

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

Chunks are the *only* class above keyed purely by content. Blobs carry
the commit hash in the key; HAMT nodes embed that blob pointer, so a
node hash is commit-scoped too. That difference decides who may delete
what: ``clean_orphans`` never deletes chunks, because a chunk an orphan
owns may be the same key a commit made a moment ago just deduped onto,
and the sweep has no way to know. Only ``deep_clean``, which requires a
quiescent store, reclaims chunks.

v3 is a strict superset of v2:

* Opening a v2 store with v3 code is allowed; the version stamp is left
  unchanged until a chunked write actually occurs.
* The first commit that includes ``chunks`` lazily stamps the store as
  v3. From then on, older code refuses to open it (intentional: it
  cannot decode chunked blobs).
* A v3 store with no chunks ever written is byte-identical to a v2
  store except for the version sentinel.

The pre-v2 layout is **not** supported. Stores written by an earlier
version raise on open and need to be rebuilt fresh.
"""

import hashlib
import json
import logging
import time
from collections.abc import Callable

from ..encoding import dumps, loads, safe_loads
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

STORAGE_VERSION_KEY = "__kvgit_version__"
STORAGE_VERSION = 3
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

# Lower versions accepted as input. v3 code reads v2 stores transparently
# and only stamps the store as v3 once a chunked write actually happens.
SUPPORTED_READ_VERSIONS = frozenset({2, 3})


def content_hash(
    parents: tuple[str, ...],
    keyset: dict[str, str],
    updates: dict[str, bytes],
    info: dict | None = None,
) -> str:
    """Compute a content-addressable commit hash.

    Hashes the parent pointers, keyset preview, update blob digests,
    and optional info to produce a deterministic 40-hex-char commit
    hash. The keyset passed here is the in-memory placeholder dict
    (with ``<pending:key>`` markers for not-yet-written blobs), the
    same shape v1 used.
    """
    h = hashlib.sha256()
    h.update(json.dumps(list(parents), separators=(",", ":")).encode())
    h.update(json.dumps(sorted(keyset.items()), separators=(",", ":")).encode())
    for key in sorted(updates):
        h.update(key.encode())
        h.update(updates[key])
    if info is not None:
        h.update(json.dumps(info, sort_keys=True, separators=(",", ":")).encode())
    return h.hexdigest()[:40]


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
    handle — currently a chunk, which needs v3. Stamps are sticky and
    are never lowered.
    """
    raw = store.get(STORAGE_VERSION_KEY)
    current = safe_loads(raw) if raw is not None else None
    if not isinstance(current, int) or isinstance(current, bool) or current < version:
        store.set(STORAGE_VERSION_KEY, dumps(version))


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
    has_existing = any(
        isinstance(k, str) and k.startswith(branch_prefix) for k in store.keys()
    )
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
    # the branch back. One can outlive it: a writer descheduled between
    # its CAS and its backup write, resuming after a concurrent delete,
    # recreates only the backup.
    #
    # Nothing legitimate needs the ungated form: ``_cas_head`` writes
    # the backup only after a successful CAS, so HEAD exists whenever
    # the backup means anything.
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
    for key in store.keys():
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
    for key in store.keys():
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
    raw = store.get(branch_key)
    if raw is None or raw == recovered:
        return False
    commit_hash = safe_loads(raw)
    if (
        isinstance(commit_hash, str)
        and store.get(COMMIT_ROOT % commit_hash) is not None
    ):
        return False
    if not store.cas(branch_key, recovered, expected=raw):
        return False
    logger.warning("Branch '%s': corrupt HEAD replaced with recovered commit", branch)
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
    for key in store.keys():
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


def clean_orphans(store: KVStore, min_age: float = 3600) -> int:
    """Remove orphaned commits unreachable from any branch HEAD.

    Traces all reachable commits from live branch HEADs, then deletes
    the commit metadata, blobs and HAMT nodes owned by the orphaned
    commits and not shared with anything still reachable. Tags need no
    special handling: a tag is a branch head under a reserved name, so
    it keeps its commit's whole ancestry alive by being walked with
    everything else.

    Handle-independent by design: it marks from ALL live branch HEADs
    and touches nothing but ``store``, so it works with or without a
    ``VersionedKV`` anchored on it. :meth:`VersionedKV.clean_orphans`
    and the anchor-free admin path (:func:`kvgit.delete_branches`)
    share this one implementation.

    Safe under concurrent writers: every deletion candidate is
    discovered by walking an orphan commit's own keyset, and every
    class it deletes is commit-scoped (blob keys carry the commit
    hash; HAMT nodes embed that pointer), so a commit that lands
    mid-sweep can never contribute one.

    **Does not reclaim chunks.** Chunk keys are pure content hashes,
    so an orphan's chunk and a brand-new commit's chunk are the same
    key whenever the bytes match — "the orphan owned it" does not
    imply "safe to delete", at any window size. Chunks are the large
    objects (numpy and pandas buffers), so on a store using chunked
    codecs they accumulate between maintenance passes; a store that
    never uses a chunked codec has none and loses nothing here. Run
    :func:`deep_clean` on a quiescent store to reclaim them, along
    with nodes and chunks no commit points at.

    The ``min_age`` guard (default 1 hour) still applies: it decides
    which unreachable commits are old enough to delete at all.

    Returns:
        Number of orphaned commits removed.
    """
    return _sweep(store, min_age, deep=False)


def deep_clean(store: KVStore, min_age: float = 3600) -> int:
    """Sweep orphans *and* every unreferenced node and chunk. Unsafe.

    Does everything :func:`clean_orphans` does, then additionally scans
    the whole ``kvgit:keyset:`` and ``kvgit:chunk:`` namespaces and
    deletes anything not reachable from a live branch head or a young
    orphan commit. That namespace scan is the only way to reclaim
    nodes and chunks that no commit references any more — leftovers
    from a crash, from an interrupted write, or from a store swept by
    an earlier kvgit — because no orphan keyset points at them.

    It is also the only way to reclaim **any** chunk at all, including
    ones a deleted orphan uniquely owned: the incremental sweep leaves
    every chunk in place, so on a store using chunked codecs this is
    the maintenance pass that gives the space back.

    **Not safe against concurrent writers.** The scan runs after the
    mark phase, so it sees, and deletes, artifacts written by any
    commit that landed in between — including a commit that has since
    become a live branch HEAD. Run it only on a quiescent store: no
    other process or thread writing, for the whole call. ``min_age``
    does not protect you here; it governs commit deletion, not the
    namespace scan.

    Returns:
        Number of orphaned commits removed.
    """
    return _sweep(store, min_age, deep=True)


def _sweep(store: KVStore, min_age: float, *, deep: bool) -> int:
    """Shared mark-and-sweep behind ``clean_orphans`` / ``deep_clean``."""
    gc_logger = logging.getLogger("kvgit.orphans")
    # Reachability is decided from the roots this code knows about, so
    # a store stamped above what this code reads must not be swept: its
    # roots may be a kind that did not exist here, and everything hanging
    # off them would look like garbage. The check belongs on this side of
    # the call rather than only in the handle constructors, because the
    # admin entry points sweep a raw store with no handle at all.
    _assert_supported_version(store)
    cutoff_time = time.time() - min_age

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

    # Mark phase: walk every branch's history, collecting reachable
    # commits, blob keys, HAMT node hashes, and chunk references.
    reachable_commits: set[str] = set()
    reachable_blobs: set[str] = set()
    reachable_nodes: set[str] = set()
    # Only the deep path deletes chunks, and it is the only consumer of
    # this set. The incremental path leaves it empty on purpose rather
    # than paying to accumulate every chunk hash in the store — if you
    # ever add a chunk deletion outside the ``if deep:`` block below,
    # you must populate this unconditionally first.
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
            if deep and entry.meta.chunks:
                reachable_chunks.update(entry.meta.chunks)
        reachable_nodes.update(new_nodes)

    # Every root is a branch head, tags included: a tag is a head under
    # the reserved ``refs/tags/`` name, so it is marked here without the
    # sweep knowing tags exist. That is the whole compatibility
    # property — a kvgit that predates tags runs this same loop and
    # keeps tagged commits alive for the same reason.
    branch_prefix = BRANCH_HEAD.replace("%s", "")
    for key in store.keys():
        if not (isinstance(key, str) and key.startswith(branch_prefix)):
            continue
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
        if branch_head is None:
            continue
        for commit in walk_history(branch_head, _parent_loader, all_parents=True):
            if commit in reachable_commits:
                continue
            reachable_commits.add(commit)
            _walk_commit_for_marks(commit)

    # Sweep phase: find orphaned commits via __commit_root__ scan.
    # Also identify "young orphans" — commits inside the min_age
    # window that aren't branch-reachable. Their chunks must be
    # protected from sweeping (they may be in-flight from another
    # writer), even though we won't delete the commits themselves
    # until they age past the cutoff.
    orphans: list[str] = []
    young_orphan_commits: list[str] = []
    root_prefix = COMMIT_ROOT.replace("%s", "")

    for key in store.keys():
        if not (isinstance(key, str) and key.startswith(root_prefix)):
            continue
        commit_hash = key[len(root_prefix) :]
        if not commit_hash or commit_hash in reachable_commits:
            continue
        time_bytes = store.get(COMMIT_TIME % commit_hash)
        if time_bytes is None:
            # No timestamp recorded — be conservative, leave it alone.
            continue
        try:
            ts_val = safe_loads(time_bytes)
            if not isinstance(ts_val, (int, float)):
                continue
            if float(ts_val) < cutoff_time:
                orphans.append(commit_hash)
            else:
                young_orphan_commits.append(commit_hash)
        except (TypeError, ValueError):
            continue

    # Protect what young orphan commits reference — they may belong to
    # in-flight writers whose CAS has not landed yet. Their nodes and
    # blobs matter to both paths (an aged orphan sharing a subtree with
    # a young one must not take it down); their chunks matter to
    # ``deep_clean``, whose namespace scan would otherwise eat them.
    for young in young_orphan_commits:
        _walk_commit_for_marks(young)

    # Collect everything to delete in one batch so the sweep is atomic
    # at the store level (defends against partial sweeps under crash).
    all_removals: list[str] = []
    keyset_prefix = Keyset.DEFAULT_PREFIX

    # Every deletion candidate comes from walking an orphan's own
    # keyset — never from a namespace scan. That is what makes this
    # safe under concurrent writers: a commit that lands after the
    # mark phase is in nobody's orphan tree, so nothing it wrote can
    # end up on this list. ``skip_nodes=reachable_nodes`` prunes
    # subtrees shared with a live commit or a young orphan, which is
    # both the correct thing (nothing under them is deletable) and
    # the cheap thing (shared structure is walked once, not per
    # orphan). Two orphans sharing a subtree may each name the same
    # hash; ``remove_many`` tolerates duplicates.
    #
    # Chunks are deliberately absent here. Blob keys are
    # ``<commit_hash>:<key>`` and HAMT nodes embed that pointer, so
    # both are commit-scoped: an orphan's node hash can only collide
    # with a commit in its own ancestry, which the mark phase already
    # covered. A chunk key is ``kvgit:chunk:<content_hash>`` and
    # carries nothing commit-derived, so an orphan's chunk and a
    # brand-new commit's chunk are the *same key* whenever the bytes
    # match. "In the orphan's tree" therefore does not imply "safe to
    # delete", and no amount of scoping fixes that — the new commit
    # was never marked. Chunk reclamation lives in ``deep_clean``.
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
        # Namespace scans. The node scan reclaims nodes no orphan
        # keyset points at; the chunk scan is the *only* place chunks
        # are ever deleted, orphan-owned ones included. Both are
        # unsafe against a concurrent writer, because anything
        # committed since the mark phase looks unreferenced here.
        # Quiescent stores only.
        for key in store.keys():
            if not (isinstance(key, str) and key.startswith(keyset_prefix)):
                continue
            node_hash = key[len(keyset_prefix) :]
            if node_hash and node_hash not in reachable_nodes:
                all_removals.append(key)

        for key in store.keys():
            if not (isinstance(key, str) and key.startswith(CHUNK_PREFIX)):
                continue
            chunk_hash = key[len(CHUNK_PREFIX) :]
            if chunk_hash and chunk_hash not in reachable_chunks:
                all_removals.append(key)

    if all_removals:
        store.remove_many(*all_removals)

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
                # Create initial empty commit
                commit_hash = content_hash((), {}, {})
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
        # Map user keys -> versioned keys, skipping missing
        vk_to_key: dict[str, str] = {}
        for key in keys:
            vk = self._commit_keys.get(key)
            if vk is not None:
                vk_to_key[vk] = key

        if not vk_to_key:
            return {}

        raw = self.store.get_many(*vk_to_key.keys())
        return {vk_to_key[vk]: value for vk, value in raw.items()}

    # -- Abstract method implementations --

    def _snapshot_state(self) -> tuple:
        """Capture in-memory state before a commit attempt."""
        return (
            self._current_commit,
            dict(self._commit_keys),
            dict(self._meta),
        )

    def _restore_state(self, saved: tuple) -> None:
        """Restore in-memory state after a failed commit attempt."""
        self._current_commit, self._commit_keys, self._meta = saved

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

        # Compute content-addressable hash from a placeholder keyset
        # (real versioned blob keys depend on the commit hash itself).
        preview_keys = dict(new_commit_keys)
        for key in updates:
            preview_keys[key] = f"<pending:{key}>"
        new_hash = content_hash(
            (self._current_commit,), preview_keys, updates, info=info
        )

        # Resolve real versioned blob keys for new updates
        diffs: dict[str, bytes] = {}
        for key, value in updates.items():
            versioned_key = f"{new_hash}:{key}"
            diffs[versioned_key] = value
            new_commit_keys[key] = versioned_key
            size = len(value)
            refs = chunk_refs.get(key)
            refs_list = list(refs) if refs else None
            created_at = new_meta[key].created_at if key in new_meta else time.time()
            new_meta[key] = MetaEntry(
                size=size,
                created_at=created_at,
                chunks=refs_list,
            )

        # Stage chunk writes under their content-addressed namespace.
        # Existing chunks (already present in the store) are skipped to
        # save a roundtrip on idempotent rewrites; the dedup property
        # holds either way because the key is the hash.
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

        # Commit metadata
        diffs[COMMIT_ROOT % new_hash] = dumps(new_ks.root)
        diffs[PARENT_COMMIT % new_hash] = dumps([self._current_commit])
        diffs[COMMIT_TIME % new_hash] = dumps(time.time())
        if info is not None:
            diffs[INFO_KEY % new_hash] = dumps(info)

        # Write everything atomically
        self.store.set_many(diffs)

        # Update in-memory state
        self._commit_keys = new_commit_keys
        self._current_commit = new_hash
        self._meta = new_meta

        return new_hash

    def _create_merge_commit(
        self,
        resolution: MergeResolution,
        parents: tuple[str, ...],
        info: dict | None,
    ) -> str:
        """Create a merge commit from a resolved three-way merge."""
        merged_keyset = resolution.merged_keyset
        merged_values = resolution.merged_values

        preview_keys = dict(merged_keyset)
        for key in merged_values:
            preview_keys[key] = f"<pending:{key}>"

        merge_hash = content_hash(parents, preview_keys, merged_values, info)

        # Build write batch
        diffs: dict[str, bytes] = {}
        for key, value in merged_values.items():
            vk = f"{merge_hash}:{key}"
            merged_keyset[key] = vk
            diffs[vk] = value

        # Build merged meta from the parents' meta. ``self._meta`` is
        # already our parent's meta (in memory). Their parent's meta
        # we have to walk via the HAMT.
        their_root = _load_root(self.store, parents[0])
        their_meta: dict[str, MetaEntry] = {}
        if their_root is not None:
            their_ks = Keyset(self.store, root=their_root)
            for key, entry in their_ks.items():
                their_meta[key] = entry.meta

        merged_meta: dict[str, MetaEntry] = {}
        for key in merged_keyset:
            if key in merged_values:
                merged_meta[key] = MetaEntry(
                    size=len(merged_values[key]),
                    created_at=time.time(),
                )
            elif key in self._meta:
                merged_meta[key] = self._meta[key]
            elif key in their_meta:
                merged_meta[key] = their_meta[key]

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

        diffs[COMMIT_ROOT % merge_hash] = dumps(new_ks.root)
        diffs[PARENT_COMMIT % merge_hash] = dumps(list(parents))
        diffs[COMMIT_TIME % merge_hash] = dumps(time.time())
        if info is not None:
            diffs[INFO_KEY % merge_hash] = dumps(info)

        self.store.set_many(diffs)

        # Update in-memory state
        self._commit_keys = merged_keyset
        self._current_commit = merge_hash
        self._meta = merged_meta

        return merge_hash

    def _cas_head(self, expected: str, new_head: str) -> bool:
        """Atomically advance branch HEAD via KVStore CAS.

        The prev-HEAD backup is written **after** the swap succeeds,
        never before. Written first, it lands whether or not the CAS
        does, so a writer that loses the race still leaves its own stale
        ``expected`` as the branch's recovery target — clobbering the
        winner's backup, and, when ``expected`` came from an injected
        recoverer, naming a commit that was never HEAD at all. Writing it
        afterwards makes it always a value ``__branch_head__`` really
        held.

        What this does **not** buy is a backup that is always exactly
        one commit back. The swap and the backup write are two steps,
        and anything that separates them — a crash, or simply losing
        the CPU while another writer completes both of its own — lets
        the older writer's backup land last. HEAD then sits two or more
        commits ahead of a backup that is still a real former HEAD, and
        recovery skips whatever came between.

        So the guarantee is the narrower one: the backup always names a
        commit ``__branch_head__`` really held, never a commit invented
        by a losing writer. Recovery may lose more than one commit; it
        cannot graft on a lineage the branch never had. There is no way
        to do better with what the store offers — ``KVStore.cas`` takes
        a single key and no backend exposes a transaction spanning two,
        so HEAD and its backup cannot move in one step.

        A CAS that fails against a *damaged* HEAD is retried once
        through :func:`_heal_head`, which repairs it atomically. That is
        the only place a corrupt HEAD is written back, now that reads
        do not.
        """
        branch_key = BRANCH_HEAD % self._branch
        expected_bytes = dumps(expected)
        new_bytes = dumps(new_head)

        won = self.store.cas(branch_key, new_bytes, expected=expected_bytes)
        if not won and _heal_head(self.store, self._branch, expected_bytes):
            won = self.store.cas(branch_key, new_bytes, expected=expected_bytes)
        if won:
            self.store.set(BRANCH_HEAD_PREV % self._branch, expected_bytes)
        return won

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
        """Find the lowest common ancestor of two commits."""
        if commit_a == commit_b:
            return commit_a

        from collections import deque

        seen_a: set[str] = {commit_a}
        seen_b: set[str] = {commit_b}
        queue_a: deque[str] = deque([commit_a])
        queue_b: deque[str] = deque([commit_b])

        while queue_a or queue_b:
            if queue_a:
                current = queue_a.popleft()
                if current in seen_b:
                    return current
                for p in self._load_parents(current):
                    if p not in seen_a:
                        seen_a.add(p)
                        queue_a.append(p)
                        if p in seen_b:
                            return p

            if queue_b:
                current = queue_b.popleft()
                if current in seen_a:
                    return current
                for p in self._load_parents(current):
                    if p not in seen_b:
                        seen_b.add(p)
                        queue_b.append(p)
                        if p in seen_a:
                            return p

        return None

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
        if at is not None and self.store.get(COMMIT_ROOT % at) is None:
            raise ValueError(f"Commit '{at}' does not exist")
        if not self.store.cas(branch_key, dumps(target), expected=None):
            raise ValueError(f"Branch '{name}' already exists")
        # A branch that has just been created has no previous HEAD, so any
        # backup under this name is stale by definition. One can outlive a
        # ``delete_branch`` — a writer descheduled between its CAS and its
        # backup write, resuming after the delete — and while the name is
        # unclaimed head resolution ignores it, but re-installing an anchor
        # would make it reachable again: corrupt this branch's fresh HEAD
        # before its first successful CAS and the prev-HEAD tier would serve
        # the *deleted* branch's tip. Dropped after the CAS, so a losing
        # attempt cannot take out the existing branch's real backup.
        self.store.remove(BRANCH_HEAD_PREV % name)
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
        self.store.remove(branch_key)
        # The prev-HEAD recovery backup goes too: left behind, a
        # same-named branch created later would "recover" the deleted
        # state through _resolve_head's fallback. Removed before
        # clean_orphans so commits only it referenced are collectable.
        self.store.remove(BRANCH_HEAD_PREV % name)
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
            raise ValueError(f"Branch '{name}' does not exist")
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
        """Reset HEAD to a specific commit."""
        if self.store.get(COMMIT_ROOT % commit_hash) is None:
            return False
        branch_key = BRANCH_HEAD % self._branch
        prev_key = BRANCH_HEAD_PREV % self._branch
        # Save current HEAD as prev before overwriting
        current = self.store.get(branch_key)
        if current is not None:
            self.store.set(prev_key, current)
        self.store.set(branch_key, dumps(commit_hash))
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
        for key in store.keys():
            if isinstance(key, str) and key.startswith(prefix):
                branch_name = key[len(prefix) :]
                if branch_name and not branch_name.startswith(TAG_BRANCH_PREFIX):
                    result.append(branch_name)
        return sorted(result)

    def list_branches(self) -> list[str]:
        """List all branch names in the store."""
        return VersionedKV.branches(self.store)

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

        There is one race, and it is narrow: tagging a commit that is
        *already* an orphan older than ``min_age`` can lose to a sweep
        running concurrently, which was free to collect that commit
        before the tag existed. Callers tag a commit they are holding —
        a head, or something a head descends from — and a commit a
        branch reaches is never a sweep candidate.
        """
        _validate_tag_name(name)
        target = at or self._current_commit
        if self.store.get(COMMIT_ROOT % target) is None:
            raise ValueError(f"Commit '{target}' does not exist")
        # Encoded before anything is written, so info that cannot be
        # serialized raises without leaving a tag behind.
        record = dumps({"time": time.time(), "info": info})

        # CAS against absence: two writers racing the same name cannot
        # both win, and an existing tag is never silently overwritten.
        head_key = BRANCH_HEAD % _tag_branch(name)
        if not self.store.cas(head_key, dumps(target), expected=None):
            raise ValueError(f"Tag '{name}' already exists")
        # A name that has just been claimed has no previous HEAD, so any
        # backup under it is stale by definition — left by an earlier tag
        # of the same name, or by something that moved this name as an
        # ordinary branch. Dropped after the CAS, so a losing attempt
        # cannot take out the winner's state.
        self.store.remove(BRANCH_HEAD_PREV % _tag_branch(name))
        # The record is a second write, so a crash in between leaves a
        # tag with no time and no info. That is why the head key alone is
        # what GC and resolution read.
        self.store.set(TAG_INFO_KEY % name, record)
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
        self.store.remove(head_key)
        self.store.remove(BRANCH_HEAD_PREV % _tag_branch(name))
        self.store.remove(TAG_INFO_KEY % name)
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
        mark-and-sweep against ``self.store``. Kept as a method so
        existing callers (and ``delete_branch``) read naturally.

        Reclaims commit metadata, blobs and HAMT nodes, but **not
        chunks** — see :func:`clean_orphans` for why, and
        :meth:`deep_clean` for the pass that reclaims them.

        Returns:
            Number of orphaned commits removed.
        """
        return clean_orphans(self.store, min_age)

    def deep_clean(self, min_age: float = 3600) -> int:
        """Orphan sweep plus a full unreferenced-node/chunk scan.

        Thin instance wrapper over :func:`deep_clean`. **Requires a
        quiescent store** — the namespace scan will delete artifacts
        written by a concurrent writer, including ones a live branch
        HEAD has since come to depend on. Use :meth:`clean_orphans`
        for routine cleanup; schedule this one when you can quiesce
        the store, since it is the only pass that reclaims chunks —
        both the ones no commit references and the ones deleted
        orphans owned.

        Returns:
            Number of orphaned commits removed.
        """
        return deep_clean(self.store, min_age)

    # -- Internal --

    def _load_commit(self, commit_hash: str, *, update_base: bool) -> None:
        """Load a commit's state into memory."""
        self._current_commit = commit_hash
        if update_base:
            self._base_commit = commit_hash
        self._populate_state(commit_hash)
