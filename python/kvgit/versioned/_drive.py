"""Run an operation of the core against a store.

The core never touches a store: an operation hands back each thing it
needs as a request, and this performs it and hands back the answer. A
request that raises is answered as failed; the operation unwinds,
undoing what it wrote, and the exception is raised once it is done.
"""

import hashlib
import logging
from collections.abc import Callable, Generator, Sequence
from typing import Any

from ..errors import (
    BranchExistsError,
    BranchMovedError,
    ConcurrencyError,
    CorruptHeadError,
    GcBusy,
    MergeConflict,
    TagExistsError,
    UnknownBranchError,
    UnknownCommitError,
    UnknownTagError,
)
from ..kv.base import KVStore
from .protocol import MergeChoice

logger = logging.getLogger("kvgit")

BLOB_PREFIX = "kvgit:blob:"

_NOTES = {
    "from_backup": "Branch '%s': HEAD corrupt, recovered from prev HEAD",
    "from_recoverer": "Branch '%s': HEAD corrupt, recovered via injected recoverer",
    "recoverer_missed": "Branch '%s': recoverer returned %r, which is not a commit "
    "in this store; treating the branch as unrecoverable",
    "healed": "Branch '%s': corrupt HEAD replaced with recovered commit",
    "lease_overrun": "a sweep ran %.1fs past its %.1fs GC lease; writers were "
    "free to write during that window. Raise lease_ttl.",
}

# The logger each note goes to, where it isn't "kvgit".
_LOGGERS = {"lease_overrun": "kvgit.orphans"}


def drive(op: Any, store: KVStore, **env: Any) -> Any:
    """Perform `op`'s requests until it is done, and return its outcome;
    see :func:`steps` for ``env``."""
    run = steps(op, store, **env)
    while True:
        try:
            next(run)
        except StopIteration as done:
            return done.value


def steps(
    op: Any,
    store: KVStore,
    *,
    held: dict[str, bytes] | None = None,
    merge_fns: Sequence[Callable[..., Any]] = (),
    post_check: Callable[[str, bytes], bool] | None = None,
    recoverer: Callable[[KVStore, str], Any] | None = None,
    owner: Callable[[], str] | None = None,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> Generator[Any, None, Any]:
    """Perform `op`'s requests, yielding each part of the answer it emits
    early, and return its outcome.

    ``held`` holds the bytes the operation writes by key without
    reading them; values merge functions make join it. ``merge_fns`` are
    the functions the operation names by number, and ``post_check`` the
    check it runs over the values they make.
    """
    held = {} if held is None else held
    errors: dict[str, Exception] = {}
    raised: BaseException | None = None
    reply: Any = None
    failed = False
    while True:
        request = op.resume(clock(), reply, failed)
        kind = request[0]
        if kind == "done":
            _, outcome, notes = request
            for note, *args in notes:
                logging.getLogger(_LOGGERS.get(note, "kvgit")).warning(
                    _NOTES[note], *args
                )
            if outcome[0] == "ok":
                return outcome[1]
            if raised is not None:
                raise raised
            raise _error(outcome[1], outcome[2], errors)
        reply, failed = None, False
        if kind == "emit":
            # Outside the handler below, so a caller that stops iterating
            # closes this generator rather than failing the request.
            yield request[1]
            continue
        try:
            if kind == "get":
                reply = dict(store.get_many(request[1]))
            elif kind == "scan":
                reply = list(store.keys(request[1]))
            elif kind == "cas":
                _, expect, writes, removes = request
                values = {k: held[k] if v is None else v for k, v in writes}
                reply = store.cas_many(dict(expect), values, removes)
            elif kind == "wait":
                sleep(request[1])
            elif kind == "merge":
                reply = _merge(store, request[1], merge_fns, held, errors)
            elif kind == "now":
                reply = clock()
            elif kind == "owner":
                reply = owner() if owner is not None else None
            elif kind == "check":
                reply = [
                    key
                    for key, blob, made in request[1]
                    if post_check is not None
                    and not post_check(key, held[blob] if made is None else made)
                ]
            elif kind == "recover":
                guess = recoverer(store, request[1]) if recoverer else None
                if guess is not None and not isinstance(guess, str):
                    logger.warning(_NOTES["recoverer_missed"], request[1], guess)
                    guess = None
                reply = guess
        except BaseException as e:  # noqa: BLE001 — raised once the op unwinds
            raised = raised or e
            failed = True


def _merge(
    store: KVStore,
    asks: list[tuple[int, str, str | None, str | None, str | None]],
    merge_fns: Sequence[Callable[..., Any]],
    held: dict[str, bytes],
    errors: dict[str, Exception],
) -> list[Any]:
    """Run each merge function asked for on its key's three sides."""
    blobs = {b for _, _, *sides in asks for b in sides if b is not None}
    values = store.get_many(list(blobs)) if blobs else {}
    answers: list[Any] = []
    for rule, key, *sides in asks:
        old, ours, theirs = (None if b is None else values.get(b) for b in sides)
        try:
            result = merge_fns[rule](old, ours, theirs)
        except Exception as e:  # noqa: BLE001 — a caller's function: a conflict
            errors[key] = e
            answers.append(("failed", str(e)))
            continue
        if result is MergeChoice.OURS:
            answers.append("ours")
        elif result is MergeChoice.THEIRS:
            answers.append("theirs")
        else:
            blob = BLOB_PREFIX + hashlib.sha256(result).hexdigest()
            held[blob] = result
            answers.append(("value", blob, len(result)))
    return answers


_ERRORS: dict[str, Callable[[str], Exception]] = {
    "unknown_branch": UnknownBranchError,
    "corrupt_head": CorruptHeadError,
    "unknown_tag": UnknownTagError,
    "unknown_commit": UnknownCommitError,
    "branch_exists": BranchExistsError,
    "tag_exists": TagExistsError,
    "concurrency": ConcurrencyError,
    "gc_busy": GcBusy,
}


def _error(
    kind: str, fields: dict[str, Any], errors: dict[str, Exception]
) -> Exception:
    """The exception the core's error `kind` is raised as."""
    if kind == "branch_moved":
        return BranchMovedError(fields["branch"], fields["base"], fields["head"])
    if kind in _ERRORS:
        return _ERRORS[kind](fields["message"])
    if kind == "merge_conflict":
        from ..merges import CantMark

        failures = {
            key: errors.get(key) or CantMark(reason)
            for key, reason in fields["failures"].items()
        }
        return MergeConflict(set(fields["keys"]), failures)
    return ValueError(fields["message"])
