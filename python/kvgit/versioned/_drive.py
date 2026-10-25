"""Run an operation of the core against a store.

The core never touches a store: an operation hands back each thing it
needs as a request, and this performs it and hands back the answer. A
request that raises is answered as failed; the operation unwinds,
undoing what it wrote, and the exception is raised once it is done.
"""

import hashlib
import logging
from collections.abc import Callable, Sequence
from typing import Any

from ..errors import (
    BranchMovedError,
    ConcurrencyError,
    MergeConflict,
    UnknownBranchError,
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
}


def drive(
    op: Any,
    store: KVStore,
    *,
    held: dict[str, bytes],
    merge_fns: Sequence[Callable[..., Any]],
    recoverer: Callable[[KVStore, str], Any] | None,
    clock: Callable[[], float],
    sleep: Callable[[float], None],
) -> dict[str, Any]:
    """Perform `op`'s requests until it is done, and return its outcome.

    ``held`` holds the bytes the operation writes by key without
    reading them; values merge functions make join it. ``merge_fns`` are
    the functions the operation names by number.
    """
    errors: dict[str, Exception] = {}
    raised: BaseException | None = None
    reply: Any = None
    failed = False
    while True:
        request = op.resume(clock(), reply, failed)
        kind = request[0]
        if kind == "done":
            _, outcome, notes = request
            for note, branch, extra in notes:
                args = (branch,) if extra is None else (branch, extra)
                logger.warning(_NOTES[note], *args)
            if outcome[0] == "ok":
                return outcome[1]
            if raised is not None:
                raise raised
            raise _error(outcome[1], outcome[2], errors)
        reply, failed = None, False
        try:
            if kind == "get":
                reply = dict(store.get_many(request[1]))
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


def _error(
    kind: str, fields: dict[str, Any], errors: dict[str, Exception]
) -> Exception:
    """The exception the core's error `kind` is raised as."""
    if kind == "branch_moved":
        return BranchMovedError(fields["branch"], fields["base"], fields["head"])
    if kind == "unknown_branch":
        return UnknownBranchError(fields["message"])
    if kind == "concurrency":
        return ConcurrencyError(fields["message"])
    if kind == "merge_conflict":
        from ..merges import CantMark

        failures = {
            key: errors.get(key) or CantMark(reason)
            for key, reason in fields["failures"].items()
        }
        return MergeConflict(set(fields["keys"]), failures)
    return ValueError(fields["message"])
