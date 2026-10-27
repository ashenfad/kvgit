"""Runs a corpus scenario (``corpus/README.md``) through kvgit's public API.

The clock and lease owners are the scenario's: ``time`` and
``_new_lease_owner`` in ``kvgit.versioned.kv`` are replaced while it
runs, so every run writes the same bytes.
"""

from __future__ import annotations

import base64
import json
import re
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from kvgit import (
    KvgitError,
    MergeChoice,
    Repo,
    Snapshot,
    Worktree,
    _sync,
    recover_by_commit_scan,
)
from kvgit.kv.memory import Memory
from kvgit.merges import make_text_merge, text_merge_result
from kvgit.versioned import kv as _kv

CORPUS = Path(__file__).resolve().parents[2] / "corpus" / "scenarios"
HOST = "python"


def scenario_paths() -> list[Path]:
    return sorted(CORPUS.rglob("*.json"))


# -- Bytes --


def decode_bytes(value: Any) -> bytes:
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, dict) and set(value) == {"b64"}:
        return base64.b64decode(value["b64"])
    raise ValueError(f"not a bytes value: {value!r}")


def encode_bytes(value: bytes) -> str | dict[str, str]:
    try:
        text = value.decode()
    except UnicodeDecodeError:
        return {"b64": base64.b64encode(value).decode()}
    return text


# -- Determinism --


class _Clock:
    """The scenario's clock, standing in for the ``time`` module."""

    def __init__(self, now: float) -> None:
        self.now = float(now)

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


@contextmanager
def _pinned(clock: _Clock) -> Iterator[None]:
    owners = (f"lease-{n}" for n in range(1, 1_000_000))
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(_kv, "time", clock)
        mp.setattr(_kv, "_new_lease_owner", lambda: next(owners))
        yield


# -- Merge rules --


def _concat(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
    return (theirs or b"") + b"+" + (ours or b"")


def _fail(old: bytes | None, ours: bytes | None, theirs: bytes | None) -> bytes:
    raise ValueError("this rule refuses every key")


RULES: dict[str, Any] = {
    "text": make_text_merge(),
    "text_strict": make_text_merge(strict=True),
    "ours": MergeChoice.OURS,
    "theirs": MergeChoice.THEIRS,
    "pick_ours": lambda old, ours, theirs: MergeChoice.OURS,
    "pick_theirs": lambda old, ours, theirs: MergeChoice.THEIRS,
    "concat": _concat,
    "fail": _fail,
}


def rules(step: dict) -> dict[str, Any]:
    """The merge rules a step names, as keyword arguments."""
    out: dict[str, Any] = {}
    if "merge_fns" in step:
        out["merge_fns"] = {k: RULES[v] for k, v in step["merge_fns"].items()}
    if "merge_prefixes" in step:
        out["merge_prefixes"] = {k: RULES[v] for k, v in step["merge_prefixes"].items()}
    if "default_merge" in step:
        out["default_merge"] = RULES[step["default_merge"]]
    return out


# -- The run --


def error_name(e: Exception) -> str:
    """The name a scenario uses for an error: kvgit's own errors by class
    name, and a caller's mistake (``ValueError``, ``TypeError``) as
    ``invalid``."""
    if isinstance(e, (ValueError, TypeError)) and not isinstance(e, KvgitError):
        return "invalid"
    return type(e).__name__


class ScenarioFailure(AssertionError):
    pass


class _Job:
    """An operation started on its own thread, to be held at a point."""

    def __init__(self, step: dict, pause: str) -> None:
        self.step = step
        self.pause = pause
        self.paused = threading.Event()
        self.go = threading.Event()
        self.done = threading.Event()
        self.outcome: tuple[Any, BaseException | None] = (None, None)


class PausableMemory(Memory):
    """A memory store that holds a job's write at a protocol point: the
    commit batch (the write landing an in-flight marker) or the publish
    (the write moving a branch head)."""

    def __init__(self) -> None:
        super().__init__()
        self.local = threading.local()

    def cas_many(self, expected, writes, removes=()) -> bool:
        job: _Job | None = getattr(self.local, "job", None)
        if job is not None and not job.paused.is_set() and _point(writes) == job.pause:
            job.paused.set()
            job.go.wait()
        return super().cas_many(expected, writes, removes)


def _point(writes: Mapping[str, bytes]) -> str | None:
    """The protocol point a write is, by the keys it writes."""
    if any(k.startswith(_kv.IN_FLIGHT_KEY % "") for k in writes):
        return "batch"
    if any(
        k.startswith(_kv.BRANCH_HEAD % "")
        and not k.startswith(_kv.BRANCH_HEAD % _kv.TAG_BRANCH_PREFIX)
        for k in writes
    ):
        return "publish"
    return None


class Run:
    def __init__(self, scenario: dict, path: Path) -> None:
        self.scenario = scenario
        self.path = path
        self.store = PausableMemory()
        self.stores: dict[str, PausableMemory] = {}
        self.remotes: dict[str, _sync.MemoryRemote] = {}
        self.jobs: dict[str, _Job] = {}
        for key, value in (scenario.get("store") or {}).items():
            self.store.set(key, decode_bytes(value))
        self.clock = _Clock(scenario["clock"])
        self.repos: dict[str, Repo] = {}
        self.handles: dict[str, Worktree | Snapshot] = {}
        self.vars: dict[str, Any] = {"$root": _kv.ROOT_COMMIT}

    # Variables

    def arg(self, value: Any) -> Any:
        if isinstance(value, str) and value.startswith("$"):
            if value not in self.vars:
                raise ScenarioFailure(f"{value} is used before it is bound")
            return self.vars[value]
        return value

    def interpolate(self, text: str) -> str:
        """``text`` with each bound variable in it replaced by its value,
        which must be a string."""

        def value(m: re.Match) -> str:
            name = m.group(0)
            if name not in self.vars:
                raise ScenarioFailure(f"{name} is used before it is bound")
            if not isinstance(self.vars[name], str):
                raise ScenarioFailure(f"{name} is {self.vars[name]!r}, not a string")
            return self.vars[name]

        return re.sub(r"\$[A-Za-z_][A-Za-z0-9_]*", value, text)

    def match(self, what: str, expected: Any, actual: Any) -> None:
        if isinstance(expected, str) and expected.startswith("$"):
            if expected not in self.vars:
                self.vars[expected] = actual
                return
            expected = self.vars[expected]
        if expected != actual:
            raise ScenarioFailure(f"{what}: expected {expected!r}, got {actual!r}")

    def match_sorted(self, what: str, expected: list, actual: Any) -> None:
        self.match(what, sorted(self.arg(e) for e in expected), sorted(actual))

    def match_fields(self, what: str, expected: dict, actual: dict) -> None:
        for field, want in expected.items():
            got = actual[field]
            if isinstance(want, dict) and got is not None:
                self.match_fields(f"{what}.{field}", want, got)
            else:
                self.match(f"{what}.{field}", want, got)

    def match_map(self, what: str, expected: dict, actual: dict) -> None:
        if sorted(expected) != sorted(actual):
            raise ScenarioFailure(
                f"{what}: expected names {sorted(expected)}, got {sorted(actual)}"
            )
        for name, value in expected.items():
            self.match(f"{what} {name!r}", value, actual[name])

    # Handles

    def store_of(self, step: dict) -> PausableMemory:
        if "store" not in step:
            return self.store
        if step["store"] not in self.stores:
            raise ScenarioFailure(f"no store {step['store']}")
        return self.stores[step["store"]]

    def remote(self, step: dict) -> _sync.MemoryRemote:
        if step["remote"] not in self.remotes:
            raise ScenarioFailure(f"no remote {step['remote']}")
        return self.remotes[step["remote"]]

    def repo(self, step: dict) -> Repo:
        return self.repos[step["repo"]]

    def wt(self, step: dict) -> Worktree:
        handle = self.handles[step["wt"]]
        assert isinstance(handle, Worktree), f"{step['wt']} is not a worktree"
        return handle

    def ref(self, step: dict) -> dict[str, str]:
        given = {k: self.arg(step[k]) for k in ("branch", "commit", "tag") if k in step}
        if len(given) != 1:
            raise ScenarioFailure(f"name exactly one of branch, commit, tag: {step}")
        return given

    # Running

    def run(self) -> dict[str, Any]:
        with _pinned(self.clock):
            for index, step in enumerate(self.scenario["steps"]):
                try:
                    self.step(step)
                except ScenarioFailure as e:
                    raise ScenarioFailure(
                        f"{self.path.name} step {index} ({step['op']}): {e}"
                    ) from None
            if self.jobs:
                raise ScenarioFailure(
                    f"{self.path.name}: never resumed: {', '.join(self.jobs)}"
                )
        return {
            key: encode_bytes(value)
            for key, value in sorted(self.store.items(), key=lambda kv: kv[0])
        }

    def step(self, step: dict) -> None:
        if step["op"] == "resume":
            self.op_resume(step)
            return
        self.judge(step, self.perform(step))

    def perform(self, step: dict) -> tuple[Any, BaseException | None]:
        """Run a step's operation; its result, or the error it raised."""
        handler = getattr(self, "op_" + step["op"], None)
        if handler is None:
            raise ScenarioFailure(f"unknown op {step['op']!r}")
        try:
            return handler(step), None
        except ScenarioFailure:
            raise
        except Exception as e:  # noqa: BLE001 — the step's expect judges it
            return None, e

    def judge(self, step: dict, outcome: tuple[Any, BaseException | None]) -> None:
        """Hold an operation's outcome to the step's ``expect``."""
        result, error = outcome
        expect = step.get("expect")
        if isinstance(expect, dict) and "error" in expect:
            if error is None:
                raise ScenarioFailure(f"expected {expect['error']}, but it succeeded")
            name = error_name(error)
            if name != expect["error"]:
                raise ScenarioFailure(
                    f"expected {expect['error']}, raised {name}: {error}"
                ) from error
            if "keys" in expect:
                self.match_sorted(
                    "conflicting keys", expect["keys"], error.conflicting_keys
                )
            return
        if error is not None:
            raise error
        if "expect" in step:
            self.check(step["op"], step["expect"], result)

    def check(self, op: str, expected: Any, actual: Any) -> None:
        if op in ("commit", "merge", "apply", "cherry_pick", "revert"):
            for field in expected:
                value = getattr(actual, field)
                if field in ("auto_merged_keys", "carried_keys"):
                    self.match_sorted(field, expected[field], value)
                else:
                    self.match(field, expected[field], value)
        elif op == "text_merge":
            for field in expected:
                value = actual[field]
                if field == "merged":
                    self.match(field, decode_bytes(expected[field]), value)
                else:
                    self.match(field, expected[field], value)
        elif op == "get":
            self.match(
                "value", None if expected is None else decode_bytes(expected), actual
            )
        elif op in ("keys", "log"):
            if op == "keys":
                self.match_sorted("keys", expected, actual)
            else:
                if len(expected) != len(actual):
                    raise ScenarioFailure(
                        f"log: expected {len(expected)} commits, got {actual}"
                    )
                for n, (e, a) in enumerate(zip(expected, actual, strict=True)):
                    self.match(f"log[{n}]", e, a)
        elif op in ("push", "pull", "sync", "delete_remote_branch"):
            self.match_fields(op, expected, actual)
        elif op in ("branches", "tags", "remote_refs"):
            self.match_map(op, expected, actual)
        elif op == "get_commit":
            for field in expected:
                value = getattr(actual, field)
                if field == "parents":
                    value = list(value)
                    if len(expected[field]) != len(value):
                        raise ScenarioFailure(
                            f"parents: expected {expected[field]}, got {value}"
                        )
                    for n, (e, a) in enumerate(
                        zip(expected[field], value, strict=True)
                    ):
                        self.match(f"parents[{n}]", e, a)
                else:
                    self.match(field, expected[field], value)
        elif op == "diff":
            for field in ("added", "removed", "modified"):
                if field in expected:
                    self.match_sorted(field, expected[field], getattr(actual, field))
        else:
            self.match(op, expected, actual)

    # -- Ops --

    def op_start(self, step: dict) -> None:
        if step["pause"] not in ("batch", "publish"):
            raise ScenarioFailure(f"unknown pause point {step['pause']!r}")
        job = _Job(step["step"], step["pause"])
        self.jobs[step["job"]] = job

        def run() -> None:
            self.store.local.job = job
            try:
                job.outcome = self.perform(job.step)
            except BaseException as e:  # noqa: BLE001 — handed to resume
                job.outcome = (None, e)
            finally:
                job.done.set()

        threading.Thread(target=run, daemon=True).start()
        while not (job.paused.wait(0.01) or job.done.is_set()):
            pass
        if not job.paused.is_set():
            raise ScenarioFailure(
                f"job {step['job']} finished without reaching {job.pause}"
            )

    def op_resume(self, step: dict) -> None:
        job = self.jobs.pop(step["job"])
        job.go.set()
        if not job.done.wait(10):
            raise ScenarioFailure(f"job {step['job']} did not finish")
        outcome = job.outcome
        if isinstance(outcome[1], ScenarioFailure):
            raise outcome[1]
        self.judge(
            {**job.step, **({"expect": step["expect"]} if "expect" in step else {})},
            outcome,
        )

    def op_clock(self, step: dict) -> None:
        if "set" in step:
            self.clock.now = float(step["set"])
        else:
            self.clock.now += step["advance"]

    def op_open(self, step: dict) -> None:
        options: dict[str, Any] = {}
        if "recover" in step:
            if step["recover"] != "commit_scan":
                raise ScenarioFailure(f"unknown recoverer {step['recover']!r}")
            options["recover_from_corrupt_head"] = recover_by_commit_scan
        self.repos[step["as"]] = Repo(
            self.store_of(step), codec="bytes", **options, **rules(step)
        )

    def op_worktree(self, step: dict) -> None:
        self.handles[step["as"]] = self.repo(step).worktree(
            step["branch"], create=step.get("create", False)
        )

    def op_snapshot(self, step: dict) -> None:
        self.handles[step["as"]] = self.repo(step).snapshot(**self.ref(step))

    def op_set(self, step: dict) -> None:
        self.wt(step)[step["key"]] = decode_bytes(step["value"])

    def op_delete(self, step: dict) -> None:
        del self.wt(step)[step["key"]]

    def op_get(self, step: dict) -> bytes | None:
        return self.handles[step["from"]].get(step["key"])

    def op_keys(self, step: dict) -> list[str]:
        return list(self.handles[step["from"]].keys())

    def op_head(self, step: dict) -> str:
        return self.wt(step).head

    def op_discard(self, step: dict) -> None:
        self.wt(step).discard()

    def op_refresh(self, step: dict) -> None:
        self.wt(step).refresh()

    def op_reset(self, step: dict) -> None:
        self.wt(step).reset(self.arg(step["commit"]))

    def op_commit(self, step: dict) -> Any:
        options: dict[str, Any] = {}
        for field in ("info", "on_moved", "on_conflict"):
            if field in step:
                options[field] = step[field]
        if "keys" in step:
            options["keys"] = set(step["keys"])
        return self.wt(step).commit(**options, **rules(step))

    def op_merge(self, step: dict) -> Any:
        options: dict[str, Any] = {}
        for field in ("info", "on_conflict", "fast_forward"):
            if field in step:
                options[field] = step[field]
        return self.wt(step).merge(**self.ref(step), **options, **rules(step))

    def _apply_options(self, step: dict) -> dict[str, Any]:
        options: dict[str, Any] = {}
        for field in ("info", "on_conflict"):
            if field in step:
                options[field] = step[field]
        return {**options, **rules(step)}

    def op_apply(self, step: dict) -> Any:
        return self.wt(step).apply(
            self.arg(step["base"]),
            self.arg(step["target"]),
            **self._apply_options(step),
        )

    def op_cherry_pick(self, step: dict) -> Any:
        return self.wt(step).cherry_pick(
            self.arg(step["commit"]), **self._apply_options(step)
        )

    def op_revert(self, step: dict) -> Any:
        return self.wt(step).revert(
            self.arg(step["commit"]), **self._apply_options(step)
        )

    def op_text_merge(self, step: dict) -> dict[str, Any]:
        sides = [
            None if step[side] is None else decode_bytes(step[side])
            for side in ("old", "ours", "theirs")
        ]
        labels = {k: step[k] for k in ("ours_label", "theirs_label") if k in step}
        if step.get("strict", False):
            return {"merged": make_text_merge(strict=True, **labels)(*sides)}
        merged, conflicted = text_merge_result(*sides, **labels)
        return {"merged": merged, "conflicted": conflicted}

    def op_branch_create(self, step: dict) -> str:
        at = step.get("at")
        return self.repo(step).branches.create(
            step["name"], self.arg(at) if at is not None else None
        )

    def op_branch_delete(self, step: dict) -> None:
        self.repo(step).branches.delete(step["name"])

    def op_repair_head(self, step: dict) -> str | None:
        return self.repo(step).repair_head(step["name"])

    def op_branches(self, step: dict) -> dict[str, str]:
        return dict(self.repo(step).branches)

    def op_tag_create(self, step: dict) -> None:
        self.repo(step).tags.create(
            step["name"], self.arg(step["commit"]), info=step.get("info")
        )

    def op_tag_delete(self, step: dict) -> None:
        self.repo(step).tags.delete(step["name"])

    def op_tags(self, step: dict) -> dict[str, str]:
        return dict(self.repo(step).tags)

    def op_get_commit(self, step: dict) -> Any:
        return self.repo(step).get_commit(self.arg(step["commit"]))

    def op_log(self, step: dict) -> list[str]:
        options: dict[str, Any] = {}
        if "first_parent" in step:
            options["first_parent"] = step["first_parent"]
        if "limit" in step:
            options["limit"] = step["limit"]
        return [c.hash for c in self.repo(step).log(**self.ref(step), **options)]

    def op_diff(self, step: dict) -> Any:
        return self.repo(step).diff(self.arg(step["a"]), self.arg(step["b"]))

    def op_merge_base(self, step: dict) -> str | None:
        return self.repo(step).merge_base(self.arg(step["a"]), self.arg(step["b"]))

    def op_gc(self, step: dict) -> int:
        return self.repo(step).gc(
            min_age=step.get("min_age", 3600),
            deep=step.get("deep", False),
            wait=step.get("wait", True),
        )

    def op_store(self, step: dict) -> None:
        self.stores[step["as"]] = PausableMemory()

    def op_remote(self, step: dict) -> None:
        self.remotes[step["as"]] = _sync.MemoryRemote(Memory(), id=step.get("id"))

    def op_push(self, step: dict) -> dict[str, Any]:
        return _sync_result(
            _sync.push_branch(self.store_of(step), self.remote(step), step["branch"])
        )

    def op_pull(self, step: dict) -> dict[str, Any]:
        return _sync_result(
            _sync.pull_branch(self.store_of(step), self.remote(step), step["branch"])
        )

    def op_sync(self, step: dict) -> dict[str, Any]:
        outcome = _sync.sync_branch(
            self.store_of(step), self.remote(step), step["branch"]
        )
        return {
            "status": outcome.status,
            "pull": _sync_result(outcome.pull),
            "push": None if outcome.push is None else _sync_result(outcome.push),
        }

    def op_delete_remote_branch(self, step: dict) -> dict[str, Any]:
        expected = self.arg(step["expected"]) if "expected" in step else None
        result = _sync.delete_remote_branch(
            self.store_of(step), self.remote(step), step["branch"], expected=expected
        )
        return {"status": result.status, "remote_head": result.remote_head}

    def op_remote_refs(self, step: dict) -> dict[str, str]:
        return {r.branch: r.head for r in self.remote(step).list_refs()}

    def op_store_set(self, step: dict) -> None:
        self.store_of(step).set(
            self.interpolate(step["key"]), decode_bytes(step["value"])
        )

    def op_store_delete(self, step: dict) -> None:
        self.store_of(step).remove(self.interpolate(step["key"]))


def _sync_result(r: _sync.SyncResult) -> dict[str, Any]:
    """A sync result as a scenario names its fields."""
    return {
        "status": r.status,
        "branch": r.branch,
        "local_head": r.local_head,
        "remote_head": r.remote_head,
        "transferred": r.transferred,
    }


def load(path: Path) -> dict:
    return json.loads(path.read_text())


def save_final(path: Path, final: dict) -> None:
    """Write ``final`` as the scenario's last field, leaving the text
    before it as it was written."""
    text = path.read_text()
    cut = text.find('\n  "final": ')
    head = text[:cut] if cut != -1 else text[: text.rindex("\n}")]
    body = json.dumps(final, indent=2, ensure_ascii=False).replace("\n", "\n  ")
    path.write_text(f'{head.rstrip().rstrip(",")},\n  "final": {body}\n}}\n')
