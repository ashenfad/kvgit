"""Runs a corpus scenario (``corpus/README.md``) through kvgit's public API.

The clock and lease owners are the scenario's: ``time`` and
``_new_lease_owner`` in ``kvgit.versioned.kv`` are replaced while it
runs, so every run writes the same bytes.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterator
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


class Run:
    def __init__(self, scenario: dict, path: Path) -> None:
        self.scenario = scenario
        self.path = path
        self.store = Memory()
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

    def match_map(self, what: str, expected: dict, actual: dict) -> None:
        if sorted(expected) != sorted(actual):
            raise ScenarioFailure(
                f"{what}: expected names {sorted(expected)}, got {sorted(actual)}"
            )
        for name, value in expected.items():
            self.match(f"{what} {name!r}", value, actual[name])

    # Handles

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
        return {
            key: encode_bytes(value)
            for key, value in sorted(self.store.items(), key=lambda kv: kv[0])
        }

    def step(self, step: dict) -> None:
        op = step["op"]
        handler = getattr(self, "op_" + op, None)
        if handler is None:
            raise ScenarioFailure(f"unknown op {op!r}")
        expect = step.get("expect")
        if isinstance(expect, dict) and "error" in expect:
            try:
                handler(step)
            except Exception as e:
                name = error_name(e)
                if name != expect["error"]:
                    raise ScenarioFailure(
                        f"expected {expect['error']}, raised {name}: {e}"
                    ) from e
                if "keys" in expect:
                    self.match_sorted(
                        "conflicting keys", expect["keys"], e.conflicting_keys
                    )
                return
            raise ScenarioFailure(f"expected {expect['error']}, but it succeeded")
        result = handler(step)
        if "expect" in step:
            self.check(op, step["expect"], result)

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
        elif op in ("branches", "tags"):
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
            self.store, codec="bytes", **options, **rules(step)
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
            min_age=step.get("min_age", 3600), deep=step.get("deep", False)
        )

    def op_store_set(self, step: dict) -> None:
        self.store.set(step["key"], decode_bytes(step["value"]))

    def op_store_delete(self, step: dict) -> None:
        self.store.remove(step["key"])


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
