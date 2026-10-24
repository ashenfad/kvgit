# The scenario corpus

Language-neutral scenarios that every kvgit implementation runs: the
Python package, the TypeScript package, and later the core itself. A
scenario names a starting store, a sequence of operations by named
actors with the results each must give, and the exact bytes of the
store at the end. Two implementations that pass the same scenario
wrote the same keys with the same bytes.

Scenarios live under `scenarios/<area>/<name>.json`. The runners are
`python/tests/test_corpus.py` and `ts/tests/corpus.test.ts`; each runs
every scenario through its package's public API.

## A scenario

```json
{
  "description": "One commit on a new branch, read back.",
  "clock": 1700000000.25,
  "steps": [
    { "op": "open", "as": "alice" },
    { "op": "worktree", "repo": "alice", "as": "w", "branch": "main", "create": true },
    { "op": "set", "wt": "w", "key": "greeting", "value": "hello" },
    { "op": "commit", "wt": "w", "expect": { "strategy": "fast_forward", "commit": "$c1" } },
    { "op": "get", "from": "w", "key": "greeting", "expect": "hello" }
  ],
  "final": { "__kvgit_version__": "4", "...": "..." }
}
```

- `description`: what the scenario pins down, in a sentence or two.
- `only` (optional): the implementations that run it, from `"python"`
  and `"ts"`. Absent means every one.
- `clock`: the time, in seconds, when the scenario starts.
- `store` (optional): the store's contents before the first step, key
  to bytes. Absent means empty.
- `steps`: the operations, in order.
- `final`: the whole store after the last step, key to bytes. Written by
  the recorder (below), never by hand.

### Bytes

A JSON string stands for its UTF-8 encoding. Bytes that aren't valid
UTF-8 are written `{ "b64": "<base64>" }`. The recorder uses a string
wherever one round-trips, so stored records read as the JSON they are.

### Names

Actors, worktrees and snapshots are named by the step that makes them
(`as`) and referred to by that name.

A string starting with `$` is a variable. In an `expect`, an unbound
variable binds to the value found, and a bound one must equal it; in
an argument, it must already be bound. `$root` is bound from the start
to the root commit every branch begins at. Variables keep commit
hashes out of hand-written steps: the `final` store pins them.

### Determinism

Every run of a scenario writes the same bytes:

- **One clock.** Commit and tag times, in-flight markers, lease expiry
  and `min_age` all read the scenario's clock, which moves only by
  `clock` steps. A wait for a lease advances it by the time waited.
- **Lease owners.** The `n`th GC lease taken in a scenario is owned by
  `lease-<n>`, counting from 1.
- **Values are bytes.** Repositories use the bytes codec in every
  implementation.

The runners arrange this through test hooks, not public API: Python
replaces `time` and `_new_lease_owner` in `kvgit.versioned.kv`;
TypeScript passes `now` to `Repo.open` and replaces `leaseEnv`.

## Steps

Each step has an `op` and the fields below. Any step may carry an
`expect`; `{ "error": "<Name>" }` expects the step to fail with that
error, named as both packages name it (`BranchMovedError`,
`MergeConflict`, `UnknownBranchError`, `UnknownCommitError`,
`UnknownTagError`, `BranchExistsError`, `TagExistsError`,
`ConcurrencyError`, `GcBusy`, `CantMark`). A caller's mistake, which
each language reports in its own idiom (`ValueError` or `TypeError` in
Python, a plain `Error`, `TypeError` or `RangeError` in TypeScript), is
`invalid`. A `MergeConflict` may also give `"keys"`, the conflicting
keys. A step without `expect` must succeed.

**Time**

| op | fields | |
|---|---|---|
| `clock` | `set` or `advance` | Set the clock, or move it forward. |

**Repositories and handles**

| op | fields | expect |
|---|---|---|
| `open` | `as`, merge rules | |
| `worktree` | `repo`, `as`, `branch`, `create` (default false) | |
| `snapshot` | `repo`, `as`, one of `branch`, `commit`, `tag` | |

Each `open` is a separate repository over the one shared store, as two
processes would be. Merge rules given to `open` are the repository's
defaults; see below.

**Reading and writing**

| op | fields | expect |
|---|---|---|
| `set` | `wt`, `key`, `value` | |
| `delete` | `wt`, `key` | |
| `get` | `from` (worktree or snapshot), `key` | bytes, or `null` for absent |
| `keys` | `from` | the keys, sorted |
| `head` | `wt` | the worktree's head commit |
| `discard` | `wt` | |
| `refresh` | `wt` | |
| `reset` | `wt`, `commit` | |

**Commits and merges**

| op | fields | expect |
|---|---|---|
| `commit` | `wt`, `info`, `on_moved`, `on_conflict`, `keys` | a result |
| `merge` | `wt`, one of `branch`, `commit`, `tag`; `info`, `on_conflict`, `fast_forward` | a result |
| `apply` | `wt`, `base`, `target`; `info`, `on_conflict` | a result |
| `cherry_pick` | `wt`, `commit`; `info`, `on_conflict` | a result |
| `revert` | `wt`, `commit`; `info`, `on_conflict` | a result |

All fields but `wt` and the commits are optional and take the
packages' defaults, and each of these also takes merge rules. A result expectation checks only the fields it gives:
`merged`, `commit`, `strategy` (`no_op`, `fast_forward`, `three_way`,
`apply`), `auto_merged_keys` and `carried_keys` (sorted lists).

**Merge rules**

`open`, `commit`, `merge`, `apply`, `cherry_pick` and `revert` take
`merge_fns` (key to rule), `merge_prefixes` (prefix to rule) and
`default_merge` (a rule). A rule is one of these names:

| rule | |
|---|---|
| `text` | the line-based text merge, marking conflicts in the value |
| `text_strict` | the text merge, refusing (a conflict) where it would mark one |
| `ours`, `theirs` | a standing policy: that side's value for every key it covers that either side changed |
| `pick_ours`, `pick_theirs` | a function answering that side for each contested key |
| `concat` | a function: theirs, `+`, ours (a missing side as empty) |
| `fail` | a function that raises for every key |

| op | fields | expect |
|---|---|---|
| `text_merge` | `old`, `ours`, `theirs` (bytes or `null`); `ours_label`, `theirs_label`, `strict` | `merged`, and `conflicted` unless `strict` |

`text_merge` calls the text merge directly, with no repository.

**Refs**

| op | fields | expect |
|---|---|---|
| `branch_create` | `repo`, `name`, `at` (default the root commit) | |
| `branch_delete` | `repo`, `name` | |
| `branches` | `repo` | name to tip commit |
| `tag_create` | `repo`, `name`, `commit`, `info` | |
| `tag_delete` | `repo`, `name` | |
| `tags` | `repo` | name to commit |

**History**

| op | fields | expect |
|---|---|---|
| `get_commit` | `repo`, `commit` | the fields it gives of `parents`, `time`, `info`, `root` |
| `log` | `repo`, one of `branch`, `commit`, `tag`; `first_parent`, `limit` | commits, newest first |
| `diff` | `repo`, `a`, `b` | `added`, `removed`, `modified` (sorted lists) |
| `merge_base` | `repo`, `a`, `b` | a commit, or `null` |

**Maintenance and the raw store**

| op | fields | expect |
|---|---|---|
| `gc` | `repo`, `min_age` (default 3600), `deep` (default false) | commits removed |
| `store_set` | `key`, `value` | |
| `store_delete` | `key` | |

`store_set` and `store_delete` write the store directly, behind every
repository's back: damage, leftovers, other writers.

## Recording

`final` comes from running the scenario. To record or refresh it:

```bash
KVGIT_CORPUS_RECORD=1 uv run --extra dev pytest python/tests/test_corpus.py
```

The Python runner then writes each scenario's `final` instead of
checking it; every other expectation is still checked. Only the
`final` block is rewritten, so it must stay the last field. A scenario
Python doesn't run is recorded by the TypeScript runner the same way
(`KVGIT_CORPUS_RECORD=1 pnpm vitest run tests/corpus.test.ts` in
`ts/`). A scenario with no `final` fails in both runners until it is
recorded. Review a
recorded `final` like code: the TypeScript runner holds its package to
those bytes, and a difference between the two packages is a bug in one
of them.
