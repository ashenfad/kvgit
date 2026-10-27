# The request protocol

kvgit's logic lives in a Rust core (`crates/core`) that never touches a
store, a clock or a thread. Each package is a host: Python through a
native extension (`crates/py`), TypeScript through WebAssembly
(`crates/wasm`). A host starts an operation, performs what it asks for,
and hands back the answers until the operation is done. This is that
exchange, the same in both hosts.

## An operation

An operation is started by name with its arguments as one JSON object
(`Op(name, args)`), or by a dedicated constructor for the three that
take host values: a commit, a merge, and a resolve. Before it starts the
host may give it the repository's content cache and the worktree state
it reads from and moves.

The host then resumes it in a loop. Each resume carries the wall-clock
time, in seconds, which the core uses for lease and marker expiry. Each
step is either a request or the end:

```
step = op.resume(wall, None)
while step is a request:
    reply = perform(step)
    step = op.resume(wall, reply)
outcome = step
```

No threads, runtime or waker are involved: an operation makes progress
only inside a resume, so the same code runs under a blocking host and an
asynchronous one.

## Requests

| Request | Asks | Reply |
|---|---|---|
| `get(keys)` | The values of these keys. | The ones the store holds; a missing key is left out. A host answers from bytes it holds for the operation before the store, since they are the same content-keyed bytes. |
| `scan(prefix)` | Every stored key starting with `prefix`. | The keys, in any order. |
| `cas(expect, writes, removes)` | One atomic batch: `writes` and `removes` land only if every key in `expect` holds the bytes given (absent where none). A write either carries its bytes or names bytes the host holds under that key (a value's blob, a chunk), which the core never copies. | Whether it landed. |
| `wait(seconds)` | Sleep that long. | Done. |
| `now` | The time to stamp a commit, tag or sweep cutoff with, from the clock the caller may replace (`Repo(now=...)`). | Seconds, a float. |
| `owner` | A name for a GC lease about to be taken, unique among sweepers. | A string. |
| `merge(asks)` | Run host merge functions: each ask names a function by number, the key, and its three sides by blob key (absent where none). | One answer each: a value (its blob key and size, the bytes held by the host), `ours`, `theirs`, or failed with a reason (a conflict). |
| `check(values)` | The caller's post-check over values a merge made: each key, the value's blob key, and its bytes where the core made them. | The keys whose values it refuses. |
| `recover(branch)` | A last-resort guess at a branch's head, for a head that is present but unusable and has no usable backup. | A commit, or none. |
| `emit(part)` | Part of the answer, ready before the rest. | Next, when the caller wants more; a host that wants no more drops the operation. |

A host that can't perform a request (its store raised, a merge function
raised something that isn't a conflict) answers *failed* and keeps the
reason. The operation unwinds, undoing what it can, and ends with an
error; the host raises the reason it kept instead.

## The end

An operation ends with its outcome and any notes it made:

- **ok**, with a JSON value (a commit or merge answers with what it
  committed: whether it landed, the commit, the strategy, the keys a
  rule decided and the keys it carried, and the roots and generations it
  learned);
- **error**, with a kind, a message and the kind's fields. The kinds:
  `unknown_branch`, `unknown_commit`, `unknown_tag`, `branch_exists`,
  `tag_exists`, `branch_moved`, `corrupt_head`, `concurrency`,
  `merge_conflict` (with the keys, and each failed function's reason),
  `gc_busy`, `storage_version`, `integrity`, `malformed`, `invalid`, and
  `host` (a failed request). Each host maps a kind to its own error
  class.

Notes are warnings for the host to log: a head recovered from its
backup or by the recoverer, a recoverer's answer that named no commit, a
head healed, a sweep that ran past its lease.

## Operations

By name, each taking a JSON object and answering JSON:

- **Heads.** `resolve_head`, `head_of`, `repair_head`, `heal_head`,
  `latest_head`, `recover_by_commit_scan`.
- **Refs.** `branches`, `branch_exists`, `create_branch`,
  `delete_branch`, `reset_branch`, `resolve_tag`, `tags`, `tag_info`,
  `create_tag`, `delete_tag`, `resolve_ref`.
- **History.** `get_commit`, `log` (emits records, newest first),
  `diff`, `merge_base`, `ancestry_delta`, `is_ancestor`,
  `ancestors_above`.
- **The store.** `check_version`, `gc`.
- **A worktree.** `wt_lookup`, `wt_contains`, `wt_keys`, `wt_move`,
  `wt_checkout`, on the worktree state the operation is given.
- **Sync.** `sync_head`, `claim_sync_head`, `set_sync_head`,
  `clear_sync_head`, `mark_syncing`, `has_sync_head`,
  `delete_branch_unless_synced`, `publish_head`, `pull_plan`,
  `pull_finish`, `push_start`, `push_plan`, `wire_walk` (emits wire
  commits), `wire_replay`.

And by constructor: **commit** (a worktree's changes, landed and
published, merging onto a branch that moved), **merge** (a branch merge,
or with a base, an applied change), and **resolve** (a three-way resolve
over values the host names, writing nothing).

Each operation's arguments and answer are listed where it is defined,
in `crates/core/src/ops.rs`.

## Merge rules

A commit, merge or resolve takes merge rules by key, by key prefix and
as a default. A rule is `"ours"`, `"theirs"`, the text merge (with its
labels and strictness), or the number of a host merge function, which
the operation asks for with a `merge` request. The core picks a key's
rule (exact key, else the longest prefix, else the default) and decides
everything a side's rule covers without reading values.

## Encoding

Arguments and answers are JSON. Values the host must keep exact cross
as the core writes them: a commit's time and info keep their floats (the
TypeScript host writes them through its Python-compatible encoder, and
reads a record's text back with it), and integers past 2^53 are read
exactly. Bytes cross out of band: the host keeps the bytes of every
value it hands in, under its blob key, and the core names them.
