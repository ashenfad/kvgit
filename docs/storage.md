# Storage format (v4)

A kvgit store is a key-value store of bytes. This is everything kvgit
writes into it. Both packages read and write it byte for byte the same:
a store one writes, the other reads, and a commit either makes gets the
same hash in both. The rules live in the Rust core
(`crates/core/src/layout.rs`, `tree.rs`, `keyset.rs`, `commit.rs`); the
corpus (`corpus/`) pins them across both packages.

## Records

Every record is JSON as Python's `json.dumps(value, separators=(",", ":"))`
writes it: no spaces, ASCII escapes for anything outside ASCII, floats as
Python's `repr` writes them (`2.0`, `1e+16`, `1.7000000000000002`),
object keys in the order given. Where a hash covers JSON, its keys are
sorted (`sort_keys=True`).

## Keys

| Key | Holds |
|---|---|
| `__kvgit_version__` | The storage version stamp, `4`. |
| `__branch_head__<branch>` | The commit a branch's head names, as a JSON string. |
| `__branch_head_prev__<branch>` | The commit the head named before, written in the same batch as the head, so it always names a commit the head really held. |
| `__branch_head__refs/tags/<tag>` | The commit a tag names. A tag is a branch head under a reserved name that the branch API hides, so every kvgit, older ones included, walks it as a root. |
| `__tag_info__<tag>` | `{"time": <float>, "info": <object or null>}`, written with the tag. |
| `__commit_root__<commit>` | The commit's keyset root: a tree node's hash. |
| `__parent_commit__<commit>` | Its parents, a list, in order. The first is the head a merge landed on. |
| `__parent_gens__<commit>` | Each parent's generation, in the same order. Absent on commits written before v4 and on the root commit. |
| `__commit_time__<commit>` | Seconds since the epoch, a float. |
| `__info__<commit>` | The caller's info object, when given. |
| `kvgit:keyset:<hash>` | A tree node, keyed by the SHA-256 of its bytes. |
| `kvgit:blob:<sha256>` | A value's bytes, keyed by the SHA-256 of those bytes. |
| `kvgit:chunk:<hash>` | A chunk of a chunked value (Python's scientific codec). |
| `<commit>:<key>` | A value written before v4, keyed by the commit that wrote it. Read, never written. |
| `__gc_lease__` | `{"owner": <string>, "expires": <float>}`: the lease every sweep runs under. |
| `__inflight__<commit>` | The time (a float) a written, not yet published commit's protection lapses. |
| `__sync_head__<branch>` | The commit a branch last synced to, for remotes without an id. |
| `__sync_head_at__<branch>/<remote>` | The same for the remote with that id, both parts escaped as JavaScript's `encodeURIComponent` escapes them. |
| `__sync_mark__<branch>` | `true` while the branch syncs somewhere. |

## Generations

A commit's generation is 0 for a commit with no parents, else one more
than its highest parent's. A commit stores its parents' generations,
not its own, so a search that reads one record learns where each parent
falls. Searches visit commits highest generation first; history without
stored generations is walked whole.

## Keysets

A commit's keys live in a hash array mapped trie. Each node is JSON:

- a leaf, `{"items":{<key>:<entry>},"kind":"leaf"}`, holding up to 8
  entries;
- a branch, `{"children":{<hex digit>:<node hash>},"kind":"branch"}`,
  sending each key on by the next hex digit of the SHA-256 of the key.

Map keys are written in code-point order. A node is stored under
`kvgit:keyset:` and the SHA-256 of its bytes, so an unchanged subtree is
shared between commits by hash alone. The empty leaf,
`{"items":{},"kind":"leaf"}`, is never written; its hash,
`a01262f23a252767acf71bcd9d1ca8deca5fda1519228657ef7d44990698fd30`, is the
root of an empty keyset.

An entry is the JSON text `[<blob key>,<meta>]`, base64-encoded into the
leaf. The meta's fields are `size` (always), `created_at` (only on
entries written before v4, kept so they re-encode to their stored
bytes) and `chunks` (the chunk keys of a chunked value, only when
there are some), in that order.

## Commits

The root commit every branch starts from is
`821bf06b4dcb406ea508a4a992eadc22f29850cd` in every store: no parents, no
info, an empty keyset.

Any other commit's hash is the first 40 hex digits of the SHA-256 of

```
["kvgit/4", [<parent>, ...], <root>, <time>, <info or null>]
```

as JSON with sorted keys. It covers the parents in order, the keyset
root, the time (a float) and the info, so one hash names exactly one
root, and every `__commit_*__<hash>` key is written once and never
rewritten. Commits written before v4 carry hashes made another way;
they read, and a transfer refuses them.

## Writing a commit

A commit lands in one conditional batch: its blobs, nodes and records,
and an `__inflight__` marker, expecting the GC lease record its writer
read after waiting out any live sweep. Publishing moves the head, writes
the backup and removes the marker in one more batch. A sweep that starts
between the two finds the marker and keeps the commit.

## Sweeping

A sweep takes `__gc_lease__` before it reads anything, marks everything
reachable from the branch heads (tags included), the in-flight markers
and commits younger than its `min_age`, and deletes the rest of what
orphan commits held. Every write a sweep must see expects the lease
record, so none lands while a sweep runs. Lease and marker times are
always wall-clock seconds.

## Versions

A store stamped with any version other than 4, or with branches and no
stamp, is refused, by every handle and every sweep, and nothing is
written to it. A fresh store is stamped 4 when it is opened. Stores
written by kvgit 0.3 read once their stamp is raised to 4, which kvgit
0.4's first commit to a store does; nothing they hold is rewritten.
