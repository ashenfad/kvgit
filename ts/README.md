# @agex-ts/kvgit

Versioned key-value store with branches, commits, and three-way merges. The TypeScript counterpart of Python's [kvgit](https://github.com/ashenfad/kvgit), on its v4 format byte for byte, redesigned around async storage so it works equally well in Node and the browser.

> **Status:** Pre-alpha. Public API is unstable; pin a specific minor version.

## Install

```bash
npm install @agex-ts/kvgit
```

The `'cbor'` codec runs on [cbor-x](https://github.com/kriszyp/cbor-x).
Its optional native decoder, `cbor-extract`, has a build script, and
nothing needs it: cbor-x falls back to its pure-JS path (the one
browsers use). npm and yarn need nothing. pnpm 11 stops the install on
an unapproved build script (`ERR_PNPM_IGNORED_BUILDS`), so decline it
in `pnpm-workspace.yaml`:

```yaml
allowBuilds:
  cbor-extract: false
```

## Concept

A `Map<string, T>` with git-like history. Every `commit()` creates a checkpoint; branches are first-class; sessions can fork cheaply because the underlying HAMT shares structure across versions. Three-way merges are pluggable per key.

## Design

The canonical type contracts live in [`src/types.ts`](./src/types.ts). `@agex-ts/world` keeps each session as a branch of one repository; [the design history](../../docs/design.md) has why the format follows Python's.

## Storage backends

A repository sits on a `KVStore`: get, set and remove of bytes, prefix
scans, and an atomic compare-and-swap of one key (`cas`) or of several
(`casMany`), which every head move and GC lease rides on. Three ship:

| Backend | Import | |
|---|---|---|
| `Memory` | `@agex-ts/kvgit/backends/memory` | `new Memory()`: tests, scratch |
| `IndexedDB` | `@agex-ts/kvgit/backends/idb` | `await IndexedDB.open({ dbName, storeName })` in the browser; `IndexedDB.deleteDatabase(name)` removes one |
| `Sqlite` | `@agex-ts/kvgit/backends/sqlite` | `Sqlite.open({ path, wal })` on Node's `node:sqlite` (`:memory:` by default, WAL on for a file); refused in a browser build |

## Repo and worktrees

The API follows Python kvgit 0.4's, made async:

```ts
import { Repo, jsonCodec } from '@agex-ts/kvgit'
import { Memory } from '@agex-ts/kvgit/backends/memory'

const repo = await Repo.open(new Memory(), { codec: jsonCodec })
const wt = await repo.worktree('main', { create: true })
wt.set('greeting', { text: 'hello' }) // pending until commit
await wt.commit({ info: { title: 'first' } })

await repo.tags.create('v1', wt.head)
const snap = await repo.snapshot({ tag: 'v1' })
await snap.get('greeting') // { text: 'hello' }
for await (const c of repo.log({ branch: 'main' })) console.log(c.hash, c.info)
```

- **`Repo`** owns the store and the settings every worktree shares: the
  codec (see "Codecs" below), default merge
  functions, HEAD recovery and the clock. It reads commit records
  (`getCommit`, `log`, `diff`, `mergeBase`), makes snapshots, repairs
  HEADs and runs `gc`.
- **`Worktree`** is one branch checked out for work: writes stay
  pending until `commit()`, reads see them, and a commit that loses the
  race to another worktree on the same branch merges onto the new tip
  with the effective merge functions (the call's, then the
  worktree's, then the repository's).
- **Refusing to merge.** Where keys are not independent (pages of one
  database, say), a merge of two disjoint changes is a state neither
  writer made. `commit({ onMoved: 'raise' })` lands only as a
  fast-forward from the worktree's head: if the branch moved, before
  the commit or inside the compare-and-swap that publishes it, it
  throws `BranchMovedError` (with nothing to commit too) (a `ConcurrencyError`; `err.head` is the
  tip it found), writes nothing reachable and keeps the pending
  changes. `refresh()` then moves to the tip to redo the work.
- **Lazy worktrees.** `repo.worktree(name, { lazy: true })` leaves the
  commit's keyset in its tree and reads it on demand, for branches of
  tens of thousands of keys and up. Opening reads one record whatever
  the size; reads walk the tree through a bounded cache of tree nodes
  (`repo.nodeCache`, 16 MB, shared by every lazy worktree on the repo,
  or `{ lazy: { cache: new NodeCache({ maxBytes }) } }`); a commit
  rewrites only the paths it changes. Everything else behaves as in an
  eager worktree, and both write the same commits. A lazy worktree
  keeps no decoded values, and `keys()` reads the whole tree on every
  call.
- **`Snapshot`** is a read-only view pinned to one commit. It looks
  keys up in the tree a batch at a time, so reading a few keys stays
  cheap however many the commit holds; `raw` gives the stored bytes.
- **`repo.branches`** and **`repo.tags`** are live collections. A tag
  never moves, and keeps its commit and its ancestry alive through
  `gc`. Tags are stored as Python kvgit stores them (a branch head under
  `refs/tags/`, plus a record of when and why), so either reads the
  other's.
- **Merging.** `wt.merge({ branch | commit | tag })` fast-forwards when
  this branch hasn't moved since the two forked (as git does;
  `fastForward: false` writes a merge commit anyway), is a no-op for a
  commit the branch already contains, and otherwise writes a two-parent
  merge commit `[ours, theirs]`. `wt.apply(base, target)` lands the
  change from `base` to `target` as one ordinary commit, with
  `cherryPick(commit)` and `revert(commit)` built on it.
- **Merge rules** come from the repository, the worktree and the call,
  in that order, each by key (`mergeFns`), by key prefix
  (`mergePrefixes`, longest match wins) or as a default. A rule is a
  merge function over decoded values, consulted only where both sides
  changed a key (it may answer with a `MergeChoice` to keep one side),
  or a `MergeChoice` itself: a standing policy giving one side every
  key it covers that either side changed. `text` (`textMerge`, or
  `makeTextMerge({ oursLabel, theirsLabel, strict })`) merges lines as
  git does, conflict markers and all, byte for byte with Python's
  `kvgit.merges`. A `merge`, `apply`, `cherryPick` or `revert` also
  takes `postCheck`, which can refuse a merged value as a conflict.
- The common ancestor is found by generation: commits record their
  parents' generations, so the search for two tips that forked
  recently reads a few records whatever the history below.
- Errors derive from `KvgitError`: `UnknownBranchError`,
  `UnknownTagError`, `UnknownCommitError`, `BranchExistsError`,
  `TagExistsError`, `CorruptHeadError`, `StorageVersionError`,
  `MergeConflict`, `GcBusy`, `ConcurrencyError` and its subclass
  `BranchMovedError`.

`Worktree` and `Repo` sit on `VersionedKV`, which stays available (as
does `Staged`) for the code that still uses it.

## Codecs

A codec turns values into stored bytes and back. It is a setting of the
`Repo` (`Repo.open(store, { codec })`), not of the store, so every
reader of a store has to use the codec its writers did. Pass a codec
(`{ encode, decode }`) or one of three names:

- **`'bytes'`** (the default): values are `Uint8Array`, stored as they
  are, so any reader in any language can take them.
- **`'json'`**: anything `JSON.stringify` writes and `JSON.parse` reads
  back the same.
- **`'cbor'`**: the polymorphic one, as `pickle` is in Python kvgit, but
  a standard ([RFC 8949](https://www.rfc-editor.org/rfc/rfc8949)).
  Values round-trip with their JS types: objects, arrays, strings,
  numbers, booleans, `null` and `undefined`, `Uint8Array` (a byte
  string, no base64), bigint, `Map` (any keys), `Set`, `Date` and the
  other typed arrays. Python reads it with `cbor2`: a `Map` arrives as a
  tagged map, and an integer Python writes past 32 bits comes back a
  bigint ([`tests/fixtures/golden-cbor.json`](./tests/fixtures/golden-cbor.json),
  from [`scripts/golden-cbor.py`](./scripts/golden-cbor.py), checks what
  TS reads from Python).

## Storage format

Stores use kvgit storage v4, the format Python kvgit 0.4 writes, byte
for byte: a store either one writes, the other reads, and a commit
either one makes gets the same hash in both. Blobs are keyed by the
SHA-256 of their bytes (`kvgit:blob:<sha256>`), HAMT nodes by their
entries, and a commit hash covers its parents, keyset root, time and
info. Records are JSON as Python's `json.dumps` writes it, which
[`src/pyjson.ts`](./src/pyjson.ts) reproduces.

Two consequences for callers:

- **Commit info is hashed as Python would write it.** JS has one
  number type, so a whole number in `info` is written as an integer.
  Wrap a value that must be a float in `PyFloat` (`new PyFloat(2)`
  writes `2.0`).
- **Commit times are seconds**, as a float. `Repo.open(store, { now })`
  takes a clock (a function returning seconds) for deterministic times;
  it defaults to the wall clock.

The golden test ([`tests/golden-v4.test.ts`](./tests/golden-v4.test.ts))
replays a scripted history and compares every key and byte against
what Python kvgit wrote for it
([`scripts/golden-v4.py`](./scripts/golden-v4.py)).

## Garbage collection

Resetting a branch, deleting one, or losing a race leaves commits
nothing points at. `repo.gc({ minAge, deep })` (or `vk.cleanOrphans` /
`vk.deepClean` on an engine handle, whose `minAge` is in milliseconds)
reclaims them, with everything only
they held: records, blobs and HAMT nodes.

**It is safe beside concurrent writers,** as in Python kvgit:

- A sweep runs under the store's GC lease (`__gc_lease__`, a record
  with an owner and an expiry). Every write that adds something a sweep
  must see (a commit's batch, a head, a tag, a synced commit) is
  conditional on the lease record its writer read after waiting any
  live sweep out, so it lands before a sweep starts or after it ends,
  never during. A writer that starts mid-sweep waits, and then lands
  intact, even holding content an orphan held.
- A commit's batch carries an in-flight marker (`__inflight__<commit>`)
  that the write publishing its head removes, so a sweep that starts
  between the two keeps it. A marker lapses after ten minutes, which is
  what eventually releases the commit of a writer that died before
  publishing.
- `minAge` (seconds for `repo.gc`, default an hour) is policy only: how
  long abandoned work lingers. Any value is safe, 0 included.
- Lease and marker expiries are always wall-clock time: they coordinate
  writers in different handles and processes. A `now` option pins only
  commit and tag times and the `minAge` cutoff.
- The lease expires (`leaseTtl`, default ten minutes), so a sweep that
  dies holding it delays writers by the rest of its term and no longer.
  A second sweep waits for the first, or with `wait: false` throws
  `GcBusy` (`deepClean` doesn't wait).

`deep` also scans the `kvgit:keyset:` and `kvgit:blob:` namespaces for
content no commit references at all: leftovers from an interrupted
write, or the blobs of an orphan whose keyset is unreadable, which no
orphan keyset points at.

## HEAD recovery

A branch HEAD lives in one key, `__branch_head__<branch>`, and a backup
of the value it held before its current one lives in
`__branch_head_prev__<branch>`. If HEAD is unreadable — truncated
bytes, a hash whose commit metadata is gone — head resolution falls
back in tiers: the backup first, then the optional
`recoverFromCorruptHead` callback you can pass to `Repo.open` (or
`VersionedKV.open`), then nothing. That third tier is caller-supplied here (kvgit-py's
built-in commit scan is not ported), so out of the box a branch
recovers only as far back as its backup.

A recoverer is not a trusted tier. Its answer is accepted only if it
is a string naming a commit whose `__commit_root__` is present in this
store; anything else is logged with what was returned, and the branch
is reported unrecoverable. Head resolution promises a valid commit or
`null`, and an unchecked answer is worse than no answer — `repairHead`
would make it durable, replacing obviously corrupt HEAD bytes with a
plausible hash that names nothing, on a store whose backup is already
gone.

Two rules govern this.

**Reads never write.** Resolving a damaged branch on a read path —
opening a handle, `peek`, `switchBranch`, `refresh`, the mark phase of
a sweep — recovers in memory and leaves the store exactly as it found
it. The mark phase is the one place that stops at the backup tier even
when the handle carries a recoverer: GC must not decide reachability
from a guess, so a branch whose HEAD does not resolve marks nothing
and keeps its commits as young orphans until `minAge` and an explicit
`repairHead` settle what it points at. A read-only consumer can therefore read a damaged store, two
concurrent readers cannot race each other repairing the same branch to
different answers, and the damage stays visible instead of being
quietly papered over. The cost is that the fallback runs on every read
until someone repairs it.

Two things persist a recovery:

- `repo.repairHead(name)` — or `repairHead(store, branch)`, or
  `vk.repairHead()` — the explicit
  maintenance call, and the one to reach for. It returns the commit
  HEAD names when it returns, which is not necessarily the one it
  recovered: if another writer repaired, advanced or deleted the
  branch in between, the answer describes the store rather than the
  attempt. `null` means the branch does not exist or nothing was
  recoverable.
- A successful write. A CAS against a damaged HEAD always fails, which
  would leave the branch permanently unwritable, so a writer that
  finds HEAD unresolvable replaces it with the recovered commit and
  retries once. The replacement is itself a CAS against the exact
  damaged bytes, so two writers racing it cannot both win; a HEAD that
  merely *moved* — an ordinary lost race — is never touched, and a
  HEAD that is *absent* is never recreated, since that is the
  deleted-branch resurrection `deleteBranch` drops the backup to
  prevent.

**The backup is exactly the HEAD the swap replaced.** A HEAD swap
writes `__branch_head__` and `__branch_head_prev__` in one atomic
`casMany` (a compare-and-swap across keys, which every backend
provides), conditional on HEAD still holding the value the writer
read. A writer that loses the race writes neither, so the backup never
names a commit HEAD did not hold, and no crash, concurrent writer or
delete can land between the swap and the backup.

## Sync & remotes

kvgit histories can be replicated between stores (cross-device session
sync, bundles). The durable design decisions, in case you're extending
this layer:

- **Sync is replication, not storage.** A remote is *not* a `KVStore`
  backend — `KVStore` is the near-storage boundary (HAMT walks assume
  cheap reads), while a remote is a far peer that exchanges history.
  Don't implement a remote by putting a slow store behind `KVStore`;
  implement the wire layer.
- **The unit of transfer is the commit, in wire form** (`WireCommit`
  in [`src/types.ts`](./src/types.ts)): parents and their
  generations, time, info, changed value bytes, removals. Produced by `walkDelta(store, { want, have })`
  ([`src/sync/walk.ts`](./src/sync/walk.ts)) — commits reachable from
  `want` but not `have`, parents-first. `have = ∅` is a full export
  (bundles are the degenerate case). Replayed by
  `applyWire(store, commits)` ([`src/sync/apply.ts`](./src/sync/apply.ts)),
  which recomputes every commit hash from the replayed state and
  refuses mismatches before writing.
- **HAMT nodes never cross the wire.** A receiver rebuilds each
  commit's keyset over its first parent's, keying each changed value's
  blob by its bytes, so the rebuilt root is the sender's; it then
  recomputes the commit hash from that root, the time and the info
  (see `commitHash` in
  [`src/versioned/layout.ts`](./src/versioned/layout.ts)) and refuses
  a mismatch. A merge commit's delta lists every key that differs from
  its first parent, the ones it took from the other side included, so
  replay never consults a second parent. The one commit not checked
  this way is the root commit, whose hash is fixed: it is checked by
  its shape (no parents, no info, an empty keyset). See the fidelity
  tests in [`tests/sync-walk.test.ts`](./tests/sync-walk.test.ts) and
  the refusal tests in
  [`tests/sync-apply.test.ts`](./tests/sync-apply.test.ts).
- **Merge never crosses the wire either.** Remotes move objects and
  CAS refs; reconciliation always happens locally via the existing
  three-way machinery (`commit()` / merge fns). Remotes can therefore
  be *passive* — any object store with compare-and-swap on refs
  qualifies; no kvgit code needs to run on the far side. The protocol
  is `Remote` (`listRefs` / `fetch` / `push`, and optionally
  `deleteBranch`, by the same compare-and-swap) in
  [`src/sync/remote.ts`](./src/sync/remote.ts), with `MemoryRemote`
  as the reference implementation.
- **Sync is fast-forward only.** `pullBranch` / `pushBranch` /
  `syncBranch` ([`src/sync/sync.ts`](./src/sync/sync.ts)) move refs
  only along their own ancestry and report `'diverged'` otherwise —
  objects may transfer on divergence (useful for a later merge), but
  no ref moves and nothing merges automatically. A vanished
  previously-synced remote ref reports `'remote-gone'` (lifecycle
  conflict) for the caller to resolve.
- **A remote branch is deleted only if nobody pushed to it since.**
  `deleteRemoteBranch` deletes it at the head it last synced to here
  (or a head the caller names) and reports `'moved'`, deleting
  nothing, when someone has pushed since; once it's gone, the record
  of where it synced goes too, so a later push of that name creates it
  afresh.
- **A sync costs the size of the change, not of the history.** Each
  commit stores its parents' generations, so the questions a sync asks
  (what one tip has that another lacks, whether one commit is
  another's ancestor) are answered by searching down from both,
  highest generation first, stopping where the two histories meet
  ([`src/versioned/ancestry.ts`](./src/versioned/ancestry.ts)). History
  written before generations were stored is searched whole.
- **Parent order is significant.** A commit that merges after losing
  its race has `parents[0]` "theirs" (the head that won) and
  `parents[1]` "ours"; a branch merge has `[ours, theirs]`, so linear
  history stays on the merging branch. Wire deltas diff against
  `parents[0]`.
- Remote-tracking state (which remote commit a branch was last synced
  to) is kept per remote: a `Remote` with an `id` (a `PackRemote`'s is
  `github:<owner>/<name>`, lowered, with the API's root before the repo
  when it isn't github.com's) keeps its own record per branch, so one
  branch synced with two remotes, or with a remote swapped for another,
  never takes one's head for the other's. Remotes without an `id` share
  one record per branch, `__sync_head__<branch>`, which is also where
  stores synced by earlier versions kept it: the first remote with an
  `id` to sync the branch takes that record up as its own. Read and
  write it with `getSyncHead` / `setSyncHead` / `clearSyncHead` in
  [`src/sync/apply.ts`](./src/sync/apply.ts), naming the remote;
  without one, `getSyncHead` answers with the branch's only sync head
  and `clearSyncHead` forgets them all. A sync also marks the branch
  (`__sync_mark__<branch>`) before a push reads it, and the mark goes
  with its last head. `hasSyncHead` says whether a branch has a head
  with any remote or the mark, and `deleteBranchUnlessSynced` deletes a
  branch only if it has neither, in one step against a push taking it
  up meanwhile.

The storage layout and commit-identity primitives live in
[`src/versioned/layout.ts`](./src/versioned/layout.ts) — the single
source of truth shared by `VersionedKV` and the sync layer. If you
change the layout or hash inputs, both move together and the storage
version sentinel must bump.

### Pack sync over GitHub (`@agex-ts/kvgit/github`)

`PackRemote` syncs through a plain GitHub repo, with no backend and no
git protocol (GitHub's smart-HTTP endpoints don't send CORS headers):
GraphQL and one REST read, on a token that lives in the browser. A
fine-grained token scoped to one sync repo, with **Contents: read and
write**, is enough. A push is one request whatever the number of
commits (up to `maxPackBytes`, below), a pull is one query plus one
read per pack, and the session list with each session's head and
metadata is one query per hundred branches. Both sync calls take the
repository's raw store, `repo.store`, as `kv` below.

```ts
import { GithubClient, PackRemote } from '@agex-ts/kvgit/github'
import { pushBranch, pullBranch } from '@agex-ts/kvgit'

const client = new GithubClient({ token, repo: 'me/agex-sync' })
const remote = new PackRemote(client, kv, { meta: (branch, head) => ({ title }) })
await pushBranch(kv, remote, 'chat-1') // fast-forward only: 'pushed', 'diverged', …
await remote.roster() // every session: head, meta, packs, archived
```

- **`GithubClient`**: GraphQL (`graphql`), raw blob reads by SHA
  (`getBlobRaw`) and REST (`request`), scoped to one repo. Mutations
  run one at a time, spaced by `writeIntervalMs`; `server`,
  `rate-limit` and network failures retry with backoff, honoring
  `Retry-After`, and `auth`, `permission` and `validation` don't (see
  `GithubError`).
- **`PackRemote`**: the `Remote`. `push` lands one pack per request,
  `fetch` reads the packs a receiver lacks, `roster` lists every
  session, live and archived. `archiveBranch`, `restoreBranch`,
  `deleteBranch` and `emptyTrash` are one atomic, CAS-checked
  `updateRefs` each. `repack`
  folds a session's packs into one.
- **Lost races are results, not throws:** a push whose expected head
  moved returns false (the sync layer reports `'diverged'`), with
  nothing written.

#### The format

Each session is one git branch; archived ones (the trash) are
`archived/<session>`. A branch holds two kinds of file:

- **`kvgit/manifest.json`**, JSON: `{format: 1, kernel, branch, head,
  packs, nextSeq, meta}`. `head` is the session's kvgit head; `meta` is
  what the pusher's `meta(branch, head)` gave (a title, say); `packs`
  lists the packs oldest first, each `{path, seq, base, tip, commits,
  bytes, sha256, blob}`: the kvgit commit its delta builds on (`base`,
  null from the root), its last commit (`tip`), and the stored file's
  size, SHA-256 and git blob SHA-1 (what a fetch reads it by). Each
  pack's `base` is the previous pack's `tip`. A manifest that isn't
  JSON, or has another `format`, makes the branch unusable here: it is
  never pushed over.
- **`kvgit/packs/<seq>-<tip12>.pack`** (`<seq>` six digits, zero-padded): gzip of CBOR `{format: 1,
  kernel, base, tip, values, commits}`. `values` is a table of distinct
  byte strings (each value once, ordered by key, so versions of a file
  compress against each other); `commits` is parents first, each
  `[record, updates]`, where `record` is the commit as kvgit hashes it
  (Python-style JSON bytes of `{hash, parents, parentGens, time,
  info, removals}`, `time` a float) and `updates` is `[key,
  valueIndex]` pairs. A receiver recomputes every hash (`applyWire`).

Each push is one git commit on the session's branch, headlined `kvgit
<session> @ <head7> (+N)` with `Kvgit-Head: <head>` and
`Kvgit-Pack-Format: 1` trailers, made by
`createCommitOnBranch` with `expectedHeadOid` as the compare-and-swap.
A new session's branch starts from the default branch's commit, its
`createRef` in the same request. A fetch walks the manifest's packs
from the newest back to one whose `base` the receiver has, and reads
them by blob SHA (`GET git/blobs/<sha>`, raw: the contents API alters
bytes it takes for text).

#### Limits

- A push larger than `maxPackBytes` (16 MiB of values) lands as
  several packs, each its own compare-and-swap. A single commit too
  large for one request (about 22 MB compressed) is refused before
  anything is sent.
- Past `maxPacks` (16), a push also folds older packs into one,
  re-encoded from the local store, in the same request: of all but the
  newest `keepRecent` (4), the newest run (two or more) that fits in
  `maxPackBytes`. A fold that wouldn't fit in the push's request waits
  for a later push.
- Packs are opaque to git, so a repo grows with what is pushed (a
  small multiple of each session's size, with folding).
- Live tests ([`tests/github-pack-live.test.ts`](./tests/github-pack-live.test.ts))
  run against a scratch repo when `KVGIT_GH_TOKEN` and `KVGIT_GH_REPO`
  are set.

#### A fake GitHub, for tests

`FakeGithub` (`@agex-ts/kvgit/github/fake`) is an in-memory stand-in
for the slice of GitHub `PackRemote` uses: its GraphQL documents and
raw blob reads. Use it in-process (`fake.client()`, or `fake.fetch` as
a client's `fetchImpl`), or over HTTP with `serveFakeGithub`
(`@agex-ts/kvgit/github/fake-server`, Node), whose URL is a client's
`apiBase`. The server answers any origin's cross-origin requests, so a
page in a browser can sync against it, and two browser profiles can
play two devices. `fake.failNext({ status, body, headers, path })`
answers the next requests with a failure (a rate limit, a server
error); `maxBodyBytes` refuses larger requests with 413, standing in
for GitHub's undocumented size limit.

```sh
pnpm -F @agex-ts/kvgit build
pnpm -F @agex-ts/kvgit fake-github -- --port 4180 --repo me/sync
```

It keeps everything in memory, gone when it stops; its commit ids
aren't git's.
