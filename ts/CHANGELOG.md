# @agex-ts/kvgit

From 0.5.0, both packages are recorded in the repository's
[CHANGELOG](https://github.com/ashenfad/kvgit/blob/main/CHANGELOG.md).
The entries below are the package's releases from agex-ts.

## 0.2.1

### Patch Changes

- 8a29999: **`IndexedDB.open` adds a `storeName` the database lacks:** opening a second store in an existing database left it uncreated, so the first read or write threw `NotFoundError`; the open now bumps the database's version to create it, waiting up to the new `blockedMs` option (default 5 s) for other connections to the database to close.
- ee3020b: **A keyset entry written before storage v4 keeps its `created_at`:** decoding dropped it, so re-encoding an untouched entry in a merge or a rewritten node gave different bytes, tree and commit hash than Python kvgit.
- e3b247f: **`keys(prefix)` and `items(prefix)` find every key under the prefix on SQLite and IndexedDB:** the scan stopped at the prefix plus U+FFFF, so SQLite missed keys with a character outside the BMP after the prefix (an emoji, say), and both missed keys with U+FFFF there; the scan now runs up to the least string past the prefix, and is open above when there is none.
- 83c2b5b: **Diffs and merges handle keys named like `Object.prototype` members:** a diff read a leaf's inherited members as keys, so removing a key such as `toString` threw instead of reporting it removed, and adding one such as `hasOwnProperty` went unreported.

## 0.2.0

### Minor Changes

- cbaa8ea: Add `cborCodec`, the polymorphic codec: values round-trip with their JS types (objects, arrays, `Uint8Array` as byte strings, bigint, `Map`, `Set`, `Date`, typed arrays, `undefined`) in standard CBOR that Python's `cbor2` reads. `Repo.open` takes a codec or its name (`'bytes'`, `'json'`, `'cbor'`); `resolveCodec` maps a name to its codec. Depends on `cbor-x`; its optional native decoder is not built. `Repo.open` types its values by the codec (`Uint8Array` for bytes, `unknown` or the caller's type for a named JSON or CBOR codec), and the `Memory` backend now copies values in and out, so mutating an array read from or written to the store can't change a commit.
- 3c938b2: **`commit({ onMoved: 'raise' })`.** A commit whose branch moved since the worktree's head throws the new `BranchMovedError` (a `ConcurrencyError` carrying the `head` it found) instead of merging (with nothing to commit too), writes nothing and keeps the pending changes; the default, `'merge'`, is unchanged.
- f72a259: **Delete a remote branch.** `Remote.deleteBranch` (optional; `MemoryRemote` and `PackRemote` have it) deletes a ref only at the head expected, and `deleteRemoteBranch` deletes one only if nobody pushed to it since it last synced, reporting `'moved'` otherwise.

  **Syncs cost the change, not the history.** Pushes, pulls, pack fetches and folds search down by generation from both tips and stop where they meet, rather than walking the whole history below; `ancestryDelta` and `isAncestor` are exported.

- 7de1515: **A fake GitHub for tests:** `FakeGithub` (`@agex-ts/kvgit/github/fake`) stands in for the GitHub API `PackRemote` uses, in-process or over HTTP with `serveFakeGithub` (`@agex-ts/kvgit/github/fake-server`), which answers cross-origin requests so a browser can sync against it; failures and a request-size limit can be injected. `pnpm -F @agex-ts/kvgit fake-github` runs one.
- 36a2042: Garbage collection is safe beside concurrent writers, as in Python kvgit: a sweep runs under the store's GC lease, every commit batch, head, tag and synced commit lands only while no lease is live, and a commit's batch carries an in-flight marker until its head is published. `repo.gc` takes `wait` and `leaseTtl`, a busy lease throws the new `GcBusy` (`deepClean` doesn't wait), and `minAge` is policy only. `applyWire` marks the commits it writes (`inFlight` in its result) and `publishHead` moves a head and drops their markers in one write; `pullBranch` and `MemoryRemote.push` use it, so a pull's head and its backup now land together. Lease and marker expiries use the wall clock whatever `now` a handle is given.
- 1494f5c: **Lazy worktrees.** `repo.worktree(name, { lazy: true })` opens without loading the commit's keyset and reads it on demand through a bounded node cache, so opening a branch of a million keys takes under a millisecond instead of seconds, and holds no memory for keys it hasn't read. Commits write the same tree as an eager worktree.
- f65aaec: Add Python kvgit 0.4's merges: `Worktree.merge` (fast-forward by default, `fastForward: false` for a merge commit, a no-op for a commit already contained), `apply`, `cherryPick` and `revert`, with `postCheck`. Merge rules may be a `MergeChoice` (a standing policy for one side) as well as a function, which may itself answer with a `MergeChoice`, and may be registered by key prefix. The common ancestor is found by generation, falling back to a full walk. Breaking: `onConflict: 'skip'` is now `'abandon'`, `resolveMerge` takes each side's changes and returns changes to ours, a commit that fast-forwards reports no carried keys, and `mergeHeads` fast-forwards when it can. Merge results match Python's byte for byte.
- d7a3f69: Breaking: sessions sync over GitHub as one pack per push. `PackRemote` (in `@agex-ts/kvgit/github`) lands each push as one gzip-CBOR pack with one GraphQL `createCommitOnBranch`, its expected head the compare-and-swap; a pull reads only the packs the receiver lacks, by blob SHA; `roster()` lists every session, live and archived, with its head and metadata in one query; archive, restore and emptying the trash are one atomic `updateRefs` each; older packs fold into one past `maxPacks`, and `repack` folds them all. The format is specified in the README. `GithubRemote`, its sidecars and the Git Data methods of `GithubClient` are removed: it could not pull a world session, since the contents API alters values it takes for text. `GithubClient` gains `graphql` and `getBlobRaw`.
- 1cfdbf5: Add the Python kvgit 0.4 API: `Repo` (commit records, `log`, `diff`, `mergeBase`, `gc`), `Worktree` (pending writes, merging commit), lazy `Snapshot`, and `branches` and `tags` collections, with tags stored as Python stores them. Errors derive from `KvgitError`, with new `UnknownCommitError`, `UnknownTagError`, `BranchExistsError`, `TagExistsError`, `CorruptHeadError` and `StorageVersionError`. Empty branch names and names under `refs/tags/` are refused, deleting a branch or tag is conditional on the head it read (a `ConcurrencyError` if it changed), `resetTo` swaps HEAD and its backup in one write, and `VersionedKV` and `Staged` stay as they are.
- 3f0b814: **`SequenceMatcher`** (Python difflib's opcode matcher, used by the marker merge) is now exported, for callers that need the same line diffs.
- b382ec7: Breaking: storage v4, byte-compatible with Python kvgit 0.4. Blobs and HAMT nodes are keyed by content, commit hashes cover parents, keyset root, time (seconds, a float) and info, and records are JSON as Python writes it (`PyFloat` marks a float in commit info). A HEAD swap and its backup land in one atomic `casMany`, new on every backend. `WireCommit` drops `meta` and `carries` and gains `parentGens`. `cleanOrphans` now needs a quiescent store, and `deepClean` also reclaims unreferenced blobs. Older stores are refused.
- 231a95e: **Sync heads per remote:** a branch's last-synced head is kept per `Remote.id` (a `PackRemote`'s is its repo), so one branch synced with two remotes never takes one's head for the other's.
- 326eedf: Breaking: Node 22 or later is required (Node 20 is past its end of life, and the AI SDK's packages need 22).

### Patch Changes

- 6115187: **A commit costs what it changes.** A handle updates its in-memory keyset in place instead of copying it, so a one-key commit on 100,000 keys takes 0.2 ms rather than 27.

  **A commit that fails partway leaves the handle as it found it.** An error after a lost race, such as a failed landing write or store read, used to leave the handle on the unpublished commit.

  **A handle's commits, merges and refreshes run one at a time.** Concurrent calls on one `VersionedKV` used to interleave and corrupt its in-memory state.

- f154ccc: **Values are stored at their own size:** the IndexedDB backend copies a view out of its larger buffer before storing it, `cborCodec.encode` returns an exact buffer rather than a view into cbor-x's scratch, and `Memory` copies Node `Buffer`s out instead of keeping a view, so a browser store no longer takes about four times its size on disk.
- 7ab2502: **`IndexedDB.deleteDatabase` waits out a closing connection:** it no longer rejects on the first `blocked` event, which IndexedDB fires even for a connection closed just before while its last transaction finishes; a connection still open after `blockedMs` (default 5 s) rejects as before.
- 62727b0: **Keys named like `Object.prototype` members work.** A `get` of an absent key such as `toString` or `constructor` returned the inherited member instead of nothing, and a key named `__proto__` could be dropped when a leaf was split or merged.
- bd9900f: **A branch syncing is marked:** a push marks it before reading it, `hasSyncHead` says whether a branch has a sync head or the mark, and `deleteBranchUnlessSynced` deletes one only without either, in one step against a push taking it up. `pullBranch` no longer claims a sync head whose commit a sweep took, so a branch deleted here pulls afresh.
- 9b4cfef: Deleting a tag is also conditional on its `__tag_info__` record, so a stale delete can't remove a tag deleted and made again at the same commit (the new record carries the time it landed). A branch made again at the same commit stays indistinguishable to a stale delete, as with git, and is now documented.

## 0.1.3

### Patch Changes

- d660ff6: Validate what an injected `recoverFromCorruptHead` returns, and thread the recoverer through `peek`.

  The last-resort recovery tier is caller-supplied, so its answer is no more trustworthy than anything else read out of a damaged store — but it was returned unchecked, while the current-HEAD and prev-HEAD tiers both verify that what they read is a string naming a commit whose `__commit_root__` is present. A recoverer that guessed wrong therefore handed back a hash naming nothing, and `repairHead` then made it durable: obviously corrupt HEAD bytes replaced with a plausible hash pointing at no commit, which is harder to diagnose than the damage it replaced, on a store whose prev-HEAD backup is already gone by the time this tier runs. The same check now applies to the third tier, and a rejected candidate is logged with what was actually returned instead of being dropped silently. A recoverer naming a real commit is honoured exactly as before.

  `peek` resolved the target branch without the handle's recoverer, so a branch that every other read path on the same handle could recover read back as absent through `peek`. It now passes it like the rest.

  The orphan sweep still resolves branch HEADs without the recoverer, and that is deliberate rather than an omission: GC must not decide reachability from a guess, since a wrong answer marks the wrong commits live and walks a guessed tip as though it were the branch's own history. That is now stated at the call site and pinned by a test.

  No API change: `CorruptHeadRecoverer` keeps its signature. A recoverer that was returning valid commit hashes is unaffected; one that was returning junk now leaves the branch reported unrecoverable rather than papering over it.

- c271f52: Fix two defects in branch-HEAD recovery, and add `repairHead` as the explicit repair path.

  `__branch_head_prev__<branch>` — the backup a damaged branch recovers _to_ — was written before the CAS that moves HEAD, so it landed whether or not the swap did. A writer that lost the race left its own stale `expected` as the branch's recovery target: it clobbered the winner's backup, and where `expected` came from a corrupt-HEAD recovery rather than a real HEAD, it made a commit that was never HEAD durable, so later reads would recover the branch onto a lineage it never had. The backup is now written after the swap succeeds. It still does **not** guarantee a backup exactly one commit back — the swap and the backup write are two steps, and a crash or a lost timeslice between them lets an older writer's backup land last — but it always names a commit HEAD really held. Recovery may lose more than one commit; it cannot invent a lineage. `pullBranch`'s fast-forward had the same ordering and gets the same fix.

  Head resolution no longer writes. It repaired a damaged HEAD in place on every read path — opening a handle, `peek`, `switchBranch`, `refresh`, the mark phase of a sweep — which a read-only consumer cannot do at all, and which let two readers race each other repairing the same branch to different answers. The `repair` option is gone from the internal resolver; recovery is returned to the caller and forgotten.

  Two things persist a recovery instead. The new `repairHead(store, branch)` (and `vk.repairHead()`) is the explicit maintenance call: it resolves as a read would, then writes the answer back with a CAS against the damaged bytes, and returns the commit HEAD names when it returns — re-resolving if that CAS lost, so the answer describes the store rather than the attempt. And the write path heals itself: a CAS against corrupt HEAD bytes always fails, so without repair-on-read a damaged branch would be permanently unwritable. A writer that finds HEAD unresolvable now swaps in the recovered commit — again by CAS against the exact damaged bytes — and retries once. A HEAD that merely moved is left alone, and an absent HEAD is never recreated, since that is the deleted-branch resurrection `deleteBranch` drops the backup to prevent.

  No API breakage: `repair` was never part of the public surface, and `repairHead` is new.

  The prev-HEAD tier is also gated on `__branch_head__` existing. An absent HEAD is not damage — `deleteBranch` removes the key — so a backup that outlives its branch must not bring the branch back. Moving the backup write after the CAS opened a route to exactly that, when a writer descheduled between the two resumes after a concurrent delete and recreates only the backup. `pullBranch`'s fast-forward shares the fix.

- d83f06c: Fix a data-corruption race in `VersionedKV.cleanOrphans`, and add `deepClean` for the reclamation it can no longer do safely.

  The sweep used to find orphaned HAMT nodes by scanning the whole `kvgit:keyset:` namespace for anything the mark phase hadn't reached. Because `store.keys()` is an async iterable, that scan interleaves with the event loop, so a commit landing between the `__commit_root__` scan and the node scan kept its commit metadata but lost its nodes — leaving a live branch HEAD whose keyset root was missing, and whose keys then read back as absent. No second thread or process was needed; a single JS context could hit it, and tabs sharing an IndexedDB or processes sharing a SQLite file widen the window. The same design leaked blobs permanently: once a namespace scan had deleted an orphan's nodes, no later pass could walk that orphan's keyset to find its blobs.

  `cleanOrphans` now collects every deletion candidate by walking an orphaned commit's own keyset, and takes its nodes and blobs in the same walk. Blob keys are `<commitHash>:<userKey>` and HAMT leaves carry that pointer, so node hashes are commit-scoped and never dedup across unrelated commits — which means a commit that lands mid-sweep is in no orphan's tree and cannot become a candidate. The race is closed by construction: no lock, no timestamps, no storage-layout change.

  What the incremental path can no longer reclaim is HAMT nodes that _no_ commit points at — an interrupted write, a crash between a node write and its CAS, damage from an older sweep — since there is no orphan keyset to find them through. The new `VersionedKV.deepClean()` does that with the old full-namespace scan. It is a separate method rather than a flag because it **requires a quiescent store**: its scan runs after the mark phase and will delete artifacts written by a concurrent writer, including ones a live HEAD has since come to depend on. Use `cleanOrphans` for routine cleanup and schedule `deepClean` for maintenance windows.

  No API breakage: `cleanOrphans` keeps its signature, its return value, and its `minAge` semantics.

## 0.1.2

### Patch Changes

- d109967: Speed up initial branch checkout over the GitHub remote. `GithubRemote.fetch` now walks the remote in `GET_CHUNK`-sized windows, fetching each window's sidecars and update blobs concurrently (bounded to `GET_CHUNK` in-flight requests) before yielding its commits in order — instead of one serial round-trip per commit gated behind the consumer's apply. A from-scratch pull of N commits costs roughly `N/GET_CHUNK` request waves rather than N back-to-back round-trips, while peak memory stays at a single window of blobs. Behavior is otherwise unchanged: full history is preserved, hashes are still verified on apply, and transport state still folds across the window seam.

## 0.1.1

### Patch Changes

- 5c222cc: Added the sync layer: kvgit histories can now replicate between stores and sync across devices through a GitHub repo.

  - **Wire form & replication**: `WireCommit` (the commit-shaped unit of transfer), `walkDelta(store, { want, have })` to stream a history delta, and `applyWire(store, commits)` to replay it — with every commit hash recomputed from replayed state and refused on mismatch.
  - **Remote protocol**: the transport-agnostic `Remote` interface (`listRefs` / `fetch` / `push`-with-CAS), `MemoryRemote` reference implementation, and fast-forward-only orchestration via `pullBranch` / `pushBranch` / `syncBranch` (divergence is detected and surfaced, never auto-merged). Remote-tracking heads via `getSyncHead` / `setSyncHead` / `clearSyncHead`.
  - **GitHub transport** (new `@agex-ts/kvgit/github` subpath): `GithubClient` (throttled, retrying Git Data API client with an error taxonomy), `GithubRemote` (each kvgit commit pushed as a real git commit — browsable sessions, incremental deltas, resumable interrupted pushes, transport-state rebuild from remote sidecars), roster lifecycle ops (`archiveBranch` / `restoreBranch` / `deleteForever` / `emptyTrash` / `listArchivedRefs`), and `readKeyAtTip` for reading a single key at a branch tip without materializing the session.
