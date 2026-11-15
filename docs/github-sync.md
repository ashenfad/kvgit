# GitHub sync past one request (proposal)

Status: accepted 2026-11-15, being built. The decisions, with the
changes they made to the proposal, are at the end. Once built, this
file becomes the GitHub transport's format contract (see "The format
as a contract").

## The problem

`PackRemote` (TypeScript, `ts/src/github/pack.ts`) lands every push as
one GraphQL `createCommitOnBranch` carrying the pack inline as base64.
GitHub drops a GraphQL request whose body hasn't fully arrived after
about 5 s. What one push can carry therefore depends on the uplink:
about 9 MiB at 1.9 MB/s, but only 450 KiB at 90 KB/s. #143 names the
failure (`too-large`) and keeps packs to 2 MiB of values. A push can
only be split at commit boundaries, though, so a single commit can't
shrink. A real session has a 12 MB commit that no ordinary connection
can land through GraphQL.

## What was measured

| Request | Body | Upload | Result |
|---|---|---|---|
| GraphQL query, padded | 600 KiB | 50 KiB/s | 499 at 6.1 s, 313 KB sent |
| GraphQL query, padded | 20 MiB | full (≈20 MB/s) | 200 in 1.6 s |
| REST `POST git/refs`, schema-invalid, padded | 600 KiB | 50 KiB/s | 422 at 12 s, all sent |
| the same | 3 MiB | 150 KiB/s | 422 at 20.7 s |
| the same | 1.2 MiB, 1.6 MiB | 30 KiB/s | 422 at 40 s, 55 s |
| the same | 2 MiB | 30 KiB/s | **401 "Bad credentials"** at 68 s |
| the same | 80 MB | full | 422 in 3 s |
| REST `POST /markdown` (2026-11-13) | | throttled | 200 at 13.5 s |

None of these probes writes anything. GitHub can only reject the
invalid `git/refs` request after reading its whole body, so a 422
shows the body arrived. `POST git/blobs` itself couldn't be probed:
`ashenfad/agex-sync-test` is archived, so every write there gets a
404. The live check of blob uploads waits until it is unarchived.

So the 5 s deadline belongs to GraphQL. REST's git-data routes take a
body for at least 55 s and at least 80 MB. Past about a minute, GitHub
answers 401 "Bad credentials", which today's client would report as a
bad token.

## Proposal

### A push that fits goes as today

A push whose GraphQL body (packs, manifest and document) stays under
`inlineBytes` (default 1 MiB, about 4 s at 2 Mbps) lands as now: one
`createCommitOnBranch`, with `expectedHeadOid` as the compare-and-swap.
Most pushes are a chat turn or a file edit, a few KB, so they stay one
request. If GitHub drops the request at its deadline (`too-large`), the
same push goes the REST way instead, and the remote lowers its own
inline cutoff below the size that was dropped for the rest of its
lifetime, so a slow link doesn't spend 5 s on every medium-sized push.
A dropped body is normally never applied; when it's unclear whether it
was, the landed check below settles it.

### A larger push goes as git objects over REST

1. **Blobs.** Each pack goes up as one or more git blobs by
   `POST git/blobs` (`{content, encoding: "base64"}`), cut into parts of
   at most `partBytes`. The default is 4 MiB of pack, a request of about
   5.6 MB, which arrives within the minute down to about 100 KB/s. Each part's SHA is computed
   locally (`gitBlobSha1`), as fetch already does.
2. **Tree.** `POST git/trees` with `base_tree` set to the branch's
   current tree. Its entries are each new part by SHA, the manifest
   inline (`content`, UTF-8), and `sha: null` for files a fold
   replaces. The roster query gains each ref's `tree { oid }`.
3. **Commit.** `POST git/commits` with `{message, tree, parents:
   [current commit]}` and today's headline and trailers. A creation's
   parent is the default branch's commit, as now.
4. **Ref.** GraphQL `updateRefs` with `beforeOid` set to the current
   commit (zero for a creation) and `afterOid` to the new one. This is
   an exact compare-and-swap, like `expectedHeadOid`. The request is
   tiny, so the deadline doesn't matter.

**Did it land?** After an error, a push re-reads the branch's manifest.
If its `head` (also the commit's `Kvgit-Head` trailer) is this push's
tip, the push landed, whichever request carried it: an `updateRefs`
whose response was lost (the client's retry then fails on its stale
`beforeOid`), or an inline request that failed ambiguously before the
REST fallback ran. Comparing git oids would get the second wrong, since
the ref holds the inline request's commit, not the fallback's. Any
other head means the race was lost, and the push returns false.
`#updateRefs` for archive, restore and delete has no manifest head to
compare, so it keeps its own check: a ref at its `afterOid` landed.
Today it counts any move as a lost race; it gets that rule.

A non-force REST `PATCH git/refs` checks only for a fast-forward. Take
a session that was deleted and then re-created as a bare ref at the
default branch's commit. The push's commit descends from that commit,
so the PATCH would land there; `updateRefs` refuses it.

All of a push's packs go into one git commit under one CAS. That
removes today's partial push ("lost the ref after i of n chunks").
Packs still split at `maxPackBytes` of values, for incremental pulls
and folds, and `maxPackBytes` can go back up to 8 MiB, since parts now
carry the request limit. Only memory limits the size of a commit.

`fetch` keeps reading blobs raw by SHA, now part by part (16 at a time,
as now). It concatenates the parts and checks the SHA-256 before
decoding.

### The REST minute

The client treats a REST request with a large body (64 KiB or more)
that fails 55 s or more in, with a 401 or a network error, as
`too-large`, the way #143 treats GraphQL's 5 s. With 4 MiB parts that
means an uplink under about 100 KB/s. A push that meets it halves
`partBytes` and tries again, down to a floor of 512 KiB, before it
fails. The manifest lists each pack's parts, so readers don't depend on
the part size; a resume after a halving re-uploads, since the parts'
SHAs change.

### Format 2

The manifest becomes `format: 2`. A pack entry keeps `path`, `seq`,
`base`, `tip`, `commits`, `bytes` and `sha256`, and replaces `blob` with
`parts`, the git SHAs of its parts in order. A one-part pack is stored
at `path`, as today. A pack of n parts is stored at `path.0` …
`path.<n-1>`, so a fold can name the files it deletes.

A reader takes format 1 as before: a format-1 entry is a one-part pack
whose part is `blob`. Writers write format 2 from then on. An older
client already treats a manifest with another format as one it can't
use: it leaves the branch alone and never pushes over it. Studio
deploys from `main`, so for a person that window is one reload.

### Resuming a push that stopped partway

A push that stops partway leaves only git objects that nothing
references: a closed tab, a rate limit or a dropped part does that. The
next push re-encodes the same commits to the same bytes. The encoding
is deterministic, checked in Node, and gzip is deterministic within one
engine. So the parts get the same SHAs. Before uploading, one GraphQL
query asks for all of them (`p0: object(oid: …) { oid }`, up to 100 a
query, a read), and the push skips what GitHub already holds. GitHub
doesn't prune unreferenced objects at once, so a resume soon after
finds them; one that finds them gone uploads them again. A git commit
made but never landed is simply made again.

### Secondary rate limits

GitHub documents these limits: 80 content-generating requests a minute
and 500 an hour, 900 points a minute for REST (5 for a POST or PATCH),
2,000 for GraphQL (5 for a mutation), and 100 concurrent requests. This
proposal assumes git object POSTs count as content-generating.

| Push | Writes today | Writes proposed |
|---|---|---|
| A chat turn, a few KB | 1 mutation | 1 mutation |
| One 12 MB commit | can't land | 3 parts + tree + commit + ref = 6 |
| A 300 MB session's first sync | 150 mutations (2 MiB), many past the deadline | 75 parts + 3 = 78 |

Writes stay serialized and spaced at 750 ms, under 80 a minute. The
hourly limit of 500 caps a burst at about 2 GB an hour with 4 MiB
parts.

When a limit is hit, the client follows GitHub's guidance, which it
doesn't today. It waits the whole `Retry-After`. Without one, it waits
until `x-ratelimit-reset` when `x-ratelimit-remaining` is 0, and
otherwise (a secondary limit) at least a minute. It never retries
sooner, since continuing while limited risks a ban. A wait longer than
a minute isn't spent inside one request: the request fails as
`rate-limit`, carrying the time to wait (`retryAfterMs`), so the caller
schedules the next sync then. That sync resumes without resending parts
already up.

## Options

### A. Large values as blobs of their own (dedup)

Values of at least `valueBlobBytes` (say 1 MiB) leave the pack. Each is
stored at `kvgit/values/<sha256>` (in parts, as above), and the pack's
value table holds a reference, `[sha256, size, [part SHAs]]`, in its
place. The sha256 is the value's kvgit blob key (`kvgit:blob:<sha256>`).
That gives:

- **On push**, one existence query covers all of a push's values. A
  value the repo already holds costs a tree entry, not an upload,
  wherever it came from: another session, a fork, an earlier snapshot.
  A fork or compacted copy of a media-heavy session resends only its
  records and small values. Studio's compact fork with about 50 MB of
  media would upload nothing for the media.
- **On pull**, a value the receiver already holds isn't downloaded:
  `fetch` reads `kvgit:blob:<sha256>` from the local store and gives
  the replay those bytes.
- **On delete**, each branch's tree names every value its packs
  reference. A value stays reachable while any branch uses it, so
  deleting one session never strands another's values.

The cost is one write per new value. With a 1 MiB threshold, that is up
to 4× the writes packing would take for values just over the threshold,
and the same for values of 4 MiB or more. Values under the threshold
stay in packs and aren't deduped; studio's are mostly screenshots of a
few hundred KB. Format 2 should allow references in the value table
from the start, so that turning A on later doesn't need a format 3.

### B. A fork reuses its parent's packs

A new branch's first push sends its whole history: `push_plan` gives
an empty `have` for a branch the remote lacks. `PackRemote` could look
in its roster for a session whose packs run from the root up to a tip
that is an ancestor of the new head. It would check ancestry in the
local store, which holds that history. The fork then lists those
entries in its manifest and names their files in its tree (same paths
and SHAs, nothing uploaded), and packs only the commits after the last
reused tip (`walkDelta` from there). There is no format change and no
change to the core or the `Remote` interface. With A, the large values
are already deduped, so what B saves is the history's records and small
values. That isn't small: a full fork of a 3,900-commit session would
still upload about 6,800 values under 1 MiB, roughly 60 MiB compressed.

## The format as a contract

`docs/protocol.md` is the core's request protocol, between a host and
the core. The pack layout is a different contract, between transports
through GitHub. So this file would become that contract, written as
what a Python transport would implement: the branch layout, manifest,
pack encoding, parts, value references, commit messages and the CAS.
The README's "The format" section moves here. Golden fixtures (a
manifest and packs, with the commits they decode to) can go in
`corpus/` once a second implementation exists to run them.

## Not changing

The core's `push_plan`, `wire_walk` and `wire_replay`, the `Remote`
interface, the sync layer, and fetch's choice of which packs to read.

## Plan

1. **Client and fake:** the REST minute, blob creation, rate-limit
   waits. The fake gains `git/blobs`, `git/trees`, `git/commits`,
   `object(oid:)` lookups and a GraphQL body cap answered with 499.
2. **`PackRemote`:** format 2, the REST path, the inline fallback and
   cutoff, part halving, resume, one git commit per push, the landed
   check.
3. **Option A** at 1 MiB, its own PR.
4. This file trimmed to the contract, with one golden format-2 fixture.
5. A live check on `ashenfad/agex-sync-test`, including that
   `updateRefs` can create a ref (zero `beforeOid`) on a fine-grained
   token; then a note for studio's floor bump.
6. **Option B**, next after A, its own PR.

## Decisions (2026-11-15)

1. **The REST path**, inline below `inlineBytes` and falling back on
   `too-large`: yes, with three changes, written in above. A push
   decides whether it landed by the manifest's kvgit head, not a git
   oid. `inlineBytes` defaults to 1 MiB, and a GraphQL `too-large`
   lowers the remote's cutoff for its lifetime. A REST `too-large`
   halves `partBytes`, down to 512 KiB, before failing.
2. **`updateRefs` for the CAS**, not `PATCH git/refs`: yes. The live
   check confirms it can create a ref on a fine-grained token.
3. **Option A**: yes, in this stack, at 1 MiB, as its own PR after the
   REST path; the format-2 reader takes value references either way. In
   one 3,900-commit studio session, 42 distinct values are 1 MiB or
   more, 88 MiB in all: 25 workspace files (15 PNG, 10 MP3, about
   50 MiB), which dedupe across compact forks, snapshots and undo
   restores, and 17 run records (38 MiB), which don't. A 256 KiB
   threshold would add 88 values, 68 of them run records: writes for
   nothing.
4. **Option B**: later, next after A, its own PR.
5. **This file as the contract**, not `docs/protocol.md`: yes. Once
   built, it's trimmed to the normative spec and a short why; the
   measurements and rate-limit arithmetic stay in PR history. One golden
   format-2 fixture (a manifest, a pack, and the commits they decode to)
   goes in this stack rather than waiting for a second implementation.
6. **The scratch repo** is unarchived, and writes to it for the live
   check are allowed.
