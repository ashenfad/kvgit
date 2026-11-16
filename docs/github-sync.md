# GitHub sync format (format 2)

How kvgit sessions are stored in a plain GitHub repository, and how a
writer changes them. TypeScript's `PackRemote`
(`@agex-ts/kvgit/github`) implements it; another implementation that
follows this file can share a repository with it. The golden fixture
[`ts/tests/fixtures/github-format-2.json`](../ts/tests/fixtures/github-format-2.json)
holds a manifest, the pack it lists, a value kept outside that pack,
and the state its commits decode to.

The transport moves kvgit's wire commits (`wire_walk` and
`wire_replay` in [`protocol.md`](protocol.md)); commit hashes are
[`storage.md`](storage.md)'s. It uses GraphQL and REST git-data calls
only, never the git protocol, so it runs in a browser on a token with
**Contents: read and write** on the one repository.

## Branches

- A session is the branch `refs/heads/<session>`. An archived session
  (the trash) is `refs/heads/archived/<session>`.
- A branch is a session when its head commit holds
  `kvgit/manifest.json`.
- A new session's branch starts from the default branch's commit. A
  branch with no manifest that sits at the default branch's commit is
  a creation that never got its first commit, and the next creation of
  that session may claim it. A writer never pushes to any other branch
  without a manifest, nor to the default branch.
- A branch whose manifest isn't JSON, or has a `format` other than 1
  or 2, is left alone: never pushed over, never listed.

## Files on a session's branch

| Path | Holds |
|---|---|
| `kvgit/manifest.json` | The manifest. |
| `kvgit/packs/<seq>-<tip12>.pack` | A pack stored in one part. `<seq>` is the pack's sequence number, six digits, zero-padded; `<tip12>` the first 12 hex digits of its tip. |
| `kvgit/packs/<seq>-<tip12>.pack.<i>` | Part `i` (from 0) of a pack stored in n > 1 parts. |
| `kvgit/values/<sha256>` | A value kept outside its pack, raw, in one part. `<sha256>` is the value's SHA-256 in hex, its kvgit blob key. |
| `kvgit/values/<sha256>.<i>` | Part `i` of such a value in n > 1 parts. |

Other files (the default branch's, which a new branch starts with) are
left as they are.

**Parts.** A stored object's parts, concatenated in order, are its
bytes. The writer chooses each part's size; a reader goes by the part
SHAs it is given, never by sizes. The parts' git blob SHAs
(`sha1("blob <len>\0" + bytes)`) are how a reader fetches them.

**Values.** A branch whose packs reference a value names that value's
files in its tree, so a value stays reachable while any branch uses it.
The same value written by two writers in different parts gives two
sets of files, both valid. A branch's tree may name value files its
packs don't reference (a fork keeps those of the branch it reused);
they are harmless.

## The manifest

JSON (UTF-8):

```json
{
  "format": 2,
  "kernel": "ts",
  "branch": "chat-1",
  "head": "<kvgit head, 40 hex>",
  "packs": [
    {
      "path": "kvgit/packs/000001-5c292a483b6a.pack",
      "seq": 1,
      "base": null,
      "tip": "<kvgit commit>",
      "commits": 4,
      "bytes": 812,
      "sha256": "<SHA-256 of the stored pack>",
      "parts": ["<git blob SHA of each part>"]
    }
  ],
  "nextSeq": 2,
  "meta": { "title": "…" }
}
```

- `head` is the session's kvgit head at this commit.
- `packs` runs oldest first. The first pack's `base` is null (it starts
  at the root); every other pack's `base` is the previous pack's `tip`,
  and the last pack's `tip` is `head`.
- `bytes` and `sha256` describe the stored pack (gzip, all parts
  together); a reader checks the SHA-256 before decoding.
- `parts` has one entry for a pack stored at `path`, n for a pack
  stored at `path.0` … `path.<n-1>`.
- `nextSeq` is the next unused sequence number.
- `kernel` names the encoder; a reader refuses packs of another kernel.
- `meta` is whatever the writer records for listing sessions without
  fetching them (a title, say), or null.

**Format 1** is the same with `"format": 1` and each pack's `parts`
replaced by `blob`, the git SHA of the pack in one part. A reader takes
a format-1 entry as a one-part pack. A writer writes format 2, keeping
format-1 entries as one-part entries.

## Packs

A pack is gzip of one CBOR map:

```
{ "format": 2, "kernel": "ts", "base": <commit or null>, "tip": <commit>,
  "values": [ <value>, … ], "commits": [ [ <record>, [ [<key>, <index>], … ] ], … ] }
```

- **CBOR.** Maps with text keys, byte strings as CBOR byte strings
  (major type 2), arrays, text, integers and null; no tags. A reader
  accepts any valid encoding of that data. Writers needn't agree byte
  for byte, since each pack is named by its own SHAs, but one writer
  must encode the same commits to the same bytes, gzip included, so
  that a push that stopped partway finds its parts again. TypeScript's
  writer (cbor-x, keys in the order shown) is pinned by the fixture's
  `packCbor`.
- **`values`** is a table of the distinct values the pack's commits
  write. Each slot is either the value's bytes, or a reference to a
  value kept outside the pack: `[sha256, size, [part SHAs]]`, the
  value's SHA-256 in hex, its length, and its parts' git blob SHAs. A
  writer orders the slots by the key that first wrote each value, then
  by first appearance, so successive versions of a file sit together
  and compress against each other.
- **`commits`** run parents first. Each is `[record, updates]`.
  `record` is a byte string: the commit's record as JSON, the way
  Python's `json.dumps(…, separators=(",", ":"))` writes it
  (`ensure_ascii`, floats as `repr`), of the object `{"hash",
  "parents", "parentGens", "time", "info", "removals"}` in that key
  order. `time` is a float (`1700000000.25`, never `1700000000`);
  `parentGens` is null on the root commit; `removals` is sorted.
  `updates` is `[key, index]` pairs, `index` a slot in `values`.
- **`base`** is the commit the pack's run builds on (null from the
  root); `tip` is its last commit. Both must match the manifest entry.

A receiver resolves each reference (from its own store when it holds
the blob, else by reading the parts and checking the size and SHA-256),
applies each commit's updates and removals to its first parent's state,
and recomputes its hash; a commit whose hash doesn't match is refused
before anything of it is written. A pack may repeat commits the
receiver has (a fold re-sends its run whole); a receiver skips those.

A format-1 pack is the same with `"format": 1` and no references.

## Writing: one git commit per push

Every change to a session (a push, a fold riding a push, a repack) is
one git commit on its branch, parented on the branch's current commit,
with the message:

```
kvgit <session> @ <head7> (+<commits>)

Kvgit-Head: <head>
Kvgit-Pack-Format: 2
```

A repack's headline is `kvgit <session> repack (<n> → 1)`. The commit
lands by one of two compare-and-swaps on the branch's commit, never by
a plain ref update:

- **Inline**: one GraphQL `createCommitOnBranch` carrying every new
  file as base64, with `expectedHeadOid` the commit the writer read.
  For a new session, `createRef` at the default branch's commit runs
  in the same request.
- **Over REST**: `POST git/blobs` for each part GitHub doesn't already
  hold (a GraphQL `object(oid:)` lookup says which), `POST git/trees`
  on the branch's tree (`base_tree`; the default branch's for a new
  session) with the new parts, the manifest, and `sha: null` for files
  removed, `POST git/commits`, then GraphQL `updateRefs` with
  `beforeOid` the commit read (zeros for a new session) and `afterOid`
  the new commit.

A REST `PATCH git/refs` without `force` checks only that the move is a
fast-forward, which a stale writer can pass; `updateRefs` checks the
exact commit.

**Did it land?** A writer that can't tell (a lost response, a network
error) re-reads the branch: if the manifest's `head` is its push's tip,
the push landed, whichever request carried it. If the branch moved
anywhere else, the push lost a race. Lifecycle changes, which have no
manifest head to compare, count refs already at their targets as
landed.

**Forks.** A new branch whose history another branch's packs already
carry may list that branch's leading manifest entries as its own,
unchanged: from the first pack (`base` null) up to one whose `tip` is
an ancestor of the new head. Its first commit's tree is built on that
branch's tree (`base_tree`), so it names the same pack files, by the
same paths and part SHAs, and the value files they reference, less the
files of that branch's later packs, which it removes. It numbers its
own packs on from that branch's `nextSeq`, and its first own pack's
`base` is the last reused `tip`. Nothing reused is uploaded. From then
on its tree is its own: a fold or repack removes files from its tree
alone. A writer reuses only when the reused packs wouldn't fit in one
inline request anyway, since building on another tree takes the REST
path.

**Folds.** A writer may replace a run of adjacent packs with one pack
re-encoding the same commits: the first pack's `base`, the last one's
`tip`, a new `seq`, the run's files removed in the same commit.
`repack` folds every pack into one.

**Lifecycle**, each one atomic `updateRefs`:

- archive: create `archived/<s>` at the session's commit and delete
  `<s>` (beforeOid that commit);
- restore: the reverse;
- delete: `<s>` to zeros from the commit holding the head the caller
  expects;
- empty trash: delete every `archived/*` at the commits read.

## Reading

A reader lists sessions with one GraphQL query per hundred branches,
each with its manifest inlined (`Commit.file`). To fetch a head, it
takes the manifest's packs from the newest back to the first whose
`base` it holds (or null), reads their parts raw by git SHA (`GET
git/blobs/<sha>` with `Accept: application/vnd.github.raw+json`; the
contents API transcodes bytes it takes for text), concatenates them,
checks the SHA-256, and decodes.

## Limits a writer works within

- **GraphQL body deadline.** GitHub drops a GraphQL request whose body
  hasn't arrived after about 5 s (a 499; a browser sees a network
  error). `PackRemote` goes inline only under 1 MiB of request, and
  falls back to REST, halving its cutoff, when GitHub drops one.
- **REST body deadline.** GitHub answers a REST request whose body is
  still arriving after about a minute with 401 "Bad credentials".
  `PackRemote` stores parts of at most 4 MiB, halving that, down to
  512 KiB, when a part doesn't arrive.
- **Secondary rate limits.** About 80 content-creating requests a
  minute and 500 an hour. Writes go one at a time, 750 ms apart; a
  rate limit waits as GitHub asks, and a wait over a minute ends the
  push for the caller to schedule. The next push re-encodes the same
  bytes (the encoding is deterministic) and skips the parts GitHub
  already holds.
- **Large values.** `PackRemote` keeps values of 1 MiB or more outside
  packs in pushes that go over REST, so each is uploaded once per
  repository and read only by receivers that lack it.

## Why this shape

GitHub's smart-HTTP endpoints send no CORS headers, so a browser can't
speak git; GraphQL and the REST git-data API can. One inline request
per push was simplest, but GraphQL's 5 s body deadline caps it by the
uplink, and a single commit can't be split, so larger pushes go as git
objects over REST, which takes a body for a minute and any size, in
parts that bound each request. Packs keep a session's many small
values compressed together; large values (images, audio) go on their
own, raw and named by content, so forks, snapshots and other sessions
share them. The measurements and arithmetic behind these numbers are in
the pull requests that built this format:
[#143](https://github.com/ashenfad/kvgit/pull/143) through
[#148](https://github.com/ashenfad/kvgit/pull/148).
