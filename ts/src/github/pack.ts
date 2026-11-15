/**
 * PackRemote: the kvgit `Remote` over a GitHub repo, as compressed
 * packs of commits. The format is specified in `docs/github-sync.md`.
 * A session's branch holds:
 *
 *   kvgit/manifest.json                  head, pack list, caller metadata
 *   kvgit/packs/<seq>-<tip12>.pack       gzip(CBOR) of a run of WireCommits
 *   kvgit/packs/<seq>-<tip12>.pack.<i>   the same in parts, when stored so
 *
 * A push lands as ONE git commit on the branch, whatever its size:
 *
 * - **Inline**, when the request stays under the inline cutoff
 *   (`inlineBytes`): one GraphQL `createCommitOnBranch` carrying the
 *   packs as base64, `expectedHeadOid` the compare-and-swap. GitHub
 *   drops a GraphQL request whose body is still arriving at about 5 s;
 *   a push dropped so goes the REST way, and lowers the cutoff.
 * - **Over REST** otherwise: each pack as git blobs of at most
 *   `partBytes` (`POST git/blobs`, skipping the ones GitHub already
 *   holds, so a push that stopped partway resumes), then a tree, a
 *   commit, and a GraphQL `updateRefs` from the branch's commit to the
 *   new one: an exact compare-and-swap. A part that doesn't arrive
 *   within GitHub's minute halves `partBytes`.
 *
 * Whether a push landed is the branch's manifest's say: after an error,
 * one whose `head` is the push's tip landed, whichever request carried
 * it. Other costs, per operation:
 *
 * - `listRefs` / `roster`: one GraphQL query for every branch, with
 *   each branch's manifest inlined (`Commit.file`), so the session
 *   list, heads and stub metadata (a title) come back together.
 * - `fetch`: one raw blob read per part of each needed pack,
 *   concurrently. The manifest says which packs a receiver needs:
 *   walking back from the newest, until a pack's base is a commit the
 *   receiver has.
 * - `archiveBranch` / `restoreBranch` / `deleteBranch` / `emptyTrash`:
 *   one atomic, CAS-checked `updateRefs`.
 *
 * Packs accumulate one per push. When a push would exceed `maxPacks`,
 * it folds the older ones (all but `keepRecent`) into one, re-encoded
 * from the local store, in the same commit. `repack` folds everything
 * into one pack, so a fresh device's first pull is the roster query
 * plus one pack's reads.
 *
 * The integrity story is applyWire's: every commit hash is recomputed
 * on receipt. A pack also carries its SHA-256 in the manifest, so a
 * damaged file fails before decoding.
 */

import { Encoder } from 'cbor-x'
import { PyFloat, pyDumpsBytes, pyLoadsBytes } from '../pyjson'
import type { RemoteRef } from '../sync/remote'
import type { Remote } from '../sync/remote'
import { walkDelta } from '../sync/walk'
import type { KVStore, WireCommit } from '../types'
import { ancestorsAbove, isAncestor } from '../versioned/ancestry'
import { BLOB_PREFIX, generationOf, sha256Hex } from '../versioned/layout'
import { bytesToBase64 } from './base64'
import type { GithubClient, GraphqlError, GraphqlResult } from './client'
import { GithubError, formatBytes } from './errors'
import { gitBlobSha1 } from './git-hash'

/** The format this code writes, of manifests and packs alike. It reads
 *  format 1 too. */
export const PACK_FORMAT = 2
export const MANIFEST_PATH = 'kvgit/manifest.json'
export const PACKS_DIR = 'kvgit/packs'
const ARCHIVED = 'archived/'
const ZERO_OID = '0000000000000000000000000000000000000000'
/** Raw blob reads in flight at once. */
const GET_CHUNK = 16
/** Object lookups in one GraphQL query. */
const LOOKUP_CHUNK = 100
/** The smallest part a push halves its parts to. */
const MIN_PART_BYTES = 512 * 1024
/** A commit request's bytes beyond its files, for the inline cutoff. */
const REQUEST_OVERHEAD = 2048

/** One pack as the manifest lists it. */
export interface PackEntry {
  path: string
  seq: number
  /** The commit this pack's delta is relative to (null: from the root). */
  base: string | null
  /** The last commit in the pack. */
  tip: string
  commits: number
  /** Stored (compressed) size. */
  bytes: number
  /** SHA-256 of the stored bytes. */
  sha256: string
  /** git blob SHAs of the stored bytes' parts, in order: fetch reads
   *  them by these. One part is stored at `path`, n at `path.0` …
   *  `path.<n-1>`. */
  parts: string[]
}

export interface PackManifest {
  format: number
  kernel: string
  branch: string
  /** kvgit head of the session at this git commit. */
  head: string
  /** Oldest first; each pack's base is the previous pack's tip. */
  packs: PackEntry[]
  /** Next pack sequence number. */
  nextSeq: number
  /** Caller metadata (`PackRemoteOptions.meta`), e.g. a title. */
  meta: unknown
}

/**
 * A value kept outside its pack, as the pack's value table names it in
 * the value's place: the value's SHA-256 (its kvgit blob key's), its
 * size, and the git blob SHAs of its parts, in order.
 */
export type ValueRef = [sha256: string, size: number, parts: string[]]

export interface RosterEntry {
  branch: string
  head: string
  meta: unknown
  packs: number
  bytes: number
  archived: boolean
}

export interface PackRemoteOptions {
  /** Encoder discriminator recorded in packs. Default 'ts'. */
  kernel?: string
  /** Stub metadata written into the manifest on each push (e.g. the
   *  session's title), read back by `roster()` without a fetch. */
  meta?: (branch: string, head: string) => unknown | Promise<unknown>
  /** Fold older packs once a push would exceed this many. Default 16. */
  maxPacks?: number
  /** Packs a fold leaves alone (incremental pulls read only these). Default 4. */
  keepRecent?: number
  /** Most uncompressed value bytes in one pack; a larger push lands as
   *  several packs, still in one commit. A fold rides a push only if
   *  the fold and the push's packs together stay under it. Default
   *  8 MiB. */
  maxPackBytes?: number
  /** The largest push sent inline, as one GraphQL request, in bytes of
   *  request. Default 1 MiB, about 4 s at 2 Mbps: GitHub drops a GraphQL
   *  request whose body hasn't arrived after about 5 s. A push GitHub
   *  drops lowers it, for this remote's lifetime, to half that push's
   *  size. */
  inlineBytes?: number
  /** The largest part a pack is stored in over REST. Default 4 MiB, a
   *  request of about 5.6 MB that arrives within GitHub's minute down
   *  to about 100 KB/s. A part that doesn't halves it, for this
   *  remote's lifetime, down to 512 KiB. */
  partBytes?: number
}

interface BranchState {
  gitOid: string
  /** The git tree at `gitOid`: the base a REST push's tree builds on. */
  treeOid: string
  /** Null for a ref without a manifest file: a creation that never got
   *  its first commit, which the next creation claims. */
  manifest: PackManifest | null
  /** Why the manifest there can't be used (unparseable, a format this
   *  code doesn't know): such a branch is left alone, never claimed. */
  unreadable?: string
}

/** A pack a push makes, before it is laid out in parts. */
interface NewPack {
  path: string
  seq: number
  base: string | null
  tip: string
  commits: number
  bytes: Uint8Array
  sha256: string
}

/** What one commit on a branch makes of it: its packs in manifest
 *  order (those it keeps, and new ones), the files it removes, and the
 *  rest of its manifest. */
interface Change {
  packs: Array<PackEntry | NewPack>
  deletions: string[]
  head: string
  nextSeq: number
  meta: unknown
  headline: string
  /** Commits the change adds, for its headline and its errors. */
  what: string
}

/** One file a change writes: a pack, or a part of one. */
interface PackFile {
  path: string
  bytes: Uint8Array
  sha: string
  pack: NewPack
}

interface Layout {
  manifest: PackManifest
  files: PackFile[]
}

// ---------------------------------------------------------------------------
// Pack codec
// ---------------------------------------------------------------------------

const cbor = new Encoder({ useRecords: false, mapsAsObjects: true, tagUint8Array: false })

export interface DecodedPack {
  kernel: string
  base: string | null
  tip: string
  commits: WireCommit[]
}

/**
 * Encode a run of wire commits (parents first) as one pack: each
 * commit's record as kvgit writes it (Python-style JSON, so floats stay
 * floats and the hash re-derives), and the values as a table of
 * distinct byte strings ordered by key, so the successive versions of
 * one file sit together and compress against each other. A value whose
 * SHA-256 is in `refs` is kept outside the pack: its slot in the table
 * holds that reference instead.
 */
export async function encodePack(
  commits: readonly WireCommit[],
  opts: {
    kernel: string
    base: string | null
    compress?: boolean
    refs?: ReadonlyMap<string, ValueRef>
  },
): Promise<Uint8Array> {
  const tip = commits.at(-1)?.hash
  if (tip === undefined) throw new Error('encodePack: no commits')
  const slots: Array<{ key: string; order: number; bytes: Uint8Array; hex: string }> = []
  const byHex = new Map<string, number>()
  const refs: Array<Array<[string, string]>> = []
  let order = 0
  for (const wc of commits) {
    const updates: Array<[string, string]> = []
    for (const [key, bytes] of wc.updates) {
      const hex = await sha256Hex(bytes)
      if (!byHex.has(hex)) {
        byHex.set(hex, -1)
        slots.push({ key, order: order++, bytes, hex })
      }
      updates.push([key, hex])
    }
    refs.push(updates)
  }
  slots.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : a.order - b.order))
  slots.forEach((slot, i) => byHex.set(slot.hex, i))

  const body = cbor.encode({
    format: PACK_FORMAT,
    kernel: opts.kernel,
    base: opts.base,
    tip,
    values: slots.map((s) => opts.refs?.get(s.hex) ?? s.bytes),
    commits: commits.map((wc, i) => [
      pyDumpsBytes(
        new Map<string, unknown>([
          ['hash', wc.hash],
          ['parents', [...wc.parents]],
          ['parentGens', wc.parentGens === null ? null : [...wc.parentGens]],
          ['time', wc.time],
          ['info', wc.info ?? null],
          ['removals', [...wc.removals].sort()],
        ]),
      ),
      (refs[i] as Array<[string, string]>).map(([key, hex]) => [key, byHex.get(hex) as number]),
    ]),
  })
  return opts.compress === false ? body : gzip(body)
}

/**
 * Decode a pack (gzip detected by its magic bytes). A value kept
 * outside the pack (format 2) is read with `resolve`; a pack holding
 * one can't be decoded without it.
 */
export async function decodePack(
  stored: Uint8Array,
  resolve?: (ref: ValueRef) => Promise<Uint8Array>,
): Promise<DecodedPack> {
  const raw = stored[0] === 0x1f && stored[1] === 0x8b ? await gunzip(stored) : stored
  const pack = cbor.decode(raw) as {
    format: unknown
    kernel: string
    base: string | null
    tip: string
    values: unknown[]
    commits: Array<[Uint8Array, Array<[string, number]>]>
  }
  if (pack.format !== 1 && pack.format !== PACK_FORMAT) {
    throw new Error(`decodePack: unsupported format ${JSON.stringify(pack.format)}`)
  }
  const values: Uint8Array[] = []
  for (const v of pack.values) {
    if (v instanceof Uint8Array) values.push(new Uint8Array(v))
    else if (pack.format === PACK_FORMAT && isValueRef(v)) {
      if (resolve === undefined) {
        throw new Error('decodePack: a value is kept outside the pack, and nothing resolves it')
      }
      values.push(await resolve(v))
    } else throw new Error('decodePack: a value is neither bytes nor a reference')
  }
  const commits = pack.commits.map(([record, updates]): WireCommit => {
    const r = pyLoadsBytes(new Uint8Array(record)) as Record<string, unknown>
    if (!(r.time instanceof PyFloat)) throw new Error('decodePack: time must be a float')
    const map = new Map<string, Uint8Array>()
    for (const [key, idx] of updates) {
      const bytes = values[idx]
      if (bytes === undefined) throw new Error(`decodePack: value ${idx} out of range`)
      map.set(key, bytes)
    }
    return {
      hash: r.hash as string,
      parents: r.parents as string[],
      parentGens: (r.parentGens as number[] | null) ?? null,
      time: r.time,
      info: r.info ?? null,
      updates: map,
      removals: new Set(r.removals as string[]),
    }
  })
  return { kernel: pack.kernel, base: pack.base, tip: pack.tip, commits }
}

function isValueRef(v: unknown): v is ValueRef {
  return (
    Array.isArray(v) &&
    v.length === 3 &&
    typeof v[0] === 'string' &&
    /^[0-9a-f]{64}$/.test(v[0]) &&
    Number.isSafeInteger(v[1]) &&
    Array.isArray(v[2]) &&
    v[2].length > 0 &&
    v[2].every((p) => typeof p === 'string' && /^[0-9a-f]{40}$/.test(p))
  )
}

async function pipe(bytes: Uint8Array, stream: GenericTransformStream): Promise<Uint8Array> {
  const out = new Blob([bytes as Uint8Array<ArrayBuffer>])
    .stream()
    .pipeThrough(stream as TransformStream)
  return new Uint8Array(await new Response(out).arrayBuffer())
}

export const gzip = (bytes: Uint8Array): Promise<Uint8Array> =>
  pipe(bytes, new CompressionStream('gzip'))
export const gunzip = (bytes: Uint8Array): Promise<Uint8Array> =>
  pipe(bytes, new DecompressionStream('gzip'))

// ---------------------------------------------------------------------------
// GraphQL documents
// ---------------------------------------------------------------------------

const REF_FIELDS = `name target { oid ... on Commit { tree { oid } file(path: "${MANIFEST_PATH}") { object { ... on Blob { text } } } } }`
const DEFAULT_REF = 'defaultBranchRef { name target { oid ... on Commit { tree { oid } } } }'

const ROSTER_QUERY = `query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    id
    ${DEFAULT_REF}
    refs(refPrefix: "refs/heads/", first: 100, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes { ${REF_FIELDS} }
    }
  }
}`

const ONE_REF_QUERY = `query($owner: String!, $name: String!, $q: String!) {
  repository(owner: $owner, name: $name) {
    id
    ${DEFAULT_REF}
    ref(qualifiedName: $q) { ${REF_FIELDS} }
  }
}`

const COMMIT_MUTATION = (create: boolean): string => `mutation(
  ${create ? '$repoId: ID!, $ref: String!,' : ''}
  $repo: String!, $branch: String!, $oid: GitObjectID!,
  $headline: String!, $body: String!,
  $additions: [FileAddition!], $deletions: [FileDeletion!]
) {
  ${create ? 'create: createRef(input: {repositoryId: $repoId, name: $ref, oid: $oid}) { ref { id } }' : ''}
  commit: createCommitOnBranch(input: {
    branch: {repositoryNameWithOwner: $repo, branchName: $branch},
    expectedHeadOid: $oid,
    message: {headline: $headline, body: $body},
    fileChanges: {additions: $additions, deletions: $deletions}
  }) { commit { oid tree { oid } } }
}`

const UPDATE_REFS = `mutation($repoId: ID!, $updates: [RefUpdate!]!) {
  updateRefs(input: {repositoryId: $repoId, refUpdates: $updates}) { clientMutationId }
}`

/** A query asking whether the repo holds each of `n` objects, by
 *  variables `$o0` … */
const OBJECTS_QUERY = (n: number): string => {
  const vars = Array.from({ length: n }, (_, i) => `$o${i}: GitObjectID!`).join(', ')
  const fields = Array.from({ length: n }, (_, i) => `o${i}: object(oid: $o${i}) { oid }`)
  return `query($owner: String!, $name: String!, ${vars}) {
  repository(owner: $owner, name: $name) { ${fields.join(' ')} }
}`
}

interface RefNode {
  name: string
  target: {
    oid: string
    tree?: { oid: string }
    file?: { object: { text?: string | null } | null } | null
  }
}

interface RosterData {
  repository: RepoHeader & {
    refs: { pageInfo: { hasNextPage: boolean; endCursor: string }; nodes: RefNode[] }
  }
}

interface RepoHeader {
  id: string
  defaultBranchRef: { name: string; target: { oid: string; tree?: { oid: string } } } | null
}

// ---------------------------------------------------------------------------
// PackRemote
// ---------------------------------------------------------------------------

export class PackRemote implements Remote {
  readonly client: GithubClient
  /** `github:<owner>/<name>` (GitHub's names ignore case, so lowered),
   *  with the API's root before the repo when it isn't github.com's: its
   *  scheme and host lowered, its path as given, which a server may
   *  read case and all. */
  readonly id: string
  readonly #store: KVStore
  readonly #kernel: string
  readonly #meta: PackRemoteOptions['meta']
  readonly #maxPacks: number
  readonly #keepRecent: number
  readonly #maxPackBytes: number
  readonly #minPartBytes: number
  readonly #owner: string
  readonly #name: string
  #inlineBytes: number
  #partBytes: number

  #repoId: string | null = null
  #defaultOid: string | null = null
  #defaultTree: string | null = null
  #defaultName: string | null = null
  /** Branch (archived ones under `archived/`) → its last-observed ref. */
  readonly #refs = new Map<string, BranchState>()

  constructor(client: GithubClient, store: KVStore, opts: PackRemoteOptions = {}) {
    this.client = client
    this.#store = store
    this.#kernel = opts.kernel ?? 'ts'
    this.#meta = opts.meta
    this.#maxPacks = opts.maxPacks ?? 16
    this.#keepRecent = opts.keepRecent ?? 4
    this.#maxPackBytes = opts.maxPackBytes ?? 8 * 1024 * 1024
    this.#inlineBytes = opts.inlineBytes ?? 1024 * 1024
    this.#partBytes = Math.max(1, opts.partBytes ?? 4 * 1024 * 1024)
    this.#minPartBytes = Math.min(this.#partBytes, MIN_PART_BYTES)
    const api = apiRoot(client.apiBase)
    this.id = `github:${api === 'https://api.github.com' ? '' : `${api}/`}${client.repo.toLowerCase()}`
    const [owner, name] = client.repo.split('/') as [string, string]
    this.#owner = owner
    this.#name = name
  }

  // -------------------------------------------------------------------------
  // Roster
  // -------------------------------------------------------------------------

  /** Re-read every branch with its manifest: one query per 100 refs. */
  async refresh(): Promise<void> {
    this.#refs.clear()
    let after: string | null = null
    for (;;) {
      const res: GraphqlResult<RosterData> = await this.client.graphql<RosterData>(ROSTER_QUERY, {
        owner: this.#owner,
        name: this.#name,
        after,
      })
      const repo: RosterData['repository'] = expectData(res, 'roster').repository
      this.#repoHeader(repo)
      for (const node of repo.refs.nodes) this.#remember(node)
      if (!repo.refs.pageInfo.hasNextPage) return
      after = repo.refs.pageInfo.endCursor
    }
  }

  async listRefs(): Promise<RemoteRef[]> {
    await this.refresh()
    const out: RemoteRef[] = []
    for (const [branch, st] of this.#refs) {
      if (branch.startsWith(ARCHIVED) || st.manifest === null) continue
      out.push({ branch, head: st.manifest.head })
    }
    return out.sort((a, b) => (a.branch < b.branch ? -1 : 1))
  }

  /** Every session, live and archived, with heads and stub metadata,
   *  from one query. */
  async roster(): Promise<RosterEntry[]> {
    await this.refresh()
    const out: RosterEntry[] = []
    for (const [name, st] of this.#refs) {
      if (st.manifest === null) continue
      const archived = name.startsWith(ARCHIVED)
      out.push({
        branch: archived ? name.slice(ARCHIVED.length) : name,
        head: st.manifest.head,
        meta: st.manifest.meta,
        packs: st.manifest.packs.length,
        bytes: st.manifest.packs.reduce((n, p) => n + p.bytes, 0),
        archived,
      })
    }
    return out.sort((a, b) => (a.branch < b.branch ? -1 : 1))
  }

  /** A session's stub metadata (from the cached roster, else one query). */
  async readMeta(branch: string): Promise<unknown> {
    const st = this.#refs.get(branch) ?? (await this.#readOne(branch))
    return st?.manifest?.meta ?? null
  }

  // -------------------------------------------------------------------------
  // Remote: fetch
  // -------------------------------------------------------------------------

  async *fetch(want: string, have: Iterable<string>): AsyncIterable<WireCommit> {
    let found = this.#findHead(want)
    if (found === null) {
      await this.refresh()
      found = this.#findHead(want)
    }
    if (found === null) {
      throw new Error(`PackRemote.fetch: no branch at kvgit head ${want.slice(0, 7)}`)
    }
    const { manifest } = found
    const haveList = [...have]
    if (await this.#holds(haveList, want)) return

    // Newest back, until a pack builds on something we have.
    const needed: PackEntry[] = []
    let anchored = false
    for (let i = manifest.packs.length - 1; i >= 0; i--) {
      const pack = manifest.packs[i] as PackEntry
      needed.unshift(pack)
      if (pack.base === null || (await this.#holds(haveList, pack.base))) {
        anchored = true
        break
      }
    }
    if (!anchored) throw new Error('PackRemote.fetch: pack chain does not reach a known commit')

    const parts = await this.#readParts(
      needed.flatMap((pack) => pack.parts.map((sha) => ({ sha, of: pack.path }))),
    )
    const stored: Uint8Array[] = []
    for (const pack of needed) {
      const bytes = concat(parts.splice(0, pack.parts.length))
      if ((await sha256Hex(bytes)) !== pack.sha256) {
        throw new Error(`PackRemote.fetch: ${pack.path} fails its checksum`)
      }
      stored.push(bytes)
    }
    // What we hold of the packs' commits, from as far down as their
    // lowest generation: a pack built on what we have can still repeat
    // commits we hold (a fold re-sends its run whole).
    let floor = Number.POSITIVE_INFINITY
    let haveSet = new Set<string>()
    for (const [i, bytes] of stored.entries()) {
      const entry = needed[i] as PackEntry
      const pack = await decodePack(bytes, (ref) => this.#readValue(ref))
      const low = lowestGeneration(pack.commits)
      if (haveList.length > 0 && low < floor) {
        floor = low
        haveSet = await ancestorsAbove(this.#store, haveList, floor)
      }
      if (pack.kernel !== this.#kernel) {
        throw new Error(
          `PackRemote.fetch: pack kernel '${pack.kernel}', expected '${this.#kernel}'`,
        )
      }
      if (pack.base !== entry.base || pack.tip !== entry.tip) {
        throw new Error(`PackRemote.fetch: ${entry.path} disagrees with the manifest`)
      }
      for (const wc of pack.commits) {
        if (haveSet.has(wc.hash)) continue
        yield wc
      }
    }
  }

  /** Blobs' bytes by git SHA, `GET_CHUNK` at a time, in order. */
  async #readParts(parts: Array<{ sha: string; of: string }>): Promise<Uint8Array[]> {
    const out: Uint8Array[] = []
    for (let i = 0; i < parts.length; i += GET_CHUNK) {
      const batch = parts.slice(i, i + GET_CHUNK)
      out.push(
        ...(await Promise.all(
          batch.map(async ({ sha, of }) => {
            const bytes = await this.client.getBlobRaw(sha)
            if (bytes === null) throw new Error(`PackRemote.fetch: ${of} missing`)
            return bytes
          }),
        )),
      )
    }
    return out
  }

  /** A value kept outside its pack: from the local store when it holds
   *  the blob, else read in parts and checked. */
  async #readValue([sha256, size, parts]: ValueRef): Promise<Uint8Array> {
    const local = await this.#store.get(`${BLOB_PREFIX}${sha256}`)
    if (local !== null && local.length === size) return local
    const of = `value ${sha256.slice(0, 12)}`
    const bytes = concat(await this.#readParts(parts.map((sha) => ({ sha, of }))))
    if (bytes.length !== size || (await sha256Hex(bytes)) !== sha256) {
      throw new Error(`PackRemote.fetch: ${of} fails its checksum`)
    }
    return bytes
  }

  // -------------------------------------------------------------------------
  // Remote: push
  // -------------------------------------------------------------------------

  async push(
    branch: string,
    expectedOld: string | null,
    newHead: string,
    commits: AsyncIterable<WireCommit> | Iterable<WireCommit>,
  ): Promise<boolean> {
    let st = this.#refs.get(branch) ?? null
    if (this.#repoId === null || (st?.manifest?.head ?? null) !== expectedOld) {
      st = await this.#readOne(branch)
    }
    if (st?.unreadable !== undefined) {
      throw new Error(`PackRemote.push: branch '${branch}' can't be synced: ${st.unreadable}`)
    }
    if (expectedOld === null) {
      if (st !== null && st.manifest !== null) return false
      // A ref without a manifest is claimed only as a creation that
      // never got its first commit: such a ref sits where creation
      // starts one, at the default branch's commit. Any other (the
      // default branch, a branch of the repo's own) isn't a session.
      if (st !== null && (branch === this.#defaultName || st.gitOid !== this.#defaultOid)) {
        throw new Error(
          `PackRemote.push: branch '${branch}' exists in the repo but isn't a kvgit session; push under another name`,
        )
      }
    } else if (st === null || st.manifest === null || st.manifest.head !== expectedOld) {
      return false
    }

    const all: WireCommit[] = []
    for await (const wc of commits) all.push(wc)
    if (all.at(-1)?.hash !== newHead) {
      throw new Error(`PackRemote.push: stream ended at ${all.at(-1)?.hash.slice(0, 7) ?? '∅'}`)
    }

    // Packs of at most maxPackBytes of values, all in one commit.
    const chunks: WireCommit[][] = []
    let current: WireCommit[] = []
    let size = 0
    for (const wc of all) {
      let n = 0
      for (const v of wc.updates.values()) n += v.length
      if (current.length > 0 && size + n > this.#maxPackBytes) {
        chunks.push(current)
        current = []
        size = 0
      }
      current.push(wc)
      size += n
    }
    chunks.push(current)

    const prior = st?.manifest ?? null
    let nextSeq = prior?.nextSeq ?? 1
    const fresh: NewPack[] = []
    let base = expectedOld
    for (const chunk of chunks) {
      const tip = (chunk.at(-1) as WireCommit).hash
      const bytes = await encodePack(chunk, { kernel: this.#kernel, base })
      fresh.push(await newPack(nextSeq++, base, tip, chunk.length, bytes))
      base = tip
    }

    // Fold older packs into one when there are too many: the newest
    // stretch of all but keepRecent that fits in what the push's own
    // packs leave of maxPackBytes. One that doesn't fit waits for a
    // later, smaller push.
    let packs: Array<PackEntry | NewPack> = [...(prior?.packs ?? []), ...fresh]
    const deletions: string[] = []
    if (packs.length > this.#maxPacks) {
      const kept = prior?.packs ?? []
      const run = kept.slice(0, Math.max(0, packs.length - this.#keepRecent))
      const spent = fresh.reduce((n, p) => n + p.bytes.length, 0)
      const folded = await this.#fold(run, nextSeq, this.#maxPackBytes - spent)
      if (folded !== null) {
        const gone = new Set(folded.replaced.map((p) => p.path))
        const firstGone = packs.findIndex((p) => gone.has(p.path))
        packs = packs.filter((p) => !gone.has(p.path))
        packs.splice(firstGone, 0, folded.pack)
        for (const p of folded.replaced) deletions.push(...filesOf(p))
        nextSeq++
      }
    }

    const tip7 = newHead.slice(0, 7)
    const change: Change = {
      packs,
      deletions,
      head: newHead,
      nextSeq,
      meta: this.#meta !== undefined ? await this.#meta(branch, newHead) : prior?.meta,
      headline: `kvgit ${branch} @ ${tip7} (+${all.length})`,
      what: all.length === 1 ? `commit ${tip7}` : `${all.length} commits up to ${tip7}`,
    }
    return this.#land(branch, st, change, (m) => m.head === newHead)
  }

  /** Re-encode a run of packs as one, from the local store: the newest
   *  stretch whose stored bytes fit `budget`. Null when that stretch is
   *  shorter than two packs. */
  async #fold(
    run: PackEntry[],
    seq: number,
    budget: number,
  ): Promise<{ pack: NewPack; replaced: PackEntry[] } | null> {
    let start = run.length
    let total = 0
    while (start > 0 && total + (run[start - 1] as PackEntry).bytes <= budget) {
      start--
      total += (run[start] as PackEntry).bytes
    }
    const replaced = run.slice(start)
    if (replaced.length < 2) return null
    const base = (replaced[0] as PackEntry).base
    const tip = (replaced.at(-1) as PackEntry).tip
    const commits: WireCommit[] = []
    for await (const wc of walkDelta(this.#store, {
      want: tip,
      have: base === null ? [] : [base],
    })) {
      commits.push(wc)
    }
    const bytes = await encodePack(commits, { kernel: this.#kernel, base })
    return { pack: await newPack(seq, base, tip, commits.length, bytes), replaced }
  }

  /**
   * Fold every pack of `branch` into one, from the local store (which
   * must hold the remote head), in one commit. Afterwards a fresh
   * device's pull is the roster query plus one pack's reads.
   *
   * @returns false if the remote moved meanwhile.
   */
  async repack(branch: string): Promise<boolean> {
    const st = (await this.#readOne(branch)) ?? null
    if (st === null || st.manifest === null) throw new Error(`repack: no session '${branch}'`)
    const { manifest } = st
    if (manifest.packs.length < 2) return true
    const commits: WireCommit[] = []
    for await (const wc of walkDelta(this.#store, { want: manifest.head })) commits.push(wc)
    const bytes = await encodePack(commits, { kernel: this.#kernel, base: null })
    const pack = await newPack(manifest.nextSeq, null, manifest.head, commits.length, bytes)
    const change: Change = {
      packs: [pack],
      deletions: manifest.packs.flatMap(filesOf),
      head: manifest.head,
      nextSeq: manifest.nextSeq + 1,
      meta: manifest.meta,
      headline: `kvgit ${branch} repack (${manifest.packs.length} → 1)`,
      what: `the whole history up to ${manifest.head.slice(0, 7)}`,
    }
    return this.#land(
      branch,
      st,
      change,
      (m) => m.packs.length === 1 && m.packs[0]?.path === pack.path,
    )
  }

  // -------------------------------------------------------------------------
  // Landing a change: inline, or as git objects over REST
  // -------------------------------------------------------------------------

  /**
   * Land `change` on `branch` as one commit, compare-and-swapped
   * against `st`: inline when the request is small enough, else over
   * REST. After an error that leaves it unclear, the branch is re-read,
   * and `landed` of its manifest says whether the change is there.
   *
   * @returns false when the branch had moved (a lost race).
   */
  async #land(
    branch: string,
    st: BranchState | null,
    change: Change,
    landed: (m: PackManifest) => boolean,
  ): Promise<boolean> {
    const before = st?.gitOid ?? ZERO_OID
    const rough = change.packs.reduce(
      (n, p) => n + (isNew(p) ? base64Length(p.bytes.length) : 0),
      0,
    )
    if (rough + REQUEST_OVERHEAD <= this.#inlineBytes) {
      const layout = await this.#layout(branch, change, null)
      const size = inlineSize(layout)
      if (size <= this.#inlineBytes) {
        try {
          if (await this.#landInline(branch, st, change, layout)) return true
          return (await this.#reread(branch, landed)).landed
        } catch (err) {
          if (!(err instanceof GithubError)) throw err
          const unclear = err.kind === 'network' || err.kind === 'server'
          if (err.kind !== 'too-large' && !unclear) throw err
          if (err.kind === 'too-large') {
            // GitHub stopped waiting for the body: that size doesn't
            // reach it on this link, so smaller ones go over REST too.
            this.#inlineBytes = Math.min(this.#inlineBytes, Math.floor(size / 2))
          }
          // A 499 or 413 is GitHub saying it applied nothing; a network
          // error, at the deadline or not, or a server error leaves it
          // unclear.
          if (err.status === 0 || unclear) {
            const now = await this.#readOne(branch)
            if (now?.manifest != null && landed(now.manifest)) return true
            // a creation's createRef can land without its commit: the
            // bare ref it leaves is this push's to claim
            const bare =
              st === null &&
              now !== null &&
              now.manifest === null &&
              now.unreadable === undefined &&
              now.gitOid === this.#defaultOid
            if (bare) st = now
            else if ((now?.gitOid ?? ZERO_OID) !== before) return false
            if (unclear) throw err
          }
        }
      }
    }
    return this.#landRest(branch, st, change, landed)
  }

  /** One `createCommitOnBranch` (creating the ref in the same request
   *  for a new session): true when it landed, false when the CAS lost. */
  async #landInline(
    branch: string,
    st: BranchState | null,
    change: Change,
    layout: Layout,
  ): Promise<boolean> {
    const create = st === null
    const res = await this.client.graphql<{
      create?: { ref: { id: string } } | null
      commit: { commit: { oid: string; tree: { oid: string } } } | null
    }>(
      COMMIT_MUTATION(create),
      {
        ...(create && { repoId: this.#repoId, ref: `refs/heads/${branch}` }),
        repo: this.client.repo,
        branch,
        oid: st?.gitOid ?? this.#defaultOid,
        headline: change.headline,
        body: trailers(change.head),
        additions: [
          ...layout.files.map((f) => ({ path: f.path, contents: bytesToBase64(f.bytes) })),
          { path: MANIFEST_PATH, contents: bytesToBase64(manifestBytes(layout.manifest)) },
        ],
        deletions: change.deletions.map((path) => ({ path })),
      },
      { mutation: true },
    )
    const made = res.data?.commit ?? null
    if (made === null) {
      if (res.errors.some(lostRace)) return false
      throw graphqlFailure('push', res.errors)
    }
    this.#refs.set(branch, {
      gitOid: made.commit.oid,
      treeOid: made.commit.tree.oid,
      manifest: layout.manifest,
    })
    return true
  }

  /** The change as git objects: its packs' parts (those GitHub lacks),
   *  a tree, a commit, and an `updateRefs` from `st`'s commit to it. */
  async #landRest(
    branch: string,
    st: BranchState | null,
    change: Change,
    landed: (m: PackManifest) => boolean,
  ): Promise<boolean> {
    let layout: Layout
    for (;;) {
      layout = await this.#layout(branch, change, this.#partBytes)
      try {
        await this.#upload(layout.files, change)
        break
      } catch (err) {
        if (!(err instanceof GithubError) || err.kind !== 'too-large') throw err
        if (this.#partBytes <= this.#minPartBytes) throw err
        // a part didn't arrive within GitHub's minute: smaller parts
        this.#partBytes = Math.max(this.#minPartBytes, Math.floor(this.#partBytes / 2))
      }
    }
    const baseTree = st === null ? this.#defaultTree : st.treeOid || null
    const tree = await this.client.request<{ sha: string }>('POST', 'git/trees', {
      ...(baseTree !== null && { base_tree: baseTree }),
      tree: [
        ...layout.files.map((f) => ({ path: f.path, mode: '100644', type: 'blob', sha: f.sha })),
        {
          path: MANIFEST_PATH,
          mode: '100644',
          type: 'blob',
          content: new TextDecoder().decode(manifestBytes(layout.manifest)),
        },
        ...change.deletions.map((path) => ({ path, mode: '100644', type: 'blob', sha: null })),
      ],
    })
    const commit = await this.client.request<{ sha: string }>('POST', 'git/commits', {
      message: `${change.headline}\n\n${trailers(change.head)}`,
      tree: tree.sha,
      parents: [st?.gitOid ?? this.#defaultOid],
    })

    const before = st?.gitOid ?? ZERO_OID
    let errors: GraphqlError[]
    try {
      const res = await this.client.graphql(
        UPDATE_REFS,
        {
          repoId: this.#repoId,
          updates: [{ name: `refs/heads/${branch}`, beforeOid: before, afterOid: commit.sha }],
        },
        { mutation: true },
      )
      errors = res.errors
    } catch (err) {
      if (!(err instanceof GithubError) || (err.kind !== 'network' && err.kind !== 'server')) {
        throw err
      }
      const after = await this.#reread(branch, landed)
      if (after.landed) return true
      if (after.oid !== before) return false
      throw err
    }
    if (errors.length === 0) {
      this.#refs.set(branch, { gitOid: commit.sha, treeOid: tree.sha, manifest: layout.manifest })
      return true
    }
    // GitHub reports a refused ref update with a generic error: the
    // re-read says whether it landed after all (an earlier attempt
    // whose response was lost), lost a race, or failed.
    const after = await this.#reread(branch, landed)
    if (after.landed) return true
    if (after.oid !== before) return false
    throw graphqlFailure('push', errors)
  }

  /** The change's manifest and files, its new packs one file each
   *  (`partBytes` null) or cut into parts of at most `partBytes`. */
  async #layout(branch: string, change: Change, partBytes: number | null): Promise<Layout> {
    const entries: PackEntry[] = []
    const files: PackFile[] = []
    for (const p of change.packs) {
      if (!isNew(p)) {
        entries.push(p)
        continue
      }
      const cuts: Uint8Array[] = []
      const step = partBytes ?? p.bytes.length
      for (let at = 0; at < p.bytes.length; at += step) cuts.push(p.bytes.subarray(at, at + step))
      const parts: string[] = []
      for (const [i, bytes] of cuts.entries()) {
        const sha = await gitBlobSha1(bytes)
        parts.push(sha)
        files.push({ path: cuts.length === 1 ? p.path : `${p.path}.${i}`, bytes, sha, pack: p })
      }
      const { bytes, ...entry } = p
      entries.push({ ...entry, bytes: bytes.length, parts })
    }
    const manifest: PackManifest = {
      format: PACK_FORMAT,
      kernel: this.#kernel,
      branch,
      head: change.head,
      packs: entries,
      nextSeq: change.nextSeq,
      meta: change.meta ?? null,
    }
    return { manifest, files }
  }

  /** Upload the files GitHub doesn't already hold (a push that stopped
   *  partway left some), one blob each. */
  async #upload(files: PackFile[], change: Change): Promise<void> {
    const held = await this.#held([...new Set(files.map((f) => f.sha))])
    for (const f of files) {
      if (held.has(f.sha)) continue
      try {
        await this.client.createBlob(f.bytes)
      } catch (err) {
        if (!(err instanceof GithubError) || err.kind !== 'too-large') throw err
        const part = f.bytes.length === f.pack.bytes.length ? 'the pack' : 'a part of the pack'
        throw new GithubError(
          err.kind,
          err.status,
          `PackRemote.push: ${part} of ${change.what} (${formatBytes(f.bytes.length)} of ${formatBytes(f.pack.bytes.length)} compressed) didn't reach GitHub: ${err.message}`,
          err.body,
        )
      }
      held.add(f.sha)
    }
  }

  /** Which of `shas` the repo holds, referenced or not: one query per
   *  `LOOKUP_CHUNK`. */
  async #held(shas: string[]): Promise<Set<string>> {
    const out = new Set<string>()
    for (let i = 0; i < shas.length; i += LOOKUP_CHUNK) {
      const batch = shas.slice(i, i + LOOKUP_CHUNK)
      const vars: Record<string, string> = { owner: this.#owner, name: this.#name }
      batch.forEach((sha, j) => {
        vars[`o${j}`] = sha
      })
      const res = await this.client.graphql<{ repository: Record<string, unknown> }>(
        OBJECTS_QUERY(batch.length),
        vars,
      )
      const repo = expectData(res, 'object lookup').repository
      batch.forEach((sha, j) => {
        if (repo[`o${j}`] != null) out.add(sha)
      })
    }
    return out
  }

  /** Re-read `branch` after an unclear outcome: whether `landed` holds
   *  of its manifest, and its git commit now (zeros when it is gone). */
  async #reread(
    branch: string,
    landed: (m: PackManifest) => boolean,
  ): Promise<{ landed: boolean; oid: string }> {
    const st = await this.#readOne(branch)
    return {
      landed: st?.manifest != null && landed(st.manifest),
      oid: st?.gitOid ?? ZERO_OID,
    }
  }

  // -------------------------------------------------------------------------
  // Lifecycle: atomic, CAS-checked ref renames via updateRefs
  // -------------------------------------------------------------------------

  /** Rename `branch` to `archived/<branch>` at the oid last observed.
   *  False when the live ref is gone or moved (re-read and retry). */
  async archiveBranch(branch: string): Promise<boolean> {
    return this.#rename(branch, `${ARCHIVED}${branch}`)
  }

  /** Rename `archived/<branch>` back to `branch`. False when either
   *  side isn't as expected. */
  async restoreBranch(branch: string): Promise<boolean> {
    return this.#rename(`${ARCHIVED}${branch}`, branch)
  }

  /** Delete `branch` if its kvgit head is still `expectedHead`, in one
   *  CAS-checked `updateRefs` at the git commit holding that head.
   *  False when it moved or is gone. Its packs go with it. */
  async deleteBranch(branch: string, expectedHead: string): Promise<boolean> {
    const st = await this.#readOne(branch)
    if (st?.manifest?.head !== expectedHead) return false
    const ok = await this.#updateRefs([
      { name: `refs/heads/${branch}`, beforeOid: st.gitOid, afterOid: ZERO_OID },
    ])
    if (ok) this.#refs.delete(branch)
    return ok
  }

  /** Delete every archived session in one request. */
  async emptyTrash(): Promise<number> {
    await this.refresh()
    const doomed = [...this.#refs].filter(([name]) => name.startsWith(ARCHIVED))
    if (doomed.length === 0) return 0
    const ok = await this.#updateRefs(
      doomed.map(([name, st]) => ({
        name: `refs/heads/${name}`,
        beforeOid: st.gitOid,
        afterOid: ZERO_OID,
      })),
    )
    if (!ok) return 0
    for (const [name] of doomed) this.#refs.delete(name)
    return doomed.length
  }

  async #rename(from: string, to: string): Promise<boolean> {
    const st = await this.#readOne(from)
    if (st === null) return false
    const ok = await this.#updateRefs([
      { name: `refs/heads/${to}`, beforeOid: ZERO_OID, afterOid: st.gitOid },
      { name: `refs/heads/${from}`, beforeOid: st.gitOid, afterOid: ZERO_OID },
    ])
    if (ok) {
      this.#refs.delete(from)
      this.#refs.set(to, st)
    }
    return ok
  }

  /** Apply ref updates atomically: false when a ref wasn't where it
   *  was expected (a lost race; the caller re-reads). GitHub reports a
   *  rejected batch with a generic error, so after an error the refs
   *  are re-read: every ref at its `afterOid` means the batch landed
   *  (an earlier attempt whose response was lost); one moved anywhere
   *  else, a lost race; none moved, a failure (a token's permission, a
   *  malformed request), which throws. */
  async #updateRefs(
    updates: Array<{ name: string; beforeOid: string; afterOid: string }>,
  ): Promise<boolean> {
    if (this.#repoId === null) await this.refresh()
    let errors: GraphqlError[]
    let failure: unknown = null
    try {
      errors = (
        await this.client.graphql(
          UPDATE_REFS,
          { repoId: this.#repoId, updates },
          { mutation: true },
        )
      ).errors
    } catch (err) {
      if (!(err instanceof GithubError) || (err.kind !== 'network' && err.kind !== 'server')) {
        throw err
      }
      failure = err
      errors = []
    }
    if (failure === null && errors.length === 0) return true
    const now: string[] = []
    for (const u of updates) {
      const st = await this.#readOne(u.name.slice('refs/heads/'.length))
      now.push(st?.gitOid ?? ZERO_OID)
    }
    if (updates.every((u, i) => now[i] === u.afterOid)) return true
    if (updates.some((u, i) => now[i] !== u.beforeOid)) return false
    throw failure ?? graphqlFailure('updateRefs', errors)
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  async #readOne(branch: string): Promise<BranchState | null> {
    const res = await this.client.graphql<{ repository: RepoHeader & { ref: RefNode | null } }>(
      ONE_REF_QUERY,
      { owner: this.#owner, name: this.#name, q: `refs/heads/${branch}` },
    )
    const repo = expectData(res, 'read ref').repository
    this.#repoHeader(repo)
    if (repo.ref === null) {
      this.#refs.delete(branch)
      return null
    }
    return this.#remember(repo.ref, branch)
  }

  #repoHeader(repo: RepoHeader): void {
    this.#repoId = repo.id
    this.#defaultOid = repo.defaultBranchRef?.target.oid ?? null
    this.#defaultTree = repo.defaultBranchRef?.target.tree?.oid ?? null
    this.#defaultName = repo.defaultBranchRef?.name ?? null
  }

  #remember(node: RefNode, branch = node.name): BranchState {
    const file = node.target.file
    const st: BranchState = {
      gitOid: node.target.oid,
      treeOid: node.target.tree?.oid ?? '',
      manifest: null,
    }
    if (file != null) {
      // a manifest that is there but unusable is never treated as
      // absent: the next creation would claim the branch over it
      const read = readManifest(file.object?.text)
      if (typeof read === 'string') st.unreadable = read
      else st.manifest = read
    }
    this.#refs.set(branch, st)
    return st
  }

  #findHead(want: string): { gitOid: string; manifest: PackManifest } | null {
    for (const [name, st] of this.#refs) {
      if (name.startsWith(ARCHIVED) || st.manifest?.head !== want) continue
      return { gitOid: st.gitOid, manifest: st.manifest }
    }
    return null
  }

  /** Whether `commit` is one of `have` or reachable from one, here. */
  async #holds(have: readonly string[], commit: string): Promise<boolean> {
    for (const h of have) if (await isAncestor(this.#store, commit, h)) return true
    return false
  }
}

async function newPack(
  seq: number,
  base: string | null,
  tip: string,
  commits: number,
  bytes: Uint8Array,
): Promise<NewPack> {
  return {
    path: `${PACKS_DIR}/${String(seq).padStart(6, '0')}-${tip.slice(0, 12)}.pack`,
    seq,
    base,
    tip,
    commits,
    bytes,
    sha256: await sha256Hex(bytes),
  }
}

const isNew = (p: PackEntry | NewPack): p is NewPack => p.bytes instanceof Uint8Array

/** The files a manifest entry is stored in. */
function filesOf(entry: PackEntry): string[] {
  return entry.parts.length === 1 ? [entry.path] : entry.parts.map((_, i) => `${entry.path}.${i}`)
}

/** How large an inline commit request for `layout` is. */
function inlineSize(layout: Layout): number {
  let n = REQUEST_OVERHEAD + base64Length(manifestBytes(layout.manifest).length)
  for (const f of layout.files) n += f.path.length + base64Length(f.bytes.length)
  return n
}

const manifestBytes = (manifest: PackManifest): Uint8Array => utf8(JSON.stringify(manifest))

const trailers = (head: string): string => `Kvgit-Head: ${head}\nKvgit-Pack-Format: ${PACK_FORMAT}`

function concat(chunks: Uint8Array[]): Uint8Array {
  if (chunks.length === 1) return chunks[0] as Uint8Array
  const out = new Uint8Array(chunks.reduce((n, c) => n + c.length, 0))
  let at = 0
  for (const c of chunks) {
    out.set(c, at)
    at += c.length
  }
  return out
}

/** The lowest generation among `commits`; 0 if one doesn't say. */
function lowestGeneration(commits: readonly WireCommit[]): number {
  let low = Number.POSITIVE_INFINITY
  for (const wc of commits) {
    const g = wc.parentGens === null ? 0 : generationOf(wc.parentGens)
    if (g < low) low = g
  }
  return low
}

/** A manifest from its text, or why it can't be used. A format-1
 *  manifest reads as format 2: each pack one part, its `blob`. */
function readManifest(text: string | null | undefined): PackManifest | string {
  if (typeof text !== 'string') return 'its manifest could not be read as text'
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return 'its manifest is not JSON'
  }
  const format = (parsed as { format?: unknown } | null)?.format
  if (format !== 1 && format !== PACK_FORMAT) {
    return `its manifest is format ${JSON.stringify(format)}, not 1 or ${PACK_FORMAT}`
  }
  const manifest = parsed as PackManifest
  if (format === 1) {
    manifest.packs = (manifest.packs as Array<PackEntry & { blob: string }>).map(
      ({ blob, ...entry }) => ({ ...entry, parts: [blob] }),
    )
  }
  return manifest
}

/** The data, or a throw. A branch without a manifest (`main`, a
 *  creation that never got its commit) reports NOT_FOUND on its
 *  `file` field beside the data; that one is expected. */
function expectData<T>(res: { data: T | null; errors: GraphqlError[] }, what: string): T {
  const errors = res.errors.filter((e) => !(e.type === 'NOT_FOUND' && e.path?.at(-1) === 'file'))
  if (res.data === null || errors.length > 0) throw graphqlFailure(what, errors)
  return res.data
}

/**
 * Whether an error from a commit mutation means the CAS lost. GitHub
 * says so two ways: `STALE_DATA` when the branch had already moved
 * when the request was checked, and `FORBIDDEN` "is at X but expected
 * Y" when it moved while the request ran (a true race; observed live).
 * A creation that finds its ref already made has lost too.
 */
function lostRace(e: GraphqlError): boolean {
  if (e.type === 'STALE_DATA') return true
  if (e.type === 'FORBIDDEN' && /is at [0-9a-f]{40} but expected [0-9a-f]{40}/.test(e.message)) {
    return true
  }
  return e.path?.[0] === 'create' && /already exists/i.test(e.message)
}

function graphqlFailure(what: string, errors: GraphqlError[]): GithubError {
  const message = errors.map((e) => `${e.type ?? 'ERROR'}: ${e.message}`).join('; ')
  return new GithubError('validation', 200, `PackRemote ${what}: ${message || 'no data'}`)
}

const utf8 = (text: string): Uint8Array => new TextEncoder().encode(text)

/** How long `n` bytes are as base64. */
const base64Length = (n: number): number => Math.ceil(n / 3) * 4

/** `apiBase` with its scheme and host lowered (they ignore case) and no
 *  trailing slash; its path as given. */
function apiRoot(apiBase: string): string {
  let root = apiBase
  try {
    const url = new URL(apiBase)
    root = `${url.protocol}//${url.host}${url.pathname}`
  } catch {
    // not a URL: as given
  }
  return root.replace(/\/+$/, '')
}
