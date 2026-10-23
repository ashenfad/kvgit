/**
 * PackRemote: the kvgit `Remote` over a GitHub repo, one compressed
 * pack per push. The formats are specified in the package README
 * ("Pack sync").
 *
 * A push's commits travel as ONE file, landed by ONE GraphQL
 * `createCommitOnBranch` whose `expectedHeadOid` is the
 * compare-and-swap. A session's branch holds:
 *
 *   kvgit/manifest.json              head, pack list, caller metadata
 *   kvgit/packs/<seq>-<tip12>.pack   gzip(CBOR) of a run of WireCommits
 *
 * Costs, per operation:
 *
 * - `listRefs` / `roster`: one GraphQL query for every branch, with
 *   each branch's manifest inlined (`Commit.file`), so the session
 *   list, heads and stub metadata (a title) come back together.
 * - `push`: one GraphQL request (creation folds `createRef` into the
 *   same document). The manifest the push builds on comes from the
 *   `listRefs` the sync layer just made.
 * - `fetch`: one raw blob read per needed pack, concurrently. The
 *   manifest says which packs a receiver needs: walking back from the
 *   newest, until a pack's base is a commit the receiver has.
 * - `archiveBranch` / `restoreBranch` / `deleteBranch` / `emptyTrash`:
 *   one atomic, CAS-checked `updateRefs`.
 *
 * Packs accumulate one per push. When a push would exceed `maxPacks`,
 * it folds the older ones (all but `keepRecent`) into one, re-encoded
 * from the local store, in the same commit: still one request.
 * `repack` folds everything into one pack, so a fresh device's first
 * pull is the roster query plus one read.
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
import { generationOf, sha256Hex } from '../versioned/layout'
import { bytesToBase64 } from './base64'
import type { GithubClient, GraphqlError, GraphqlResult } from './client'
import { GithubError } from './errors'
import { gitBlobSha1 } from './git-hash'

export const PACK_FORMAT = 1
export const MANIFEST_PATH = 'kvgit/manifest.json'
export const PACKS_DIR = 'kvgit/packs'
const ARCHIVED = 'archived/'
const ZERO_OID = '0000000000000000000000000000000000000000'
const GET_CHUNK = 16
/** The most base64 one request may carry: GitHub refuses GraphQL bodies
 *  over 45MB and has timed out on 40MB; 30MB has landed. */
const MAX_BODY_BASE64 = 30 * 1024 * 1024

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
  /** git blob SHA-1 of the stored bytes: fetch reads the blob by it. */
  blob: string
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
  /** Most uncompressed value bytes in one pack (and one request); a
   *  larger push lands as several commits. Default 16 MiB (GitHub
   *  refuses GraphQL bodies over 45MB and times out well before). */
  maxPackBytes?: number
}

interface BranchState {
  refId: string
  gitOid: string
  /** Null for a ref without a manifest file: a creation that never got
   *  its first commit, which the next creation claims. */
  manifest: PackManifest | null
  /** Why the manifest there can't be used (unparseable, a format this
   *  code doesn't know): such a branch is left alone, never claimed. */
  unreadable?: string
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
 * one file sit together and compress against each other.
 */
export async function encodePack(
  commits: readonly WireCommit[],
  opts: { kernel: string; base: string | null; compress?: boolean },
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
    values: slots.map((s) => s.bytes),
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

/** Decode a pack (gzip detected by its magic bytes). */
export async function decodePack(stored: Uint8Array): Promise<DecodedPack> {
  const raw = stored[0] === 0x1f && stored[1] === 0x8b ? await gunzip(stored) : stored
  const pack = cbor.decode(raw) as {
    format: unknown
    kernel: string
    base: string | null
    tip: string
    values: Uint8Array[]
    commits: Array<[Uint8Array, Array<[string, number]>]>
  }
  if (pack.format !== PACK_FORMAT) {
    throw new Error(`decodePack: unsupported format ${JSON.stringify(pack.format)}`)
  }
  const values = pack.values.map((v) => new Uint8Array(v))
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

const REF_FIELDS = `id name target { oid ... on Commit { file(path: "${MANIFEST_PATH}") { object { ... on Blob { text } } } } }`

const ROSTER_QUERY = `query($owner: String!, $name: String!, $after: String) {
  repository(owner: $owner, name: $name) {
    id
    defaultBranchRef { name target { oid } }
    refs(refPrefix: "refs/heads/", first: 100, after: $after) {
      pageInfo { hasNextPage endCursor }
      nodes { ${REF_FIELDS} }
    }
  }
}`

const ONE_REF_QUERY = `query($owner: String!, $name: String!, $q: String!) {
  repository(owner: $owner, name: $name) {
    id
    defaultBranchRef { name target { oid } }
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
  }) { commit { oid } ref { id } }
}`

const UPDATE_REFS = `mutation($repoId: ID!, $updates: [RefUpdate!]!) {
  updateRefs(input: {repositoryId: $repoId, refUpdates: $updates}) { clientMutationId }
}`

interface RefNode {
  id: string
  name: string
  target: { oid: string; file?: { object: { text?: string | null } | null } | null }
}

interface RosterData {
  repository: RepoHeader & {
    refs: { pageInfo: { hasNextPage: boolean; endCursor: string }; nodes: RefNode[] }
  }
}

interface RepoHeader {
  id: string
  defaultBranchRef: { name: string; target: { oid: string } } | null
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
  readonly #owner: string
  readonly #name: string

  #repoId: string | null = null
  #defaultOid: string | null = null
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
    this.#maxPackBytes = opts.maxPackBytes ?? 16 * 1024 * 1024
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

    const stored: Uint8Array[] = []
    for (let i = 0; i < needed.length; i += GET_CHUNK) {
      const part = needed.slice(i, i + GET_CHUNK)
      stored.push(
        ...(await Promise.all(
          part.map(async (pack) => {
            const bytes = await this.client.getBlobRaw(pack.blob)
            if (bytes === null) throw new Error(`PackRemote.fetch: ${pack.path} missing`)
            if ((await sha256Hex(bytes)) !== pack.sha256) {
              throw new Error(`PackRemote.fetch: ${pack.path} fails its checksum`)
            }
            return bytes
          }),
        )),
      )
    }
    // What we hold of the packs' commits, from as far down as their
    // lowest generation: a pack built on what we have can still repeat
    // commits we hold (a fold re-sends its run whole).
    let floor = Number.POSITIVE_INFINITY
    let haveSet = new Set<string>()
    for (const [i, bytes] of stored.entries()) {
      const entry = needed[i] as PackEntry
      const pack = await decodePack(bytes)
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

    // Split oversized pushes; each chunk is its own CAS'd commit.
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

    // encoded and checked up front, so a push too large to land sends
    // nothing rather than its first chunks
    const packs: Uint8Array[] = []
    let base = expectedOld
    for (const chunk of chunks) {
      const bytes = await encodePack(chunk, { kernel: this.#kernel, base })
      if (base64Length(bytes.length) > MAX_BODY_BASE64) {
        throw new Error(
          `PackRemote.push: commit ${(chunk.at(-1) as WireCommit).hash.slice(0, 7)} is ${bytes.length} bytes compressed, more than one request can carry`,
        )
      }
      packs.push(bytes)
      base = (chunk.at(-1) as WireCommit).hash
    }

    base = expectedOld
    for (const [i, chunk] of chunks.entries()) {
      const landed = await this.#pushChunk(
        branch,
        st,
        base,
        chunk,
        packs[i] as Uint8Array,
        i === chunks.length - 1,
      )
      if (landed === null) {
        if (i === 0) return false
        throw new Error(`PackRemote.push: lost the ref after ${i} of ${chunks.length} chunks`)
      }
      st = landed
      base = (chunk.at(-1) as WireCommit).hash
    }
    return true
  }

  /** Land one pack; null when the CAS lost. */
  async #pushChunk(
    branch: string,
    st: BranchState | null,
    base: string | null,
    chunk: WireCommit[],
    bytes: Uint8Array,
    last: boolean,
  ): Promise<BranchState | null> {
    const tip = (chunk.at(-1) as WireCommit).hash
    const prior = st?.manifest ?? null
    const seq = prior?.nextSeq ?? 1
    const entry: PackEntry = {
      path: `${PACKS_DIR}/${String(seq).padStart(6, '0')}-${tip.slice(0, 12)}.pack`,
      seq,
      base,
      tip,
      commits: chunk.length,
      bytes: bytes.length,
      sha256: await sha256Hex(bytes),
      blob: await gitBlobSha1(bytes),
    }
    const additions: Array<{ path: string; contents: string }> = [
      { path: entry.path, contents: bytesToBase64(bytes) },
    ]
    let packs = [...(prior?.packs ?? []), entry]
    let nextSeq = seq + 1
    const deletions: Array<{ path: string }> = []

    // Fold older packs into one when there are too many.
    if (last && packs.length > this.#maxPacks) {
      const folded = await this.#fold(packs.slice(0, packs.length - this.#keepRecent), nextSeq)
      // the fold rides this push's request, so both must fit in it; a
      // fold that doesn't is left for a later, smaller push
      if (
        folded !== null &&
        base64Length(bytes.length) + base64Length(folded.bytes.length) <= MAX_BODY_BASE64
      ) {
        additions.push({ path: folded.entry.path, contents: bytesToBase64(folded.bytes) })
        deletions.push(...folded.replaced.map((p) => ({ path: p.path })))
        const gone = new Set(folded.replaced.map((p) => p.path))
        const firstKept = packs.findIndex((p) => gone.has(p.path))
        packs = packs.filter((p) => !gone.has(p.path))
        packs.splice(firstKept, 0, folded.entry)
        nextSeq++
      }
    }

    const meta = last && this.#meta !== undefined ? await this.#meta(branch, tip) : prior?.meta
    const manifest: PackManifest = {
      format: PACK_FORMAT,
      kernel: this.#kernel,
      branch,
      head: tip,
      packs,
      nextSeq,
      meta: meta ?? null,
    }
    additions.push({ path: MANIFEST_PATH, contents: bytesToBase64(utf8(JSON.stringify(manifest))) })

    const create = st === null
    const res = await this.client.graphql<{
      create?: { ref: { id: string } } | null
      commit: { commit: { oid: string }; ref: { id: string } } | null
    }>(
      COMMIT_MUTATION(create),
      {
        ...(create && { repoId: this.#repoId, ref: `refs/heads/${branch}` }),
        repo: this.client.repo,
        branch,
        oid: st?.gitOid ?? this.#defaultOid,
        headline: `kvgit ${branch} @ ${tip.slice(0, 7)} (+${chunk.length})`,
        body: `Kvgit-Head: ${tip}\nKvgit-Pack-Format: ${PACK_FORMAT}`,
        additions,
        deletions,
      },
      { mutation: true },
    )
    const landed = res.data?.commit ?? null
    if (landed === null) {
      const lost = res.errors.some(lostRace)
      if (lost) return null
      throw graphqlFailure('push', res.errors)
    }
    const state = { refId: landed.ref.id, gitOid: landed.commit.oid, manifest }
    this.#refs.set(branch, state)
    return state
  }

  /** Re-encode a run of packs as one, from the local store. Null when
   *  the run is too short or too large to be worth one request. */
  async #fold(
    run: PackEntry[],
    seq: number,
  ): Promise<{ entry: PackEntry; bytes: Uint8Array; replaced: PackEntry[] } | null> {
    // Fold the newest stretch that fits one pack.
    let start = run.length
    let total = 0
    while (start > 0 && total + (run[start - 1] as PackEntry).bytes <= this.#maxPackBytes) {
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
    const entry: PackEntry = {
      path: `${PACKS_DIR}/${String(seq).padStart(6, '0')}-${tip.slice(0, 12)}.pack`,
      seq,
      base,
      tip,
      commits: commits.length,
      bytes: bytes.length,
      sha256: await sha256Hex(bytes),
      blob: await gitBlobSha1(bytes),
    }
    return { entry, bytes, replaced }
  }

  /**
   * Fold every pack of `branch` into one, from the local store (which
   * must hold the remote head). One request; afterwards a fresh
   * device's pull is the roster query plus one read.
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
    if (base64Length(bytes.length) > MAX_BODY_BASE64) {
      throw new Error(
        `PackRemote.repack: '${branch}' is ${bytes.length} bytes as one pack, more than one request can carry`,
      )
    }
    const seq = manifest.nextSeq
    const entry: PackEntry = {
      path: `${PACKS_DIR}/${String(seq).padStart(6, '0')}-${manifest.head.slice(0, 12)}.pack`,
      seq,
      base: null,
      tip: manifest.head,
      commits: commits.length,
      bytes: bytes.length,
      sha256: await sha256Hex(bytes),
      blob: await gitBlobSha1(bytes),
    }
    const next: PackManifest = { ...manifest, packs: [entry], nextSeq: seq + 1 }
    const res = await this.client.graphql<{
      commit: { commit: { oid: string }; ref: { id: string } } | null
    }>(
      COMMIT_MUTATION(false),
      {
        repo: this.client.repo,
        branch,
        oid: st.gitOid,
        headline: `kvgit ${branch} repack (${manifest.packs.length} → 1)`,
        body: `Kvgit-Head: ${manifest.head}\nKvgit-Pack-Format: ${PACK_FORMAT}`,
        additions: [
          { path: entry.path, contents: bytesToBase64(bytes) },
          { path: MANIFEST_PATH, contents: bytesToBase64(utf8(JSON.stringify(next))) },
        ],
        deletions: manifest.packs.map((p) => ({ path: p.path })),
      },
      { mutation: true },
    )
    const landed = res.data?.commit ?? null
    if (landed === null) {
      if (res.errors.some(lostRace)) return false
      throw graphqlFailure('repack', res.errors)
    }
    this.#refs.set(branch, { refId: landed.ref.id, gitOid: landed.commit.oid, manifest: next })
    return true
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
   *  rejected batch with a generic error, so an error is read as a lost
   *  race only if a re-read shows one of the refs moved; any other
   *  (a token's permission, a malformed request) throws. */
  async #updateRefs(
    updates: Array<{ name: string; beforeOid: string; afterOid: string }>,
  ): Promise<boolean> {
    if (this.#repoId === null) await this.refresh()
    const res = await this.client.graphql(
      UPDATE_REFS,
      { repoId: this.#repoId, updates },
      { mutation: true },
    )
    if (res.errors.length === 0) return true
    for (const u of updates) {
      const st = await this.#readOne(u.name.slice('refs/heads/'.length))
      if ((st?.gitOid ?? ZERO_OID) !== u.beforeOid) return false
    }
    throw graphqlFailure('updateRefs', res.errors)
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
    this.#defaultName = repo.defaultBranchRef?.name ?? null
  }

  #remember(node: RefNode, branch = node.name): BranchState {
    const file = node.target.file
    const st: BranchState = { refId: node.id, gitOid: node.target.oid, manifest: null }
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

/** The lowest generation among `commits`; 0 if one doesn't say. */
function lowestGeneration(commits: readonly WireCommit[]): number {
  let low = Number.POSITIVE_INFINITY
  for (const wc of commits) {
    const g = wc.parentGens === null ? 0 : generationOf(wc.parentGens)
    if (g < low) low = g
  }
  return low
}

/** A manifest from its text, or why it can't be used. */
function readManifest(text: string | null | undefined): PackManifest | string {
  if (typeof text !== 'string') return 'its manifest could not be read as text'
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    return 'its manifest is not JSON'
  }
  const format = (parsed as { format?: unknown } | null)?.format
  if (format !== PACK_FORMAT)
    return `its manifest is format ${JSON.stringify(format)}, not ${PACK_FORMAT}`
  return parsed as PackManifest
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
