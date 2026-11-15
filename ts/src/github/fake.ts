/**
 * An in-memory stand-in for the slice of GitHub the pack transport
 * uses: the GraphQL documents `PackRemote` sends (roster / one-ref
 * queries, `object(oid:)` lookups, `createRef` +
 * `createCommitOnBranch`, `updateRefs`), the REST git-data writes
 * (`git/blobs`, `git/trees`, `git/commits`) and the raw blob reads.
 * Commits and trees are flat path → bytes snapshots; blob SHAs are
 * git's, commit and tree oids are counters. A simulated uplink
 * (`uplinkBytesPerSecond`) makes requests take time on the fake's own
 * clock, and GitHub's body deadlines apply to them: a GraphQL body
 * still arriving at 5 s gets a 499, a REST one at 60 s a 401. For
 * tests: in-process through
 * {@link FakeGithub.client} (or {@link FakeGithub.fetch} as a client's
 * `fetchImpl`), or over HTTP with `serveFakeGithub`
 * (`@agex-ts/kvgit/github/fake-server`), so a browser can sync
 * against it.
 *
 * Dispatch is by the operation's shape (which root fields the document
 * names), which is enough for the documents the transport sends. It
 * holds everything in memory, and keeps no history of deleted refs.
 */

import { base64ToBytes } from './base64'
import { GithubClient } from './client'
import { gitBlobSha1 } from './git-hash'

interface Commit {
  oid: string
  parent: string | null
  files: Map<string, Uint8Array>
  tree: string
  message: string
}

export interface FakeCall {
  /** `rest`: a REST write (`detail` names it: `blob`, `tree`, `commit`). */
  kind: 'graphql-query' | 'graphql-mutation' | 'raw' | 'rest'
  detail: string
}

export interface FakeGithubOptions {
  /** The repo it serves, `owner/name`. Default `o/r`. */
  repo?: string
  /**
   * The largest request body it takes, in bytes; a larger one is
   * answered 413 without being read. GitHub refuses requests much past
   * 30 MB (its exact limit and answer aren't documented). Default
   * unlimited.
   */
  maxBodyBytes?: number
  /**
   * How fast request bodies arrive, in bytes a second. Each request
   * advances the fake's clock ({@link FakeGithub.now}) by its body's
   * time, and GitHub's deadlines apply: a GraphQL body still arriving
   * at 5 s is answered 499 and a REST one at 60 s 401 "Bad
   * credentials", neither applied. Default unlimited: no time passes.
   */
  uplinkBytesPerSecond?: number
}

/** A failure the fake answers with in place of the next requests. */
export interface FakeFailure {
  status: number
  /** The response's JSON body. Default `{ message: 'injected failure' }`. */
  body?: unknown
  /** Its headers (`retry-after`, say). */
  headers?: Record<string, string>
  /** Only requests whose path ends with this (`/graphql`, say). */
  path?: string
}

const ZERO = '0'.repeat(40)
/** The node id of the fake's repository. */
const REPO_ID = 'R_1'
/** When GitHub stops waiting for a request's body (measured). */
const GRAPHQL_DEADLINE_MS = 5_000
const REST_DEADLINE_MS = 60_000
const enc = new TextEncoder()
const dec = new TextDecoder()

let counter = 0
const nextOid = (): string => (++counter).toString(16).padStart(40, 'c')

export class FakeGithub {
  readonly repo: string
  maxBodyBytes: number
  uplinkBytesPerSecond: number
  readonly commits = new Map<string, Commit>()
  /** Tree oid → its files. */
  readonly trees = new Map<string, Map<string, Uint8Array>>()
  /** Full ref name (`refs/heads/x`) → oid. */
  readonly refs = new Map<string, string>()
  readonly calls: FakeCall[] = []
  /** git blob SHA-1 → bytes, for every blob ever made, referenced or
   *  not (GitHub keeps unreferenced objects a while; see `prune`). */
  readonly blobs = new Map<string, Uint8Array>()
  private pending: Uint8Array[] = []
  /** The git SHA of each file's bytes, once hashed. */
  readonly #shaOf = new WeakMap<Uint8Array, string>()
  #clock = 0
  /** Called before a mutation applies: lets a test move refs under it. */
  beforeMutation: (() => void) | null = null
  /** Report a lost CAS as GitHub does when the ref moves while the
   *  request runs (FORBIDDEN "is at X but expected Y"), rather than
   *  when it had already moved (STALE_DATA). */
  raceStyle: 'stale' | 'forbidden' = 'stale'
  /** An error `updateRefs` answers with, refs unchanged (a token
   *  without the permission, say), in place of applying it. */
  updateRefsError: string | null = null
  /** Failures to answer the next matching requests with, in order. */
  readonly #failures: FakeFailure[] = []

  constructor(opts: FakeGithubOptions = {}) {
    this.repo = opts.repo ?? 'o/r'
    this.maxBodyBytes = opts.maxBodyBytes ?? Number.POSITIVE_INFINITY
    this.uplinkBytesPerSecond = opts.uplinkBytesPerSecond ?? Number.POSITIVE_INFINITY
    const root = this.#commit(null, new Map([['README.md', enc.encode('# sync\n')]]), 'init')
    this.refs.set('refs/heads/main', root.oid)
  }

  /** The fake's clock, in ms: advanced by request bodies on a
   *  simulated uplink, and by its clients' sleeps. */
  now(): number {
    return this.#clock
  }

  /** A client on this fake, in-process, retrying nothing and waiting
   *  for nothing: its clock is the fake's, which a sleep advances. */
  client(): GithubClient {
    return new GithubClient({
      token: 'tok',
      repo: this.repo,
      fetchImpl: this.fetch,
      maxRetries: 0,
      writeIntervalMs: 0,
      sleeper: async (ms) => {
        this.#clock += ms
      },
      now: () => this.#clock,
    })
  }

  /** Forget every blob no ref's history names, as GitHub's garbage
   *  collection eventually does with unreferenced objects. */
  async prune(): Promise<void> {
    await this.#hashPending()
    const kept = new Set<string>()
    for (const oid of this.refs.values()) {
      for (let c = this.commits.get(oid); c !== undefined; ) {
        for (const bytes of c.files.values()) kept.add(this.#shaOf.get(bytes) as string)
        c = c.parent === null ? undefined : this.commits.get(c.parent)
      }
    }
    for (const sha of this.blobs.keys()) if (!kept.has(sha)) this.blobs.delete(sha)
  }

  /** Point `branch` at `oid` (a ref made by hand). */
  setRef(branch: string, oid: string): void {
    this.refs.set(`refs/heads/${branch}`, oid)
  }

  /** Put a commit holding `files` (text or bytes) on `branch`, as another
   *  writer would: on top of its head, or of main's for a new branch. */
  plant(branch: string, files: Record<string, string | Uint8Array>): void {
    const ref = `refs/heads/${branch}`
    const parent = this.refs.get(ref) ?? (this.refs.get('refs/heads/main') as string)
    const next = new Map((this.commits.get(parent) as Commit).files)
    for (const [path, data] of Object.entries(files)) {
      next.set(path, typeof data === 'string' ? enc.encode(data) : data)
    }
    this.refs.set(ref, this.#commit(parent, next, 'planted').oid)
  }

  /** The commit message at a branch's head. */
  messageAt(branch: string): string | undefined {
    const oid = this.refs.get(`refs/heads/${branch}`)
    return oid === undefined ? undefined : this.commits.get(oid)?.message
  }

  #tree(files: Map<string, Uint8Array>): string {
    const oid = nextOid()
    this.trees.set(oid, files)
    for (const bytes of files.values()) if (!this.#shaOf.has(bytes)) this.pending.push(bytes)
    return oid
  }

  /** A commit of `files`, on `tree` when given (a REST commit names
   *  the tree it commits), else on a new tree of them. */
  #commit(
    parent: string | null,
    files: Map<string, Uint8Array>,
    message: string,
    tree = this.#tree(files),
  ): Commit {
    const commit: Commit = { oid: nextOid(), parent, files, tree, message }
    this.commits.set(commit.oid, commit)
    return commit
  }

  /**
   * Answer the next `times` requests (default 1) matching
   * `failure.path` with `failure` instead: a rate limit, a server
   * error, a refused token.
   */
  failNext(failure: FakeFailure, times = 1): void {
    for (let i = 0; i < times; i++) this.#failures.push(failure)
  }

  count(kind: FakeCall['kind']): number {
    return this.calls.filter((c) => c.kind === kind).length
  }

  /** Paths of the files at a branch's head. */
  filesAt(branch: string): string[] {
    const oid = this.refs.get(`refs/heads/${branch}`)
    return oid === undefined ? [] : [...(this.commits.get(oid) as Commit).files.keys()].sort()
  }

  /** Answer one request, as `fetch` would: a client's `fetchImpl`. */
  readonly fetch = (async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init)
    const body =
      request.method === 'GET' || request.method === 'HEAD' ? undefined : await request.text()
    return this.#handle(request.method, request.url, body)
  }) as typeof fetch

  async #handle(method: string, url: string, body: string | undefined): Promise<Response> {
    await this.#hashPending()
    const path = new URL(url, 'http://fake').pathname
    const at = this.#failures.findIndex((f) => f.path === undefined || path.endsWith(f.path))
    if (at >= 0) {
      const [failure] = this.#failures.splice(at, 1) as [FakeFailure]
      return json(failure.status, failure.body ?? { message: 'injected failure' }, failure.headers)
    }
    const size = body === undefined ? 0 : enc.encode(body).byteLength
    if (size > this.maxBodyBytes) {
      return json(413, { message: 'Request body too large' })
    }
    const graphql = path.endsWith('/graphql')
    if (size > 0) {
      const ms = (size / this.uplinkBytesPerSecond) * 1000
      const deadline = graphql ? GRAPHQL_DEADLINE_MS : REST_DEADLINE_MS
      this.#clock += Math.min(ms, deadline)
      if (ms > deadline) {
        return graphql
          ? new Response('', { status: 499 })
          : json(401, { message: 'Bad credentials' })
      }
    }
    if (graphql) {
      const { query, variables } = JSON.parse(body as string) as {
        query: string
        variables: Record<string, unknown>
      }
      const answer = this.graphql(query, variables)
      await this.#hashPending()
      return json(200, answer)
    }
    const git = `/repos/${this.repo}/git/`
    const route = path.startsWith(git) ? path.slice(git.length) : ''
    if (method === 'POST' && ['blobs', 'trees', 'commits'].includes(route)) {
      const answer = await this.#rest(route, JSON.parse(body as string) as Record<string, unknown>)
      await this.#hashPending()
      return answer
    }
    const sha = route.startsWith('blobs/') ? route.slice('blobs/'.length) : ''
    if (method === 'GET' && /^[0-9a-f]{40}$/.test(sha)) {
      this.calls.push({ kind: 'raw', detail: sha })
      const bytes = this.blobs.get(sha)
      if (bytes === undefined) return json(404, { message: 'Not Found' })
      return new Response(bytes as Uint8Array<ArrayBuffer>, { status: 200 })
    }
    return json(404, { message: `no route for ${method} ${url}` })
  }

  async #hashPending(): Promise<void> {
    for (const bytes of this.pending.splice(0)) {
      if (this.#shaOf.has(bytes)) continue
      const sha = await gitBlobSha1(bytes)
      this.#shaOf.set(bytes, sha)
      this.blobs.set(sha, bytes)
    }
  }

  /** `POST git/blobs`, `git/trees` or `git/commits`, as GitHub answers
   *  them (201 with the new object's `sha`, 422 for what it refuses). */
  async #rest(route: string, body: Record<string, unknown>): Promise<Response> {
    const refuse = (message: string) => json(422, { message })
    if (route === 'blobs') {
      this.calls.push({ kind: 'rest', detail: 'blob' })
      const content = body.content
      if (typeof content !== 'string') return refuse('Invalid request: content is required')
      const bytes = body.encoding === 'base64' ? base64ToBytes(content) : enc.encode(content)
      const sha = await gitBlobSha1(bytes)
      this.#shaOf.set(bytes, sha)
      this.blobs.set(sha, bytes)
      return json(201, { sha, url: `https://api.github.com/repos/${this.repo}/git/blobs/${sha}` })
    }
    if (route === 'trees') {
      this.calls.push({ kind: 'rest', detail: 'tree' })
      const base = body.base_tree
      const start = base === undefined || base === null ? new Map() : this.trees.get(String(base))
      if (start === undefined) return refuse('Invalid tree info: base_tree not found')
      const files = new Map(start)
      for (const e of (body.tree ?? []) as Array<Record<string, unknown>>) {
        const path = e.path as string
        if (e.type !== 'blob' || e.mode !== '100644') return refuse(`Invalid tree entry: ${path}`)
        if ('content' in e) files.set(path, enc.encode(e.content as string))
        else if (e.sha === null) {
          if (!files.delete(path)) return refuse(`Invalid tree info: ${path} not in base_tree`)
        } else {
          const bytes = this.blobs.get(e.sha as string)
          if (bytes === undefined) return refuse(`Invalid tree info: ${String(e.sha)} not found`)
          files.set(path, bytes)
        }
      }
      return json(201, { sha: this.#tree(files) })
    }
    this.calls.push({ kind: 'rest', detail: 'commit' })
    const files = this.trees.get(body.tree as string)
    if (files === undefined) return refuse('Invalid tree: not found')
    const parents = (body.parents ?? []) as string[]
    if (parents.some((p) => !this.commits.has(p))) return refuse('Invalid parent: not found')
    const commit = this.#commit(
      parents[0] ?? null,
      files,
      String(body.message ?? ''),
      body.tree as string,
    )
    return json(201, { sha: commit.oid, tree: { sha: commit.tree } })
  }

  /** Whether the repo holds a git object by this oid (a blob, tree or
   *  commit), referenced or not. */
  #holds(oid: string): boolean {
    return this.blobs.has(oid) || this.trees.has(oid) || this.commits.has(oid)
  }

  private graphql(query: string, vars: Record<string, unknown>): unknown {
    const wrong = this.#otherRepo(vars)
    if (wrong !== null) {
      return {
        data: null,
        errors: [
          {
            type: 'NOT_FOUND',
            message: `Could not resolve to a Repository with the name '${wrong}'.`,
          },
        ],
      }
    }
    if (query.trimStart().startsWith('mutation')) {
      this.beforeMutation?.()
      if (query.includes('updateRefs(')) {
        this.calls.push({ kind: 'graphql-mutation', detail: 'updateRefs' })
        return this.updateRefs(vars.updates as Array<Record<string, string>>)
      }
      this.calls.push({ kind: 'graphql-mutation', detail: 'commit' })
      return this.commitMutation(query, vars)
    }
    if (query.includes('refs(refPrefix')) {
      this.calls.push({ kind: 'graphql-query', detail: 'roster' })
      const nodes = [...this.refs]
        .filter(([name]) => name.startsWith('refs/heads/'))
        .map(([name, oid]) => this.refNode(name, oid))
      return {
        data: {
          repository: {
            ...this.header(),
            refs: { pageInfo: { hasNextPage: false, endCursor: 'x' }, nodes },
          },
        },
        ...this.missingManifests(nodes, (i) => [
          'repository',
          'refs',
          'nodes',
          i,
          'target',
          'file',
        ]),
      }
    }
    if (query.includes('object(oid:')) {
      this.calls.push({ kind: 'graphql-query', detail: 'objects' })
      const found: Record<string, unknown> = {}
      for (const m of query.matchAll(/(\w+)\s*:\s*object\(oid:\s*\$(\w+)\)/g)) {
        const oid = vars[m[2] as string] as string
        found[m[1] as string] = this.#holds(oid) ? { oid } : null
      }
      return { data: { repository: found } }
    }
    if (query.includes('ref(qualifiedName')) {
      this.calls.push({ kind: 'graphql-query', detail: 'ref' })
      const name = vars.q as string
      const oid = this.refs.get(name)
      const node = oid === undefined ? null : this.refNode(name, oid)
      return {
        data: { repository: { ...this.header(), ref: node } },
        ...this.missingManifests(node === null ? [] : [node], () => [
          'repository',
          'ref',
          'target',
          'file',
        ]),
      }
    }
    throw new Error(`FakePackGithub: unhandled query ${query.slice(0, 80)}`)
  }

  /**
   * The repo a request's variables name, if it isn't this fake's (by
   * `owner` and `name`, `repo` as `owner/name`, or `repoId`): GitHub's
   * names ignore case. Null if they name this one, or none.
   */
  #otherRepo(vars: Record<string, unknown>): string | null {
    const mine = this.repo.toLowerCase()
    if (typeof vars.owner === 'string' || typeof vars.name === 'string') {
      const named = `${String(vars.owner)}/${String(vars.name)}`
      if (named.toLowerCase() !== mine) return named
    }
    if (typeof vars.repo === 'string' && vars.repo.toLowerCase() !== mine) return vars.repo
    if (typeof vars.repoId === 'string' && vars.repoId !== REPO_ID) return vars.repoId
    return null
  }

  /** GitHub reports a missing `Commit.file` as a NOT_FOUND error
   *  beside the data. */
  private missingManifests(
    nodes: unknown[],
    pathOf: (i: number) => Array<string | number>,
  ): { errors?: unknown[] } {
    const errors = nodes.flatMap((node, i) =>
      (node as { target: { file: unknown } }).target.file === null
        ? [
            {
              type: 'NOT_FOUND',
              path: pathOf(i),
              message: "Could not resolve file for path 'kvgit/manifest.json'.",
            },
          ]
        : [],
    )
    return errors.length > 0 ? { errors } : {}
  }

  private header(): Record<string, unknown> {
    const oid = this.refs.get('refs/heads/main') as string
    return {
      id: REPO_ID,
      defaultBranchRef: {
        name: 'main',
        target: { oid, tree: { oid: (this.commits.get(oid) as Commit).tree } },
      },
    }
  }

  private refNode(name: string, oid: string): unknown {
    const commit = this.commits.get(oid)
    const manifest = commit?.files.get('kvgit/manifest.json')
    return {
      id: `REF_${name}`,
      name: name.slice('refs/heads/'.length),
      target: {
        oid,
        tree: { oid: commit?.tree },
        file: manifest === undefined ? null : { object: { text: dec.decode(manifest) } },
      },
    }
  }

  private commitMutation(query: string, vars: Record<string, unknown>): unknown {
    const errors: Array<Record<string, unknown>> = []
    const data: Record<string, unknown> = {}
    if (query.includes('createRef(')) {
      const ref = vars.ref as string
      if (this.refs.has(ref)) {
        errors.push({
          type: 'UNPROCESSABLE',
          path: ['create'],
          message: `A ref named "${ref}" already exists in the repository.`,
        })
        data.create = null
      } else {
        this.refs.set(ref, vars.oid as string)
        data.create = { ref: { id: `REF_${ref}` } }
      }
    }
    const ref = `refs/heads/${vars.branch as string}`
    const head = this.refs.get(ref)
    if (head === undefined || head !== vars.oid) {
      errors.push(
        this.raceStyle === 'forbidden' && head !== undefined
          ? {
              type: 'FORBIDDEN',
              path: ['commit'],
              message: `is at ${head} but expected ${vars.oid as string}`,
            }
          : {
              type: 'STALE_DATA',
              path: ['commit'],
              message: `Expected branch to point to "${vars.oid as string}" but it did not.  Pull and try again.`,
            },
      )
      data.commit = null
      return { data, errors }
    }
    const files = new Map((this.commits.get(head) as Commit).files)
    for (const d of (vars.deletions ?? []) as Array<{ path: string }>) files.delete(d.path)
    for (const a of (vars.additions ?? []) as Array<{ path: string; contents: string }>) {
      files.set(a.path, base64ToBytes(a.contents))
    }
    const message = `${String(vars.headline)}\n\n${String(vars.body)}`
    const commit = this.#commit(head, files, message)
    this.refs.set(ref, commit.oid)
    data.commit = { commit: { oid: commit.oid, tree: { oid: commit.tree } } }
    return errors.length > 0 ? { data, errors } : { data }
  }

  private updateRefs(updates: Array<Record<string, string>>): unknown {
    if (this.updateRefsError !== null) {
      return { errors: [{ type: 'FORBIDDEN', message: this.updateRefsError }] }
    }
    for (const u of updates) {
      const current = this.refs.get(u.name as string) ?? ZERO
      const target = u.afterOid as string
      if (current !== u.beforeOid || (target !== ZERO && !this.commits.has(target))) {
        return { errors: [{ message: 'Something went wrong while executing your query.' }] }
      }
    }
    for (const u of updates) {
      if (u.afterOid === ZERO) this.refs.delete(u.name as string)
      else this.refs.set(u.name as string, u.afterOid as string)
    }
    return { data: { updateRefs: { clientMutationId: null } } }
  }
}

function json(status: number, body: unknown, headers?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json', ...headers },
  })
}
