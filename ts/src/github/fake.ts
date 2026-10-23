/**
 * An in-memory stand-in for the slice of GitHub the pack transport
 * uses: the GraphQL documents `PackRemote` sends (roster / one-ref
 * queries, `createRef` + `createCommitOnBranch`, `updateRefs`) and the
 * raw blob reads. Commits are flat path → bytes snapshots; oids are
 * counters, not git's hashes. For tests: in-process through
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
}

export interface FakeCall {
  kind: 'graphql-query' | 'graphql-mutation' | 'raw'
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
const enc = new TextEncoder()
const dec = new TextDecoder()

let counter = 0
const nextOid = (): string => (++counter).toString(16).padStart(40, 'c')

export class FakeGithub {
  readonly repo: string
  maxBodyBytes: number
  readonly commits = new Map<string, Commit>()
  /** Full ref name (`refs/heads/x`) → oid. */
  readonly refs = new Map<string, string>()
  readonly calls: FakeCall[] = []
  /** git blob SHA-1 → bytes, for every file ever committed. */
  readonly blobs = new Map<string, Uint8Array>()
  private pending: Uint8Array[] = []
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
    const root: Commit = {
      oid: nextOid(),
      parent: null,
      files: new Map([['README.md', enc.encode('# sync\n')]]),
    }
    this.commits.set(root.oid, root)
    this.refs.set('refs/heads/main', root.oid)
  }

  /** A client on this fake, in-process, retrying nothing and waiting for nothing. */
  client(): GithubClient {
    return new GithubClient({
      token: 'tok',
      repo: this.repo,
      fetchImpl: this.fetch,
      maxRetries: 0,
      writeIntervalMs: 0,
      sleeper: async () => {},
    })
  }

  /** Point `branch` at `oid` (a ref made by hand). */
  setRef(branch: string, oid: string): void {
    this.refs.set(`refs/heads/${branch}`, oid)
  }

  /** Put a commit holding `files` (text) on `branch`, as another
   *  writer would: on top of its head, or of main's for a new branch. */
  plant(branch: string, files: Record<string, string>): void {
    const ref = `refs/heads/${branch}`
    const parent = this.refs.get(ref) ?? (this.refs.get('refs/heads/main') as string)
    const base = (this.commits.get(parent) as Commit).files
    const commit: Commit = { oid: nextOid(), parent, files: new Map(base) }
    for (const [path, text] of Object.entries(files)) commit.files.set(path, enc.encode(text))
    this.commits.set(commit.oid, commit)
    this.refs.set(ref, commit.oid)
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
    return this.#handle(request.url, body)
  }) as typeof fetch

  async #handle(url: string, body: string | undefined): Promise<Response> {
    for (const bytes of this.pending.splice(0)) this.blobs.set(await gitBlobSha1(bytes), bytes)
    const path = new URL(url, 'http://fake').pathname
    const at = this.#failures.findIndex((f) => f.path === undefined || path.endsWith(f.path))
    if (at >= 0) {
      const [failure] = this.#failures.splice(at, 1) as [FakeFailure]
      return json(failure.status, failure.body ?? { message: 'injected failure' }, failure.headers)
    }
    if (body !== undefined && new TextEncoder().encode(body).byteLength > this.maxBodyBytes) {
      return json(413, { message: 'Request body too large' })
    }
    if (path.endsWith('/graphql')) {
      const { query, variables } = JSON.parse(body as string) as {
        query: string
        variables: Record<string, unknown>
      }
      return json(200, this.graphql(query, variables))
    }
    const blobs = `/repos/${this.repo}/git/blobs/`
    const sha = path.startsWith(blobs) ? path.slice(blobs.length) : ''
    if (/^[0-9a-f]{40}$/.test(sha)) {
      this.calls.push({ kind: 'raw', detail: sha })
      const bytes = this.blobs.get(sha)
      if (bytes === undefined) return json(404, { message: 'Not Found' })
      return new Response(bytes as Uint8Array<ArrayBuffer>, { status: 200 })
    }
    return json(404, { message: `no route for ${url}` })
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
    return {
      id: REPO_ID,
      defaultBranchRef: { name: 'main', target: { oid: this.refs.get('refs/heads/main') } },
    }
  }

  private refNode(name: string, oid: string): unknown {
    const manifest = this.commits.get(oid)?.files.get('kvgit/manifest.json')
    return {
      id: `REF_${name}`,
      name: name.slice('refs/heads/'.length),
      target: {
        oid,
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
      const bytes = base64ToBytes(a.contents)
      files.set(a.path, bytes)
      this.pending.push(bytes)
    }
    const commit: Commit = { oid: nextOid(), parent: head, files }
    this.commits.set(commit.oid, commit)
    this.refs.set(ref, commit.oid)
    data.commit = { commit: { oid: commit.oid }, ref: { id: `REF_${ref}` } }
    return errors.length > 0 ? { data, errors } : { data }
  }

  private updateRefs(updates: Array<Record<string, string>>): unknown {
    if (this.updateRefsError !== null) {
      return { errors: [{ type: 'FORBIDDEN', message: this.updateRefsError }] }
    }
    for (const u of updates) {
      const current = this.refs.get(u.name as string) ?? ZERO
      if (current !== u.beforeOid) {
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
