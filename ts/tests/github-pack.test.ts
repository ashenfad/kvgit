/**
 * PackRemote (spike) against an in-memory GitHub: one pack per push,
 * fetched by manifest, with CAS on `expectedHeadOid`.
 */

import { Encoder } from 'cbor-x'
import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { FakeGithub } from '../src/github/fake'
import {
  GithubClient,
  GithubError,
  MANIFEST_PATH,
  type PackManifest,
  PackRemote,
  decodePack,
  encodePack,
  gitBlobSha1,
} from '../src/github/index'
import { gunzip, gzip } from '../src/github/pack'
import {
  Repo,
  type Worktree,
  applyWire,
  deleteRemoteBranch,
  pullBranch,
  pushBranch,
  walkDelta,
} from '../src/index'
import type { WireCommit } from '../src/types'
import { sha256Hex } from '../src/versioned/layout'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | null | undefined): string | null => (b == null ? null : dec.decode(b))

const BRANCH = 'chat-1'

async function device(gh: FakeGithub, opts: ConstructorParameters<typeof PackRemote>[2] = {}) {
  const store = new Memory()
  const remote = new PackRemote(gh.client(), store, opts)
  return { store, remote }
}

describe('PackRemote id', () => {
  it('names the repo, case aside, and the API when not github.com', () => {
    const gh = new FakeGithub()
    expect(new PackRemote(gh.client(), new Memory()).id).toBe('github:o/r')
    const client = new GithubClient({
      token: 't',
      repo: 'Owner/Repo',
      apiBase: 'https://ghe.example/api/v3/',
    })
    expect(new PackRemote(client, new Memory()).id).toBe(
      'github:https://ghe.example/api/v3/owner/repo',
    )
  })

  it("lowers the API's scheme and host but keeps its path's case", () => {
    const at = (apiBase: string) =>
      new PackRemote(new GithubClient({ token: 't', repo: 'o/r', apiBase }), new Memory()).id
    expect(at('HTTPS://GHE.Example/GitHub/api/v3')).toBe(
      'github:https://ghe.example/GitHub/api/v3/o/r',
    )
    expect(at('https://ghe.example/GitHub/api/v3')).not.toBe(
      at('https://ghe.example/github/api/v3'),
    )
    expect(at('https://API.GitHub.com/')).toBe('github:o/r')
  })
})

/** A worktree on `branch` in `store`, which it creates if need be. */
async function open(store: Memory, branch = BRANCH): Promise<Worktree> {
  return (await Repo.open(store)).worktree(branch, { create: true })
}

async function commitN(store: Memory, n: number, prefix: string): Promise<Worktree> {
  const wt = await open(store)
  for (let i = 0; i < n; i++) {
    wt.set(`files/${prefix}${i}.txt`, bytes(`${prefix} ${i} `.repeat(20)))
    wt.set('index', bytes(`${prefix}${i}`))
    await wt.commit({ info: { tool: 'file_write', t: 1.5 } })
  }
  return wt
}

/** `n` random bytes, which don't compress: a pack is about their size. */
function random(n: number): Uint8Array {
  const out = new Uint8Array(n)
  for (let i = 0; i < n; i += 65536) crypto.getRandomValues(out.subarray(i, Math.min(n, i + 65536)))
  return out
}

/** Commit `value` at `key` on the branch; the new head. */
async function commitValue(store: Memory, key: string, value: Uint8Array): Promise<string> {
  const wt = await open(store)
  wt.set(key, value)
  await wt.commit()
  return wt.head
}

/** The manifest at a branch's head on the fake. */
function manifestAt(gh: FakeGithub, branch = BRANCH): PackManifest {
  const oid = gh.refs.get(`refs/heads/${branch}`) as string
  const bytes = gh.commits.get(oid)?.files.get(MANIFEST_PATH) as Uint8Array
  return JSON.parse(dec.decode(bytes)) as PackManifest
}

/**
 * A client on `gh` whose fetch passes through `wrap` first, so a test
 * can lose a response or fail a request; it keeps a log of what each
 * request was (`createCommitOnBranch`, `updateRefs`, `POST git/blobs`,
 * …) and runs on a clock `wrap` may advance.
 */
function wrapped(
  gh: FakeGithub,
  wrap: (
    what: string,
    send: () => Promise<Response>,
    clock: { ms: number },
  ) => Promise<Response> = (_, send) => send(),
  opts: { maxRetries?: number } = {},
) {
  const log: string[] = []
  const clock = { ms: 0 }
  const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const body = typeof init?.body === 'string' ? init.body : ''
    const name =
      /createCommitOnBranch|updateRefs/.exec(body)?.[0] ??
      (body.includes('object(oid:') ? 'objects' : null) ??
      (body.startsWith('{"query"') ? 'query' : null) ??
      `${init?.method ?? 'GET'} ${new URL(String(input)).pathname.split('/git/')[1]}`
    log.push(name)
    return wrap(name, () => gh.fetch(input, init), clock)
  }) as typeof fetch
  const client = new GithubClient({
    token: 't',
    repo: gh.repo,
    fetchImpl,
    maxRetries: opts.maxRetries ?? 0,
    writeIntervalMs: 0,
    sleeper: async (ms) => {
      clock.ms += ms
    },
    now: () => clock.ms,
  })
  return { client, log }
}

async function collect(iter: AsyncIterable<WireCommit>): Promise<WireCommit[]> {
  const out: WireCommit[] = []
  for await (const wc of iter) out.push(wc)
  return out
}

describe('pack codec', () => {
  it('round-trips wire commits so applyWire re-derives every hash', async () => {
    const src = new Memory()
    const wt = await commitN(src, 4, 'a')
    await wt.delete('files/a0.txt')
    await wt.commit()
    const commits = await collect(walkDelta(src, { want: wt.head }))
    const pack = await encodePack(commits, { kernel: 'ts', base: null })
    expect(pack[0]).toBe(0x1f) // gzip
    const decoded = await decodePack(pack)
    expect(decoded.tip).toBe(wt.head)
    const dst = new Memory()
    const res = await applyWire(dst, decoded.commits, { createBranch: BRANCH })
    expect(res.head).toBe(wt.head)
    const copy = await open(dst)
    expect(text(await copy.get('files/a3.txt'))).toContain('a 3')
    expect(await copy.get('files/a0.txt')).toBeUndefined()
  })

  it('reads a value kept outside its pack, from the local store when it holds it', async () => {
    const gh = new FakeGithub()
    const src = new Memory()
    const value = random(300_000)
    const head = await commitValue(src, 'files/big.bin', value)
    const sha = await sha256Hex(value)
    const part = await gh.client().createBlob(value)
    const pack = await encodePack(await collect(walkDelta(src, { want: head })), {
      kernel: 'ts',
      base: null,
      refs: new Map([[sha, [sha, value.length, [part]]]]),
    })
    expect(pack.length).toBeLessThan(10_000)
    await expect(decodePack(pack)).rejects.toThrow(/kept outside the pack/)
    const path = `kvgit/packs/000001-${head.slice(0, 12)}.pack`
    const entry = {
      path,
      seq: 1,
      base: null,
      tip: head,
      commits: 2,
      bytes: pack.length,
      sha256: await sha256Hex(pack),
      parts: [await gitBlobSha1(pack)],
    }
    const manifest = {
      format: 2,
      kernel: 'ts',
      branch: BRANCH,
      head,
      packs: [entry],
      nextSeq: 2,
      meta: null,
    }
    gh.plant(BRANCH, { [MANIFEST_PATH]: JSON.stringify(manifest), [path]: pack })

    gh.calls.length = 0
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
    expect(await (await open(b.store)).get('files/big.bin')).toEqual(value)
    expect(gh.count('raw')).toBe(2) // the pack, then the value

    // a store that holds the value already reads only the pack
    gh.calls.length = 0
    const c = await device(gh)
    await c.store.set(`kvgit:blob:${sha}`, value)
    expect((await pullBranch(c.store, c.remote, BRANCH)).localHead).toBe(head)
    expect(gh.count('raw')).toBe(1)
  })

  it('stores a value repeated across commits once', async () => {
    const src = new Memory()
    const wt = await open(src)
    const big = bytes('x'.repeat(5000))
    wt.set('a', big)
    await wt.commit()
    wt.set('b', big)
    await wt.commit()
    const commits = await collect(walkDelta(src, { want: wt.head }))
    const raw = await encodePack(commits, { kernel: 'ts', base: null, compress: false })
    expect(raw.length).toBeLessThan(5000 * 1.5)
  })
})

describe('PackRemote push / fetch', () => {
  it('first push is one roster query and one mutation; fresh pull one query and one read', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await commitN(a.store, 5, 'a')

    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect(gh.count('graphql-query')).toBe(1)
    expect(gh.count('graphql-mutation')).toBe(1)
    expect(gh.filesAt(BRANCH)).toContain(MANIFEST_PATH)

    gh.calls.length = 0
    const b = await device(gh)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.status).toBe('created')
    expect(pulled.localHead).toBe(wt.head)
    expect(pulled.transferred).toBe(6) // root + 5
    expect(gh.count('graphql-query')).toBe(1)
    expect(gh.count('raw')).toBe(1)
    const bWt = await open(b.store)
    expect(text(await bWt.get('index'))).toBe('a4')
  })

  it('incremental push adds one pack; incremental pull reads only it', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 3, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)

    const wt = await commitN(a.store, 4, 'b')
    gh.calls.length = 0
    const pushed = await pushBranch(a.store, a.remote, BRANCH)
    expect(pushed.status).toBe('pushed')
    expect(pushed.transferred).toBe(4)
    expect(gh.count('graphql-mutation')).toBe(1)
    expect(gh.count('graphql-query')).toBe(1) // the sync layer's listRefs; push reuses it

    gh.calls.length = 0
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.status).toBe('fast-forwarded')
    expect(pulled.transferred).toBe(4)
    expect(gh.count('raw')).toBe(1)
    expect(pulled.localHead).toBe(wt.head)
  })

  it('a pull that has part of a pack applies only what it lacks', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 2, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    // B pulls, then A pushes twice more: B needs two packs.
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)
    await commitN(a.store, 2, 'b')
    await pushBranch(a.store, a.remote, BRANCH)
    await commitN(a.store, 2, 'c')
    await pushBranch(a.store, a.remote, BRANCH)
    gh.calls.length = 0
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.transferred).toBe(4)
    expect(gh.count('raw')).toBe(2)
  })

  it('a stale expectedHeadOid loses cleanly: diverged, remote unchanged', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 2, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)

    await commitN(a.store, 1, 'a2')
    await commitN(b.store, 1, 'b2')
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('pushed')
    const before = gh.refs.get(`refs/heads/${BRANCH}`)
    expect((await pushBranch(b.store, b.remote, BRANCH)).status).toBe('diverged')
    expect(gh.refs.get(`refs/heads/${BRANCH}`)).toBe(before)
  })

  it('a race inside the CAS window returns false from push (no partial write)', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await commitN(a.store, 2, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    await a.remote.listRefs()
    const old = wt.head
    await commitN(a.store, 1, 'x')
    // Someone moves the ref between our roster read and our mutation.
    gh.beforeMutation = () => {
      gh.refs.set(`refs/heads/${BRANCH}`, gh.refs.get('refs/heads/main') as string)
      gh.beforeMutation = null
    }
    const commits = walkDelta(a.store, {
      want: (await open(a.store)).head,
      have: [old],
    })
    const ok = await a.remote.push(BRANCH, old, (await open(a.store)).head, commits)
    expect(ok).toBe(false)
  })

  it('the FORBIDDEN shape of a lost race (ref moved mid-request) is a lost CAS too', async () => {
    const gh = new FakeGithub()
    gh.raceStyle = 'forbidden'
    const a = await device(gh)
    const old = (await commitN(a.store, 1, 'a')).head
    await pushBranch(a.store, a.remote, BRANCH)
    await a.remote.listRefs()
    const head = (await commitN(a.store, 1, 'x')).head
    gh.beforeMutation = () => {
      gh.refs.set(`refs/heads/${BRANCH}`, gh.refs.get('refs/heads/main') as string)
      gh.beforeMutation = null
    }
    const pushed = a.remote.push(BRANCH, old, head, walkDelta(a.store, { want: head, have: [old] }))
    expect(await pushed).toBe(false)
  })

  it('creation races: the second device to create loses', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const b = await device(gh)
    await commitN(a.store, 1, 'a')
    await commitN(b.store, 1, 'b')
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect((await pushBranch(b.store, b.remote, BRANCH)).status).toBe('diverged')
  })

  it('splits a push larger than maxPackBytes into several packs, landed in one commit', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPackBytes: 150 })
    const wt = await commitN(a.store, 6, 'a') // ~80 bytes of values per commit
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect(gh.count('graphql-mutation')).toBe(1)
    expect(manifestAt(gh).packs.length).toBeGreaterThan(1)
    const b = await device(gh)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.localHead).toBe(wt.head)
  })
})

describe('PackRemote over REST', () => {
  it('lands a push past the inline cutoff as blobs, a tree, a commit and one updateRefs', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { inlineBytes: 0 })
    const wt = await commitN(a.store, 3, 'a')
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    const rest = gh.calls.filter((c) => c.kind === 'rest').map((c) => c.detail)
    expect(rest).toEqual(['blob', 'tree', 'commit'])
    expect(gh.calls.filter((c) => c.kind === 'graphql-mutation').map((c) => c.detail)).toEqual([
      'updateRefs',
    ])
    expect(gh.messageAt(BRANCH)).toMatch(
      new RegExp(
        `^kvgit ${BRANCH} @ ${wt.head.slice(0, 7)} \\(\\+4\\)\n\nKvgit-Head: ${wt.head}\n`,
      ),
    )
    expect(manifestAt(gh)).toMatchObject({ format: 2, head: wt.head })

    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(wt.head)
    // the next push builds on the branch's tree
    const next = await commitN(a.store, 1, 'b')
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('pushed')
    expect(gh.filesAt(BRANCH).filter((p) => p.endsWith('.pack'))).toHaveLength(2)
    expect(gh.filesAt(BRANCH)).toContain('README.md')
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(next.head)
  })

  it('stores a pack larger than partBytes in parts, read back in order', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { partBytes: 100_000, valueBlobBytes: Number.POSITIVE_INFINITY })
    const value = random(1_200_000) // past the 1 MiB inline cutoff
    const head = await commitValue(a.store, 'files/big.bin', value)
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    const [entry] = manifestAt(gh).packs
    expect(entry?.parts).toHaveLength(13)
    const files = gh.filesAt(BRANCH).filter((p) => p.includes('.pack'))
    expect(files).toEqual(entry?.parts.map((_, i) => `${entry.path}.${i}`).sort())

    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
    expect(await (await open(b.store)).get('files/big.bin')).toEqual(value)
  })

  it('resumes a push that stopped partway, sending only the parts GitHub lacks', async () => {
    const gh = new FakeGithub()
    let blobs = 0
    const { client } = wrapped(gh, async (what, send) =>
      what === 'POST blobs' && ++blobs === 3
        ? new Response('{"message": "boom"}', { status: 502 })
        : send(),
    )
    const store = new Memory()
    const remote = new PackRemote(client, store, { inlineBytes: 0, partBytes: 50_000 })
    const head = await commitValue(store, 'files/big.bin', random(200_000))
    await expect(pushBranch(store, remote, BRANCH)).rejects.toMatchObject({ kind: 'server' })
    expect(gh.refs.has(`refs/heads/${BRANCH}`)).toBe(false)
    expect(gh.count('rest')).toBe(2) // two parts went up

    gh.calls.length = 0
    expect((await pushBranch(store, remote, BRANCH)).status).toBe('created')
    const parts = manifestAt(gh).packs[0]?.parts.length ?? 0
    expect(parts).toBe(5)
    const sent = gh.calls.filter((c) => c.kind === 'rest' && c.detail === 'blob')
    expect(sent).toHaveLength(parts - 2)
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
  })

  it('sends again the parts GitHub has forgotten', async () => {
    const gh = new FakeGithub()
    const { client } = wrapped(gh, async (what, send) =>
      what === 'updateRefs' ? new Response('{"message": "boom"}', { status: 502 }) : send(),
    )
    const store = new Memory()
    const remote = new PackRemote(client, store, { inlineBytes: 0, partBytes: 50_000 })
    await commitValue(store, 'files/big.bin', random(120_000))
    await expect(pushBranch(store, remote, BRANCH)).rejects.toMatchObject({ kind: 'server' })
    await gh.prune()
    gh.calls.length = 0
    expect(
      (
        await pushBranch(
          store,
          new PackRemote(gh.client(), store, { inlineBytes: 0, partBytes: 50_000 }),
          BRANCH,
        )
      ).status,
    ).toBe('created')
    expect(gh.calls.filter((c) => c.kind === 'rest' && c.detail === 'blob')).toHaveLength(3)
  })

  it('deletes every part of the packs a fold replaces', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { inlineBytes: 0, partBytes: 20_000, maxPacks: 3, keepRecent: 1 })
    for (let i = 0; i < 4; i++) {
      await commitValue(a.store, `files/${i}.bin`, random(30_000))
      await pushBranch(a.store, a.remote, BRANCH)
    }
    const manifest = manifestAt(gh)
    expect(manifest.packs).toHaveLength(2)
    const expected = manifest.packs.flatMap((p) =>
      p.parts.length === 1 ? [p.path] : p.parts.map((_, i) => `${p.path}.${i}`),
    )
    expect(gh.filesAt(BRANCH).filter((p) => p.includes('.pack'))).toEqual(expected.sort())
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(manifest.head)
  })

  it('loses a race at the ref cleanly: false, the ref where the other writer left it', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { inlineBytes: 0 })
    const old = (await commitN(a.store, 1, 'a')).head
    await pushBranch(a.store, a.remote, BRANCH)
    const head = (await commitN(a.store, 1, 'x')).head
    const main = gh.refs.get('refs/heads/main') as string
    gh.beforeMutation = () => {
      gh.refs.set(`refs/heads/${BRANCH}`, main)
      gh.beforeMutation = null
    }
    const pushed = a.remote.push(BRANCH, old, head, walkDelta(a.store, { want: head, have: [old] }))
    expect(await pushed).toBe(false)
    expect(gh.refs.get(`refs/heads/${BRANCH}`)).toBe(main)
  })

  it('throws an updateRefs failure that is no race, the ref unmoved', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { inlineBytes: 0 })
    await commitN(a.store, 1, 'a')
    gh.updateRefsError = 'Resource not accessible by personal access token'
    await expect(pushBranch(a.store, a.remote, BRANCH)).rejects.toThrow(/Resource not accessible/)
    expect(gh.refs.has(`refs/heads/${BRANCH}`)).toBe(false)
  })
})

describe('PackRemote and large values', () => {
  it('stores a large value as a blob of its own, which the pack references', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const value = random(1_500_000)
    const head = await commitValue(a.store, 'files/photo.png', value)
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    const sha = await sha256Hex(value)
    expect(gh.filesAt(BRANCH)).toContain(`kvgit/values/${sha}`)
    const [entry] = manifestAt(gh).packs
    expect(entry?.bytes).toBeLessThan(10_000)

    gh.calls.length = 0
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
    expect(await (await open(b.store)).get('files/photo.png')).toEqual(value)
    expect(gh.count('raw')).toBe(2) // the pack, then the value
  })

  it('sends a value the repo holds from any branch once, and reads one the store holds not at all', async () => {
    const gh = new FakeGithub()
    const value = random(1_500_000)
    const a = await device(gh)
    await commitValue(a.store, 'files/photo.png', value)
    await pushBranch(a.store, a.remote, BRANCH)

    // another session, with its own history, holding the same bytes
    gh.calls.length = 0
    const b = await device(gh)
    const head = await commitValue(b.store, 'copy.png', value)
    await b.remote.listRefs()
    await expect(
      b.remote.push('chat-2', null, head, walkDelta(b.store, { want: head })),
    ).resolves.toBe(true)
    expect(gh.calls.filter((c) => c.kind === 'rest' && c.detail === 'blob')).toHaveLength(1) // the pack
    expect(gh.filesAt('chat-2')).toContain(`kvgit/values/${await sha256Hex(value)}`)

    // a device that holds it (from the first session) pulls the second without reading it
    gh.calls.length = 0
    const c = await device(gh)
    await pullBranch(c.store, c.remote, BRANCH)
    gh.calls.length = 0
    const pulled = await pullBranch(c.store, c.remote, 'chat-2')
    expect(pulled.localHead).toBe(head)
    expect(gh.count('raw')).toBe(1) // the pack alone
  })

  it('keeps a value another session uses when one session is deleted', async () => {
    const gh = new FakeGithub()
    const value = random(1_500_000)
    const a = await device(gh)
    await commitValue(a.store, 'files/photo.png', value)
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    const head = await commitValue(b.store, 'copy.png', value)
    await b.remote.listRefs()
    await b.remote.push('chat-2', null, head, walkDelta(b.store, { want: head }))

    expect((await deleteRemoteBranch(a.store, a.remote, BRANCH)).status).toBe('deleted')
    await gh.prune()
    const c = await device(gh)
    expect((await pullBranch(c.store, c.remote, 'chat-2')).localHead).toBe(head)
    expect(await (await open(c.store, 'chat-2')).get('copy.png')).toEqual(value)
  })

  it('keeps large values in the pack when a push goes inline', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    // 2 MB that compresses to almost nothing fits one GraphQL request
    const head = await commitValue(a.store, 'files/log.txt', bytes('x'.repeat(2_000_000)))
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect(gh.count('rest')).toBe(0)
    expect(gh.filesAt(BRANCH).some((p) => p.startsWith('kvgit/values/'))).toBe(false)
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
  })

  it('a fold references the values its packs did, uploading nothing for them again', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPacks: 3, keepRecent: 1, maxPackBytes: 64 * 1024 * 1024 })
    const values: Uint8Array[] = []
    for (let i = 0; i < 4; i++) {
      values.push(random(1_100_000))
      await commitValue(a.store, `files/${i}.png`, values[i] as Uint8Array)
      gh.calls.length = 0
      await pushBranch(a.store, a.remote, BRANCH)
    }
    // the last push folded: its blob uploads are its own value and two packs
    expect(manifestAt(gh).packs).toHaveLength(2)
    expect(gh.calls.filter((c) => c.kind === 'rest' && c.detail === 'blob')).toHaveLength(3)
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)
    const wt = await open(b.store)
    for (const [i, v] of values.entries()) expect(await wt.get(`files/${i}.png`)).toEqual(v)
  })
})

/** Branch `to` at `from`'s head in `store`, as a fork is made. */
async function forkAt(store: Memory, from: string, to: string): Promise<void> {
  const repo = await Repo.open(store)
  await repo.branches.create(to, await repo.branches.get(from))
}

/** Commit `value` at `key` on `branch`; the new head. */
async function commitOn(
  store: Memory,
  branch: string,
  key: string,
  value: Uint8Array,
): Promise<string> {
  const wt = await open(store, branch)
  wt.set(key, value)
  await wt.commit()
  return wt.head
}

const blobPosts = (gh: FakeGithub): number =>
  gh.calls.filter((c) => c.kind === 'rest' && c.detail === 'blob').length

/** The files a manifest's packs are stored in. */
const packFiles = (m: PackManifest): string[] =>
  m.packs.flatMap((p) =>
    p.parts.length === 1 ? [p.path] : p.parts.map((_, i) => `${p.path}.${i}`),
  )

describe('PackRemote and forks', () => {
  /** A session of three pushes of 400 kB each: 1.2 MB of packs, more
   *  than one inline request takes. */
  async function parent(gh: FakeGithub, opts: ConstructorParameters<typeof PackRemote>[2] = {}) {
    const a = await device(gh, opts)
    for (let i = 0; i < 3; i++) {
      await commitValue(a.store, `files/${i}.bin`, random(400_000))
      await pushBranch(a.store, a.remote, BRANCH)
    }
    return a
  }

  it("reuses its parent's packs, sending only the commits after the fork point", async () => {
    const gh = new FakeGithub()
    const a = await parent(gh)
    const before = manifestAt(gh)
    await forkAt(a.store, BRANCH, 'fork')
    const head = await commitOn(a.store, 'fork', 'notes.txt', bytes('mine'))

    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, 'fork')).status).toBe('created')
    expect(blobPosts(gh)).toBe(1) // the fork's one new pack
    const fork = manifestAt(gh, 'fork')
    expect(fork.packs.slice(0, 3)).toEqual(before.packs)
    expect(fork.packs[3]).toMatchObject({ base: before.head, tip: head, commits: 1 })
    expect(fork.packs[3]?.seq).toBe(before.nextSeq)
    expect(fork).toMatchObject({ format: 2, branch: 'fork', head, meta: null })
    expect(gh.filesAt('fork').filter((f) => f.includes('.pack'))).toEqual(packFiles(fork).sort())
    expect(gh.filesAt('fork')).toContain('README.md')

    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, 'fork')).localHead).toBe(head)
    expect(text(await (await open(b.store, 'fork')).get('notes.txt'))).toBe('mine')
  })

  it('reuses only the packs up to the fork point, dropping the later ones from its tree', async () => {
    const gh = new FakeGithub()
    const a = await parent(gh)
    // the fork starts at the second push's head; the parent moves on
    const repo = await Repo.open(a.store)
    const second = manifestAt(gh).packs[1]?.tip as string
    await repo.branches.create('fork', second)
    const head = await commitOn(a.store, 'fork', 'notes.txt', bytes('mine'))
    expect((await pushBranch(a.store, a.remote, 'fork')).status).toBe('created')
    const fork = manifestAt(gh, 'fork')
    expect(fork.packs.slice(0, 2)).toEqual(manifestAt(gh).packs.slice(0, 2))
    expect(fork.packs[2]).toMatchObject({ base: second, tip: head })
    expect(gh.filesAt('fork').filter((f) => f.includes('.pack'))).toEqual(packFiles(fork).sort())
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, 'fork')).localHead).toBe(head)
  })

  it('lists the parent packs alone when the fork adds nothing', async () => {
    const gh = new FakeGithub()
    const a = await parent(gh)
    await forkAt(a.store, BRANCH, 'copy')
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, 'copy')).status).toBe('created')
    expect(blobPosts(gh)).toBe(0)
    expect(manifestAt(gh, 'copy').packs).toEqual(manifestAt(gh).packs)
  })

  it('a fork of a fork reuses the packs of the one covering the most history', async () => {
    const gh = new FakeGithub()
    const a = await parent(gh)
    await forkAt(a.store, BRANCH, 'one')
    for (let i = 0; i < 2; i++) {
      await commitOn(a.store, 'one', `one/${i}.bin`, random(50_000))
      await pushBranch(a.store, a.remote, 'one')
    }
    await forkAt(a.store, 'one', 'two')
    const head = await commitOn(a.store, 'two', 'two.txt', bytes('two'))
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, 'two')).status).toBe('created')
    expect(blobPosts(gh)).toBe(1)
    const one = manifestAt(gh, 'one')
    const two = manifestAt(gh, 'two')
    expect(two.packs.slice(0, 5)).toEqual(one.packs)
    expect(two.packs[5]).toMatchObject({ base: one.head, tip: head })
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, 'two')).localHead).toBe(head)
  })

  it('keeps the large values its reused packs reference, after the parent is deleted', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const value = random(1_500_000)
    await commitValue(a.store, 'files/photo.png', value)
    await pushBranch(a.store, a.remote, BRANCH)
    await forkAt(a.store, BRANCH, 'fork')
    const head = await commitOn(a.store, 'fork', 'notes.txt', bytes('mine'))
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, 'fork')).status).toBe('created')
    expect(blobPosts(gh)).toBe(1)
    expect(gh.filesAt('fork')).toContain(`kvgit/values/${await sha256Hex(value)}`)

    expect((await deleteRemoteBranch(a.store, a.remote, BRANCH)).status).toBe('deleted')
    await gh.prune()
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, 'fork')).localHead).toBe(head)
    expect(await (await open(b.store, 'fork')).get('files/photo.png')).toEqual(value)
  })

  it("folds and repacks only its own tree, leaving the parent's", async () => {
    const gh = new FakeGithub()
    const a = await parent(gh, { maxPacks: 4, keepRecent: 1, maxPackBytes: 8 * 1024 * 1024 })
    await forkAt(a.store, BRANCH, 'fork')
    const parentFiles = gh.filesAt(BRANCH)
    for (let i = 0; i < 3; i++) {
      await commitOn(a.store, 'fork', `fork/${i}.txt`, bytes(`fork ${i}`))
      await pushBranch(a.store, a.remote, 'fork')
    }
    expect(manifestAt(gh, 'fork').packs.length).toBeLessThan(6) // it folded
    expect(gh.filesAt('fork').filter((f) => f.includes('.pack'))).toEqual(
      packFiles(manifestAt(gh, 'fork')).sort(),
    )
    expect(await a.remote.repack('fork')).toBe(true)
    expect(gh.filesAt('fork').filter((f) => f.includes('.pack'))).toHaveLength(1)
    expect(gh.filesAt(BRANCH)).toEqual(parentFiles)
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(manifestAt(gh).head)
    expect((await pullBranch(b.store, b.remote, 'fork')).localHead).toBe(
      manifestAt(gh, 'fork').head,
    )
  })

  it('reuses a parent folded since, as far as its packs still match', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPacks: 3, keepRecent: 1 })
    const heads: string[] = []
    for (let i = 0; i < 4; i++) {
      heads.push(await commitValue(a.store, `files/${i}.bin`, random(400_000)))
      await pushBranch(a.store, a.remote, BRANCH)
    }
    // the first three pushes folded into one pack up to heads[2]
    const folded = manifestAt(gh)
    expect(folded.packs.map((p) => p.tip)).toEqual([heads[2], heads[3]])

    // a fork at heads[3] reuses both packs; one at heads[1], inside the
    // folded run, reuses nothing and sends its history whole
    const repo = await Repo.open(a.store)
    await repo.branches.create('late', heads[3] as string)
    await repo.branches.create('early', heads[1] as string)
    for (const [name, reused] of [
      ['late', 2],
      ['early', 0],
    ] as const) {
      const head = await commitOn(a.store, name, 'notes.txt', bytes(name))
      expect((await pushBranch(a.store, a.remote, name)).status).toBe('created')
      const m = manifestAt(gh, name)
      expect(m.packs.slice(0, reused)).toEqual(folded.packs.slice(0, reused))
      expect(m.packs[reused]?.base ?? null).toBe(reused === 0 ? null : heads[3])
      const b = await device(gh)
      expect((await pullBranch(b.store, b.remote, name)).localHead).toBe(head)
    }
  })

  it('reuses a format-1 parent as one-part format-2 entries', async () => {
    const gh = new FakeGithub()
    const src = new Memory()
    let head = ''
    for (let i = 0; i < 3; i++) head = await commitValue(src, `files/${i}.bin`, random(400_000))
    const codec = new Encoder({ useRecords: false, mapsAsObjects: true, tagUint8Array: false })
    const body = codec.decode(
      await gunzip(
        await encodePack(await collect(walkDelta(src, { want: head })), {
          kernel: 'ts',
          base: null,
        }),
      ),
    ) as Record<string, unknown>
    const pack = await gzip(codec.encode({ ...body, format: 1 }))
    const path = `kvgit/packs/000001-${head.slice(0, 12)}.pack`
    const blob = await gitBlobSha1(pack)
    const entry = {
      path,
      seq: 1,
      base: null,
      tip: head,
      commits: 4,
      bytes: pack.length,
      sha256: await sha256Hex(pack),
    }
    gh.plant(BRANCH, {
      [MANIFEST_PATH]: JSON.stringify({
        format: 1,
        kernel: 'ts',
        branch: BRANCH,
        head,
        packs: [{ ...entry, blob }],
        nextSeq: 2,
        meta: null,
      }),
      [path]: pack,
    })

    const remote = new PackRemote(gh.client(), src)
    await forkAt(src, BRANCH, 'fork')
    const next = await commitOn(src, 'fork', 'notes.txt', bytes('mine'))
    expect((await pushBranch(src, remote, 'fork')).status).toBe('created')
    const fork = manifestAt(gh, 'fork')
    expect(fork.format).toBe(2)
    expect(fork.packs[0]).toEqual({ ...entry, parts: [blob] })
    expect(JSON.stringify(fork)).not.toContain('"blob"')
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, 'fork')).localHead).toBe(next)
  })

  it('sends a small fork whole, in one inline request', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 3, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    await forkAt(a.store, BRANCH, 'fork')
    await commitOn(a.store, 'fork', 'notes.txt', bytes('mine'))
    gh.calls.length = 0
    expect((await pushBranch(a.store, a.remote, 'fork')).status).toBe('created')
    expect(gh.count('rest')).toBe(0)
    expect(manifestAt(gh, 'fork').packs).toHaveLength(1)
  })
})

describe('PackRemote on a slow link', () => {
  it('sends a push GitHub drops at its deadline over REST, and lowers its inline cutoff', async () => {
    const gh = new FakeGithub({ uplinkBytesPerSecond: 50_000 }) // GraphQL carries 250 kB
    const { client, log } = wrapped(gh)
    const store = new Memory()
    const remote = new PackRemote(client, store)
    await commitValue(store, 'files/a.bin', random(400_000))
    expect((await pushBranch(store, remote, BRANCH)).status).toBe('created')
    expect(log.filter((w) => w === 'createCommitOnBranch')).toHaveLength(1) // dropped
    expect(log).toContain('POST blobs')

    // the cutoff is now half the dropped request: this one goes over REST at once
    log.length = 0
    await commitValue(store, 'files/b.bin', random(250_000))
    expect((await pushBranch(store, remote, BRANCH)).status).toBe('pushed')
    expect(log).not.toContain('createCommitOnBranch')
    // and a small one inline
    log.length = 0
    const head = await commitValue(store, 'k', bytes('small'))
    expect((await pushBranch(store, remote, BRANCH)).status).toBe('pushed')
    expect(log).toContain('createCommitOnBranch')
    expect(log).not.toContain('POST blobs')

    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
  })

  it("halves its parts while one doesn't arrive within GitHub's minute", async () => {
    const gh = new FakeGithub({ uplinkBytesPerSecond: 20_000 }) // REST carries 1.2 MB
    const a = await device(gh)
    const value = random(1_500_000)
    const head = await commitValue(a.store, 'files/a.bin', value)
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    const sha = await sha256Hex(value)
    expect(gh.filesAt(BRANCH).filter((p) => p.startsWith('kvgit/values/'))).toEqual(
      [0, 1, 2].map((i) => `kvgit/values/${sha}.${i}`), // 512 KiB parts
    )
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
  })

  it("names the part that doesn't arrive even at 512 KiB, and lands nothing", async () => {
    const gh = new FakeGithub({ uplinkBytesPerSecond: 5_000 }) // REST carries 300 kB
    const a = await device(gh)
    const head = await commitValue(a.store, 'files/a.bin', random(600_000))
    const err = await pushBranch(a.store, a.remote, BRANCH).catch((e) => e)
    expect(err).toBeInstanceOf(GithubError)
    expect((err as GithubError).kind).toBe('too-large')
    expect((err as GithubError).message).toMatch(
      new RegExp(
        `^PackRemote.push: a part of the pack of 2 commits up to ${head.slice(0, 7)} \\(525 kB of 6\\d\\d kB\\) didn't reach GitHub: GitHub answered`,
      ),
    )
    expect(gh.refs.has(`refs/heads/${BRANCH}`)).toBe(false)
  })
})

describe('PackRemote and a push whose outcome is unclear', () => {
  for (const maxRetries of [0, 1]) {
    it(`counts a lost updateRefs response as landed (retries: ${maxRetries})`, async () => {
      const gh = new FakeGithub()
      const { client, log } = wrapped(
        gh,
        async (what, send) => {
          const resp = await send()
          if (what === 'updateRefs' && log.filter((w) => w === what).length === 1) {
            throw new TypeError('Failed to fetch')
          }
          return resp
        },
        { maxRetries },
      )
      const store = new Memory()
      const head = (await commitN(store, 2, 'a')).head
      const remote = new PackRemote(client, store, { inlineBytes: 0 })
      expect((await pushBranch(store, remote, BRANCH)).status).toBe('created')
      expect(manifestAt(gh).head).toBe(head)
    })

    it(`counts an inline push whose response was lost as landed, sending nothing more (retries: ${maxRetries})`, async () => {
      const gh = new FakeGithub()
      const { client, log } = wrapped(
        gh,
        async (what, send) => {
          const resp = await send()
          if (what === 'createCommitOnBranch' && log.filter((w) => w === what).length === 1) {
            throw new TypeError('Failed to fetch')
          }
          return resp
        },
        { maxRetries },
      )
      const store = new Memory()
      const head = (await commitN(store, 2, 'a')).head
      expect((await pushBranch(store, new PackRemote(client, store), BRANCH)).status).toBe(
        'created',
      )
      expect(manifestAt(gh).head).toBe(head)
      expect(gh.count('rest')).toBe(0)
    })
  }

  it('claims the bare ref a creation left when only its createRef landed', async () => {
    const gh = new FakeGithub()
    const { client } = wrapped(gh, async (what, send, clock) => {
      if (what !== 'createCommitOnBranch') return send()
      gh.setRef(BRANCH, gh.refs.get('refs/heads/main') as string) // the createRef, alone
      clock.ms += 5_000
      throw new TypeError('Failed to fetch')
    })
    const store = new Memory()
    const head = await commitValue(store, 'files/a.bin', random(100_000))
    expect((await pushBranch(store, new PackRemote(client, store), BRANCH)).status).toBe('created')
    expect(manifestAt(gh).head).toBe(head)
    expect(gh.count('rest')).toBeGreaterThan(0)
  })

  it('counts a request that failed at the deadline but landed as landed, with no REST fallback', async () => {
    const gh = new FakeGithub()
    const { client } = wrapped(gh, async (what, send, clock) => {
      const resp = await send()
      if (what !== 'createCommitOnBranch') return resp
      clock.ms += 5_000 // applied, then the browser saw a network error at 5 s
      throw new TypeError('Failed to fetch')
    })
    const store = new Memory()
    const head = await commitValue(store, 'files/a.bin', random(100_000))
    expect((await pushBranch(store, new PackRemote(client, store), BRANCH)).status).toBe('created')
    expect(manifestAt(gh).head).toBe(head)
    expect(gh.count('rest')).toBe(0)
  })
})

describe('PackRemote and manifests it cannot use', () => {
  for (const [what, text] of [
    ['a newer format', '{"format": 3, "head": "x", "packs": []}'],
    ['text that is not JSON', '{"format": 1, "head":'],
  ] as const) {
    it(`never claims a branch whose manifest is ${what}: the push refuses, the roster skips it`, async () => {
      const gh = new FakeGithub()
      gh.plant(BRANCH, { [MANIFEST_PATH]: text })
      const before = gh.refs.get(`refs/heads/${BRANCH}`)
      const a = await device(gh)
      await commitN(a.store, 1, 'a')
      await expect(pushBranch(a.store, a.remote, BRANCH)).rejects.toThrow(
        new RegExp(`branch '${BRANCH}' can't be synced: its manifest`),
      )
      expect(gh.refs.get(`refs/heads/${BRANCH}`)).toBe(before)
      expect(await a.remote.roster()).toEqual([])
    })
  }

  it('lands a commit too large for one request, as parts', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    // random bytes don't compress: 24MB is over what GraphQL takes at all
    const big = random(24 * 1024 * 1024)
    const head = await commitValue(a.store, 'files/big.bin', big)
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    // the value on its own in six 4 MiB parts; the packs small
    expect(manifestAt(gh).packs.map((p) => p.parts.length)).toEqual([1, 1])
    const sha = await sha256Hex(big)
    expect(gh.filesAt(BRANCH).filter((p) => p.startsWith('kvgit/values/'))).toHaveLength(6)
    expect(gh.filesAt(BRANCH)).toContain(`kvgit/values/${sha}.5`)
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
  }, 30_000)
})

describe('PackRemote and branches that are not sessions', () => {
  it("never pushes onto the default branch, or a branch of the repo's own", async () => {
    const gh = new FakeGithub()
    gh.plant('feature', { 'notes.md': 'mine' })
    for (const branch of ['main', 'feature']) {
      const store = new Memory()
      const wt = await open(store, branch)
      wt.set('k', bytes('v'))
      await wt.commit()
      const before = gh.refs.get(`refs/heads/${branch}`)
      await expect(pushBranch(store, new PackRemote(gh.client(), store), branch)).rejects.toThrow(
        `branch '${branch}' exists in the repo but isn't a kvgit session`,
      )
      expect(gh.refs.get(`refs/heads/${branch}`)).toBe(before)
    }
  })

  it('claims a creation that never got its first commit (a ref at main)', async () => {
    const gh = new FakeGithub()
    gh.setRef(BRANCH, gh.refs.get('refs/heads/main') as string)
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect(gh.filesAt(BRANCH)).toContain(MANIFEST_PATH)
  })
})

describe('PackRemote packs over time', () => {
  it('folds older packs once a push would exceed maxPacks, still one mutation', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPacks: 4, keepRecent: 2 })
    let wt: Worktree | null = null
    for (let i = 0; i < 8; i++) {
      wt = await commitN(a.store, 2, `t${i}-`)
      gh.calls.length = 0
      await pushBranch(a.store, a.remote, BRANCH)
      expect(gh.count('graphql-mutation')).toBe(1)
    }
    const packs = gh.filesAt(BRANCH).filter((p) => p.endsWith('.pack'))
    expect(packs.length).toBeLessThanOrEqual(4)
    const b = await device(gh)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.localHead).toBe(wt?.head)
    expect(pulled.transferred).toBe(17)
  })

  it('folds only within what the push leaves of maxPackBytes', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPacks: 3, keepRecent: 2, maxPackBytes: 64 * 1024 })
    const wt = await open(a.store)
    const packsAfter = async (kib: number): Promise<number> => {
      // random bytes don't compress: a pack is about the value's size
      const value = new Uint8Array(kib * 1024)
      crypto.getRandomValues(value)
      wt.set(`files/${kib}-${Math.random()}`, value)
      await wt.commit()
      await pushBranch(a.store, a.remote, BRANCH)
      return (await a.remote.roster())[0]?.packs ?? 0
    }
    for (let i = 0; i < 3; i++) await packsAfter(10)
    // a 50 KiB push leaves ~14 KiB, less than the two oldest 10 KiB packs
    expect(await packsAfter(50)).toBe(4)
    // a 10 KiB push leaves room to fold the three older 10 KiB packs
    expect(await packsAfter(10)).toBe(3)
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(wt.head)
  })

  it('repack folds everything into one pack; a fresh pull is one query + one read', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    for (let i = 0; i < 5; i++) {
      await commitN(a.store, 2, `t${i}-`)
      await pushBranch(a.store, a.remote, BRANCH)
    }
    expect(await a.remote.repack(BRANCH)).toBe(true)
    expect(gh.filesAt(BRANCH).filter((p) => p.endsWith('.pack'))).toHaveLength(1)
    gh.calls.length = 0
    const b = await device(gh)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.transferred).toBe(11)
    expect(gh.count('graphql-query')).toBe(1)
    expect(gh.count('raw')).toBe(1)
    // A device that had the pre-repack history pulls on as before.
    await commitN(a.store, 1, 'after')
    await pushBranch(a.store, a.remote, BRANCH)
    expect((await pullBranch(b.store, b.remote, BRANCH)).status).toBe('fast-forwarded')
  })
})

describe('PackRemote.repack over REST', () => {
  it('folds a history past the inline cutoff, deleting every old part', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { inlineBytes: 0, partBytes: 50_000 })
    for (const name of ['one', 'two']) {
      await commitValue(a.store, `files/${name}.bin`, random(120_000))
      await pushBranch(a.store, a.remote, BRANCH)
    }
    expect(await a.remote.repack(BRANCH)).toBe(true)
    const [entry, ...rest] = manifestAt(gh).packs
    expect(rest).toEqual([])
    expect(gh.filesAt(BRANCH).filter((p) => p.includes('.pack'))).toEqual(
      entry?.parts.map((_, i) => `${entry.path}.${i}`).sort(),
    )
    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(entry?.tip)
  })
})

describe('PackRemote and format 1', () => {
  it('pulls a format-1 branch, and pushes onto it as format 2', async () => {
    const gh = new FakeGithub()
    const src = new Memory()
    const head = (await commitN(src, 3, 'a')).head
    // a format-1 pack differs from a format-2 one by its format alone
    const codec = new Encoder({ useRecords: false, mapsAsObjects: true, tagUint8Array: false })
    const body = codec.decode(
      await gunzip(
        await encodePack(await collect(walkDelta(src, { want: head })), {
          kernel: 'ts',
          base: null,
        }),
      ),
    ) as Record<string, unknown>
    const pack = await gzip(codec.encode({ ...body, format: 1 }))
    const path = `kvgit/packs/000001-${head.slice(0, 12)}.pack`
    const manifest = {
      format: 1,
      kernel: 'ts',
      branch: BRANCH,
      head,
      packs: [
        {
          path,
          seq: 1,
          base: null,
          tip: head,
          commits: 4,
          bytes: pack.length,
          sha256: await sha256Hex(pack),
          blob: await gitBlobSha1(pack),
        },
      ],
      nextSeq: 2,
      meta: null,
    }
    gh.plant(BRANCH, { [MANIFEST_PATH]: JSON.stringify(manifest), [path]: pack })

    const b = await device(gh)
    expect((await pullBranch(b.store, b.remote, BRANCH)).localHead).toBe(head)
    const next = (await commitN(b.store, 1, 'b')).head
    expect((await pushBranch(b.store, b.remote, BRANCH)).status).toBe('pushed')
    const now = manifestAt(gh)
    expect(now.format).toBe(2)
    expect(now.packs[0]).toEqual({
      ...manifest.packs[0],
      blob: undefined,
      parts: [manifest.packs[0]?.blob],
    })
    expect(JSON.stringify(now)).not.toContain('"blob"')
    const c = await device(gh)
    expect((await pullBranch(c.store, c.remote, BRANCH)).localHead).toBe(next)
  })
})

describe('PackRemote roster and lifecycle', () => {
  it('throws a lifecycle error that is not a lost race, rather than reporting one', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    gh.updateRefsError = 'Resource not accessible by personal access token'
    await expect(a.remote.archiveBranch(BRANCH)).rejects.toThrow(/Resource not accessible/)
    await expect(a.remote.emptyTrash()).resolves.toBe(0) // nothing archived: no request
    gh.updateRefsError = null
    expect(await a.remote.archiveBranch(BRANCH)).toBe(true)
    gh.updateRefsError = 'Resource not accessible by personal access token'
    await expect(a.remote.emptyTrash()).rejects.toThrow(/Resource not accessible/)
  })

  it('roster carries stub metadata from the manifest, in one query', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { meta: (branch) => ({ title: `Title of ${branch}` }) })
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    gh.calls.length = 0
    const roster = await b.remote.roster()
    expect(roster).toEqual([
      expect.objectContaining({
        branch: BRANCH,
        meta: { title: `Title of ${BRANCH}` },
        archived: false,
      }),
    ])
    expect(gh.calls).toHaveLength(1)
  })

  it('archive / restore / empty trash are single atomic updateRefs', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)

    gh.calls.length = 0
    expect(await a.remote.archiveBranch(BRANCH)).toBe(true)
    expect(gh.count('graphql-mutation')).toBe(1)
    expect(await a.remote.listRefs()).toEqual([])
    expect((await a.remote.roster())[0]).toMatchObject({ branch: BRANCH, archived: true })
    // The other device sees the lifecycle conflict.
    expect((await pullBranch(b.store, b.remote, BRANCH)).status).toBe('remote-gone')
    // Archiving again: nothing live to archive.
    expect(await a.remote.archiveBranch(BRANCH)).toBe(false)

    expect(await a.remote.restoreBranch(BRANCH)).toBe(true)
    expect((await a.remote.listRefs()).map((r) => r.branch)).toEqual([BRANCH])
    expect(await a.remote.archiveBranch(BRANCH)).toBe(true)
    expect(await a.remote.emptyTrash()).toBe(1)
    expect(await a.remote.roster()).toEqual([])
  })

  it('archive loses cleanly when the branch moved after it was read', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    gh.beforeMutation = () => {
      gh.refs.set(`refs/heads/${BRANCH}`, gh.refs.get('refs/heads/main') as string)
      gh.beforeMutation = null
    }
    expect(await a.remote.archiveBranch(BRANCH)).toBe(false)
    expect(gh.refs.has(`refs/heads/archived/${BRANCH}`)).toBe(false)
  })
})

describe('PackRemote.deleteBranch', () => {
  it('deletes a session at the head expected, in one updateRefs', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await commitN(a.store, 2, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    expect(await a.remote.deleteBranch(BRANCH, 'f'.repeat(40))).toBe(false)

    gh.calls.length = 0
    const result = await deleteRemoteBranch(a.store, a.remote, BRANCH)
    expect(result.status).toBe('deleted')
    expect(gh.count('graphql-mutation')).toBe(1)
    expect(gh.refs.has(`refs/heads/${BRANCH}`)).toBe(false)
    expect(await a.remote.roster()).toEqual([])
    expect(await a.remote.deleteBranch(BRANCH, wt.head)).toBe(false)
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
  })

  it('leaves a session someone pushed to since', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const b = await device(gh)
    await pullBranch(b.store, b.remote, BRANCH)
    const bWt = await commitN(b.store, 1, 'b')
    await pushBranch(b.store, b.remote, BRANCH)

    const result = await deleteRemoteBranch(a.store, a.remote, BRANCH)
    expect(result).toMatchObject({ status: 'moved', remoteHead: bWt.head })
    expect((await a.remote.listRefs()).map((r) => r.head)).toEqual([bWt.head])
  })

  it('loses cleanly when the session moves after it was read', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    gh.beforeMutation = () => {
      gh.refs.set(`refs/heads/${BRANCH}`, gh.refs.get('refs/heads/main') as string)
      gh.beforeMutation = null
    }
    expect(await a.remote.deleteBranch(BRANCH, wt.head)).toBe(false)
    expect(gh.refs.has(`refs/heads/${BRANCH}`)).toBe(true)
  })

  it('tries again when the session was repacked meanwhile, its head the same', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 1, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    // a new git commit at the same kvgit head, as a repack makes,
    // lands just before the delete
    const ref = `refs/heads/${BRANCH}`
    let mutations = 0
    gh.beforeMutation = () => {
      if (++mutations > 1) return
      const tip = gh.commits.get(gh.refs.get(ref) as string)
      if (tip === undefined) throw new Error('no session')
      const again = { ...tip, oid: 'e'.repeat(40), parent: tip.oid }
      gh.commits.set(again.oid, again)
      gh.refs.set(ref, again.oid)
    }
    const result = await deleteRemoteBranch(a.store, a.remote, BRANCH)
    expect(result.status).toBe('deleted')
    expect(mutations).toBe(2)
    expect(gh.refs.has(ref)).toBe(false)
  })
})

describe('PackRemote pulls on a long history', () => {
  it('read a few local records, not the history', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    await commitN(a.store, 200, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    const store = new Memory()
    let reads = 0
    const counted = new Proxy(store, {
      get(target, prop) {
        const value = Reflect.get(target, prop, target)
        if (prop === 'get') {
          return (key: string) => {
            reads++
            return target.get(key)
          }
        }
        if (prop === 'getMany') {
          return (keys: Iterable<string>) => {
            const list = [...keys]
            reads += list.length
            return target.getMany(list)
          }
        }
        return typeof value === 'function' ? value.bind(target) : value
      },
    })
    const b = new PackRemote(gh.client(), counted)
    await pullBranch(counted, b, BRANCH)

    await commitN(a.store, 1, 'b')
    await pushBranch(a.store, a.remote, BRANCH)
    reads = 0
    const pulled = await pullBranch(counted, b, BRANCH)
    expect(pulled).toMatchObject({ status: 'fast-forwarded', transferred: 1 })
    expect(reads).toBeLessThan(80)
  })

  it('skip what they hold of a folded pack', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPacks: 4, keepRecent: 1 })
    const b = await device(gh)
    for (let i = 0; i < 6; i++) {
      await commitN(a.store, 2, `t${i}-`)
      await pushBranch(a.store, a.remote, BRANCH)
      if (i === 1) await pullBranch(b.store, b.remote, BRANCH)
    }
    // the first four pushes folded into one, which holds what b has
    expect(gh.filesAt(BRANCH).filter((p) => p.endsWith('.pack'))).toHaveLength(3)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled).toMatchObject({ status: 'fast-forwarded', transferred: 8 })
  })
})

describe('PackRemote fetch and what the store lacks', () => {
  it('takes a head it was told it has, but lacks, as not held', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await commitN(a.store, 2, 'a')
    await pushBranch(a.store, a.remote, BRANCH)
    // a store claiming the remote head (a sync head whose commits a
    // sweep took) still gets every commit
    const b = await device(gh)
    await b.remote.listRefs()
    const got = await collect(b.remote.fetch(wt.head, [wt.head]))
    expect(got.at(-1)?.hash).toBe(wt.head)
    expect((await applyWire(b.store, got)).applied).toBe(got.length)
  })
})
