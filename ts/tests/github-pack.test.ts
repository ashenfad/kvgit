/**
 * PackRemote (spike) against an in-memory GitHub: one pack per push,
 * fetched by manifest, with CAS on `expectedHeadOid`.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { FakeGithub } from '../src/github/fake'
import {
  GithubClient,
  MANIFEST_PATH,
  PackRemote,
  decodePack,
  encodePack,
} from '../src/github/index'
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

  it('splits a push larger than maxPackBytes into several CAS-chained commits', async () => {
    const gh = new FakeGithub()
    const a = await device(gh, { maxPackBytes: 150 })
    const wt = await commitN(a.store, 6, 'a') // ~80 bytes of values per commit
    expect((await pushBranch(a.store, a.remote, BRANCH)).status).toBe('created')
    expect(gh.count('graphql-mutation')).toBeGreaterThan(1)
    const b = await device(gh)
    const pulled = await pullBranch(b.store, b.remote, BRANCH)
    expect(pulled.localHead).toBe(wt.head)
  })
})

describe('PackRemote and manifests it cannot use', () => {
  for (const [what, text] of [
    ['a newer format', '{"format": 2, "head": "x", "packs": []}'],
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

  it('refuses a commit too large for one request, before sending it', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await open(a.store)
    // random bytes don't compress: 24MB stays over the 30MB base64 cap
    const big = new Uint8Array(24 * 1024 * 1024)
    for (let i = 0; i < big.length; i += 65536) crypto.getRandomValues(big.subarray(i, i + 65536))
    wt.set('files/big.bin', big)
    await wt.commit()
    await expect(pushBranch(a.store, a.remote, BRANCH)).rejects.toThrow(
      /more than one request can carry/,
    )
    expect(gh.count('graphql-mutation')).toBe(0)
  })
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

describe('PackRemote.repack and its request size', () => {
  // 24MB committed, pushed and packed: seconds, more on a loaded runner.
  it('refuses a history too large for one request, before sending it', async () => {
    const gh = new FakeGithub()
    const a = await device(gh)
    const wt = await open(a.store)
    // random bytes don't compress: two 12MB pushes fit, one 24MB pack doesn't
    for (const name of ['one', 'two']) {
      const big = new Uint8Array(12 * 1024 * 1024)
      for (let i = 0; i < big.length; i += 65536) crypto.getRandomValues(big.subarray(i, i + 65536))
      wt.set(`files/${name}.bin`, big)
      await wt.commit()
      expect((await pushBranch(a.store, a.remote, BRANCH)).status).not.toBe('diverged')
    }
    const mutations = gh.count('graphql-mutation')
    await expect(a.remote.repack(BRANCH)).rejects.toThrow(/more than one request can carry/)
    expect(gh.count('graphql-mutation')).toBe(mutations)
  }, 30_000)
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
