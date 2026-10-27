/**
 * PackRemote (spike) against a real GitHub repo: round trip, the CAS
 * under a real race, and the lifecycle renames. Env-gated like the
 * other live suites:
 *
 *   KVGIT_GH_TOKEN=$(gh auth token) KVGIT_GH_REPO=you/scratch \
 *     npx vitest run tests/github-pack-live.test.ts
 */

import { afterAll, describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { GithubClient, PackRemote } from '../src/github/index'
import { Repo, pullBranch, pushBranch, walkDelta } from '../src/index'

const TOKEN = process.env.KVGIT_GH_TOKEN ?? ''
const REPO = process.env.KVGIT_GH_REPO ?? ''
const LIVE = TOKEN.length > 0 && REPO.length > 0

const enc = new TextEncoder()
const stamp = Date.now().toString(36)

function device(): { store: Memory; remote: PackRemote } {
  const store = new Memory()
  const client = new GithubClient({ token: TOKEN, repo: REPO })
  return { store, remote: new PackRemote(client, store, { meta: () => ({ title: 'live' }) }) }
}

async function extend(store: Memory, branch: string, tag: string): Promise<string> {
  const wt = await (await Repo.open(store)).worktree(branch, { create: true })
  wt.set(`k/${tag}`, enc.encode(`value ${tag}`))
  await wt.commit()
  return wt.head
}

describe.runIf(LIVE)('PackRemote (live)', () => {
  const branches: string[] = []
  afterAll(async () => {
    const cleanup = device()
    for (const b of branches) {
      for (const ref of [b, `archived/${b}`]) {
        await cleanup.remote.client
          .request('DELETE', `git/refs/heads/${ref}`)
          .catch(() => undefined)
      }
    }
  })

  it('two devices race a push from the same head: exactly one lands, the other gets false', async () => {
    const branch = `pack-race-${stamp}`
    branches.push(branch)
    const a = device()
    const b = device()
    await extend(a.store, branch, 'base')
    expect((await pushBranch(a.store, a.remote, branch)).status).toBe('created')
    expect((await pullBranch(b.store, b.remote, branch)).status).toBe('created')
    const base = await (await Repo.open(a.store)).branches.get(branch)

    const headA = await extend(a.store, branch, 'from-a')
    const headB = await extend(b.store, branch, 'from-b')
    await Promise.all([a.remote.listRefs(), b.remote.listRefs()])
    const t0 = performance.now()
    const [okA, okB] = await Promise.all([
      a.remote.push(branch, base, headA, walkDelta(a.store, { want: headA, have: [base] })),
      b.remote.push(branch, base, headB, walkDelta(b.store, { want: headB, have: [base] })),
    ])
    const ms = Math.round(performance.now() - t0)
    console.log(JSON.stringify({ race: 'push', okA, okB, ms }))
    expect([okA, okB].filter(Boolean)).toHaveLength(1)

    const winner = okA ? headA : headB
    const refs = await device().remote.listRefs()
    expect(refs.find((r) => r.branch === branch)?.head).toBe(winner)
    // The loser, through the sync layer: divergence, nothing moved.
    const loser = okA ? b : a
    expect((await pushBranch(loser.store, loser.remote, branch)).status).toBe('diverged')
  }, 120_000)

  it('two devices race to create the same session: exactly one lands', async () => {
    const branch = `pack-create-${stamp}`
    branches.push(branch)
    const a = device()
    const b = device()
    const headA = await extend(a.store, branch, 'a')
    const headB = await extend(b.store, branch, 'b')
    await Promise.all([a.remote.listRefs(), b.remote.listRefs()])
    const [okA, okB] = await Promise.all([
      a.remote.push(branch, null, headA, walkDelta(a.store, { want: headA })),
      b.remote.push(branch, null, headB, walkDelta(b.store, { want: headB })),
    ])
    console.log(JSON.stringify({ race: 'create', okA, okB }))
    expect([okA, okB].filter(Boolean)).toHaveLength(1)
    const refs = await device().remote.listRefs()
    expect(refs.find((r) => r.branch === branch)?.head).toBe(okA ? headA : headB)
  }, 120_000)

  it('archive, roster, restore: one updateRefs each', async () => {
    const branch = `pack-life-${stamp}`
    branches.push(branch)
    const a = device()
    await extend(a.store, branch, 'x')
    await pushBranch(a.store, a.remote, branch)
    expect(await a.remote.archiveBranch(branch)).toBe(true)
    const roster = await a.remote.roster()
    expect(roster.find((r) => r.branch === branch)).toMatchObject({
      archived: true,
      meta: { title: 'live' },
    })
    expect((await a.remote.listRefs()).some((r) => r.branch === branch)).toBe(false)
    expect(await a.remote.restoreBranch(branch)).toBe(true)
    expect((await a.remote.listRefs()).some((r) => r.branch === branch)).toBe(true)
  }, 120_000)
})

describe.runIf(!LIVE)('PackRemote (live)', () => {
  it.skip('skipped: set KVGIT_GH_TOKEN and KVGIT_GH_REPO to run', () => {})
})
