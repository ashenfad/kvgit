import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  type BytesMergeFn,
  ConcurrencyError,
  MergeConflict,
  UnknownBranchError,
  VersionedKV,
} from '../src/index'
import { Keyset } from '../src/keyset'
import { PyFloat } from '../src/pyjson'
import { BRANCH_HEAD, COMMIT_ROOT, GC_LEASE_KEY, loads } from '../src/versioned/layout'
import { failReads, racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array): string => dec.decode(b)

describe('VersionedKV — open', () => {
  it('creates an initial empty commit on a fresh store', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    expect(vk.currentBranch).toBe('main')
    expect(vk.currentCommit).toBeTypeOf('string')
    expect(vk.currentCommit.length).toBe(40)
    expect(vk.currentCommit).toBe(vk.baseCommit)
    expect(await vk.get('any')).toBeNull()
  })

  it('reopens an existing store at HEAD', async () => {
    const store = new Memory()
    const vk1 = await VersionedKV.open(store)
    await vk1.commit({ updates: new Map([['k', bytes('v')]]) })
    const head = vk1.currentCommit

    const vk2 = await VersionedKV.open(store)
    expect(vk2.currentCommit).toBe(head)
    expect(text((await vk2.get('k')) as Uint8Array)).toBe('v')
  })

  it('rejects an existing store with the wrong storage version', async () => {
    const store = new Memory()
    await store.set('__kvgit_version__', enc.encode('999'))
    await store.set('__branch_head__main', enc.encode('"deadbeef"'))
    await expect(VersionedKV.open(store)).rejects.toThrow(/storage version/)
  })

  it('opens a different branch independently', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store, { branch: 'main' })
    await main.commit({ updates: new Map([['k', bytes('main-v')]]) })

    const otherBranch = (await main.createBranch('feature')) as VersionedKV
    expect(otherBranch.currentBranch).toBe('feature')
    expect(text((await otherBranch.get('k')) as Uint8Array)).toBe('main-v')
  })

  it("opens at the winner's head when another writer creates the branch first", async () => {
    // The opener decides the branch is missing, then loses the create
    // to a writer that makes it at a commit of its own.
    class RacingMemory extends Memory {
      beforeCreate: (() => Promise<void>) | null = null
      override async casMany(
        expected: ReadonlyMap<string, Uint8Array | null>,
        writes: ReadonlyMap<string, Uint8Array>,
        removes: Iterable<string> = [],
      ): Promise<boolean> {
        const fire = this.beforeCreate
        if (fire !== null && writes.has(BRANCH_HEAD('dev'))) {
          this.beforeCreate = null
          await fire()
        }
        return super.casMany(expected, writes, removes)
      }
    }
    const store = new RacingMemory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('v')]]) })
    store.beforeCreate = async () => {
      await main.createBranch('dev')
    }

    const dev = await VersionedKV.open(store, { branch: 'dev' })
    expect(dev.currentCommit).toBe(main.currentCommit)
    expect(text((await dev.get('k')) as Uint8Array)).toBe('v')
  })
})

describe('VersionedKV — commit info', () => {
  it('reads back integers past the safe range exactly, and floats as numbers', async () => {
    const vk = await VersionedKV.open(new Memory())
    const big = 2n ** 64n + 1n
    await vk.commit({
      updates: new Map([['k', bytes('v')]]),
      info: { big, whole: new PyFloat(2), half: 0.5, list: [new PyFloat(1.5), 3] },
    })
    expect(await vk.commitInfo()).toEqual({ big, whole: 2, half: 0.5, list: [1.5, 3] })
  })
})

describe('VersionedKV — open without creating', () => {
  it('mints a missing branch by default', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store, { branch: 'dev' })
    expect(vk.currentBranch).toBe('dev')
    expect(await vk.listBranches()).toContain('dev')
  })

  it('open with create:false throws and lists no branch', async () => {
    const store = new Memory()
    await expect(VersionedKV.open(store, { branch: 'dev', create: false })).rejects.toThrow(
      UnknownBranchError,
    )
    expect(await VersionedKV.exists(store, 'dev')).toBe(false)
    const vk = await VersionedKV.open(store)
    expect(await vk.listBranches()).not.toContain('dev')
  })

  it('open with create:false opens an existing branch', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    const vk2 = await VersionedKV.open(store, { branch: 'main', create: false })
    expect(text((await vk2.get('k')) as Uint8Array)).toBe('v')
  })

  it('exists and branchExists track branches without writing', async () => {
    const store = new Memory()
    expect(await VersionedKV.exists(store, 'dev')).toBe(false)
    await VersionedKV.open(store, { branch: 'dev' })
    expect(await VersionedKV.exists(store, 'dev')).toBe(true)
    expect(await VersionedKV.exists(store, 'nope')).toBe(false)
    const vk = await VersionedKV.open(store)
    expect(await vk.branchExists('main')).toBe(true)
    expect(await vk.branchExists('dev')).toBe(true)
    expect(await vk.branchExists('nope')).toBe(false)
  })

  it('delete then open does not resurrect or block recreate', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.createBranch('dev')
    await vk.deleteBranch('dev')
    expect(await VersionedKV.exists(store, 'dev')).toBe(false)
    await expect(VersionedKV.open(store, { branch: 'dev', create: false })).rejects.toThrow(
      UnknownBranchError,
    )
    expect(await VersionedKV.exists(store, 'dev')).toBe(false)
    await vk.createBranch('dev')
    expect(await VersionedKV.exists(store, 'dev')).toBe(true)
  })

  it('switchBranch throws UnknownBranchError for a missing branch', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await expect(vk.switchBranch('nope')).rejects.toThrow(UnknownBranchError)
  })
})

describe('VersionedKV — fast-forward commit', () => {
  it('commits a single update and advances HEAD', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const initial = vk.currentCommit
    const r = await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('fast_forward')
    expect(r.commit).not.toBe(initial)
    expect(vk.currentCommit).toBe(r.commit)
    expect(text((await vk.get('k')) as Uint8Array)).toBe('v')
  })

  it('treats an empty commit as a no-op', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const r = await vk.commit()
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('no_op')
    expect(r.commit).toBe(vk.currentCommit)
  })

  it('removes a key', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })
    await vk.commit({ removals: new Set(['k']) })
    expect(await vk.get('k')).toBeNull()
  })

  it('round-trips an info dict', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({
      updates: new Map([['k', bytes('v')]]),
      info: { author: 'alice', message: 'add k' },
    })
    expect(await vk.commitInfo()).toEqual({ author: 'alice', message: 'add k' })
  })

  it('history walks newest to oldest', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    const c0 = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })
    const c2 = vk.currentCommit

    const seen: string[] = []
    for await (const c of vk.history()) seen.push(c)
    expect(seen).toEqual([c2, c1, c0])
  })
})

describe('VersionedKV — branches', () => {
  it('writes on a fork do not leak to parent', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['shared', bytes('original')]]) })

    const feature = (await main.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['shared', bytes('forked')]]) })

    // main's view didn't change in-memory
    expect(text((await main.get('shared')) as Uint8Array)).toBe('original')
    // and a fresh handle on main also sees the original
    const main2 = await VersionedKV.open(store, { branch: 'main' })
    expect(text((await main2.get('shared')) as Uint8Array)).toBe('original')
  })

  it('peek reads a key from another branch without switching', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('main-v')]]) })

    const feature = (await main.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['k', bytes('feature-v')]]) })

    expect(text((await main.peek('k', { branch: 'feature' })) as Uint8Array)).toBe('feature-v')
    // main itself didn't switch
    expect(main.currentBranch).toBe('main')
    expect(text((await main.get('k')) as Uint8Array)).toBe('main-v')
  })

  it('switchBranch updates state in place', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('main')]]) })
    const feature = (await main.createBranch('feature')) as VersionedKV
    await feature.commit({ updates: new Map([['k', bytes('feature')]]) })

    await main.switchBranch('feature')
    expect(main.currentBranch).toBe('feature')
    expect(text((await main.get('k')) as Uint8Array)).toBe('feature')
  })

  it('listBranches returns sorted names', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.createBranch('zeta')
    await vk.createBranch('alpha')
    await vk.createBranch('mu')
    expect(await vk.listBranches()).toEqual(['alpha', 'main', 'mu', 'zeta'])
  })

  it('rejects creating a branch that already exists', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.createBranch('feature')
    await expect(vk.createBranch('feature')).rejects.toThrow(/already exists/)
  })

  it('deleteBranch removes a branch', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.createBranch('toremove')
    await vk.deleteBranch('toremove')
    expect((await vk.listBranches()).includes('toremove')).toBe(false)
  })

  it('cannot delete the current branch', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await expect(vk.deleteBranch('main')).rejects.toThrow(/current branch/)
  })
})

describe('VersionedKV — mergeBase', () => {
  it('returns the commit itself for identical inputs', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('v')]]) })
    expect(await main.mergeBase(main.currentCommit, main.currentCommit)).toBe(main.currentCommit)
  })

  it('resolves diverged branches to the fork point, either order', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('v')]]) })
    const fork = main.currentCommit
    const worker = (await main.createBranch('worker')) as VersionedKV
    await main.commit({ updates: new Map([['a', bytes('1')]]) })
    await worker.commit({ updates: new Map([['b', bytes('2')]]) })
    expect(await main.mergeBase(main.currentCommit, worker.currentCommit)).toBe(fork)
    expect(await worker.mergeBase(worker.currentCommit, main.currentCommit)).toBe(fork)
  })

  it('returns the ancestor for linear history', async () => {
    const store = new Memory()
    const main = await VersionedKV.open(store)
    await main.commit({ updates: new Map([['k', bytes('v')]]) })
    const base = main.currentCommit
    await main.commit({ updates: new Map([['a', bytes('1')]]) })
    expect(await main.mergeBase(base, main.currentCommit)).toBe(base)
  })
})

describe('VersionedKV — three-way merge', () => {
  it('auto-merges non-overlapping changes', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    await a.commit({ updates: new Map([['shared', bytes('base')]]) })

    // Two writers on the same branch, both based on the same commit.
    const b = await VersionedKV.open(store)

    await a.commit({ updates: new Map([['from-a', bytes('a-val')]]) })
    // b is now stale; b's base_commit doesn't match HEAD.
    const r = await b.commit({ updates: new Map([['from-b', bytes('b-val')]]) })
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('three_way')
    expect(text((await b.get('from-a')) as Uint8Array)).toBe('a-val')
    expect(text((await b.get('from-b')) as Uint8Array)).toBe('b-val')
    expect(text((await b.get('shared')) as Uint8Array)).toBe('base')
  })

  it('uses a per-key merge fn for contested keys', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    await a.commit({ updates: new Map([['counter', bytes('0')]]) })

    const b = await VersionedKV.open(store)

    await a.commit({ updates: new Map([['counter', bytes('1')]]) })

    // Sum-merge fn: parses both sides as ints and adds the deltas to base.
    const sumMerge: BytesMergeFn = (oldV, ours, theirs) => {
      const o = oldV ? Number.parseInt(text(oldV), 10) : 0
      const u = ours ? Number.parseInt(text(ours), 10) : o
      const t = theirs ? Number.parseInt(text(theirs), 10) : o
      return bytes(String(u + t - o))
    }

    const r = await b.commit({
      updates: new Map([['counter', bytes('5')]]),
      mergeFns: new Map([['counter', sumMerge]]),
    })
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('three_way')
    expect(r.autoMergedKeys).toEqual(['counter'])
    // a wrote 1 (delta +1), b wrote 5 (delta +5), base 0 → merged should be 6.
    expect(text((await b.get('counter')) as Uint8Array)).toBe('6')
  })

  it('throws MergeConflict for a contested key with no merge fn', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    await a.commit({ updates: new Map([['k', bytes('base')]]) })
    const b = await VersionedKV.open(store)

    await a.commit({ updates: new Map([['k', bytes('a')]]) })
    await expect(b.commit({ updates: new Map([['k', bytes('b')]]) })).rejects.toBeInstanceOf(
      MergeConflict,
    )
  })

  it("with onConflict 'abandon', returns merged=false instead of throwing", async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    await a.commit({ updates: new Map([['k', bytes('base')]]) })
    const b = await VersionedKV.open(store)

    await a.commit({ updates: new Map([['k', bytes('a')]]) })
    const r = await b.commit({
      updates: new Map([['k', bytes('b')]]),
      onConflict: 'abandon',
    })
    expect(r.merged).toBe(false)
    expect(r.commit).toBeNull()
    // b's in-memory base was restored (didn't move to a's commit).
    expect(text((await b.get('k')) as Uint8Array)).toBe('base')
  })

  it('default merge fn handles all contested keys', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    await a.commit({ updates: new Map([['k', bytes('base')]]) })
    const b = await VersionedKV.open(store)

    await a.commit({ updates: new Map([['k', bytes('a')]]) })
    const r = await b.commit({
      updates: new Map([['k', bytes('b')]]),
      defaultMerge: (_, ours, theirs) =>
        bytes(`${text(ours ?? bytes(''))}+${text(theirs ?? bytes(''))}`),
    })
    expect(r.merged).toBe(true)
    expect(text((await b.get('k')) as Uint8Array)).toMatch(/^[ab]\+[ab]$/)
  })
})

describe('VersionedKV — diff between commits', () => {
  it('reports added/removed/modified at the key level', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({
      updates: new Map([
        ['k1', bytes('v1')],
        ['k2', bytes('v2')],
      ]),
    })
    const c1 = vk.currentCommit
    await vk.commit({
      updates: new Map([
        ['k2', bytes('v2-new')], // modified
        ['k3', bytes('v3')], // added
      ]),
      removals: new Set(['k1']), // removed
    })
    const c2 = vk.currentCommit

    const d = await vk.diff(c1, c2)
    expect([...d.added]).toEqual(['k3'])
    expect([...d.removed]).toEqual(['k1'])
    expect([...d.modified]).toEqual(['k2'])
  })
})

describe('VersionedKV — checkout and resetTo', () => {
  it('checkout returns a Versioned at a historical commit', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })

    const historical = (await vk.checkout(c1)) as VersionedKV
    expect(historical.currentCommit).toBe(c1)
    expect(text((await historical.get('k')) as Uint8Array)).toBe('v1')

    // The original handle wasn't affected.
    expect(text((await vk.get('k')) as Uint8Array)).toBe('v2')
  })

  it('resetTo moves HEAD to a previous commit', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const c1 = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })

    expect(await vk.resetTo(c1)).toBe(true)
    expect(vk.currentCommit).toBe(c1)
    expect(text((await vk.get('k')) as Uint8Array)).toBe('v1')
  })
})

describe('VersionedKV — corrupt-HEAD recovery', () => {
  it('recovers via prev-HEAD when current HEAD points to a missing commit', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const goodHead = vk.currentCommit
    await vk.commit({ updates: new Map([['k', bytes('v2')]]) })
    // The CAS that just landed wrote `goodHead` to BRANCH_HEAD_PREV.

    // Simulate corruption: overwrite current HEAD with a string that
    // points to a nonexistent commit. The prev-HEAD backup still holds
    // `goodHead`, so a fresh open should recover to it.
    await store.set(
      '__branch_head__main',
      enc.encode(JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')),
    )

    const vk2 = await VersionedKV.open(store)
    expect(vk2.currentCommit).toBe(goodHead)
    expect(text((await vk2.get('k')) as Uint8Array)).toBe('v1')
  })

  it('throws when both HEAD and prev-HEAD are corrupt and no recoverer is set', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v')]]) })

    // Corrupt both pointers.
    const dead = JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')
    await store.set('__branch_head__main', enc.encode(dead))
    await store.set('__branch_head_prev__main', enc.encode(dead))

    await expect(VersionedKV.open(store)).rejects.toThrow(/corrupt and unrecoverable/)
  })

  it('uses an injected commit-scan recoverer when both HEAD pointers are bad', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit({ updates: new Map([['k', bytes('v1')]]) })
    const goodHead = vk.currentCommit

    // Corrupt both pointers.
    const dead = JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')
    await store.set('__branch_head__main', enc.encode(dead))
    await store.set('__branch_head_prev__main', enc.encode(dead))

    const vk2 = await VersionedKV.open(store, {
      recoverFromCorruptHead: async () => goodHead,
    })
    expect(vk2.currentCommit).toBe(goodHead)
  })
})

describe('VersionedKV — fast-forward race retry', () => {
  /** Make `loser`'s next CAS lose: advance HEAD first, then run it. */
  function raceOnce(loser: VersionedKV, advance: () => Promise<unknown>): void {
    racePublish(loser.store, loser.currentBranch, advance)
  }

  const one = (key: string, value: string) => ({ updates: new Map([[key, bytes(value)]]) })

  it('a lost race merges instead of raising', async () => {
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, () => v1.commit(one('other', '1')))

    const result = await v2.commit(one('mine', '2'))

    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
    expect(text((await v2.get('mine')) as Uint8Array)).toBe('2')
    expect(text((await v2.get('other')) as Uint8Array)).toBe('1')
  })

  it('a lost race with skip merges a clean change', async () => {
    // A lost race is not a conflict: like the base-behind-head case,
    // `skip` still attempts the merge and succeeds when clean.
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, () => v1.commit(one('other', '1')))

    const result = await v2.commit({ ...one('mine', '2'), onConflict: 'abandon' })

    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
  })

  it('a lost race with skip bails on a true conflict', async () => {
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('x', '1'))
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, () => v1.commit(one('x', 'v1')))

    const result = await v2.commit({ ...one('x', 'v2'), onConflict: 'abandon' })

    expect(result.merged).toBe(false)
    expect(result.strategy).toBe('three_way')
    expect(v1.currentCommit).toBe(await v2.latestHead()) // branch untouched
    expect(text((await v2.get('x')) as Uint8Array)).toBe('1') // loser restored
  })

  it('a repeated race still raises', async () => {
    // The internal retry is bounded: a race on the merge CAS too still
    // surfaces ConcurrencyError instead of looping.
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const v2 = await VersionedKV.open(store)
    racePublish(store, 'main', () => v1.commit(one('other', '1')), null)
    await expect(v2.commit(one('mine', '2'))).rejects.toThrow(ConcurrencyError)
  })

  it('a retry that finds its branch gone restores pre-commit state', async () => {
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const baseHead = v1.currentCommit
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, () => store.remove(BRANCH_HEAD('main')))

    await expect(v2.commit(one('mine', '2'))).rejects.toThrow(UnknownBranchError)
    expect(v2.currentCommit).toBe(baseHead)
    expect(await v2.get('mine')).toBeNull()
    expect(text((await v2.get('base')) as Uint8Array)).toBe('0')
  })

  it('a failing HEAD re-read restores pre-commit state', async () => {
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const baseHead = v1.currentCommit
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, async () => {
      await v1.commit(one('other', '1'))
      // A head read reads the head, the lease and the base's root
      // together; the heal check before it reads them one by one.
      failReads(
        store,
        (keys) => keys.includes(BRANCH_HEAD('main')) && keys.includes(GC_LEASE_KEY),
        new Error('transient storage boom'),
      )
    })

    await expect(v2.commit(one('mine', '2'))).rejects.toThrow('transient storage boom')
    expect(v2.currentCommit).toBe(baseHead)
    expect(await v2.get('mine')).toBeNull()
    // The handle stays usable.
    const result = await v2.commit(one('mine', '2'))
    expect(result.merged).toBe(true)
    expect(text((await v2.get('mine')) as Uint8Array)).toBe('2')
  })

  it('a raced merge strands no HAMT nodes', async () => {
    // The retry must keep its first attempt as our side: rebuilding the
    // commit after a lost CAS hashes identically but writes different
    // HAMT nodes, detaching the first attempt's nodes where the
    // incremental sweep cannot find them.
    const store = new Memory()
    const v1 = await VersionedKV.open(store)
    await v1.commit(one('base', '0'))
    const v2 = await VersionedKV.open(store)
    raceOnce(v2, () => v1.commit(one('other', '1')))
    const result = await v2.commit(one('mine', '2'))
    expect(result.merged).toBe(true)

    const reachable = new Set<string>()
    for await (const h of v2.history(v2.currentCommit, { allParents: true })) {
      const raw = await store.get(COMMIT_ROOT(h))
      if (raw === null) throw new Error(`commit ${h} has no root`)
      const [, nodes] = await Keyset.fromRoot(store, loads(raw) as string).walk()
      for (const n of nodes) reachable.add(n)
    }
    const present = new Set<string>()
    for await (const k of store.keys()) {
      if (k.startsWith(Keyset.DEFAULT_PREFIX)) present.add(k.slice(Keyset.DEFAULT_PREFIX.length))
    }
    expect([...present].filter((h) => !reachable.has(h))).toEqual([])
  })
})

describe('VersionedKV — mergeBase best common ancestor', () => {
  const one = (key: string, value: string) => ({ updates: new Map([[key, bytes(value)]]) })
  const oursFn: BytesMergeFn = (_old, ours, _theirs) => {
    if (ours === null) throw new Error('unreachable in these tests')
    return ours
  }
  const counterFn: BytesMergeFn = (oldB, oursB, theirsB) => {
    const num = (b: Uint8Array | null): number => (b === null ? 0 : Number(text(b)))
    return bytes(String(num(oursB) + num(theirsB) - num(oldB)))
  }

  it('a fork resolves to the fork point either order', async () => {
    const store = new Memory()
    const v = await VersionedKV.open(store)
    await v.commit(one('x', '0'))
    const fork = v.currentCommit
    const a = (await v.createBranch('a')) as VersionedKV
    const b = (await v.createBranch('b')) as VersionedKV
    await a.commit(one('a', '1'))
    await b.commit(one('b', '2'))

    expect(await v.mergeBase(a.currentCommit, b.currentCommit)).toBe(fork)
    expect(await v.mergeBase(b.currentCommit, a.currentCommit)).toBe(fork)
  })

  it('merge commits resolve through either order', async () => {
    // h2 loses the fast-forward race and lands a merge commit; the
    // base is the fork point whichever order the tips are given in.
    const store = new Memory()
    const v = await VersionedKV.open(store)
    await v.commit(one('x', '0'))
    const fork = v.currentCommit
    const h1 = await VersionedKV.open(store)
    const h2 = await VersionedKV.open(store)
    await h1.commit(one('a', '1'))
    const merged = await h2.commit(one('b', '2'))
    expect(merged.strategy).toBe('three_way')

    const tipA = h1.currentCommit
    const tipM = h2.currentCommit
    expect(await v.mergeBase(tipM, tipA)).toBe(tipA)
    expect(await v.mergeBase(tipA, tipM)).toBe(tipA)
    expect(await v.mergeBase(tipM, fork)).toBe(fork)
    expect(await v.mergeBase(tipM, tipM)).toBe(tipM)
  })

  it('merging the root into branches keeps the base', async () => {
    // Issue kvgit#47: naming the root as a merge parent must not
    // demote the base from the fork back to the root.
    const store = new Memory()
    const v = await VersionedKV.open(store)
    const root = v.currentCommit
    await v.commit(one('x', '1'))
    const fork = v.currentCommit
    const a = (await v.createBranch('a')) as VersionedKV
    const b = (await v.createBranch('b')) as VersionedKV
    await a.commit(one('a', '1'))
    await b.commit(one('b', '2'))
    await a.mergeHeads(root)
    await b.mergeHeads(root)

    expect(await v.mergeBase(a.currentCommit, b.currentCommit)).toBe(fork)
    expect(await v.mergeBase(b.currentCommit, a.currentCommit)).toBe(fork)
  })

  it('criss-cross base is deterministic', async () => {
    // Two best ancestors: the smallest hash wins, whichever order
    // the commits are given in.
    const store = new Memory()
    const v = await VersionedKV.open(store)
    const pa = (await v.createBranch('pa')) as VersionedKV
    const pb = (await v.createBranch('pb')) as VersionedKV
    await pa.commit(one('k', 'v1'))
    await pb.commit(one('k', 'v2'))
    const p1 = pa.currentCommit
    const p2 = pb.currentCommit
    const m = (await v.createBranch('m', { at: p1 })) as VersionedKV
    await m.mergeHeads(p2, { mergeFns: new Map([['k', oursFn]]) })
    const q = (await v.createBranch('q', { at: p2 })) as VersionedKV
    await q.mergeHeads(p1, { mergeFns: new Map([['k', oursFn]]) })

    const lo = p1 < p2 ? p1 : p2
    expect(await v.mergeBase(m.currentCommit, q.currentCommit)).toBe(lo)
    expect(await v.mergeBase(q.currentCommit, m.currentCommit)).toBe(lo)
  })

  it('criss-cross merge is direction independent', async () => {
    // Both merge directions use the same base now (and a symmetric
    // counter fn), so they resolve to the same value instead of each
    // side winning its own merge.
    const store = new Memory()
    const v = await VersionedKV.open(store)
    const pa = (await v.createBranch('pa')) as VersionedKV
    const pb = (await v.createBranch('pb')) as VersionedKV
    await pa.commit(one('n', '10'))
    await pb.commit(one('n', '20'))
    const p1 = pa.currentCommit
    const p2 = pb.currentCommit
    const m = (await v.createBranch('m', { at: p1 })) as VersionedKV
    await m.mergeHeads(p2, { mergeFns: new Map([['n', oursFn]]) })
    const q = (await v.createBranch('q', { at: p2 })) as VersionedKV
    await q.mergeHeads(p1, { mergeFns: new Map([['n', oursFn]]) })

    const fwd = (await v.createBranch('fwd', { at: m.currentCommit })) as VersionedKV
    await fwd.mergeHeads(q.currentCommit, { mergeFns: new Map([['n', counterFn]]) })
    const back = (await v.createBranch('back', { at: q.currentCommit })) as VersionedKV
    await back.mergeHeads(m.currentCommit, { mergeFns: new Map([['n', counterFn]]) })
    expect(text((await fwd.get('n')) as Uint8Array)).toBe(text((await back.get('n')) as Uint8Array))
  })
})

describe('VersionedKV — sanity: ConcurrencyError import path', () => {
  it('exports ConcurrencyError as a real class', () => {
    expect(typeof ConcurrencyError).toBe('function')
    const err = new ConcurrencyError('test')
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe('ConcurrencyError')
  })
})
