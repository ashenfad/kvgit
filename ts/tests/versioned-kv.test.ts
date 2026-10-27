import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  type BytesMergeFn,
  ConcurrencyError,
  type KVStore,
  MergeConflict,
  type MergeResult,
  Repo,
  UnknownBranchError,
  type Worktree,
  type WorktreeCommitOptions,
} from '../src/index'
import { PyFloat } from '../src/pyjson'
import { BRANCH_HEAD, COMMIT_ROOT, EMPTY_HASH, GC_LEASE_KEY, loads } from '../src/versioned/layout'
import { failReads, racePublish } from './races'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array): string => dec.decode(b)

/** A key's value as text, or undefined if absent. */
async function read(
  from: { get(key: string): Promise<Uint8Array | undefined> },
  key: string,
): Promise<string | undefined> {
  const value = await from.get(key)
  return value === undefined ? undefined : text(value)
}

/** Write `key` and commit. */
function put(
  wt: Worktree,
  key: string,
  value: string,
  opts: WorktreeCommitOptions<Uint8Array> = {},
): Promise<MergeResult> {
  wt.set(key, bytes(value))
  return wt.commit(opts)
}

/** A repository over `store` and a worktree on `branch`, created if
 *  missing. */
async function open(
  store: KVStore,
  branch = 'main',
): Promise<{ repo: Repo<Uint8Array>; wt: Worktree }> {
  const repo = await Repo.open(store)
  const wt = await repo.worktree(branch, { create: true })
  return { repo, wt }
}

/** A worktree on a new branch `name` at `at`. */
async function fork(repo: Repo<Uint8Array>, name: string, at: string): Promise<Worktree> {
  await repo.branches.create(name, at)
  return repo.worktree(name)
}

/** Commit hashes from a ref's log, newest first. */
async function history(
  repo: Repo<Uint8Array>,
  branch: string,
  opts: { firstParent?: boolean } = {},
): Promise<string[]> {
  const seen: string[] = []
  for await (const c of repo.log({ branch }, opts)) seen.push(c.hash)
  return seen
}

describe('Repo and Worktree — open', () => {
  it('starts a fresh store at the empty root commit', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    expect(wt.branch).toBe('main')
    expect(wt.head).toBeTypeOf('string')
    expect(wt.head.length).toBe(40)
    expect(await wt.get('any')).toBeUndefined()
  })

  it('reopens an existing store at HEAD', async () => {
    const store = new Memory()
    const { wt: wt1 } = await open(store)
    await put(wt1, 'k', 'v')
    const head = wt1.head

    const { wt: wt2 } = await open(store)
    expect(wt2.head).toBe(head)
    expect(await read(wt2, 'k')).toBe('v')
  })

  it('rejects an existing store with the wrong storage version', async () => {
    const store = new Memory()
    await store.set('__kvgit_version__', enc.encode('999'))
    await store.set('__branch_head__main', enc.encode('"deadbeef"'))
    await expect(Repo.open(store)).rejects.toThrow(/storage version/)
  })

  it('opens a different branch independently', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'main-v')

    const otherBranch = await fork(repo, 'feature', main.head)
    expect(otherBranch.branch).toBe('feature')
    expect(await read(otherBranch, 'k')).toBe('main-v')
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
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'v')
    store.beforeCreate = async () => {
      await repo.branches.create('dev', main.head)
    }

    const dev = await repo.worktree('dev', { create: true })
    expect(dev.head).toBe(main.head)
    expect(await read(dev, 'k')).toBe('v')
  })
})

describe('Repo and Worktree — commit info', () => {
  it('reads back integers past the safe range exactly, and floats as numbers', async () => {
    const { repo, wt } = await open(new Memory())
    const big = 2n ** 64n + 1n
    await put(wt, 'k', 'v', {
      info: { big, whole: new PyFloat(2), half: 0.5, list: [new PyFloat(1.5), 3] },
    })
    expect((await repo.getCommit(wt.head)).info).toEqual({
      big,
      whole: 2,
      half: 0.5,
      list: [1.5, 3],
    })
  })
})

describe('Repo and Worktree — open without creating', () => {
  it('worktree with create mints a missing branch', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store, 'dev')
    expect(wt.branch).toBe('dev')
    expect(await repo.branches.list()).toContain('dev')
  })

  it('worktree without create throws and lists no branch', async () => {
    const store = new Memory()
    const repo = await Repo.open(store)
    await expect(repo.worktree('dev')).rejects.toThrow(UnknownBranchError)
    expect(await repo.branches.has('dev')).toBe(false)
    expect(await repo.branches.list()).not.toContain('dev')
  })

  it('worktree without create opens an existing branch', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v')
    const repo2 = await Repo.open(store)
    const wt2 = await repo2.worktree('main')
    expect(await read(wt2, 'k')).toBe('v')
  })

  it('branches.has tracks branches without writing', async () => {
    const store = new Memory()
    const repo = await Repo.open(store)
    expect(await repo.branches.has('dev')).toBe(false)
    await repo.worktree('dev', { create: true })
    expect(await repo.branches.has('dev')).toBe(true)
    expect(await repo.branches.has('nope')).toBe(false)
    await repo.worktree('main', { create: true })
    const repo2 = await Repo.open(store)
    expect(await repo2.branches.has('main')).toBe(true)
    expect(await repo2.branches.has('dev')).toBe(true)
    expect(await repo2.branches.has('nope')).toBe(false)
  })

  it('delete then open does not resurrect or block recreate', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await repo.branches.create('dev', wt.head)
    await repo.branches.delete('dev')
    expect(await repo.branches.has('dev')).toBe(false)
    await expect(repo.worktree('dev')).rejects.toThrow(UnknownBranchError)
    expect(await repo.branches.has('dev')).toBe(false)
    await repo.branches.create('dev', wt.head)
    expect(await repo.branches.has('dev')).toBe(true)
  })
})

describe('Worktree — fast-forward commit', () => {
  it('commits a single update and advances HEAD', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    const initial = wt.head
    const r = await put(wt, 'k', 'v')
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('fast_forward')
    expect(r.commit).not.toBe(initial)
    expect(wt.head).toBe(r.commit)
    expect(await read(wt, 'k')).toBe('v')
  })

  it('treats an empty commit as a no-op', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    const r = await wt.commit()
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('no_op')
    expect(r.commit).toBe(wt.head)
  })

  it('removes a key', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v')
    await wt.delete('k')
    await wt.commit()
    expect(await wt.get('k')).toBeUndefined()
  })

  it('round-trips an info dict', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await put(wt, 'k', 'v', { info: { author: 'alice', message: 'add k' } })
    expect((await repo.getCommit(wt.head)).info).toEqual({ author: 'alice', message: 'add k' })
  })

  it('history walks newest to oldest', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    const c0 = wt.head
    await put(wt, 'k', 'v1')
    const c1 = wt.head
    await put(wt, 'k', 'v2')
    const c2 = wt.head

    expect(await history(repo, 'main')).toEqual([c2, c1, c0])
  })
})

describe('Repo — branches', () => {
  it('writes on a fork do not leak to parent', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'shared', 'original')

    const feature = await fork(repo, 'feature', main.head)
    await put(feature, 'shared', 'forked')

    // main's view didn't change in-memory
    expect(await read(main, 'shared')).toBe('original')
    // and a fresh worktree on main also sees the original
    const main2 = await repo.worktree('main')
    expect(await read(main2, 'shared')).toBe('original')
  })

  it('a snapshot reads a key from another branch', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'main-v')

    const feature = await fork(repo, 'feature', main.head)
    await put(feature, 'k', 'feature-v')

    expect(await read(await repo.snapshot({ branch: 'feature' }), 'k')).toBe('feature-v')
    // main itself didn't move
    expect(main.branch).toBe('main')
    expect(await read(main, 'k')).toBe('main-v')
  })

  it('branches.list returns sorted names', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await repo.branches.create('zeta', wt.head)
    await repo.branches.create('alpha', wt.head)
    await repo.branches.create('mu', wt.head)
    expect(await repo.branches.list()).toEqual(['alpha', 'main', 'mu', 'zeta'])
  })

  it('rejects creating a branch that already exists', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await repo.branches.create('feature', wt.head)
    await expect(repo.branches.create('feature', wt.head)).rejects.toThrow(/already exists/)
  })

  it('branches.delete removes a branch', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await repo.branches.create('toremove', wt.head)
    await repo.branches.delete('toremove')
    expect((await repo.branches.list()).includes('toremove')).toBe(false)
  })
})

describe('Repo — mergeBase', () => {
  it('returns the commit itself for identical inputs', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'v')
    expect(await repo.mergeBase(main.head, main.head)).toBe(main.head)
  })

  it('resolves diverged branches to the fork point, either order', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'v')
    const forkPoint = main.head
    const worker = await fork(repo, 'worker', main.head)
    await put(main, 'a', '1')
    await put(worker, 'b', '2')
    expect(await repo.mergeBase(main.head, worker.head)).toBe(forkPoint)
    expect(await repo.mergeBase(worker.head, main.head)).toBe(forkPoint)
  })

  it('returns the ancestor for linear history', async () => {
    const store = new Memory()
    const { repo, wt: main } = await open(store)
    await put(main, 'k', 'v')
    const base = main.head
    await put(main, 'a', '1')
    expect(await repo.mergeBase(base, main.head)).toBe(base)
  })
})

describe('Worktree — three-way merge', () => {
  it('auto-merges non-overlapping changes', async () => {
    const store = new Memory()
    const { repo, wt: a } = await open(store)
    await put(a, 'shared', 'base')

    // Two writers on the same branch, both based on the same commit.
    const b = await repo.worktree('main')

    await put(a, 'from-a', 'a-val')
    // b is now stale; its head doesn't match the branch's.
    const r = await put(b, 'from-b', 'b-val')
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('three_way')
    expect(await read(b, 'from-a')).toBe('a-val')
    expect(await read(b, 'from-b')).toBe('b-val')
    expect(await read(b, 'shared')).toBe('base')
  })

  it('uses a per-key merge fn for contested keys', async () => {
    const store = new Memory()
    const { repo, wt: a } = await open(store)
    await put(a, 'counter', '0')

    const b = await repo.worktree('main')

    await put(a, 'counter', '1')

    // Sum-merge fn: parses both sides as ints and adds the deltas to base.
    const sumMerge: BytesMergeFn = (oldV, ours, theirs) => {
      const o = oldV ? Number.parseInt(text(oldV), 10) : 0
      const u = ours ? Number.parseInt(text(ours), 10) : o
      const t = theirs ? Number.parseInt(text(theirs), 10) : o
      return bytes(String(u + t - o))
    }

    const r = await put(b, 'counter', '5', { mergeFns: new Map([['counter', sumMerge]]) })
    expect(r.merged).toBe(true)
    expect(r.strategy).toBe('three_way')
    expect(r.autoMergedKeys).toEqual(['counter'])
    // a wrote 1 (delta +1), b wrote 5 (delta +5), base 0 → merged should be 6.
    expect(await read(b, 'counter')).toBe('6')
  })

  it('throws MergeConflict for a contested key with no merge fn', async () => {
    const store = new Memory()
    const { repo, wt: a } = await open(store)
    await put(a, 'k', 'base')
    const b = await repo.worktree('main')

    await put(a, 'k', 'a')
    await expect(put(b, 'k', 'b')).rejects.toBeInstanceOf(MergeConflict)
  })

  it("with onConflict 'abandon', returns merged=false instead of throwing", async () => {
    const store = new Memory()
    const { repo, wt: a } = await open(store)
    await put(a, 'k', 'base')
    const b = await repo.worktree('main')
    const bHead = b.head

    await put(a, 'k', 'a')
    const r = await put(b, 'k', 'b', { onConflict: 'abandon' })
    expect(r.merged).toBe(false)
    expect(r.commit).toBeNull()
    // b's head stayed put (didn't move to a's commit).
    expect(b.head).toBe(bHead)
    expect(await read(await repo.snapshot({ commit: b.head }), 'k')).toBe('base')
  })

  it('default merge fn handles all contested keys', async () => {
    const store = new Memory()
    const { repo, wt: a } = await open(store)
    await put(a, 'k', 'base')
    const b = await repo.worktree('main')

    await put(a, 'k', 'a')
    const r = await put(b, 'k', 'b', {
      defaultMerge: (_, ours, theirs) =>
        bytes(`${text(ours ?? bytes(''))}+${text(theirs ?? bytes(''))}`),
    })
    expect(r.merged).toBe(true)
    expect(await read(b, 'k')).toMatch(/^[ab]\+[ab]$/)
  })
})

describe('Repo — diff between commits', () => {
  it('reports added/removed/modified at the key level', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    wt.set('k1', bytes('v1'))
    wt.set('k2', bytes('v2'))
    await wt.commit()
    const c1 = wt.head
    wt.set('k2', bytes('v2-new')) // modified
    wt.set('k3', bytes('v3')) // added
    await wt.delete('k1') // removed
    await wt.commit()
    const c2 = wt.head

    const d = await repo.diff(c1, c2)
    expect([...d.added]).toEqual(['k3'])
    expect([...d.removed]).toEqual(['k1'])
    expect([...d.modified]).toEqual(['k2'])
  })
})

describe('Repo and Worktree — snapshot and reset', () => {
  it('a snapshot reads a historical commit', async () => {
    const store = new Memory()
    const { repo, wt } = await open(store)
    await put(wt, 'k', 'v1')
    const c1 = wt.head
    await put(wt, 'k', 'v2')

    const historical = await repo.snapshot({ commit: c1 })
    expect(historical.commit).toBe(c1)
    expect(await read(historical, 'k')).toBe('v1')

    // The worktree wasn't affected.
    expect(await read(wt, 'k')).toBe('v2')
  })

  it('reset moves HEAD to a previous commit', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v1')
    const c1 = wt.head
    await put(wt, 'k', 'v2')

    await expect(wt.reset(c1)).resolves.toBeUndefined()
    expect(wt.head).toBe(c1)
    expect(await read(wt, 'k')).toBe('v1')
  })
})

describe('Repo — corrupt-HEAD recovery', () => {
  it('recovers via prev-HEAD when current HEAD points to a missing commit', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v1')
    const goodHead = wt.head
    await put(wt, 'k', 'v2')
    // The CAS that just landed wrote `goodHead` to BRANCH_HEAD_PREV.

    // Simulate corruption: overwrite current HEAD with a string that
    // points to a nonexistent commit. The prev-HEAD backup still holds
    // `goodHead`, so a fresh open should recover to it.
    await store.set(
      '__branch_head__main',
      enc.encode(JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')),
    )

    const wt2 = await (await Repo.open(store)).worktree('main')
    expect(wt2.head).toBe(goodHead)
    expect(await read(wt2, 'k')).toBe('v1')
  })

  it('throws when both HEAD and prev-HEAD are corrupt and no recoverer is set', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v')

    // Corrupt both pointers.
    const dead = JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')
    await store.set('__branch_head__main', enc.encode(dead))
    await store.set('__branch_head_prev__main', enc.encode(dead))

    const repo2 = await Repo.open(store)
    await expect(repo2.worktree('main')).rejects.toThrow(/corrupt and unrecoverable/)
  })

  it('uses an injected commit-scan recoverer when both HEAD pointers are bad', async () => {
    const store = new Memory()
    const { wt } = await open(store)
    await put(wt, 'k', 'v1')
    const goodHead = wt.head

    // Corrupt both pointers.
    const dead = JSON.stringify('deadbeefdeadbeefdeadbeefdeadbeefdeadbeef')
    await store.set('__branch_head__main', enc.encode(dead))
    await store.set('__branch_head_prev__main', enc.encode(dead))

    const repo2 = await Repo.open(store, { recoverFromCorruptHead: async () => goodHead })
    const wt2 = await repo2.worktree('main')
    expect(wt2.head).toBe(goodHead)
  })
})

describe('Worktree — fast-forward race retry', () => {
  /** Make `loser`'s next CAS lose: advance HEAD first, then run it. */
  function raceOnce(loser: Worktree, advance: () => Promise<unknown>): void {
    racePublish(loser.repo.store, loser.branch, advance)
  }

  /** The node hashes under a keyset root, the root included. */
  async function walkNodes(store: KVStore, root: string, into: Set<string>): Promise<void> {
    if (root === EMPTY_HASH || into.has(root)) return
    into.add(root)
    const raw = await store.get(`kvgit:keyset:${root}`)
    if (raw === null) throw new Error(`node ${root} is missing`)
    const node = JSON.parse(text(raw)) as { children?: Record<string, string> }
    for (const child of Object.values(node.children ?? {})) await walkNodes(store, child, into)
  }

  it('a lost race merges instead of raising', async () => {
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const v2 = await repo.worktree('main')
    raceOnce(v2, () => put(v1, 'other', '1'))

    const result = await put(v2, 'mine', '2')

    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
    expect(await read(v2, 'mine')).toBe('2')
    expect(await read(v2, 'other')).toBe('1')
  })

  it('a lost race with abandon merges a clean change', async () => {
    // A lost race is not a conflict: like the head-behind-branch case,
    // `abandon` still attempts the merge and succeeds when clean.
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const v2 = await repo.worktree('main')
    raceOnce(v2, () => put(v1, 'other', '1'))

    const result = await put(v2, 'mine', '2', { onConflict: 'abandon' })

    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('three_way')
  })

  it('a lost race with abandon bails on a true conflict', async () => {
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'x', '1')
    const v2 = await repo.worktree('main')
    const v2Head = v2.head
    raceOnce(v2, () => put(v1, 'x', 'v1'))

    const result = await put(v2, 'x', 'v2', { onConflict: 'abandon' })

    expect(result.merged).toBe(false)
    expect(result.strategy).toBe('three_way')
    expect(v1.head).toBe(await repo.branches.get('main')) // branch untouched
    // The loser stays at its head.
    expect(v2.head).toBe(v2Head)
    expect(await read(await repo.snapshot({ commit: v2.head }), 'x')).toBe('1')
  })

  it('a repeated race still raises', async () => {
    // The internal retry is bounded: a race on the merge CAS too still
    // surfaces ConcurrencyError instead of looping.
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const v2 = await repo.worktree('main')
    racePublish(store, 'main', () => put(v1, 'other', '1'), null)
    await expect(put(v2, 'mine', '2')).rejects.toThrow(ConcurrencyError)
  })

  it('a retry that finds its branch gone keeps the pre-commit state', async () => {
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const baseHead = v1.head
    const v2 = await repo.worktree('main')
    raceOnce(v2, () => store.remove(BRANCH_HEAD('main')))

    await expect(put(v2, 'mine', '2')).rejects.toThrow(UnknownBranchError)
    expect(v2.head).toBe(baseHead)
    expect(await (await repo.snapshot({ commit: v2.head })).has('mine')).toBe(false)
    expect(v2.status().updated).toEqual(new Set(['mine']))
    expect(await read(v2, 'base')).toBe('0')
  })

  it('a failing HEAD re-read keeps the pre-commit state', async () => {
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const baseHead = v1.head
    const v2 = await repo.worktree('main')
    raceOnce(v2, async () => {
      await put(v1, 'other', '1')
      // A head read reads the head, the lease and the base's root
      // together; the heal check before it reads them one by one.
      failReads(
        store,
        (keys) => keys.includes(BRANCH_HEAD('main')) && keys.includes(GC_LEASE_KEY),
        new Error('transient storage boom'),
      )
    })

    await expect(put(v2, 'mine', '2')).rejects.toThrow('transient storage boom')
    expect(v2.head).toBe(baseHead)
    expect(await (await repo.snapshot({ commit: v2.head })).has('mine')).toBe(false)
    // The worktree stays usable.
    const result = await put(v2, 'mine', '2')
    expect(result.merged).toBe(true)
    expect(await read(v2, 'mine')).toBe('2')
  })

  it('a raced merge strands no HAMT nodes', async () => {
    // The retry must keep its first attempt as our side: rebuilding the
    // commit after a lost CAS hashes identically but writes different
    // HAMT nodes, detaching the first attempt's nodes where the
    // incremental sweep cannot find them.
    const store = new Memory()
    const { repo, wt: v1 } = await open(store)
    await put(v1, 'base', '0')
    const v2 = await repo.worktree('main')
    raceOnce(v2, () => put(v1, 'other', '1'))
    const result = await put(v2, 'mine', '2')
    expect(result.merged).toBe(true)

    const reachable = new Set<string>()
    for await (const c of repo.log({ commit: v2.head })) {
      const raw = await store.get(COMMIT_ROOT(c.hash))
      if (raw === null) throw new Error(`commit ${c.hash} has no root`)
      await walkNodes(store, loads(raw) as string, reachable)
    }
    const prefix = 'kvgit:keyset:'
    const present = new Set<string>()
    for await (const k of store.keys()) {
      if (k.startsWith(prefix)) present.add(k.slice(prefix.length))
    }
    expect([...present].filter((h) => !reachable.has(h))).toEqual([])
  })
})

describe('Repo — mergeBase best common ancestor', () => {
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
    const { repo, wt: v } = await open(store)
    await put(v, 'x', '0')
    const forkPoint = v.head
    const a = await fork(repo, 'a', v.head)
    const b = await fork(repo, 'b', v.head)
    await put(a, 'a', '1')
    await put(b, 'b', '2')

    expect(await repo.mergeBase(a.head, b.head)).toBe(forkPoint)
    expect(await repo.mergeBase(b.head, a.head)).toBe(forkPoint)
  })

  it('merge commits resolve through either order', async () => {
    // h2 loses the fast-forward race and lands a merge commit; the
    // base is the fork point whichever order the tips are given in.
    const store = new Memory()
    const { repo, wt: v } = await open(store)
    await put(v, 'x', '0')
    const forkPoint = v.head
    const h1 = await repo.worktree('main')
    const h2 = await repo.worktree('main')
    await put(h1, 'a', '1')
    const merged = await put(h2, 'b', '2')
    expect(merged.strategy).toBe('three_way')

    const tipA = h1.head
    const tipM = h2.head
    expect(await repo.mergeBase(tipM, tipA)).toBe(tipA)
    expect(await repo.mergeBase(tipA, tipM)).toBe(tipA)
    expect(await repo.mergeBase(tipM, forkPoint)).toBe(forkPoint)
    expect(await repo.mergeBase(tipM, tipM)).toBe(tipM)
  })

  it('merging the root into branches keeps the base', async () => {
    // Issue kvgit#47: naming the root as a merge parent must not
    // demote the base from the fork back to the root.
    const store = new Memory()
    const { repo, wt: v } = await open(store)
    const root = v.head
    await put(v, 'x', '1')
    const forkPoint = v.head
    const a = await fork(repo, 'a', v.head)
    const b = await fork(repo, 'b', v.head)
    await put(a, 'a', '1')
    await put(b, 'b', '2')
    await a.merge({ commit: root })
    await b.merge({ commit: root })

    expect(await repo.mergeBase(a.head, b.head)).toBe(forkPoint)
    expect(await repo.mergeBase(b.head, a.head)).toBe(forkPoint)
  })

  it('criss-cross base is deterministic', async () => {
    // Two best ancestors: the smallest hash wins, whichever order
    // the commits are given in.
    const store = new Memory()
    const { repo, wt: v } = await open(store)
    const pa = await fork(repo, 'pa', v.head)
    const pb = await fork(repo, 'pb', v.head)
    await put(pa, 'k', 'v1')
    await put(pb, 'k', 'v2')
    const p1 = pa.head
    const p2 = pb.head
    const m = await fork(repo, 'm', p1)
    await m.merge({ commit: p2 }, { mergeFns: new Map([['k', oursFn]]) })
    const q = await fork(repo, 'q', p2)
    await q.merge({ commit: p1 }, { mergeFns: new Map([['k', oursFn]]) })

    const lo = p1 < p2 ? p1 : p2
    expect(await repo.mergeBase(m.head, q.head)).toBe(lo)
    expect(await repo.mergeBase(q.head, m.head)).toBe(lo)
  })

  it('criss-cross merge is direction independent', async () => {
    // Both merge directions use the same base (and a symmetric counter
    // fn), so they resolve to the same value instead of each side
    // winning its own merge.
    const store = new Memory()
    const { repo, wt: v } = await open(store)
    const pa = await fork(repo, 'pa', v.head)
    const pb = await fork(repo, 'pb', v.head)
    await put(pa, 'n', '10')
    await put(pb, 'n', '20')
    const p1 = pa.head
    const p2 = pb.head
    const m = await fork(repo, 'm', p1)
    await m.merge({ commit: p2 }, { mergeFns: new Map([['n', oursFn]]) })
    const q = await fork(repo, 'q', p2)
    await q.merge({ commit: p1 }, { mergeFns: new Map([['n', oursFn]]) })

    const fwd = await fork(repo, 'fwd', m.head)
    await fwd.merge({ commit: q.head }, { mergeFns: new Map([['n', counterFn]]) })
    const back = await fork(repo, 'back', q.head)
    await back.merge({ commit: m.head }, { mergeFns: new Map([['n', counterFn]]) })
    expect(await read(fwd, 'n')).toBe(await read(back, 'n'))
  })
})

describe('sanity: ConcurrencyError import path', () => {
  it('exports ConcurrencyError as a real class', () => {
    expect(typeof ConcurrencyError).toBe('function')
    const err = new ConcurrencyError('test')
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe('ConcurrencyError')
  })
})
