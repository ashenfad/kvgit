import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  ConcurrencyError,
  MergeChoice,
  MergeConflict,
  ROOT_COMMIT,
  Repo,
  UnknownCommitError,
  jsonCodec,
} from '../src/index'
import { VersionedKV, mergeBase } from '../src/versioned/kv'
import { PARENT_GENS } from '../src/versioned/layout'

/** A JSON repo with `main` holding `seed`, and a clock the test moves. */
async function setup(seed: Record<string, unknown> = { base: 0 }) {
  let clock = 1_760_000_000
  const store = new Memory()
  const repo = await Repo.open(store, {
    codec: jsonCodec,
    now: () => {
      clock += 1
      return clock
    },
  })
  const main = await repo.worktree('main', { create: true })
  for (const [k, v] of Object.entries(seed)) main.set(k, v)
  await main.commit()
  return { repo, store, main }
}

async function state(repo: Repo<unknown>, branch: string): Promise<Record<string, unknown>> {
  const snap = await repo.snapshot({ branch })
  return Object.fromEntries(await snap.getMany(await snap.keys()))
}

describe('Worktree.merge', () => {
  it('fast-forwards a branch that has not moved, unless asked for a merge commit', async () => {
    const { repo, main } = await setup()
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('d', 1)
    const devHead = (await dev.commit()).commit as string

    const result = await main.merge({ branch: 'dev' })
    expect(result.strategy).toBe('fast_forward')
    expect(result.commit).toBe(devHead)
    expect(result.carriedKeys).toEqual(['d'])
    expect(main.head).toBe(devHead)
    expect(await main.get('d')).toBe(1)

    // Merging it again is a no-op.
    expect((await main.merge({ branch: 'dev' })).strategy).toBe('no_op')

    // fastForward: false writes a two-parent commit anyway.
    dev.set('d', 2)
    const next = (await dev.commit()).commit as string
    const forced = await main.merge({ commit: next }, { fastForward: false, info: { m: 1 } })
    expect(forced.strategy).toBe('three_way')
    expect((await repo.getCommit(forced.commit as string)).parents).toEqual([devHead, next])
    expect((await repo.getCommit(forced.commit as string)).info).toEqual({ m: 1 })
  })

  it('writes a two-parent merge commit when both sides moved', async () => {
    const { repo, main } = await setup()
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('d', 1)
    const devHead = (await dev.commit()).commit as string
    main.set('m', 1)
    const ourHead = (await main.commit()).commit as string

    const result = await main.merge({ branch: 'dev' })
    expect(result.strategy).toBe('three_way')
    expect(result.carriedKeys).toEqual(['d'])
    expect((await repo.getCommit(result.commit as string)).parents).toEqual([ourHead, devHead])
    expect(await state(repo, 'main')).toEqual({ base: 0, d: 1, m: 1 })
  })

  it('resolves contested keys with value-level rules, and conflicts without them', async () => {
    const { repo, main } = await setup({ count: 10, title: 'base' })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('count', 13)
    dev.set('title', 'dev')
    await dev.commit()
    main.set('count', 11)
    main.set('title', 'main')
    await main.commit()

    await expect(main.merge({ branch: 'dev' })).rejects.toThrow(MergeConflict)
    expect((await main.merge({ branch: 'dev' }, { onConflict: 'abandon' })).merged).toBe(false)

    const result = await main.merge(
      { branch: 'dev' },
      {
        mergeFns: new Map<string, (o: unknown, a: unknown, b: unknown) => unknown>([
          ['count', (old, ours, theirs) => (ours as number) + (theirs as number) - (old as number)],
          // A merge function may answer with a side instead of a value.
          ['title', () => MergeChoice.OURS],
        ]),
      },
    )
    expect(result.autoMergedKeys).toEqual(['count', 'title'])
    expect(await state(repo, 'main')).toEqual({ count: 14, title: 'main' })
  })

  it('refuses a merged value postCheck rejects', async () => {
    const { repo, main } = await setup({ n: 1 })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('n', 2)
    await dev.commit()
    main.set('n', 3)
    await main.commit()
    const sum = new Map([
      ['n', (_o: unknown, a: unknown, b: unknown) => (a as number) + (b as number)],
    ])
    await expect(
      main.merge({ branch: 'dev' }, { mergeFns: sum, postCheck: () => false }),
    ).rejects.toThrow(MergeConflict)
    const ok = await main.merge({ branch: 'dev' }, { mergeFns: sum, postCheck: () => true })
    expect(ok.merged).toBe(true)
    expect(await main.get('n')).toBe(5)
  })

  it('applies registered MergeChoice policies to every key they cover', async () => {
    const { repo, main } = await setup({ 'cfg/a': 'base', 'cfg/gone': 'base', other: 'base' })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('cfg/a', 'dev')
    dev.set('cfg/new', 'dev')
    await dev.delete('cfg/gone')
    dev.set('other', 'dev')
    await dev.commit()
    main.set('m', 1)
    await main.commit()

    // OURS keeps main's state for every cfg/ key the other side
    // touched: no cfg/new, cfg/gone still there. Other keys merge
    // normally.
    main.setMergePrefix('cfg/', MergeChoice.OURS)
    const result = await main.merge({ branch: 'dev' })
    expect([...result.autoMergedKeys].sort()).toEqual(['cfg/a', 'cfg/gone', 'cfg/new'])
    expect(await state(repo, 'main')).toEqual({
      'cfg/a': 'base',
      'cfg/gone': 'base',
      m: 1,
      other: 'dev',
    })
  })

  it('takes the most specific registration', async () => {
    const { repo, main } = await setup({ 'a/b/c': 0, 'a/x': 0 })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('a/b/c', 'dev')
    dev.set('a/x', 'dev')
    await dev.commit()
    main.set('a/b/c', 'main')
    main.set('a/x', 'main')
    await main.commit()

    const result = await main.merge(
      { branch: 'dev' },
      {
        mergePrefixes: new Map([
          ['a/', MergeChoice.OURS],
          ['a/b/', MergeChoice.THEIRS],
        ]),
      },
    )
    expect(result.merged).toBe(true)
    expect(await state(repo, 'main')).toEqual({ 'a/b/c': 'dev', 'a/x': 'main' })
  })

  it('refuses with pending changes, and a commit not in the store', async () => {
    const { main } = await setup()
    main.set('pending', 1)
    await expect(main.merge({ branch: 'main' })).rejects.toThrow(/pending changes/)
    main.discard()
    await expect(main.merge({ commit: 'f'.repeat(40) })).rejects.toThrow(UnknownCommitError)
  })
})

describe('Worktree.apply, cherryPick and revert', () => {
  it('picks one change onto another branch, and undoes it', async () => {
    const { repo, main } = await setup({ a: 1, b: 1 })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('early', 1)
    await dev.commit()
    dev.set('a', 2)
    await dev.delete('b')
    const change = (await dev.commit()).commit as string

    const pick = await main.cherryPick(change, { info: { picked: change } })
    expect(pick.strategy).toBe('apply')
    expect(pick.carriedKeys).toEqual(['a', 'b'])
    // Only that change: `early` stays on dev.
    expect(await state(repo, 'main')).toEqual({ a: 2 })
    const record = await repo.getCommit(pick.commit as string)
    expect(record.parents.length).toBe(1)
    expect(record.info).toEqual({ picked: change })

    // Picking it again changes nothing.
    expect((await main.cherryPick(change)).strategy).toBe('no_op')

    const undo = await main.revert(change)
    expect(undo.strategy).toBe('apply')
    expect(await state(repo, 'main')).toEqual({ a: 1, b: 1 })
  })

  it('keeps changes made since, and conflicts where both changed a key', async () => {
    const { repo, main } = await setup({ a: 1 })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('a', 2)
    const change = (await dev.commit()).commit as string
    main.set('a', 3)
    await main.commit()

    await expect(main.cherryPick(change)).rejects.toThrow(MergeConflict)
    expect((await main.cherryPick(change, { onConflict: 'abandon' })).merged).toBe(false)
    const theirs = await main.cherryPick(change, { defaultMerge: MergeChoice.THEIRS })
    expect(theirs.merged).toBe(true)
    expect(await main.get('a')).toBe(2)
  })

  it('refuses commits that are not in the store', async () => {
    const { main } = await setup()
    await expect(main.cherryPick('f'.repeat(40))).rejects.toThrow(UnknownCommitError)
    await expect(main.apply(main.head, 'f'.repeat(40))).rejects.toThrow(UnknownCommitError)
  })

  it('does not report a no-op from a stale handle', async () => {
    // The stale handle already holds the picked change, but another
    // writer has since reverted it on the branch: "nothing to do" holds
    // for the handle, not the branch.
    const { repo, main } = await setup({ a: 1 })
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    dev.set('a', 2)
    const change = (await dev.commit()).commit as string
    await main.cherryPick(change)
    const stale = await repo.worktree('main')
    await main.revert(change)

    await expect(stale.cherryPick(change)).rejects.toThrow(ConcurrencyError)
    const abandoned = await stale.cherryPick(change, { onConflict: 'abandon' })
    expect(abandoned.merged).toBe(false)
    expect(await main.get('a')).toBe(1)

    await stale.refresh()
    expect((await stale.cherryPick(change)).strategy).toBe('apply')
    expect(await (await repo.snapshot({ branch: 'main' })).get('a')).toBe(2)
  })
})

describe('the engine checks the commits it is handed', () => {
  it('refuses an unknown commit to apply or merge, rather than reading it as empty', async () => {
    const store = new Memory()
    const engine = await VersionedKV.open(store)
    await engine.commit({ updates: new Map([['k', new Uint8Array([1])]]) })
    const known = engine.currentCommit
    const missing = 'f'.repeat(40)

    await expect(engine.applyChange(known, missing)).rejects.toThrow(UnknownCommitError)
    await expect(engine.applyChange(missing, known)).rejects.toThrow(UnknownCommitError)
    await expect(engine.mergeHeads(missing)).rejects.toThrow(UnknownCommitError)
    // Nothing was published.
    expect(await engine.latestHead()).toBe(known)
    expect(await engine.get('k')).toEqual(new Uint8Array([1]))
  })
})

describe('a commit losing its race merges with the effective rules', () => {
  it('uses the repository rules, then the worktree, then the call', async () => {
    let clock = 1_760_000_000
    const store = new Memory()
    const repo = await Repo.open(store, {
      codec: jsonCodec,
      now: () => {
        clock += 1
        return clock
      },
      mergePrefixes: new Map([['log/', MergeChoice.THEIRS]]),
    })
    const seed = await repo.worktree('main', { create: true })
    seed.set('log/1', 'base')
    seed.set('k', 'base')
    await seed.commit()
    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
    a.set('log/1', 'a')
    a.set('k', 'a')
    await a.commit()
    b.set('log/1', 'b')
    b.set('k', 'b')
    await expect(b.commit()).rejects.toThrow(MergeConflict)
    b.setDefaultMerge(MergeChoice.OURS)
    const result = await b.commit()
    expect(result.strategy).toBe('three_way')
    // log/ follows the repository's THEIRS (the moved head's value);
    // k follows the worktree's OURS default.
    expect(await state(repo, 'main')).toEqual({ 'log/1': 'a', k: 'b' })
  })
})

describe('mergeBase by generation', () => {
  /** A long linear history with two short forks at its tip. */
  async function longHistory() {
    const { repo, store, main } = await setup()
    for (let i = 0; i < 60; i++) {
      main.set('i', i)
      await main.commit()
    }
    const fork = main.head
    await repo.branches.create('dev', fork)
    const dev = await repo.worktree('dev')
    dev.set('d', 1)
    await dev.commit()
    main.set('m', 1)
    await main.commit()
    return { repo, store, main, dev, fork }
  }

  it('finds a recent fork in a few reads, whatever the history below', async () => {
    const { store, main, dev, fork } = await longHistory()
    let reads = 0
    const counting = Object.create(store) as Memory
    counting.getMany = async (keys) => {
      reads++
      return store.getMany(keys)
    }
    counting.get = async (key) => {
      reads++
      return store.get(key)
    }
    expect(await mergeBase(counting, main.head, dev.head)).toBe(fork)
    expect(reads).toBeLessThan(6)
  })

  it('falls back to a whole-history walk for commits without generations', async () => {
    const { store, main, dev, fork } = await longHistory()
    for (const commit of [main.head, dev.head]) await store.remove(PARENT_GENS(commit))
    expect(await mergeBase(store, main.head, dev.head)).toBe(fork)
  })

  it('picks the smallest hash on a criss-cross tie', async () => {
    const { repo, main } = await setup()
    await repo.branches.create('dev', main.head)
    const dev = await repo.worktree('dev')
    main.set('m', 1)
    const m1 = (await main.commit()).commit as string
    dev.set('d', 1)
    const d1 = (await dev.commit()).commit as string
    // Each merges the other's first commit: two lowest common ancestors.
    await main.merge({ commit: d1 }, { fastForward: false })
    await dev.merge({ commit: m1 }, { fastForward: false })
    const base = await repo.mergeBase(main.head, dev.head)
    expect(base).toBe([m1, d1].sort()[0])
  })

  it('reports a race on a merge no-op when the branch moved', async () => {
    const { repo, main } = await setup()
    await repo.branches.create('dev', main.head)
    const other = await repo.worktree('main')
    other.set('x', 1)
    await other.commit()
    // main's handle is stale; dev is its own ancestor, so the merge is a
    // no-op only if the branch is still where main left it.
    await expect(main.merge({ branch: 'dev' })).rejects.toThrow(ConcurrencyError)
  })
})

describe('keys named like Object.prototype members', () => {
  it('diff and merge add, remove and modify them like any key', async () => {
    const { repo, main } = await setup()
    main.set('constructor', 1)
    main.set('toString', 1)
    main.set('__proto__', 1)
    const base = (await main.commit()).commit as string
    await repo.branches.create('dev', base)
    const dev = await repo.worktree('dev')
    dev.set('constructor', 2)
    await dev.delete('toString')
    dev.set('hasOwnProperty', 1)
    const devHead = (await dev.commit()).commit as string
    main.set('__proto__', 2)
    await main.commit()

    expect(await repo.diff(base, devHead)).toEqual({
      added: new Set(['hasOwnProperty']),
      removed: new Set(['toString']),
      modified: new Set(['constructor']),
    })
    expect(await repo.diff(devHead, base)).toEqual({
      added: new Set(['toString']),
      removed: new Set(['hasOwnProperty']),
      modified: new Set(['constructor']),
    })

    const result = await main.merge({ branch: 'dev' })
    expect(result.strategy).toBe('three_way')
    const snap = await repo.snapshot({ branch: 'main' })
    expect(new Map(await snap.getMany(await snap.keys()))).toEqual(
      new Map<string, unknown>([
        ['base', 0],
        ['constructor', 2],
        ['hasOwnProperty', 1],
        ['__proto__', 2],
      ]),
    )
  })
})
