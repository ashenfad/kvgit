import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  BranchExistsError,
  ConcurrencyError,
  CorruptHeadError,
  KvgitError,
  MergeConflict,
  type MergeFn,
  ROOT_COMMIT,
  Repo,
  StorageVersionError,
  TagExistsError,
  UnknownBranchError,
  UnknownCommitError,
  UnknownTagError,
  jsonCodec,
} from '../src/index'
import { BRANCH_HEAD, BRANCH_HEAD_PREV } from '../src/versioned/layout'

/** A Memory that runs a one-shot hook just before a `casMany` that
 *  removes `key`: the window between a delete's read and its removal. */
class DeleteRaceMemory extends Memory {
  #hooks = new Map<string, () => Promise<void>>()

  beforeRemoving(key: string, hook: () => Promise<void>): void {
    this.#hooks.set(key, hook)
  }

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    const list = [...removes]
    for (const key of list) {
      const hook = this.#hooks.get(key)
      if (hook !== undefined) {
        this.#hooks.delete(key)
        await hook()
      }
    }
    return super.casMany(expected, writes, list)
  }
}

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const text = (b: Uint8Array | undefined): string | undefined =>
  b === undefined ? undefined : dec.decode(b)

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = []
  for await (const x of it) out.push(x)
  return out
}

/** A JSON repo with a clock the test moves. */
async function jsonRepo(store = new Memory()) {
  let clock = 1_760_000_000
  const repo = await Repo.open(store, { codec: jsonCodec, now: () => clock })
  return {
    repo,
    store,
    tick: (seconds = 1) => {
      clock += seconds
    },
  }
}

describe('Repo.open', () => {
  it('stamps a fresh store and refuses one of another version', async () => {
    const store = new Memory()
    await Repo.open(store)
    expect(dec.decode((await store.get('__kvgit_version__')) as Uint8Array)).toBe('4')

    const other = new Memory()
    await other.set('__kvgit_version__', bytes('3'))
    await expect(Repo.open(other)).rejects.toThrow(StorageVersionError)
    await expect(Repo.open(other)).rejects.toThrow(KvgitError)
  })
})

describe('Repo.worktree', () => {
  it('refuses a missing branch unless asked to create it, at the root commit', async () => {
    const { repo } = await jsonRepo()
    await expect(repo.worktree('main')).rejects.toThrow(UnknownBranchError)
    const wt = await repo.worktree('main', { create: true })
    expect(wt.branch).toBe('main')
    expect(wt.head).toBe(ROOT_COMMIT)
    expect(await repo.branches.get('main')).toBe(ROOT_COMMIT)
    // A second create is a plain open.
    expect((await repo.worktree('main', { create: true })).head).toBe(ROOT_COMMIT)
  })

  it('refuses a corrupt, unrecoverable HEAD', async () => {
    const { repo, store } = await jsonRepo()
    await repo.branches.create('main')
    await store.set(BRANCH_HEAD('main'), bytes(''))
    await expect(repo.worktree('main')).rejects.toThrow(CorruptHeadError)
  })

  it('refuses branch names in the tag namespace, and the empty name', async () => {
    const { repo } = await jsonRepo()
    await expect(repo.worktree('refs/tags/v1', { create: true })).rejects.toThrow(RangeError)
    // An empty name would be a branch no listing shows.
    await expect(repo.worktree('', { create: true })).rejects.toThrow(RangeError)
    await expect(repo.branches.create('')).rejects.toThrow(RangeError)
    expect(await repo.branches.has('')).toBe(false)
  })
})

describe('Worktree', () => {
  it('keeps writes pending until commit, and reads see them', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('a', { n: 1 })
    wt.set('b', [1, 2])
    expect(await wt.get('a')).toEqual({ n: 1 })
    expect(await wt.keys()).toEqual(['a', 'b'])
    expect(wt.status().updated).toEqual(new Set(['a', 'b']))
    expect(wt.head).toBe(ROOT_COMMIT)

    const result = await wt.commit({ info: { title: 'first' } })
    expect(result.merged).toBe(true)
    expect(result.strategy).toBe('fast_forward')
    expect(wt.head).toBe(result.commit)
    expect(wt.status().clean).toBe(true)
    expect(await repo.branches.get('main')).toBe(result.commit)

    // A fresh worktree reads the committed values.
    const other = await repo.worktree('main')
    expect(await other.getMany(['a', 'b', 'missing'])).toEqual(
      new Map<string, unknown>([
        ['a', { n: 1 }],
        ['b', [1, 2]],
      ]),
    )
  })

  it('deletes committed and pending keys', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('kept', 1)
    wt.set('gone', 2)
    await wt.commit()

    expect(await wt.delete('gone')).toBe(true)
    wt.set('scratch', 3)
    expect(await wt.delete('scratch')).toBe(true)
    expect(await wt.delete('never')).toBe(false)
    expect(await wt.has('gone')).toBe(false)
    expect(await wt.keys()).toEqual(['kept'])
    // Only the committed key needs a removal.
    expect(wt.status().removed).toEqual(new Set(['gone']))
    expect(wt.status().updated.size).toBe(0)

    await wt.commit()
    expect(await (await repo.worktree('main')).keys()).toEqual(['kept'])
  })

  it('commits a subset of keys and leaves the rest pending', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('a', 1)
    wt.set('b', 2)
    await wt.commit({ keys: ['a'] })
    expect(wt.status().updated).toEqual(new Set(['b']))
    expect(await (await repo.snapshot({ branch: 'main' })).keys()).toEqual(['a'])
  })

  it('merges a lost race automatically, with repository merge rules for contested keys', async () => {
    const store = new Memory()
    const repo = await Repo.open(store, {
      codec: jsonCodec,
      mergeFns: new Map<string, MergeFn<unknown>>([
        ['count', (old, ours, theirs) => (ours as number) + (theirs as number) - (old as number)],
      ]),
    })
    const seed = await repo.worktree('main', { create: true })
    seed.set('count', 10)
    await seed.commit()

    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
    a.set('count', 11)
    a.set('a', 'from a')
    await a.commit()
    b.set('count', 15)
    b.set('b', 'from b')
    const result = await b.commit()
    expect(result.strategy).toBe('three_way')

    const tip = await repo.snapshot({ branch: 'main' })
    expect(await tip.getMany(['count', 'a', 'b'])).toEqual(
      new Map<string, unknown>([
        ['count', 16],
        ['a', 'from a'],
        ['b', 'from b'],
      ]),
    )
  })

  it('raises or abandons on a conflict no rule resolves', async () => {
    const { repo } = await jsonRepo()
    const seed = await repo.worktree('main', { create: true })
    seed.set('k', 'base')
    await seed.commit()
    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
    a.set('k', 'a')
    await a.commit()
    b.set('k', 'b')

    await expect(b.commit()).rejects.toThrow(MergeConflict)
    const abandoned = await b.commit({ onConflict: 'abandon' })
    expect(abandoned.merged).toBe(false)
    expect(b.status().updated).toEqual(new Set(['k']))

    // A worktree rule resolves it.
    b.setMergeFn('k', (_old, ours, theirs) => `${String(theirs)}+${String(ours)}`)
    await b.commit()
    expect(await (await repo.snapshot({ branch: 'main' })).get('k')).toBe('a+b')
  })

  it('discards, resets and refreshes', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const first = (await wt.commit()).commit as string
    wt.set('k', 2)
    await wt.commit()

    wt.set('k', 3)
    wt.discard()
    expect(await wt.get('k')).toBe(2)

    await wt.reset(first)
    expect(await wt.get('k')).toBe(1)
    expect(await repo.branches.get('main')).toBe(first)
    await expect(wt.reset('f'.repeat(40))).rejects.toThrow(UnknownCommitError)

    const other = await repo.worktree('main')
    other.set('k', 4)
    await other.commit()
    expect(await wt.get('k')).toBe(1)
    await wt.refresh()
    expect(await wt.get('k')).toBe(4)
  })

  it('throws UnknownBranchError committing to a deleted branch', async () => {
    const { repo } = await jsonRepo()
    await repo.branches.create('dev')
    const wt = await repo.worktree('dev')
    await repo.branches.delete('dev')
    wt.set('k', 1)
    await expect(wt.commit()).rejects.toThrow(UnknownBranchError)
  })

  it('stores bytes as they are by default', async () => {
    const repo = await Repo.open(new Memory())
    const wt = await repo.worktree('main', { create: true })
    wt.set('raw', new Uint8Array([0xff, 0x00]))
    await wt.commit()
    const snap = await repo.snapshot({ branch: 'main' })
    expect(await snap.get('raw')).toEqual(new Uint8Array([0xff, 0x00]))
  })
})

describe('Branches', () => {
  it('creates, lists, reads and deletes, and keeps tags out', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const commit = (await wt.commit()).commit as string

    expect(await repo.branches.create('dev', commit)).toBe(commit)
    expect(await repo.branches.create('empty')).toBe(ROOT_COMMIT)
    await repo.tags.create('v1', commit)
    expect(await repo.branches.list()).toEqual(['dev', 'empty', 'main'])
    expect(await repo.branches.has('dev')).toBe(true)
    expect(await repo.branches.has('refs/tags/v1')).toBe(false)
    expect(await repo.branches.get('dev')).toBe(commit)

    await expect(repo.branches.create('dev')).rejects.toThrow(BranchExistsError)
    await expect(repo.branches.create('x', 'f'.repeat(40))).rejects.toThrow(UnknownCommitError)
    await expect(repo.branches.create('refs/tags/v2')).rejects.toThrow(RangeError)

    await repo.branches.delete('dev')
    expect(await repo.branches.has('dev')).toBe(false)
    await expect(repo.branches.get('dev')).rejects.toThrow(UnknownBranchError)
    await expect(repo.branches.delete('dev')).rejects.toThrow(UnknownBranchError)
  })

  it('a stale delete does not take a branch made again in the meantime', async () => {
    const store = new DeleteRaceMemory()
    const { repo } = await jsonRepo(store)
    await repo.branches.create('dev')
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 'new history')
    const newer = (await wt.commit()).commit as string

    // Between the stale delete's read and its removal, another writer
    // deletes the branch and makes it again somewhere else.
    store.beforeRemoving(BRANCH_HEAD('dev'), async () => {
      await repo.branches.delete('dev')
      await repo.branches.create('dev', newer)
    })
    await expect(repo.branches.delete('dev')).rejects.toThrow(ConcurrencyError)
    expect(await repo.branches.get('dev')).toBe(newer)
  })

  it('drops a stale backup when a name is reused', async () => {
    const { repo, store } = await jsonRepo()
    await repo.branches.create('dev')
    await store.set(BRANCH_HEAD_PREV('dev'), bytes('"stale"'))
    await repo.branches.delete('dev')
    await repo.branches.create('dev')
    expect(await store.get(BRANCH_HEAD_PREV('dev'))).toBeNull()
  })
})

describe('Tags', () => {
  it('names a commit permanently, with its time and info', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const commit = (await wt.commit()).commit as string

    await repo.tags.create('v1', commit, { info: { note: 'release' } })
    expect(await repo.tags.get('v1')).toBe(commit)
    expect(await repo.tags.has('v1')).toBe(true)
    expect(await repo.tags.info('v1')).toEqual({
      name: 'v1',
      commit,
      time: 1_760_000_000,
      info: { note: 'release' },
      dangling: false,
    })

    await repo.tags.create('pub/v2', ROOT_COMMIT)
    expect(await repo.tags.list()).toEqual(['pub/v2', 'v1'])
    expect(await repo.tags.entries()).toEqual(
      new Map([
        ['pub/v2', ROOT_COMMIT],
        ['v1', commit],
      ]),
    )

    await expect(repo.tags.create('v1', ROOT_COMMIT)).rejects.toThrow(TagExistsError)
    await expect(repo.tags.create('v3', 'f'.repeat(40))).rejects.toThrow(UnknownCommitError)
    await expect(repo.tags.create('50%', commit)).rejects.toThrow(RangeError)

    await repo.tags.delete('v1')
    await expect(repo.tags.get('v1')).rejects.toThrow(UnknownTagError)
    await expect(repo.tags.info('v1')).rejects.toThrow(UnknownTagError)
    await expect(repo.tags.delete('v1')).rejects.toThrow(UnknownTagError)
  })

  it('a stale delete does not take a tag made again in the meantime', async () => {
    const store = new DeleteRaceMemory()
    const { repo } = await jsonRepo(store)
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const commit = (await wt.commit()).commit as string
    await repo.tags.create('v1', ROOT_COMMIT)

    store.beforeRemoving(BRANCH_HEAD('refs/tags/v1'), async () => {
      await repo.tags.delete('v1')
      await repo.tags.create('v1', commit, { info: { note: 'replacement' } })
    })
    await expect(repo.tags.delete('v1')).rejects.toThrow(ConcurrencyError)
    expect(await repo.tags.get('v1')).toBe(commit)
    expect((await repo.tags.info('v1')).info).toEqual({ note: 'replacement' })
  })

  it('a stale delete does not take a tag made again at the same commit', async () => {
    const store = new DeleteRaceMemory()
    const { repo, tick } = await jsonRepo(store)
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const commit = (await wt.commit()).commit as string
    await repo.tags.create('v1', commit, { info: { note: 'first' } })

    store.beforeRemoving(BRANCH_HEAD('refs/tags/v1'), async () => {
      await repo.tags.delete('v1')
      tick() // the new record carries the time it landed
      await repo.tags.create('v1', commit, { info: { note: 'second' } })
    })
    await expect(repo.tags.delete('v1')).rejects.toThrow(ConcurrencyError)
    expect(await repo.tags.get('v1')).toBe(commit)
    expect((await repo.tags.info('v1')).info).toEqual({ note: 'second' })
  })

  it('still deletes a tag whose record is missing', async () => {
    const { repo, store } = await jsonRepo()
    await repo.branches.create('main') // mints the root commit
    await repo.tags.create('v1', ROOT_COMMIT)
    await store.remove('__tag_info__v1')
    await repo.tags.delete('v1')
    expect(await repo.tags.has('v1')).toBe(false)
  })

  it('keeps a tagged commit alive through gc after its branch is gone', async () => {
    const { repo, tick } = await jsonRepo()
    await repo.branches.create('dev')
    const wt = await repo.worktree('dev')
    wt.set('k', 'kept by a tag')
    const tagged = (await wt.commit()).commit as string
    wt.set('k', 'orphaned')
    const orphan = (await wt.commit()).commit as string
    await repo.tags.create('keep', tagged)
    await repo.branches.create('main')
    await repo.branches.delete('dev')

    tick(7200)
    expect(await repo.gc()).toBe(1)
    await expect(repo.getCommit(orphan)).rejects.toThrow(UnknownCommitError)
    expect(await (await repo.snapshot({ tag: 'keep' })).get('k')).toBe('kept by a tag')
    expect((await repo.tags.info('keep')).dangling).toBe(false)
  })
})

describe('Repo — commits and history', () => {
  async function history() {
    const ctx = await jsonRepo()
    const { repo, tick } = ctx
    const wt = await repo.worktree('main', { create: true })
    wt.set('a', 1)
    const c1 = (await wt.commit({ info: { step: 1 } })).commit as string
    tick()
    await repo.branches.create('side', c1)
    const side = await repo.worktree('side')
    side.set('s', 1)
    const s1 = (await side.commit()).commit as string
    tick()
    wt.set('a', 2)
    wt.set('b', 1)
    const c2 = (await wt.commit()).commit as string
    return { ...ctx, c1, c2, s1 }
  }

  it('reads one commit record', async () => {
    const { repo, c1 } = await history()
    expect(await repo.getCommit(c1)).toEqual({
      hash: c1,
      parents: [ROOT_COMMIT],
      time: 1_760_000_000,
      info: { step: 1 },
      root: expect.any(String),
    })
    const root = await repo.getCommit(ROOT_COMMIT)
    expect(root.parents).toEqual([])
    await expect(repo.getCommit('f'.repeat(40))).rejects.toThrow(UnknownCommitError)
  })

  it('logs from a branch, commit or tag, newest first', async () => {
    const { repo, c1, c2 } = await history()
    const hashes = async (it: AsyncIterable<{ hash: string }>) =>
      (await collect(it)).map((c) => c.hash)
    expect(await hashes(repo.log({ branch: 'main' }))).toEqual([c2, c1, ROOT_COMMIT])
    expect(await hashes(repo.log({ commit: c1 }))).toEqual([c1, ROOT_COMMIT])
    expect(await hashes(repo.log({ branch: 'main' }, { limit: 1 }))).toEqual([c2])
    await repo.tags.create('t', c1)
    expect(await hashes(repo.log({ tag: 't' }))).toEqual([c1, ROOT_COMMIT])
    await expect(collect(repo.log({ branch: 'nope' }))).rejects.toThrow(UnknownBranchError)
    await expect(collect(repo.log({ tag: 'nope' }))).rejects.toThrow(UnknownTagError)
    await expect(
      collect(repo.log({ branch: 'main', commit: c1 } as unknown as { branch: string })),
    ).rejects.toThrow(TypeError)
  })

  it('logs first parents only through a merge', async () => {
    const { repo, s1 } = await history()
    // Land a merge on main: a stale worktree loses the race.
    const stale = await repo.worktree('main')
    const fresh = await repo.worktree('main')
    fresh.set('x', 1)
    await fresh.commit()
    stale.set('y', 1)
    const merge = (await stale.commit()).commit as string
    const all = (await collect(repo.log({ commit: merge }))).map((c) => c.hash)
    const first = (await collect(repo.log({ commit: merge }, { firstParent: true }))).map(
      (c) => c.hash,
    )
    expect(first.length).toBeLessThan(all.length)
    expect(all).not.toContain(s1)
  })

  it('diffs two commits and finds their merge base', async () => {
    const { repo, c1, c2, s1 } = await history()
    expect(await repo.diff(c1, c2)).toEqual({
      added: new Set(['b']),
      removed: new Set(),
      modified: new Set(['a']),
    })
    expect(await repo.mergeBase(c2, s1)).toBe(c1)
    await expect(repo.diff(c1, 'f'.repeat(40))).rejects.toThrow(UnknownCommitError)
  })
})

describe('Snapshot', () => {
  it('is pinned to the commit it resolved, and reads lazily', async () => {
    const { repo } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    for (let i = 0; i < 50; i++) wt.set(`k${i}`, i)
    await wt.commit()
    const snap = await repo.snapshot({ branch: 'main' })

    wt.set('k0', 'moved')
    await wt.commit()

    expect(await snap.get('k0')).toBe(0)
    expect(await snap.get('missing')).toBeUndefined()
    expect(await snap.has('k49')).toBe(true)
    expect(await snap.getMany(['k1', 'k2', 'nope'])).toEqual(
      new Map([
        ['k1', 1],
        ['k2', 2],
      ]),
    )
    expect(await snap.size()).toBe(50)
    expect((await snap.keys()).slice(0, 3)).toEqual(['k0', 'k1', 'k10'])
    expect(text(await snap.raw.get('k3'))).toBe('3')
    expect(await (await repo.snapshot({ branch: 'main' })).get('k0')).toBe('moved')
  })

  it('refuses a commit that is not in the store', async () => {
    const { repo } = await jsonRepo()
    await expect(repo.snapshot({ commit: 'f'.repeat(40) })).rejects.toThrow(UnknownCommitError)
  })
})

describe('Repo.repairHead', () => {
  it('persists the recovery a read makes in memory', async () => {
    const { repo, store } = await jsonRepo()
    const wt = await repo.worktree('main', { create: true })
    wt.set('k', 1)
    const first = (await wt.commit()).commit as string
    wt.set('k', 2)
    await wt.commit()
    await store.set(BRANCH_HEAD('main'), bytes(''))

    expect(await repo.branches.get('main')).toBe(first)
    expect(text((await store.get(BRANCH_HEAD('main'))) ?? undefined)).toBe('')
    expect(await repo.repairHead('main')).toBe(first)
    expect(text((await store.get(BRANCH_HEAD('main'))) ?? undefined)).toBe(`"${first}"`)
  })
})
