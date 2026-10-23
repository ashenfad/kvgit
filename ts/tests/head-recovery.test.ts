/**
 * HEAD backup, recovery, and repair semantics.
 *
 * Three contracts live here.
 *
 * **prev-HEAD only ever names a commit HEAD really held.**
 * `__branch_head_prev__<branch>` is the input to `resolveHead`'s
 * recovery fallback, so what it names decides what a damaged branch
 * recovers *to*. A value that was never in `__branch_head__<branch>`
 * hands the branch a lineage it never had. The swap and the backup are
 * one atomic `casMany`, so the backup is exactly the HEAD the swap
 * replaced: no writer, crash or delete can land between them.
 *
 * **Reading never writes.** Recovery on a read path is in-memory only.
 * Persisting it is either an explicit `repairHead` call or a side
 * effect of a write that has to move HEAD anyway.
 *
 * **`repairHead` reports the store, not its own attempt.** Its answer
 * is the commit HEAD names when it returns, even when another writer
 * moved HEAD underneath it.
 *
 * Every race here is a seam, not a sleep. `HookStore` runs a one-shot
 * callback at a chosen point in a chosen store operation, so "the
 * winner lands between the loser's commit write and its CAS" is
 * expressed exactly and repeats identically. The seams are points
 * every version of the code passes through — a commit's write batch, a
 * read of the HEAD key — never one that exists only in the buggy
 * ordering, so a test written against them means the same thing before
 * and after a fix.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { ConcurrencyError, UnknownBranchError, VersionedKV, repairHead } from '../src/index'
import type { CorruptHeadRecoverer } from '../src/index'
import {
  BRANCH_HEAD,
  BRANCH_HEAD_PREFIX,
  BRANCH_HEAD_PREV,
  COMMIT_ROOT,
  dumps,
  safeLoads,
} from '../src/versioned/layout'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const one = (key: string, value: string) => ({ updates: new Map([[key, bytes(value)]]) })

/** Bytes that are not a valid encoded commit hash. */
const CORRUPT = bytes('')
const COMMIT_ROOT_PREFIX = COMMIT_ROOT('')

// ---------------------------------------------------------------------------
// Test seam
// ---------------------------------------------------------------------------

/**
 * A `Memory` that records HEAD history and arms one-shot seams.
 *
 * `headHistory(branch)` is the ground truth for "was this commit ever
 * HEAD of that branch": every successful write to a `__branch_head__`
 * key is appended, whichever store method made it. Assertions about
 * prev-HEAD compare against this rather than against what the code
 * under test believes.
 *
 * Three seams, each one-shot:
 *
 * - `armCommitBatch` fires when a commit's write batch is written —
 *   after its writer has read HEAD, before it reaches its CAS.
 * - `armGet` fires after the *nth* read of a key has been served but
 *   before the caller sees it.
 * - `armSet` fires before a chosen key is written (by `set` or as part
 *   of a `casMany`), so a writer can be paused mid-sequence while
 *   another completes.
 */
class HookStore extends Memory {
  private readonly history = new Map<string, string[]>()
  private onCommitBatch: (() => Promise<void>) | null = null
  private readonly onGet = new Map<string, { fire: () => Promise<void>; nth: number }>()
  private readonly onSet = new Map<string, () => Promise<void>>()

  /** Every commit `__branch_head__<branch>` has ever held, in order. */
  headHistory(branch: string): string[] {
    return this.history.get(branch) ?? []
  }

  armCommitBatch(fire: () => Promise<void>): void {
    this.onCommitBatch = fire
  }

  armGet(key: string, fire: () => Promise<void>, nth = 1): void {
    this.onGet.set(key, { fire, nth })
  }

  armSet(key: string, fire: () => Promise<void>): void {
    this.onSet.set(key, fire)
  }

  private record(key: string, value: Uint8Array): void {
    if (!key.startsWith(BRANCH_HEAD_PREFIX)) return
    const commit = safeLoads(value)
    if (typeof commit !== 'string') return
    const branch = key.slice(BRANCH_HEAD_PREFIX.length)
    const seen = this.history.get(branch)
    if (seen === undefined) this.history.set(branch, [commit])
    else seen.push(commit)
  }

  override async get(key: string): Promise<Uint8Array | null> {
    const value = await super.get(key)
    const armed = this.onGet.get(key)
    if (armed !== undefined) {
      armed.nth -= 1
      if (armed.nth <= 0) {
        this.onGet.delete(key)
        await armed.fire()
      }
    }
    return value
  }

  override async set(key: string, value: Uint8Array): Promise<void> {
    const fire = this.onSet.get(key)
    if (fire !== undefined) {
      this.onSet.delete(key)
      await fire()
    }
    await super.set(key, value)
    this.record(key, value)
  }

  override async setMany(items: Iterable<readonly [string, Uint8Array]>): Promise<void> {
    const batch = [...items]
    if (this.onCommitBatch !== null && batch.some(([k]) => k.startsWith(COMMIT_ROOT_PREFIX))) {
      const fire = this.onCommitBatch
      this.onCommitBatch = null
      await fire()
    }
    await super.setMany(batch)
    for (const [k, v] of batch) this.record(k, v)
  }

  override async cas(
    key: string,
    value: Uint8Array,
    expected: Uint8Array | null,
  ): Promise<boolean> {
    const won = await super.cas(key, value, expected)
    if (won) this.record(key, value)
    return won
  }

  override async casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes: Iterable<string> = [],
  ): Promise<boolean> {
    for (const key of writes.keys()) {
      const fire = this.onSet.get(key)
      if (fire !== undefined) {
        this.onSet.delete(key)
        await fire()
      }
    }
    const won = await super.casMany(expected, writes, removes)
    if (won) for (const [k, v] of writes) this.record(k, v)
    return won
  }
}

/** Read `__branch_head_prev__<branch>` as a commit hash. */
async function prevHead(store: Memory, branch = 'main'): Promise<unknown> {
  const raw = await store.get(BRANCH_HEAD_PREV(branch))
  return raw === null ? null : safeLoads(raw)
}

/** Read `__branch_head__<branch>` as a commit hash. */
async function head(store: Memory, branch = 'main'): Promise<unknown> {
  const raw = await store.get(BRANCH_HEAD(branch))
  return raw === null ? null : safeLoads(raw)
}

/** Every key/value in the store, comparable with `toEqual`. */
async function contents(store: Memory): Promise<Map<string, string>> {
  const out = new Map<string, string>()
  for await (const [k, v] of store.items()) out.set(k, dec.decode(v))
  return out
}

// ---------------------------------------------------------------------------

describe('prev-HEAD names a commit HEAD really held', () => {
  it('is not planted with a commit that was never HEAD by a losing writer', async () => {
    // `casHead`'s `expected` is whatever head resolution returned. On a
    // branch whose HEAD is corrupt and whose backup is gone, that is
    // the injected recoverer's answer — a recovery *candidate*, not a
    // HEAD. Writing it into the backup on the way into a CAS that then
    // fails makes the candidate durable, and every later read
    // short-circuits recovery onto a lineage the branch never had.
    const store = new HookStore()
    let candidate: string | null = null
    const recoverer: CorruptHeadRecoverer = async () => candidate
    const vk = await VersionedKV.open(store, { recoverFromCorruptHead: recoverer })
    await vk.commit(one('a', '1'))

    // A commit that is never main's HEAD: a branch tip that outlives
    // its branch, which is what a commit scan would surface.
    const tmp = (await vk.createBranch('tmp')) as VersionedKV
    await tmp.commit(one('t', 'tmp-only'))
    const tmpTip = tmp.currentCommit
    await vk.deleteBranch('tmp')
    candidate = tmpTip

    // The damage the recovery path exists for: unreadable HEAD, and no
    // backup to fall back on.
    await store.set(BRANCH_HEAD('main'), CORRUPT)
    await store.remove(BRANCH_HEAD_PREV('main'))

    try {
      await vk.commit(one('a', '2'))
    } catch (e) {
      if (!(e instanceof ConcurrencyError)) throw e
    }

    const prev = await prevHead(store)
    const history = store.headHistory('main')
    expect(
      history,
      `prev-HEAD names ${String(prev)}, which was never main's HEAD (history: ${history.join(
        ', ',
      )}). It is the deleted branch's tip (${tmpTip}), so recovery would resurrect that branch onto main.`,
    ).toContain(prev)
  })

  it('is not overwritten by a losing writer', async () => {
    // The loser reads HEAD, builds its commit, and only then reaches
    // the CAS. Two commits land in that window. Writing the backup on
    // the way *into* the CAS makes the loser's stale value the last
    // one written, so recovery from here silently drops both of the
    // winner's commits.
    const store = new HookStore()
    const seed = await VersionedKV.open(store)
    await seed.commit(one('a', '1'))

    const loser = await VersionedKV.open(store)
    const winner = await VersionedKV.open(store)
    store.armCommitBatch(async () => {
      await winner.commit(one('w', '1'))
      await winner.commit(one('w', '2'))
    })

    // Issue #39 (kvgit-py): the lost fast-forward race merges internally
    // instead of raising, so the loser lands a merge commit on top. The
    // invariant under test is unchanged: whatever wins the final CAS
    // writes the backup, so prev-HEAD is still the immediately-prior
    // HEAD rather than the loser's stale value.
    await loser.commit(one('a', '2'))

    const prev = await prevHead(store)
    const history = store.headHistory('main')
    expect(
      prev,
      `prev-HEAD is ${String(prev)}, the loser's stale value; the immediately-previous HEAD is ${
        history[history.length - 2]
      } (history: ${history.join(', ')})`,
    ).toBe(history[history.length - 2])
  })

  it('leaves HEAD and its backup together when the process dies at the swap', async () => {
    // The swap and the backup are one atomic write, so a crash at that
    // point lands neither: HEAD and the backup still name the two
    // commits they did before.
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('a', '1'))
    const first = vk.currentCommit
    await vk.commit(one('a', '2'))
    const second = vk.currentCommit

    class Died extends Error {}
    store.armSet(BRANCH_HEAD_PREV('main'), async () => {
      throw new Died('the process dies at the swap')
    })
    await expect(vk.commit(one('a', '3'))).rejects.toThrow(Died)

    expect(await head(store)).toBe(second)
    expect(await prevHead(store)).toBe(first)

    // HEAD is then damaged; recovery lands one commit back.
    await store.set(BRANCH_HEAD('main'), CORRUPT)
    const recovered = await VersionedKV.open(store)
    expect(recovered.currentCommit).toBe(first)
  })
})

describe('reads never write', () => {
  it('opening, peeking at and switching to a damaged branch leave the store alone', async () => {
    // Every read path resolves HEAD. Healing the damage in place makes
    // an ordinary read a mutation — impossible for a read-only
    // consumer, and a race between two readers repairing the same
    // branch to different answers.
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('x', '1'))
    const dev = (await vk.createBranch('dev')) as VersionedKV
    await dev.commit(one('d', '1'))
    const good = dev.currentCommit
    await dev.commit(one('d', '2'))
    await store.set(BRANCH_HEAD('dev'), CORRUPT)

    const before = await contents(store)

    const reader = await VersionedKV.open(store, { branch: 'dev' })
    expect(reader.currentCommit, 'recovery must still happen').toBe(good)
    expect(await contents(store), 'opening a damaged branch repaired it in place').toEqual(before)

    expect(await vk.peek('d', { branch: 'dev' })).toEqual(bytes('1'))
    expect(await contents(store), 'peek wrote to the store').toEqual(before)

    await vk.switchBranch('dev')
    expect(vk.currentCommit).toBe(good)
    expect(await contents(store), 'switchBranch wrote to the store').toEqual(before)

    await vk.refresh()
    expect(await contents(store), 'refresh wrote to the store').toEqual(before)
  })

  it('still lets the write path heal a damaged HEAD', async () => {
    // A CAS against corrupt HEAD bytes always fails, so with nothing
    // repairing on read a damaged branch would be permanently
    // unwritable. The heal moves to the writer, which has to move HEAD
    // anyway, and is itself a CAS against the exact damaged bytes.
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('x', '1'))
    const good = vk.currentCommit
    await vk.commit(one('x', '2'))
    await store.set(BRANCH_HEAD('main'), CORRUPT)

    const writer = await VersionedKV.open(store)
    expect(writer.currentCommit, 'the read should recover').toBe(good)
    expect(await store.get(BRANCH_HEAD('main')), 'and not persist it').toEqual(CORRUPT)

    const result = await writer.commit(one('x', '3'))
    expect(result.merged, 'a damaged HEAD must not make a branch read-only').toBe(true)
    expect(await head(store)).toBe(result.commit)

    const history = store.headHistory('main')
    expect(history[history.length - 1]).toBe(result.commit)
    expect(history[history.length - 2], 'the heal is itself a recorded HEAD write').toBe(good)
    expect(await prevHead(store)).toBe(good)
  })
})

describe('repairHead', () => {
  it('persists the recovery a read would have made', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('x', '1'))
    const good = vk.currentCommit
    await vk.commit(one('x', '2'))
    await store.set(BRANCH_HEAD('main'), CORRUPT)

    expect(await repairHead(store, 'main')).toBe(good)
    expect(await head(store)).toBe(good)
    // Idempotent, and a no-op on an already-healthy branch.
    expect(await repairHead(store, 'main')).toBe(good)
    expect(await (await VersionedKV.open(store)).repairHead()).toBe(good)
  })

  it('reports what HEAD names when another writer wins the race', async () => {
    // The contract is "the commit HEAD now names". When the heal CAS
    // declines because someone else repaired, advanced or deleted the
    // branch in between, the recovery candidate is stale, and returning
    // it hands the caller an *older* commit than the store holds.
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('k', '1'))
    await vk.commit(one('k', '2'))
    const second = vk.currentCommit
    await store.set(BRANCH_HEAD('main'), CORRUPT) // resolves to the first commit

    // Fire between head resolution's read of HEAD and the heal's, so
    // the heal CAS finds a HEAD it must not touch and declines.
    store.armGet(
      BRANCH_HEAD('main'),
      async () => {
        await store.set(BRANCH_HEAD('main'), dumps(second))
      },
      2,
    )

    const returned = await repairHead(store, 'main')
    const actual = await head(store)
    expect(actual, 'the other writer should hold HEAD').toBe(second)
    expect(
      returned,
      `repairHead returned ${String(returned)}, but HEAD names ${String(
        actual,
      )}; its contract is "the commit HEAD now names", and returning the stale candidate hands the caller an older commit than the store has`,
    ).toBe(actual)
  })

  it('is a no-op on a healthy branch that still reports HEAD', async () => {
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('k', '1'))
    await vk.commit(one('k', '2'))
    const expected = vk.currentCommit

    const before = await contents(store)
    expect(await repairHead(store, 'main')).toBe(expected)
    expect(await contents(store), 'a healthy branch must not be rewritten').toEqual(before)
  })

  it('returns null for a missing branch, and for an unrecoverable one', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('k', '1'))
    expect(await repairHead(store, 'no-such-branch')).toBeNull()

    // HEAD is corrupt, the backup is gone, and nothing is injected to
    // recover with: there is no answer to persist. Recreating the key
    // for a *deleted* branch is likewise refused — that is the
    // resurrection `deleteBranch` drops the backup to prevent.
    await store.set(BRANCH_HEAD('main'), CORRUPT)
    await store.remove(BRANCH_HEAD_PREV('main'))
    expect(await repairHead(store, 'main')).toBeNull()
    expect(await store.get(BRANCH_HEAD('main')), 'nothing was recoverable to write').toEqual(
      CORRUPT,
    )
  })
})

describe('the backup is one commit back', () => {
  it('holds even when another writer lands just before the swap', async () => {
    // A writer paused at its swap while another commits loses its CAS,
    // merges, and swaps again. Whichever swap lands, the backup it
    // writes is the HEAD it replaced.
    const store = new HookStore()
    const writer = await VersionedKV.open(store)
    await writer.commit(one('k', '1'))

    let other: string | null = null
    store.armSet(BRANCH_HEAD_PREV('main'), async () => {
      const second = await VersionedKV.open(store)
      await second.commit(one('k2', '3'))
      other = second.currentCommit
    })
    const result = await writer.commit(one('k', '2'))
    expect(result.merged).toBe(true)

    expect(await head(store)).toBe(writer.currentCommit)
    expect(await prevHead(store)).toBe(other)
    const history = store.headHistory('main')
    expect(history.slice(-2)).toEqual([other, writer.currentCommit])
  })
})

describe('an absent HEAD means the branch is gone, not damaged', () => {
  it('a writer swapping after a delete does not resurrect the branch', async () => {
    // A writer paused at its swap while the branch is deleted must not
    // bring the branch, or a lone backup of it, back. Its CAS expects
    // the old HEAD, which is gone, so nothing is written.
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('anchor', '1'))
    const doomed = (await vk.createBranch('doomed')) as VersionedKV
    await doomed.commit(one('secret', 'classified'))

    store.armSet(BRANCH_HEAD_PREV('doomed'), async () => {
      await vk.deleteBranch('doomed')
    })
    await expect(doomed.commit(one('secret', 'classified-v2'))).rejects.toThrow(UnknownBranchError)

    expect(await store.get(BRANCH_HEAD('doomed')), 'the delete should have won').toBeNull()
    expect(await store.get(BRANCH_HEAD_PREV('doomed'))).toBeNull()
    expect(await repairHead(store, 'doomed')).toBeNull()
    const recreated = (await vk.createBranch('doomed')) as VersionedKV
    expect(await recreated.get('secret'), 'the deleted branch was resurrected').toBeNull()
  })

  it('still recovers a HEAD that is present but corrupt', async () => {
    // The gate must not cost the tier its actual purpose.
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('k', '1'))
    const first = vk.currentCommit
    await vk.commit(one('k', '2'))
    await store.set(BRANCH_HEAD('main'), bytes(''))
    expect(await repairHead(store, 'main')).toBe(first)
  })

  it('leaves a healthy branch alone', async () => {
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('k', '1'))
    expect(await repairHead(store, 'main')).toBe(vk.currentCommit)
  })
})

describe('deleteBranch drops the backup with the branch', () => {
  it('leaves nothing for a same-named branch to recover onto', async () => {
    // A backup left behind would let a same-named branch recover onto
    // the deleted one's history. The prev-HEAD tier also requires
    // `__branch_head__` to exist, so a lone backup is never used.
    const store = new Memory()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('a', '1'))
    const dev = (await vk.createBranch('dev')) as VersionedKV
    await dev.commit(one('d', '1'))
    await dev.commit(one('d', '2'))
    await vk.deleteBranch('dev')

    expect(await store.get(BRANCH_HEAD_PREV('dev'))).toBeNull()
    expect(await repairHead(store, 'dev')).toBeNull()

    const recreated = (await vk.createBranch('dev')) as VersionedKV
    expect(await recreated.get('d'), 'the deleted branch was resurrected').toBeNull()
  })
})

describe('resetTo backs up the bytes HEAD actually held', () => {
  it('never writes a commit HEAD did not hold', async () => {
    // `resetTo` is a blind overwrite with no CAS to lose, and it copies
    // the bytes it just read out of `__branch_head__` rather than a
    // resolved value, so the backup still names a real former HEAD.
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('a', '1'))
    const first = vk.currentCommit
    await vk.commit(one('a', '2'))
    const second = vk.currentCommit

    expect(await vk.resetTo(first)).toBe(true)
    expect(await head(store)).toBe(first)
    expect(await prevHead(store)).toBe(second)
    expect(store.headHistory('main')).toContain(second)
  })

  it('does not launder a corrupt HEAD into the backup as a commit', async () => {
    const store = new HookStore()
    const vk = await VersionedKV.open(store)
    await vk.commit(one('a', '1'))
    const first = vk.currentCommit
    await vk.commit(one('a', '2'))
    await store.set(BRANCH_HEAD('main'), CORRUPT)

    expect(await vk.resetTo(first)).toBe(true)
    // The corrupt bytes are copied verbatim, so the backup names no
    // commit at all — recovery rejects it rather than following it.
    expect(await store.get(BRANCH_HEAD_PREV('main'))).toEqual(CORRUPT)
    expect(await prevHead(store)).toBeNull()
    expect(await head(store)).toBe(first)
  })
})

// ---------------------------------------------------------------------------
// The injected third tier
// ---------------------------------------------------------------------------

/**
 * A store whose `main` is healthy and whose `branch` is doubly damaged:
 * HEAD unreadable *and* the prev-HEAD backup gone. That is the only
 * shape that reaches the injected tier — tiers 1 and 2 both fail, and
 * HEAD still exists so the branch is damage rather than a deletion.
 *
 * Returns the tip the damaged branch last really had, which is what a
 * correct recoverer would name.
 */
async function doublyDamaged(
  store: Memory,
  branch: string,
): Promise<{ tip: string; mainTip: string }> {
  const vk = await VersionedKV.open(store)
  await vk.commit(one('anchor', '1'))
  const mainTip = vk.currentCommit
  let tip = mainTip
  if (branch !== 'main') {
    const other = (await vk.createBranch(branch)) as VersionedKV
    await other.commit(one('k', 'payload'))
    tip = other.currentCommit
  }
  await store.set(BRANCH_HEAD(branch), CORRUPT)
  await store.remove(BRANCH_HEAD_PREV(branch))
  return { tip, mainTip }
}

/** A recoverer that records the branches it was asked about. */
function spy(answer: (branch: string) => string | null): {
  recoverer: CorruptHeadRecoverer
  calls: string[]
} {
  const calls: string[] = []
  return {
    calls,
    recoverer: async (_store, branch) => {
      calls.push(branch)
      return answer(branch)
    },
  }
}

describe('an injected recoverer is checked, not trusted', () => {
  const DANGLING = 'deadbeef'.repeat(5)

  it('rejects a hash that names no commit in this store', async () => {
    // A recoverer is caller-supplied, so its answer is no more
    // trustworthy than anything else read out of the store. Head
    // resolution promises a valid commit or null.
    const store = new Memory()
    const { mainTip } = await doublyDamaged(store, 'main')
    const { recoverer } = spy(() => DANGLING)

    const vk = await VersionedKV.open(store, {
      commitHash: mainTip,
      recoverFromCorruptHead: recoverer,
    })
    expect(
      await vk.latestHead(),
      'a hash naming no commit was accepted as a recovered HEAD',
    ).toBeNull()
  })

  it('does not let repairHead make a rejected candidate durable', async () => {
    // The half that turns a bad answer into a bad store: repairHead
    // CASes the candidate over the damaged bytes, replacing visible
    // corruption with a plausible hash that names nothing — harder to
    // diagnose than the damage it replaced, on a store whose backup is
    // already gone.
    const store = new Memory()
    await doublyDamaged(store, 'main')
    const { recoverer } = spy(() => DANGLING)

    const repaired = await repairHead(store, 'main', { recoverFromCorruptHead: recoverer })
    expect(
      await store.get(BRANCH_HEAD('main')),
      'a rejected candidate was written into HEAD, replacing visible damage ' +
        'with a plausible hash that names nothing',
    ).toEqual(CORRUPT)
    expect(repaired).toBeNull()
  })

  it('rejects an answer that is not a string at all', async () => {
    // The type says `string | null`, but a recoverer crossing from
    // untyped JS can return anything, and `dumps` would happily encode
    // it into HEAD.
    const store = new Memory()
    await doublyDamaged(store, 'main')
    const bogus = (async () => 12345) as unknown as CorruptHeadRecoverer

    const repaired = await repairHead(store, 'main', { recoverFromCorruptHead: bogus })
    expect(await store.get(BRANCH_HEAD('main')), 'a non-string was encoded into HEAD').toEqual(
      CORRUPT,
    )
    expect(repaired).toBeNull()
  })

  it('still honours a recoverer that names a real commit', async () => {
    // The check must not cost the tier its purpose.
    const store = new Memory()
    const { mainTip } = await doublyDamaged(store, 'main')
    const { recoverer, calls } = spy(() => mainTip)

    const vk = await VersionedKV.open(store, {
      commitHash: mainTip,
      recoverFromCorruptHead: recoverer,
    })
    expect(await vk.latestHead()).toBe(mainTip)
    expect(calls).toEqual(['main'])

    expect(await repairHead(store, 'main', { recoverFromCorruptHead: recoverer })).toBe(mainTip)
    expect(await head(store)).toBe(mainTip)
  })
})

describe('a handle applies its recoverer to every read it makes', () => {
  it('peek resolves another branch through it', async () => {
    const store = new Memory()
    const { tip } = await doublyDamaged(store, 'dev')
    const { recoverer, calls } = spy(() => tip)

    const vk = await VersionedKV.open(store, { recoverFromCorruptHead: recoverer })
    expect(await vk.peek('k', { branch: 'dev' })).toEqual(bytes('payload'))
    expect(calls).toEqual(['dev'])

    const bare = await VersionedKV.open(store)
    expect(await bare.peek('k', { branch: 'dev' })).toBeNull()
  })

  it('never uses it for the mark phase of a sweep', async () => {
    // GC must not decide reachability from a guess. Deliberate, and
    // the inconsistency with the read paths is the point: a wrong
    // answer marks the wrong commits live, so real garbage survives
    // forever and a guessed tip gets walked as though it were this
    // branch's own history, pinning another branch's ancestry into
    // this one's mark set.
    const store = new Memory()
    const { tip } = await doublyDamaged(store, 'dev')
    const { recoverer, calls } = spy(() => tip)

    const vk = await VersionedKV.open(store, { recoverFromCorruptHead: recoverer })
    expect(await vk.cleanOrphans({ minAge: -1 })).toBe(1)
    expect(calls, 'the mark phase must not resolve a branch by guessing').toEqual([])
    expect(
      await store.get(COMMIT_ROOT(tip)),
      "the unresolvable branch's tip was marked live off a guess",
    ).toBeNull()
  })
})
