import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Namespaced } from '../src/namespaced'
import { Repo } from '../src/repo'

async function freshWorktree() {
  const store = new Memory()
  const repo = await Repo.open(store, { codec: 'json' })
  const wt = await repo.worktree('main', { create: true })
  return { store, repo, wt }
}

describe('Namespaced over a Worktree — basic', () => {
  it('rejects a namespace containing a slash', async () => {
    const { wt } = await freshWorktree()
    expect(() => new Namespaced(wt, 'a/b')).toThrow(/cannot contain/)
  })

  it('writes are isolated by prefix', async () => {
    const { wt } = await freshWorktree()
    const alice = new Namespaced(wt, 'alice')
    const bob = new Namespaced(wt, 'bob')

    alice.set('color', 'red')
    bob.set('color', 'blue')

    expect(await alice.get('color')).toBe('red')
    expect(await bob.get('color')).toBe('blue')
    // The underlying worktree sees the prefixed keys.
    expect(await wt.get('alice/color')).toBe('red')
    expect(await wt.get('bob/color')).toBe('blue')
  })

  it('delete only affects the namespaced view', async () => {
    const { wt } = await freshWorktree()
    const alice = new Namespaced(wt, 'alice')
    const bob = new Namespaced(wt, 'bob')
    alice.set('k', 1)
    bob.set('k', 2)
    expect(await alice.delete('k')).toBe(true)

    expect(await alice.get('k')).toBeUndefined()
    expect(await bob.get('k')).toBe(2)
  })

  it('has() respects the namespace boundary', async () => {
    const { wt } = await freshWorktree()
    const alice = new Namespaced(wt, 'alice')
    alice.set('k', 'v')
    expect(await alice.has('k')).toBe(true)
    expect(await alice.has('absent')).toBe(false)
  })
})

describe('Namespaced over a Worktree — nesting flattens', () => {
  it('nested Namespaced combines prefixes', async () => {
    const { wt } = await freshWorktree()
    const outer = new Namespaced(wt, 'a')
    const inner = new Namespaced(outer, 'b')
    expect(inner.namespace).toBe('a/b')

    inner.set('k', 'v')
    expect(await wt.get('a/b/k')).toBe('v')
    expect(await inner.get('k')).toBe('v')
  })
})

describe('Namespaced over a Worktree — keys vs descendantKeys', () => {
  it('keys() returns only direct children, descendantKeys() returns all', async () => {
    const { wt } = await freshWorktree()
    const alice = new Namespaced(wt, 'alice')
    const aliceProfile = new Namespaced(alice, 'profile')

    alice.set('top', 1)
    alice.set('also-top', 2)
    aliceProfile.set('name', 'Alice')
    aliceProfile.set('age', 30)
    // Commit some keys and leave one pending, so listing covers both
    // (Worktree.keys() lists committed plus pending).
    await wt.commit()
    alice.set('staged-only', 99)

    expect(await alice.keys()).toEqual(['also-top', 'staged-only', 'top'])
    expect(await alice.descendantKeys()).toEqual([
      'also-top',
      'profile/age',
      'profile/name',
      'staged-only',
      'top',
    ])
  })
})

describe('Namespaced over a Worktree — interplay with commits', () => {
  it('writes via Namespaced flush through Worktree.commit()', async () => {
    const { repo, wt } = await freshWorktree()
    const alice = new Namespaced(wt, 'alice')
    alice.set('color', 'red')
    expect(wt.status().clean).toBe(false)
    await wt.commit()
    expect(wt.status().clean).toBe(true)

    // The commit holds the prefixed key, stored as the codec's bytes.
    const raw = (await (
      await repo.snapshot({ commit: wt.head })
    ).raw.get('alice/color')) as Uint8Array
    expect(JSON.parse(new TextDecoder().decode(raw))).toBe('red')
  })
})
