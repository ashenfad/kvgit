import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { Namespaced } from '../src/namespaced'
import { Staged } from '../src/staged'
import { VersionedKV } from '../src/versioned/kv'

async function freshStaged() {
  const store = new Memory()
  const vk = await VersionedKV.open(store)
  return { store, vk, staged: new Staged(vk) }
}

describe('Namespaced — basic', () => {
  it('rejects a namespace containing a slash', async () => {
    const { staged } = await freshStaged()
    expect(() => new Namespaced(staged, 'a/b')).toThrow(/cannot contain/)
  })

  it('writes are isolated by prefix', async () => {
    const { staged } = await freshStaged()
    const alice = new Namespaced(staged, 'alice')
    const bob = new Namespaced(staged, 'bob')

    alice.set('color', 'red')
    bob.set('color', 'blue')

    expect(await alice.get('color')).toBe('red')
    expect(await bob.get('color')).toBe('blue')
    // The underlying store sees the prefixed keys.
    expect(await staged.get('alice/color')).toBe('red')
    expect(await staged.get('bob/color')).toBe('blue')
  })

  it('delete only affects the namespaced view', async () => {
    const { staged } = await freshStaged()
    const alice = new Namespaced(staged, 'alice')
    const bob = new Namespaced(staged, 'bob')
    alice.set('k', 1)
    bob.set('k', 2)
    alice.delete('k')

    expect(await alice.get('k')).toBeUndefined()
    expect(await bob.get('k')).toBe(2)
  })

  it('has() respects the namespace boundary', async () => {
    const { staged } = await freshStaged()
    const alice = new Namespaced(staged, 'alice')
    alice.set('k', 'v')
    expect(await alice.has('k')).toBe(true)
    expect(await alice.has('absent')).toBe(false)
  })
})

describe('Namespaced — nesting flattens', () => {
  it('nested Namespaced combines prefixes', async () => {
    const { staged } = await freshStaged()
    const outer = new Namespaced(staged, 'a')
    const inner = new Namespaced(outer, 'b')
    expect(inner.namespace).toBe('a/b')

    inner.set('k', 'v')
    expect(await staged.get('a/b/k')).toBe('v')
    expect(await inner.get('k')).toBe('v')
  })
})

describe('Namespaced — keys vs descendantKeys', () => {
  it('keys() returns only direct children, descendantKeys() returns all', async () => {
    const { staged } = await freshStaged()
    const alice = new Namespaced(staged, 'alice')
    const aliceProfile = new Namespaced(alice, 'profile')

    alice.set('top', 1)
    alice.set('also-top', 2)
    aliceProfile.set('name', 'Alice')
    aliceProfile.set('age', 30)
    // Commit so keys also reaches into Versioned (Staged.keys() merges
    // committed + staged; here we want both to be exercised).
    await staged.commit()
    alice.set('staged-only', 99)

    const direct = new Set<string>()
    for await (const k of alice.keys()) direct.add(k)
    expect(direct).toEqual(new Set(['top', 'also-top', 'staged-only']))

    const all = new Set<string>()
    for await (const k of alice.descendantKeys()) all.add(k)
    expect(all).toEqual(new Set(['top', 'also-top', 'staged-only', 'profile/name', 'profile/age']))
  })
})

describe('Namespaced — interplay with Staged commits', () => {
  it('writes via Namespaced flush through Staged.commit()', async () => {
    const { vk, staged } = await freshStaged()
    const alice = new Namespaced(staged, 'alice')
    alice.set('color', 'red')
    expect(staged.hasChanges).toBe(true)
    await staged.commit()
    expect(staged.hasChanges).toBe(false)

    // Underlying Versioned has the prefixed key as bytes.
    const raw = (await vk.get('alice/color')) as Uint8Array
    expect(JSON.parse(new TextDecoder().decode(raw))).toBe('red')
  })
})
