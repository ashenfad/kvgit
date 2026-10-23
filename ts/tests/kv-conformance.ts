/**
 * KVStore conformance suite.
 *
 * Backend-agnostic tests that any `KVStore` implementation must
 * satisfy. Consumers (memory, idb, opfs, sqlite, custom) call
 * `runConformance(name, makeStore)` from a `*.test.ts` file; the
 * suite registers `describe`/`it` blocks in their test environment.
 *
 * `makeStore` should return a *fresh* store on each call. The suite
 * uses one store per top-level test for isolation.
 */

import { describe, expect, it } from 'vitest'
import type { KVStore } from '../src/types'

const enc = new TextEncoder()
const bytes = (s: string): Uint8Array => enc.encode(s)

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false
  return true
}

export type StoreFactory = () => KVStore | Promise<KVStore>

export function runConformance(name: string, makeStore: StoreFactory): void {
  describe(`${name} — KVStore conformance`, () => {
    describe('get / set / remove / has', () => {
      it('get on a missing key returns null', async () => {
        const s = await makeStore()
        expect(await s.get('absent')).toBeNull()
        expect(await s.has('absent')).toBe(false)
      })

      it('set then get round-trips bytes', async () => {
        const s = await makeStore()
        await s.set('k', bytes('v'))
        const got = await s.get('k')
        expect(got).not.toBeNull()
        expect(bytesEqual(got as Uint8Array, bytes('v'))).toBe(true)
        expect(await s.has('k')).toBe(true)
      })

      it('set overwrites a previous value', async () => {
        const s = await makeStore()
        await s.set('k', bytes('v1'))
        await s.set('k', bytes('v2'))
        expect(bytesEqual((await s.get('k')) as Uint8Array, bytes('v2'))).toBe(true)
      })

      it('remove makes a key absent', async () => {
        const s = await makeStore()
        await s.set('k', bytes('v'))
        await s.remove('k')
        expect(await s.get('k')).toBeNull()
        expect(await s.has('k')).toBe(false)
      })

      it('remove on a missing key is a no-op', async () => {
        const s = await makeStore()
        await expect(s.remove('absent')).resolves.toBeUndefined()
      })
    })

    describe('bulk methods', () => {
      it('setMany inserts multiple at once', async () => {
        const s = await makeStore()
        await s.setMany([
          ['a', bytes('1')],
          ['b', bytes('2')],
          ['c', bytes('3')],
        ])
        expect(bytesEqual((await s.get('a')) as Uint8Array, bytes('1'))).toBe(true)
        expect(bytesEqual((await s.get('b')) as Uint8Array, bytes('2'))).toBe(true)
        expect(bytesEqual((await s.get('c')) as Uint8Array, bytes('3'))).toBe(true)
      })

      it('getMany returns only present keys', async () => {
        const s = await makeStore()
        await s.set('a', bytes('1'))
        await s.set('c', bytes('3'))
        const got = await s.getMany(['a', 'b', 'c'])
        expect(got.size).toBe(2)
        expect(bytesEqual(got.get('a') as Uint8Array, bytes('1'))).toBe(true)
        expect(bytesEqual(got.get('c') as Uint8Array, bytes('3'))).toBe(true)
        expect(got.has('b')).toBe(false)
      })

      it('getMany on an empty key set returns an empty Map', async () => {
        const s = await makeStore()
        await s.set('a', bytes('1'))
        const got = await s.getMany([])
        expect(got.size).toBe(0)
      })

      it('removeMany deletes multiple at once', async () => {
        const s = await makeStore()
        await s.setMany([
          ['a', bytes('1')],
          ['b', bytes('2')],
          ['c', bytes('3')],
        ])
        await s.removeMany(['a', 'c'])
        expect(await s.get('a')).toBeNull()
        expect(bytesEqual((await s.get('b')) as Uint8Array, bytes('2'))).toBe(true)
        expect(await s.get('c')).toBeNull()
      })

      it('removeMany ignores absent keys', async () => {
        const s = await makeStore()
        await s.set('a', bytes('1'))
        await expect(s.removeMany(['a', 'absent'])).resolves.toBeUndefined()
        expect(await s.get('a')).toBeNull()
      })
    })

    describe('cas', () => {
      it('succeeds when expected matches current', async () => {
        const s = await makeStore()
        await s.set('k', bytes('v1'))
        const ok = await s.cas('k', bytes('v2'), bytes('v1'))
        expect(ok).toBe(true)
        expect(bytesEqual((await s.get('k')) as Uint8Array, bytes('v2'))).toBe(true)
      })

      it('fails when expected does not match current', async () => {
        const s = await makeStore()
        await s.set('k', bytes('v1'))
        const ok = await s.cas('k', bytes('v2'), bytes('different'))
        expect(ok).toBe(false)
        expect(bytesEqual((await s.get('k')) as Uint8Array, bytes('v1'))).toBe(true)
      })

      it("succeeds with expected=null when key doesn't exist", async () => {
        const s = await makeStore()
        const ok = await s.cas('k', bytes('v'), null)
        expect(ok).toBe(true)
        expect(bytesEqual((await s.get('k')) as Uint8Array, bytes('v'))).toBe(true)
      })

      it('fails with expected=null when key already exists', async () => {
        const s = await makeStore()
        await s.set('k', bytes('existing'))
        const ok = await s.cas('k', bytes('v'), null)
        expect(ok).toBe(false)
        expect(bytesEqual((await s.get('k')) as Uint8Array, bytes('existing'))).toBe(true)
      })

      it('fails when key is absent and expected is non-null', async () => {
        const s = await makeStore()
        const ok = await s.cas('k', bytes('v'), bytes('expected-something'))
        expect(ok).toBe(false)
        expect(await s.get('k')).toBeNull()
      })
    })

    describe('casMany', () => {
      const text = (b: Uint8Array | null) => (b === null ? null : new TextDecoder().decode(b))

      it('lands every write and removal when every expectation holds', async () => {
        const s = await makeStore()
        await s.setMany([
          ['head', bytes('h1')],
          ['gone', bytes('x')],
        ])
        const ok = await s.casMany(
          new Map([
            ['head', bytes('h1')],
            ['new', null],
          ]),
          new Map([
            ['head', bytes('h2')],
            ['prev', bytes('h1')],
          ]),
          ['gone'],
        )
        expect(ok).toBe(true)
        expect(text(await s.get('head'))).toBe('h2')
        expect(text(await s.get('prev'))).toBe('h1')
        expect(await s.get('gone')).toBeNull()
      })

      it('changes nothing when one expectation fails', async () => {
        const s = await makeStore()
        await s.setMany([
          ['head', bytes('h1')],
          ['gone', bytes('x')],
        ])
        for (const expected of [
          new Map([['head', bytes('other')]]),
          new Map<string, Uint8Array | null>([['head', null]]),
          new Map([['missing', bytes('x')]]),
        ]) {
          const ok = await s.casMany(expected, new Map([['head', bytes('h2')]]), ['gone'])
          expect(ok).toBe(false)
        }
        expect(text(await s.get('head'))).toBe('h1')
        expect(text(await s.get('gone'))).toBe('x')
      })

      it('lands unconditionally with nothing expected', async () => {
        const s = await makeStore()
        expect(await s.casMany(new Map(), new Map([['k', bytes('v')]]))).toBe(true)
        expect(text(await s.get('k'))).toBe('v')
      })

      it('refuses a key both written and removed', async () => {
        const s = await makeStore()
        await expect(s.casMany(new Map(), new Map([['k', bytes('v')]]), ['k'])).rejects.toThrow()
      })
    })

    describe('iteration', () => {
      it('keys() yields all stored keys', async () => {
        const s = await makeStore()
        await s.setMany([
          ['a', bytes('1')],
          ['b', bytes('2')],
          ['c', bytes('3')],
        ])
        const got = new Set<string>()
        for await (const k of s.keys()) got.add(k)
        expect(got).toEqual(new Set(['a', 'b', 'c']))
      })

      it('keys() on an empty store yields nothing', async () => {
        const s = await makeStore()
        const got: string[] = []
        for await (const k of s.keys()) got.push(k)
        expect(got).toEqual([])
      })

      it('items() yields all (key, value) pairs', async () => {
        const s = await makeStore()
        await s.setMany([
          ['a', bytes('1')],
          ['b', bytes('2')],
        ])
        const got = new Map<string, Uint8Array>()
        for await (const [k, v] of s.items()) got.set(k, v)
        expect(got.size).toBe(2)
        expect(bytesEqual(got.get('a') as Uint8Array, bytes('1'))).toBe(true)
        expect(bytesEqual(got.get('b') as Uint8Array, bytes('2'))).toBe(true)
      })
    })

    describe('iteration with prefix', () => {
      it('keys(prefix) returns only keys matching the prefix', async () => {
        const s = await makeStore()
        await s.setMany([
          ['user:alice', bytes('1')],
          ['user:bob', bytes('2')],
          ['post:hello', bytes('3')],
          ['admin', bytes('4')],
        ])
        const got = new Set<string>()
        for await (const k of s.keys('user:')) got.add(k)
        expect(got).toEqual(new Set(['user:alice', 'user:bob']))
      })

      it('items(prefix) returns only items matching the prefix', async () => {
        const s = await makeStore()
        await s.setMany([
          ['user:alice', bytes('alice')],
          ['post:hello', bytes('hello')],
        ])
        const got = new Map<string, Uint8Array>()
        for await (const [k, v] of s.items('user:')) got.set(k, v)
        expect(got.size).toBe(1)
        expect(bytesEqual(got.get('user:alice') as Uint8Array, bytes('alice'))).toBe(true)
      })

      it('keys(prefix) on a non-matching prefix yields nothing', async () => {
        const s = await makeStore()
        await s.set('user:alice', bytes('1'))
        const got: string[] = []
        for await (const k of s.keys('absent:')) got.push(k)
        expect(got).toEqual([])
      })

      it('keys(prefix) excludes near-misses (longer prefix in same range)', async () => {
        // Keys that share a prefix beyond the requested one should be
        // included; keys whose prefix is shorter must be excluded.
        const s = await makeStore()
        await s.setMany([
          ['foo', bytes('1')], // shorter — must NOT match prefix 'foo:'
          ['foo:a', bytes('2')], // exact prefix match
          ['foo:a:b', bytes('3')], // longer key sharing the prefix
          ['fop', bytes('4')], // adjacent key beyond the range
        ])
        const got = new Set<string>()
        for await (const k of s.keys('foo:')) got.add(k)
        expect(got).toEqual(new Set(['foo:a', 'foo:a:b']))
      })

      it('keys(prefix) and items(prefix) include any character after the prefix', async () => {
        // Backends order keys by UTF-8 bytes or by UTF-16 code units, so
        // no single character bounds every key under a prefix: not
        // U+FFFF, which characters outside the BMP sort above in UTF-8,
        // and which may itself follow the prefix.
        const s = await makeStore()
        const under = [
          'user:',
          'user:a',
          'user:\u{1F600}',
          'user:\u{10FFFF}',
          'user:\uFFFF',
          'user:\uFFFFx',
          'user:\u{10FFFF}\u{10FFFF}',
        ]
        await s.setMany(
          [...under, 'user', 'user;', 'user;\u{1F600}', 'usera'].map((k) => [k, bytes(k)] as const),
        )
        const keys = new Set<string>()
        for await (const k of s.keys('user:')) keys.add(k)
        expect(keys).toEqual(new Set(under))
        const items = new Map<string, Uint8Array>()
        for await (const [k, v] of s.items('user:')) items.set(k, v)
        expect(new Set(items.keys())).toEqual(new Set(under))
        expect(bytesEqual(items.get('user:\u{1F600}') as Uint8Array, bytes('user:\u{1F600}'))).toBe(
          true,
        )
      })

      it('keys(prefix) handles a prefix ending in U+D7FF, U+FFFF or U+10FFFF', async () => {
        const s = await makeStore()
        await s.setMany(
          [
            'k\uD7FF',
            'k\uD7FF\u{1F600}',
            'k\uE000',
            'k\uFFFF',
            'k\uFFFFa',
            'k\uFFFF\uFFFF',
            'k\uFFFF\u{1F600}',
            'k\u{10000}',
            'k\u{10FFFF}',
            'k\u{10FFFF}a',
            'k\u{10FFFF}\u{10FFFF}',
            'l',
          ].map((k) => [k, bytes(k)] as const),
        )
        const collect = async (prefix: string): Promise<Set<string>> => {
          const got = new Set<string>()
          for await (const k of s.keys(prefix)) got.add(k)
          return got
        }
        expect(await collect('k\uD7FF')).toEqual(new Set(['k\uD7FF', 'k\uD7FF\u{1F600}']))
        expect(await collect('k\uFFFF')).toEqual(
          new Set(['k\uFFFF', 'k\uFFFFa', 'k\uFFFF\uFFFF', 'k\uFFFF\u{1F600}']),
        )
        expect(await collect('k\u{10FFFF}')).toEqual(
          new Set(['k\u{10FFFF}', 'k\u{10FFFF}a', 'k\u{10FFFF}\u{10FFFF}']),
        )
        expect(await collect('\u{10FFFF}')).toEqual(new Set())
      })

      it('keys(prefix) with a prefix of only U+FFFF or U+10FFFF, or none', async () => {
        const s = await makeStore()
        const all = ['', 'a', '\uFFFF', '\uFFFF\uFFFF', '\uFFFFz', '\u{10FFFF}', '\u{10FFFF}z']
        await s.setMany(all.map((k) => [k, bytes(k)] as const))
        const collect = async (prefix: string): Promise<Set<string>> => {
          const got = new Set<string>()
          for await (const k of s.keys(prefix)) got.add(k)
          return got
        }
        expect(await collect('')).toEqual(new Set(all))
        expect(await collect('\uFFFF')).toEqual(new Set(['\uFFFF', '\uFFFF\uFFFF', '\uFFFFz']))
        expect(await collect('\u{10FFFF}')).toEqual(new Set(['\u{10FFFF}', '\u{10FFFF}z']))
      })
    })

    describe('clear', () => {
      it('removes everything', async () => {
        const s = await makeStore()
        await s.setMany([
          ['a', bytes('1')],
          ['b', bytes('2')],
        ])
        await s.clear()
        expect(await s.get('a')).toBeNull()
        expect(await s.get('b')).toBeNull()
        const remaining: string[] = []
        for await (const k of s.keys()) remaining.push(k)
        expect(remaining).toEqual([])
      })
    })

    describe('byte fidelity', () => {
      it('round-trips arbitrary byte sequences including nulls and high bytes', async () => {
        const s = await makeStore()
        const payload = new Uint8Array([0, 1, 2, 254, 255, 0, 128])
        await s.set('k', payload)
        const got = (await s.get('k')) as Uint8Array
        expect(got.length).toBe(payload.length)
        for (let i = 0; i < payload.length; i++) {
          expect(got[i]).toBe(payload[i])
        }
      })

      it('stores a view as exactly its own bytes, through every write', async () => {
        // A view into a larger buffer, at an offset and at the start: a
        // store that kept the whole buffer would read back a view too
        // (IndexedDB's structured clone keeps it) or pin the buffer.
        const under = new Uint8Array(4096)
        for (let i = 0; i < under.length; i++) under[i] = i & 0xff
        const atOffset = under.subarray(100, 108)
        const atStart = under.subarray(0, 8)
        const s = await makeStore()
        await s.set('set', atOffset)
        await s.setMany([['setMany', atStart]])
        await s.cas('cas', atOffset, null)
        await s.casMany(new Map([['casMany', null]]), new Map([['casMany', atStart]]))
        under.fill(0)
        for (const [key, want] of [
          ['set', [100, 101, 102, 103, 104, 105, 106, 107]],
          ['setMany', [0, 1, 2, 3, 4, 5, 6, 7]],
          ['cas', [100, 101, 102, 103, 104, 105, 106, 107]],
          ['casMany', [0, 1, 2, 3, 4, 5, 6, 7]],
        ] as const) {
          const got = (await s.get(key)) as Uint8Array
          expect([...got], key).toEqual(want)
          expect(got.byteOffset, key).toBe(0)
          expect(got.buffer.byteLength, key).toBe(8)
        }
      })

      it('handles a large payload (1 MiB)', async () => {
        const s = await makeStore()
        const big = new Uint8Array(1024 * 1024)
        for (let i = 0; i < big.length; i++) big[i] = i & 0xff
        await s.set('big', big)
        const got = (await s.get('big')) as Uint8Array
        expect(got.length).toBe(big.length)
        // Spot-check a few positions; full byte-by-byte would be slow
        // and isn't more informative.
        expect(got[0]).toBe(0)
        expect(got[255]).toBe(255)
        expect(got[1000]).toBe(1000 & 0xff)
        expect(got[big.length - 1]).toBe((big.length - 1) & 0xff)
      })
    })
  })
}
