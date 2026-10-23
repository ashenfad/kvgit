import { readFileSync } from 'node:fs'
import { describe, expect, expectTypeOf, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { type MergeFn, Repo, bytesCodec, cborCodec, jsonCodec, resolveCodec } from '../src/index'

describe('cborCodec', () => {
  it('round-trips values with their JS types', () => {
    const value = {
      text: 'é ünï 😀',
      n: 1.5,
      int: -17,
      big: 2n ** 70n,
      small: 5n,
      flag: true,
      nothing: null,
      missing: undefined,
      bytes: new Uint8Array([0xff, 0x00, 0x01]),
      list: [1, 'two', [3]],
      map: new Map<unknown, unknown>([
        [1, 'one'],
        ['k', new Uint8Array([7])],
        [true, new Map([['nested', 1]])],
      ]),
      set: new Set(['a', 'b']),
      when: new Date(1_760_000_000_123),
      floats: new Float32Array([1.5, -2]),
    }
    expect(cborCodec.decode(cborCodec.encode(value))).toEqual(value)
  })

  it('reads byte strings back as plain Uint8Array, wherever they sit', () => {
    const back = cborCodec.decode(
      cborCodec.encode({ top: new Uint8Array([1]), deep: [new Map([['k', new Uint8Array([2])]])] }),
    ) as { top: Uint8Array; deep: [Map<string, Uint8Array>] }
    expect(back.top.constructor).toBe(Uint8Array)
    expect((back.deep[0].get('k') as Uint8Array).constructor).toBe(Uint8Array)
  })

  it('encodes into a buffer of exactly the encoding, not a view into scratch', () => {
    for (const value of [1, { text: 'a'.repeat(4096) }, new Uint8Array(65536)]) {
      const out = cborCodec.encode(value)
      expect(out.constructor).toBe(Uint8Array)
      expect(out.byteOffset).toBe(0)
      expect(out.buffer.byteLength).toBe(out.byteLength)
    }
  })

  it('writes plain CBOR: a string-keyed object is a CBOR map, bytes a byte string', () => {
    const hex = (v: unknown) => Buffer.from(cborCodec.encode(v)).toString('hex')
    expect(hex({ a: 1 })).toMatch(/^(a1|b90001)616101$/)
    expect(hex(new Uint8Array([0xff]))).toBe('41ff')
  })
})

describe('cborCodec reads what Python cbor2 writes', () => {
  interface Expect {
    number?: number
    bigint?: string
    string?: string
    bytes?: string
    null?: true
    boolean?: boolean
    array?: Expect[]
    object?: Record<string, Expect>
    set?: Expect[]
  }
  const golden = JSON.parse(
    readFileSync(new URL('./fixtures/golden-cbor.json', import.meta.url), 'utf8'),
  ) as { cases: Array<{ name: string; hex: string; expect: Expect }> }

  const build = (e: Expect): unknown => {
    if (e.number !== undefined) return e.number
    if (e.bigint !== undefined) return BigInt(e.bigint)
    if (e.string !== undefined) return e.string
    if (e.bytes !== undefined) return new Uint8Array(Buffer.from(e.bytes, 'hex'))
    if (e.null === true) return null
    if (e.boolean !== undefined) return e.boolean
    if (e.array !== undefined) return e.array.map(build)
    if (e.set !== undefined) return new Set(e.set.map(build))
    if (e.object !== undefined) {
      return Object.fromEntries(Object.entries(e.object).map(([k, v]) => [k, build(v)]))
    }
    throw new Error('unknown expectation')
  }

  for (const c of golden.cases) {
    it(c.name, () => {
      const decoded = cborCodec.decode(new Uint8Array(Buffer.from(c.hex, 'hex')))
      expect(decoded).toEqual(build(c.expect))
    })
  }
})

describe('codec names', () => {
  it('resolves bytes, json and cbor, and refuses others', () => {
    expect(resolveCodec('bytes')).toBe(bytesCodec)
    expect(resolveCodec('json')).toBe(jsonCodec)
    expect(resolveCodec('cbor')).toBe(cborCodec)
    expect(resolveCodec(jsonCodec)).toBe(jsonCodec)
    expect(() => resolveCodec('pickle' as 'cbor')).toThrow(RangeError)
  })

  it('jsonCodec refuses a value JSON cannot hold', () => {
    expect(() => jsonCodec.encode(undefined)).toThrow(TypeError)
  })
})

describe('a repository on the cbor codec', () => {
  it('stores rich values, and merges them with value-level functions', async () => {
    const repo = await Repo.open(new Memory(), { codec: 'cbor' })
    const seed = await repo.worktree('main', { create: true })
    seed.set('tags', new Set(['base']))
    seed.set('when', new Date(1_760_000_000_000))
    seed.set('blob', new Uint8Array([1, 2, 3]))
    await seed.commit()

    const a = await repo.worktree('main')
    const b = await repo.worktree('main')
    a.set('tags', new Set(['base', 'a']))
    await a.commit()
    b.set('tags', new Set(['base', 'b']))
    const union: MergeFn<unknown> = (_old, ours, theirs) =>
      new Set([...(ours as Set<string>), ...(theirs as Set<string>)])
    const result = await b.commit({ mergeFns: new Map([['tags', union]]) })
    expect(result.strategy).toBe('three_way')

    const snap = await repo.snapshot({ branch: 'main' })
    expect(await snap.get('tags')).toEqual(new Set(['base', 'a', 'b']))
    expect(await snap.get('when')).toEqual(new Date(1_760_000_000_000))
    expect(await snap.get('blob')).toEqual(new Uint8Array([1, 2, 3]))
  })
})

describe('Repo.open types its values by the codec', () => {
  it('bytes by default, unknown for a named json or cbor codec, V for a Codec<V>', async () => {
    expectTypeOf(await Repo.open(new Memory())).toEqualTypeOf<Repo<Uint8Array>>()
    expectTypeOf(await Repo.open(new Memory(), { codec: 'bytes' })).toEqualTypeOf<
      Repo<Uint8Array>
    >()
    expectTypeOf(await Repo.open(new Memory(), { codec: 'cbor' })).toEqualTypeOf<Repo<unknown>>()
    expectTypeOf(await Repo.open(new Memory(), { codec: 'json' })).toEqualTypeOf<Repo<unknown>>()
    expectTypeOf(await Repo.open(new Memory(), { codec: jsonCodec })).toEqualTypeOf<Repo<unknown>>()
    interface State {
      n: number
    }
    const typed = await Repo.open<State>(new Memory(), { codec: 'json' })
    expectTypeOf(typed).toEqualTypeOf<Repo<State>>()
    const wt = await typed.worktree('main', { create: true })
    wt.set('s', { n: 1 })
    await wt.commit()
    expect(await wt.get('s')).toEqual({ n: 1 })
  })
})

describe('values read are copies', () => {
  for (const codec of ['bytes', 'cbor'] as const) {
    it(`mutating a value read under '${codec}' leaves the stored commit alone`, async () => {
      const repo = await Repo.open(new Memory(), { codec: codec as 'cbor' })
      const wt = await repo.worktree('main', { create: true })
      wt.set('k', new Uint8Array([1, 2, 3]))
      const commit = (await wt.commit()).commit as string

      const snap = await repo.snapshot({ commit })
      ;((await snap.get('k')) as Uint8Array)[0] = 99
      ;((await wt.get('k')) as Uint8Array)[1] = 99
      const fresh = await repo.snapshot({ commit })
      expect(await fresh.get('k')).toEqual(new Uint8Array([1, 2, 3]))
    })
  }

  it('mutating an array after it was written leaves the store alone', async () => {
    const repo = await Repo.open(new Memory())
    const wt = await repo.worktree('main', { create: true })
    const value = new Uint8Array([1, 2, 3])
    wt.set('k', value)
    const commit = (await wt.commit()).commit as string
    value[0] = 99
    const other = await repo.worktree('main')
    expect(await other.get('k')).toEqual(new Uint8Array([1, 2, 3]))
    expect(await (await repo.snapshot({ commit })).get('k')).toEqual(new Uint8Array([1, 2, 3]))
  })

  it('a decoded byte string has its own memory', () => {
    const encoded = cborCodec.encode({ b: new Uint8Array([1, 2, 3]) })
    const decoded = cborCodec.decode(encoded) as { b: Uint8Array }
    decoded.b[0] = 99
    expect([...encoded]).not.toContain(99)
  })
})
