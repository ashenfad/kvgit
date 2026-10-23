/**
 * The canonical encoder against Python's own `json.dumps` output
 * (tests/fixtures/golden-v4.json, from scripts/golden-v4.py).
 */

import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { PyFloat, compareCodePoints, pyDumps, pyFloat, pyLoads } from '../src/pyjson'

interface Case {
  kind: 'string' | 'float' | 'dict'
  value: unknown
  dumps: string
  sorted?: string
}
const golden = JSON.parse(
  readFileSync(new URL('./fixtures/golden-v4.json', import.meta.url), 'utf8'),
) as { json: Case[] }
const cases = (kind: Case['kind']) => golden.json.filter((c) => c.kind === kind)

/** A Python float's repr, read back to the double it names. */
const fromRepr = (repr: string): number =>
  repr === 'inf'
    ? Number.POSITIVE_INFINITY
    : repr === '-inf'
      ? Number.NEGATIVE_INFINITY
      : repr === 'nan'
        ? Number.NaN
        : Number(repr)

describe("pyDumps matches Python's json.dumps", () => {
  it.each(cases('string').map((c) => [c.value as string, c.dumps]))('string %j', (s, dumps) => {
    expect(pyDumps(s)).toBe(dumps)
  })

  it.each(cases('float').map((c) => [c.value as string, c.dumps]))('float %s', (repr, dumps) => {
    expect(pyFloat(fromRepr(repr))).toBe(dumps)
    expect(pyDumps(new PyFloat(fromRepr(repr)))).toBe(dumps)
  })

  it.each(cases('dict').map((c, i) => [i, c]))('dict %i, in order and sorted', (_i, c) => {
    // The fixture's JSON reader turned 2.0 into 2: mark it a float again
    // through pyLoads of Python's own output, as a reader of stored text does.
    const value = pyLoads(c.dumps)
    expect(pyDumps(value)).toBe(c.dumps)
    expect(pyDumps(value, { sortKeys: true })).toBe(c.sorted)
  })
})

describe('pyLoads keeps what Python keeps', () => {
  it('reads a fraction or exponent as a float, a long integer as a bigint', () => {
    expect(pyLoads('2.0')).toBeInstanceOf(PyFloat)
    expect(pyDumps(pyLoads('1e5'))).toBe('100000.0')
    expect(pyLoads('2')).toBe(2)
    expect(pyLoads('123456789012345678901')).toBe(123456789012345678901n)
    expect(pyDumps(pyLoads('[NaN,Infinity,-Infinity]'))).toBe('[NaN,Infinity,-Infinity]')
  })

  it('round-trips nested values', () => {
    const text = '{"a":[1,2.5,{"b":null}],"c":"\\u00e9","d":true}'
    expect(pyDumps(pyLoads(text))).toBe(text)
  })

  it('refuses malformed text', () => {
    expect(() => pyLoads('{"a":1')).toThrow(SyntaxError)
    expect(() => pyLoads('[1,]')).toThrow(SyntaxError)
    expect(() => pyLoads('1 2')).toThrow(SyntaxError)
  })
})

describe('compareCodePoints', () => {
  it('orders a character outside the basic plane after U+FFFF', () => {
    expect(compareCodePoints('😀', '￿')).toBe(1)
    expect(['😀', '￿', 'a'].sort(compareCodePoints)).toEqual(['a', '￿', '😀'])
  })
})
