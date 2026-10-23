import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import {
  CantMark,
  MergeConflict,
  Staged,
  type TextMergeOptions,
  VersionedKV,
  makeTextMerge,
  text,
  textMerge,
  textMergeResult,
} from '../src/index'
import { SequenceMatcher } from '../src/sequenceMatcher'

const enc = new TextEncoder()
const dec = new TextDecoder()
const bytes = (s: string): Uint8Array => enc.encode(s)
const textOf = (b: Uint8Array): string => dec.decode(b)
const b64 = (b: Uint8Array): string => Buffer.from(b).toString('base64')
const unb64 = (s: string): Uint8Array => new Uint8Array(Buffer.from(s, 'base64'))

describe('SequenceMatcher', () => {
  it('emits equal/insert opcodes like Python', () => {
    const sm = new SequenceMatcher<string>(null, ['a', 'b', 'c'], ['a', 'X', 'b', 'c'], false)
    expect(sm.getOpcodes()).toEqual([
      { tag: 'equal', aStart: 0, aEnd: 1, bStart: 0, bEnd: 1 },
      { tag: 'insert', aStart: 1, aEnd: 1, bStart: 1, bEnd: 2 },
      { tag: 'equal', aStart: 1, aEnd: 3, bStart: 2, bEnd: 4 },
    ])
  })

  it('treats object-key-hostile lines as ordinary values', () => {
    // A plain-object index would corrupt on '__proto__'; the Map index
    // must match Python: ours changes line 2, theirs is unchanged.
    const sm = new SequenceMatcher<string>(null, ['__proto__', 'a'], ['__proto__', 'B'], false)
    expect(sm.getOpcodes()).toEqual([
      { tag: 'equal', aStart: 0, aEnd: 1, bStart: 0, bEnd: 1 },
      { tag: 'replace', aStart: 1, aEnd: 2, bStart: 1, bEnd: 2 },
    ])
  })

  it('matches Python on long popular-element inputs with autojunk off', () => {
    const popular = Array.from({ length: 250 }, () => 'common')
    const sm = new SequenceMatcher<string>(null, [...popular, 'OLD'], [...popular, 'NEW'], false)
    expect(sm.getOpcodes()).toEqual([
      { tag: 'equal', aStart: 0, aEnd: 250, bStart: 0, bEnd: 250 },
      { tag: 'replace', aStart: 250, aEnd: 251, bStart: 250, bEnd: 251 },
    ])
  })
})

interface GoldenCase {
  inputs: [string | null, string | null, string | null]
  labels: { ours_label?: string; theirs_label?: string }
  merged?: string
  conflicted?: boolean
  strict_raised?: boolean
  strict_bytes?: string | null
  error?: string
}

const golden: Record<string, GoldenCase> = JSON.parse(
  readFileSync(new URL('./fixtures/merge-golden.json', import.meta.url), 'utf8'),
)

describe('text merges — byte-identical with Python kvgit', () => {
  for (const [name, tc] of Object.entries(golden)) {
    it(name, () => {
      const [oldB = null, oursB = null, theirsB = null] = tc.inputs.map((s) =>
        s === null ? null : unb64(s),
      )
      const labels: TextMergeOptions = {}
      if (tc.labels.ours_label !== undefined) labels.oursLabel = tc.labels.ours_label
      if (tc.labels.theirs_label !== undefined) labels.theirsLabel = tc.labels.theirs_label
      if (tc.error !== undefined) {
        expect(tc.error).toBe('CantMark')
        expect(() => textMergeResult(oldB, oursB, theirsB, labels)).toThrow(CantMark)
        return
      }
      const [merged, conflicted] = textMergeResult(oldB, oursB, theirsB, labels)
      expect(b64(merged)).toBe(tc.merged)
      expect(conflicted).toBe(tc.conflicted)
      if (tc.strict_raised === true) {
        expect(() => makeTextMerge({ ...labels, strict: true })(oldB, oursB, theirsB)).toThrow(
          CantMark,
        )
      } else {
        const strictOut = makeTextMerge({ ...labels, strict: true })(oldB, oursB, theirsB)
        expect(b64(strictOut)).toBe(tc.strict_bytes)
      }
    })
  }
})

describe('strict mode and conflict flag', () => {
  const base = bytes('a\nb\nc\nd\n')

  it('strict raises on conflict', () => {
    expect(() =>
      makeTextMerge({ strict: true })(base, bytes('a\nX\nc\nd\n'), bytes('a\nY\nc\nd\n')),
    ).toThrow(CantMark)
  })

  it('strict passes clean merges through', () => {
    expect(
      makeTextMerge({ strict: true })(base, bytes('a\nB\nc\nd\n'), bytes('a\nb\nc\nD\n')),
    ).toEqual(bytes('a\nB\nc\nD\n'))
  })

  it('strict ignores carried marker-like content', () => {
    const markerish = bytes('<<<<<<< not a real hunk\nsame\n')
    expect(makeTextMerge({ strict: true })(base, markerish, base)).toEqual(markerish)
  })

  it('the default still marks', () => {
    expect(textOf(makeTextMerge()(base, bytes('a\nX\nc\nd\n'), bytes('a\nY\nc\nd\n')))).toContain(
      '<<<<<<< ours\n',
    )
  })

  it('flag is false for clean and carried-marker merges, true on conflict', () => {
    expect(textMergeResult(base, bytes('a\nB\nc\nd\n'), bytes('a\nb\nc\nD\n'))[1]).toBe(false)
    const markerish = bytes('<<<<<<< not a real hunk\nsame\n')
    const [carried, carriedFlag] = textMergeResult(base, markerish, base)
    expect(carriedFlag).toBe(false)
    expect(carried).toEqual(markerish)
    const [merged, conflicted] = textMergeResult(base, bytes('a\nX\nc\nd\n'), bytes('a\nY\nc\nd\n'))
    expect(conflicted).toBe(true)
    expect(merged).toEqual(makeTextMerge()(base, bytes('a\nX\nc\nd\n'), bytes('a\nY\nc\nd\n')))
    expect(merged).toEqual(text(base, bytes('a\nX\nc\nd\n'), bytes('a\nY\nc\nd\n')))
  })

  it('custom labels ride git positions', () => {
    const [merged, conflicted] = textMergeResult(
      base,
      bytes('a\nX\nc\nd\n'),
      bytes('a\nY\nc\nd\n'),
      {
        oursLabel: 'main',
        theirsLabel: 'dev',
      },
    )
    expect(conflicted).toBe(true)
    expect(textOf(merged)).toContain('<<<<<<< main\n')
    expect(textOf(merged)).toContain('>>>>>>> dev\n')
  })

  it('unmarkable input still throws', () => {
    expect(() => textMergeResult(base, bytes('a\nb\n'), new Uint8Array([0xff]))).toThrow(CantMark)
    expect(() => makeTextMerge({ strict: true })(base, new Uint8Array([0xff]), base)).toThrow(
      CantMark,
    )
  })

  it('newline labels are rejected', () => {
    expect(() => makeTextMerge({ oursLabel: 'a\nb' })).toThrow(TypeError)
  })

  it.each(['\r', '\v', '\f', '\x1c', '\x1d', '\x1e', '\x85', '\u2028', '\u2029'])(
    'other line-break labels are rejected (%j)',
    (br) => {
      expect(() => makeTextMerge({ oursLabel: `a${br}b` })).toThrow(TypeError)
      expect(() => makeTextMerge({ theirsLabel: `a${br}b` })).toThrow(TypeError)
    },
  )

  it('lone-CR endings survive a clean merge byte-exact', () => {
    const base = bytes('a\rb\rc\r')
    expect(textOf(text(base, bytes('a\rB\rc\r'), bytes('a\rb\rC\r')))).toBe('a\rB\rC\r')
  })

  it('conflict markers stay newline-terminated over CR content', () => {
    const out = textOf(text(bytes('a\rb\rc\r'), bytes('a\rB\rc\r'), bytes('a\rX\rc\r')))
    expect(out).toBe('a\r<<<<<<< ours\nB\r=======\nX\r>>>>>>> theirs\nc\r')
  })

  it('strict conflict aborts a Staged merge', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    const stagedA = new Staged(a)
    stagedA.set('doc', 'a\nb\nc\nd\n')
    await stagedA.commit()

    const b = await VersionedKV.open(store)
    const stagedB = new Staged(b)

    stagedA.set('doc', 'a\nX\nc\nd\n')
    await stagedA.commit()

    stagedB.set('doc', 'a\nY\nc\nd\n')
    await expect(stagedB.commit({ defaultMerge: textMerge({ strict: true }) })).rejects.toThrow(
      MergeConflict,
    )
  })

  it('strict clean change still merges at Staged level', async () => {
    const store = new Memory()
    const a = await VersionedKV.open(store)
    const stagedA = new Staged(a)
    stagedA.set('doc', 'a\nb\nc\nd\n')
    await stagedA.commit()

    const b = await VersionedKV.open(store)
    const stagedB = new Staged(b)

    stagedA.set('doc', 'a\nB\nc\nd\n')
    await stagedA.commit()

    stagedB.set('doc', 'a\nb\nc\nD\n')
    const r = await stagedB.commit({ defaultMerge: textMerge({ strict: true }) })
    expect(r.merged).toBe(true)
    expect(await stagedB.get('doc')).toBe('a\nB\nc\nD\n')
  })

  it('value-level merge keeps str/bytes spelling', () => {
    expect(textMerge()('a\nb\n', 'a\nB\n', 'a\nb\n')).toBe('a\nB\n')
    expect(textMerge()(bytes('a\nb\n'), bytes('a\nB\n'), bytes('a\nb\n'))).toEqual(bytes('a\nB\n'))
    expect(textMerge()(bytes('a\nb\n'), 'a\nB\n', bytes('a\nb\n'))).toBe('a\nB\n')
    expect(() => textMerge()(null, 42, null)).toThrow(CantMark)
  })
})
