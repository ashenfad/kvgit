/**
 * Marker merge for line-oriented text over bytes: disjoint line changes
 * merge cleanly, overlapping ones come back with git-style `<<<<<<<`
 * markers. The merge itself is the core's, the one Python kvgit makes.
 */
import { loadedCore } from './core'
import type { MergeFn } from './types'

/** Raised instead of returning bytes when a value cannot be marker-merged. */
export class CantMark extends Error {
  override readonly name = 'CantMark'
}

/** Cap on the combined input size accepted for marking. */
export const MAX_MARK_BYTES = 1024 * 1024

export interface TextMergeOptions {
  oursLabel?: string
  theirsLabel?: string
  strict?: boolean
}

const textEncoder = new TextEncoder()

/** Extra `str.splitlines` boundaries beyond `\n`, in priority order. */
const LINE_BOUNDARIES = new Set([
  '\n',
  '\r',
  '\v',
  '\f',
  '\x1c',
  '\x1d',
  '\x1e',
  '\x85',
  '\u2028',
  '\u2029',
])

function checkLabels(oursLabel: string, theirsLabel: string): void {
  for (const [name, label] of [
    ['oursLabel', oursLabel],
    ['theirsLabel', theirsLabel],
  ] as const) {
    for (const boundary of LINE_BOUNDARIES) {
      if (label.includes(boundary)) {
        throw new TypeError(`${name} may not contain a newline: ${JSON.stringify(label)}`)
      }
    }
  }
}

/**
 * Split text into lines keeping endings, like Python's
 * `str.splitlines(keepends=True)`: `\r\n` pairs stay together, lone
 * `\r` and the other Unicode boundaries split, and a trailing newline
 * leaves no empty tail.
 */
export function splitLinesKeepEnds(text: string): string[] {
  const lines: string[] = []
  let start = 0
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]
    const next = text[i + 1]
    if (ch === '\r' && next === '\n') {
      lines.push(text.slice(start, i + 2))
      start = i + 2
      i++
    } else if (ch !== undefined && LINE_BOUNDARIES.has(ch)) {
      lines.push(text.slice(start, i + 1))
      start = i + 1
    }
  }
  if (start < text.length) lines.push(text.slice(start))
  return lines
}

/**
 * Marker-merge returning the merged bytes plus whether the merge
 * introduced conflict hunks — reliably, where counting `<<<<<<<`
 * lines cannot, because the inputs may legitimately contain
 * marker-like lines that merge through untouched. Anything unmarkable
 * still throws `CantMark`.
 */
export function textMergeResult(
  oldB: Uint8Array | null,
  oursB: Uint8Array | null,
  theirsB: Uint8Array | null,
  opts: Omit<TextMergeOptions, 'strict'> = {},
): [Uint8Array, boolean] {
  const oursLabel = opts.oursLabel ?? 'ours'
  const theirsLabel = opts.theirsLabel ?? 'theirs'
  checkLabels(oursLabel, theirsLabel)
  const result = loadedCore().textMerge(
    oldB ?? undefined,
    oursB ?? undefined,
    theirsB ?? undefined,
    oursLabel,
    theirsLabel,
  )
  try {
    if (result.refused !== undefined) throw new CantMark(result.refused)
    return [result.merged, result.conflicted]
  } finally {
    result.free()
  }
}

/** A bytes merge function that always produces a value (a marker merge
 *  never answers with a `MergeChoice`); usable wherever a
 *  `BytesMergeFn` is. */
export type TextBytesMergeFn = (
  old: Uint8Array | null,
  ours: Uint8Array | null,
  theirs: Uint8Array | null,
) => Uint8Array

/**
 * Build a marker-merge fn with custom conflict labels. Labels ride
 * git's positions (`<<<<<<< <ours>` / `>>>>>>> <theirs>`); pass branch
 * names so conflicts read attributably. Labels may not contain
 * newlines (any `splitLinesKeepEnds` boundary: LF, lone CR, VT/FF,
 * FS/GS/RS, NEL, U+2028/2029).
 *
 * With `strict: true` the fn throws `CantMark` instead of writing
 * markers when sides conflict — for branches where a true conflict
 * must abort the commit rather than land hunks. The merge machinery
 * files it as an ordinary conflict, same as unmarkable input.
 */
export function makeTextMerge(opts: TextMergeOptions = {}): TextBytesMergeFn {
  const oursLabel = opts.oursLabel ?? 'ours'
  const theirsLabel = opts.theirsLabel ?? 'theirs'
  const strict = opts.strict ?? false
  checkLabels(oursLabel, theirsLabel)
  return (oldB, oursB, theirsB) => {
    const [merged, conflicted] = textMergeResult(oldB, oursB, theirsB, {
      oursLabel,
      theirsLabel,
    })
    if (conflicted && strict) {
      throw new CantMark('strict text merge: conflicting changes cannot be marked')
    }
    return merged
  }
}

/** Marker merge with default labels, for use as a `BytesMergeFn`. */
export const text: TextBytesMergeFn = makeTextMerge()

/**
 * Marker merge for decoded `string` values (or raw bytes), as a
 * worktree's merge rule. Encodes `string` sides as UTF-8, marker-merges, and
 * decodes the result back to `string` when any side was `string`.
 */
export function textMerge(opts: TextMergeOptions = {}): MergeFn {
  const mergeBytes = makeTextMerge(opts)
  const encode = (value: unknown): Uint8Array | null => {
    if (value === null || value instanceof Uint8Array) return value
    if (typeof value === 'string') return textEncoder.encode(value)
    throw new CantMark(`not text: ${typeof value}`)
  }
  return (oldV, oursV, theirsV) => {
    // One side holding a string is enough to make the merged value a
    // string: the sides are the same key's value at different commits,
    // so a bytes side is the same text in another spelling.
    const asStr =
      typeof oldV === 'string' || typeof oursV === 'string' || typeof theirsV === 'string'
    const merged = mergeBytes(encode(oldV), encode(oursV), encode(theirsV))
    return asStr ? new TextDecoder().decode(merged) : merged
  }
}
