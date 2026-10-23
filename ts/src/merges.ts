import { SequenceMatcher } from './sequenceMatcher'
/**
 * Marker merge for line-oriented text over bytes: disjoint line changes
 * merge cleanly, overlapping ones come back with git-style `<<<<<<<`
 * markers. Byte-identical with the Python `kvgit.merges` implementation
 * by construction — same opcodes (`SequenceMatcher`, autojunk off),
 * same `splitlines(keepends=True)` semantics, same grouping rules.
 */
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
const textDecoder = new TextDecoder('utf-8', { fatal: true })

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
 * Split one side into lines; a removed side is the empty list. The
 * markers then show one populated side against nothing, which reads
 * correctly.
 */
function decode(data: Uint8Array | null): string[] {
  if (data === null) return []
  if (data.includes(0)) throw new CantMark('NUL byte: not text')
  let text: string
  try {
    text = textDecoder.decode(data)
  } catch {
    throw new CantMark('not UTF-8: undecodable bytes')
  }
  return splitLinesKeepEnds(text)
}

interface Change {
  /** Base interval replaced... */
  start: number
  end: number
  lines: string[]
}

/** Non-equal opcode intervals of `side` against `base`. */
function changes(base: string[], side: string[]): Change[] {
  const matcher = new SequenceMatcher<string>(null, base, side, false)
  const out: Change[] = []
  for (const op of matcher.getOpcodes()) {
    if (op.tag === 'equal') continue
    out.push({ start: op.aStart, end: op.aEnd, lines: side.slice(op.bStart, op.bEnd) })
  }
  return out
}

/**
 * Ensure every non-final line ends with a line break. A line without
 * a trailing break glued onto following output would corrupt both;
 * clean regions stay byte-exact (a final line keeps its missing
 * break, and lines ending in a non-LF split boundary such as lone
 * `\r` are already terminated, so no `\n` is added), markers pay the
 * newline tax.
 */
function terminate(lines: string[]): string[] {
  return lines.map((ln, i) =>
    i + 1 >= lines.length || LINE_BOUNDARIES.has(ln.slice(-1)) ? ln : `${ln}\n`,
  )
}

interface Event {
  side: 'ours' | 'theirs'
  start: number
  end: number
  lines: string[]
}

/**
 * One side's full content over `base[start:end]`: base lines outside
 * the side's own hunks, replacement lines inside. Same-side events
 * come from one SequenceMatcher pass, so they are disjoint and the
 * replay is well-defined.
 */
function replay(base: string[], start: number, end: number, events: Event[]): string[] {
  const out: string[] = []
  let pos = start
  const ordered = [...events].sort((x, y) => x.start - y.start || x.end - y.end)
  for (const ev of ordered) {
    out.push(...base.slice(pos, ev.start))
    out.push(...ev.lines)
    pos = Math.max(pos, ev.end)
  }
  out.push(...base.slice(pos, end))
  return out
}

/**
 * Three-way line merge. Returns the merged lines plus whether the
 * merge introduced conflict hunks (as opposed to carrying
 * marker-like content through untouched).
 */
export function mergeLines(
  base: string[],
  ours: string[],
  theirs: string[],
  oursLabel: string,
  theirsLabel: string,
  oursDeleted = false,
  theirsDeleted = false,
): { lines: string[]; conflicted: boolean } {
  if (arraysEqual(ours, theirs)) return { lines: [...ours], conflicted: false }

  // Whole-file deletion on one side against any change on the other is
  // a modify/delete conflict with the survivor shown whole.
  if (oursDeleted && !arraysEqual(theirs, base)) {
    return {
      lines: terminate([
        `<<<<<<< ${oursLabel}\n`,
        '=======\n',
        ...theirs,
        `>>>>>>> ${theirsLabel}\n`,
      ]),
      conflicted: true,
    }
  }
  if (theirsDeleted && !arraysEqual(ours, base)) {
    return {
      lines: terminate([
        `<<<<<<< ${oursLabel}\n`,
        ...ours,
        '=======\n',
        `>>>>>>> ${theirsLabel}\n`,
      ]),
      conflicted: true,
    }
  }

  const ourChanges = changes(base, ours)
  const theirChanges = changes(base, theirs)
  if (ourChanges.length === 0) return { lines: [...theirs], conflicted: false }
  if (theirChanges.length === 0) return { lines: [...ours], conflicted: false }

  // Merge overlapping change intervals into conflict groups; disjoint
  // intervals resolve independently. Intervals are half-open over base
  // positions; pure insertions are points. Join on strict overlap, plus
  // same-point inserts colliding with each other.
  const events: Event[] = [
    ...ourChanges.map((c) => ({ side: 'ours' as const, ...c })),
    ...theirChanges.map((c) => ({ side: 'theirs' as const, ...c })),
  ]
  events.sort((a, b) => a.start - b.start || a.end - b.end)

  const groups: Event[][] = []
  const groupEnds: number[] = []
  for (const ev of events) {
    const last = groups[groups.length - 1]
    const lastEnd = groupEnds[groupEnds.length - 1]
    if (
      last !== undefined &&
      lastEnd !== undefined &&
      (ev.start < lastEnd ||
        (ev.start === ev.end && last.some((g) => g.start === g.end && g.start === ev.start)))
    ) {
      last.push(ev)
      groupEnds[groupEnds.length - 1] = Math.max(lastEnd, ev.end)
    } else {
      groups.push([ev])
      groupEnds.push(ev.end)
    }
  }

  const out: string[] = []
  let conflicted = false
  let pos = 0
  for (const group of groups) {
    const start = Math.min(...group.map((ev) => ev.start))
    const end = Math.max(...group.map((ev) => ev.end))
    out.push(...base.slice(pos, start))
    const oursEv = group.filter((ev) => ev.side === 'ours')
    const theirsEv = group.filter((ev) => ev.side === 'theirs')
    if (theirsEv.length === 0) {
      out.push(...replay(base, start, end, oursEv))
    } else if (oursEv.length === 0) {
      out.push(...replay(base, start, end, theirsEv))
    } else {
      const oursFull = replay(base, start, end, oursEv)
      const theirsFull = replay(base, start, end, theirsEv)
      if (arraysEqual(oursFull, theirsFull)) {
        out.push(...oursFull)
      } else {
        conflicted = true
        out.push(
          `<<<<<<< ${oursLabel}\n`,
          ...oursFull,
          '=======\n',
          ...theirsFull,
          `>>>>>>> ${theirsLabel}\n`,
        )
      }
    }
    pos = end
  }
  out.push(...base.slice(pos))

  return { lines: terminate(out), conflicted }
}

function arraysEqual(a: string[], b: string[]): boolean {
  return a.length === b.length && a.every((v, i) => v === b[i])
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
  const total = (oldB?.length ?? 0) + (oursB?.length ?? 0) + (theirsB?.length ?? 0)
  if (total > MAX_MARK_BYTES) {
    throw new CantMark(`inputs total ${total} bytes over cap ${MAX_MARK_BYTES}`)
  }
  const { lines, conflicted } = mergeLines(
    decode(oldB),
    decode(oursB),
    decode(theirsB),
    oursLabel,
    theirsLabel,
    oursB === null && oldB !== null,
    theirsB === null && oldB !== null,
  )
  return [textEncoder.encode(lines.join('')), conflicted]
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
 * Marker merge for decoded `string` values (or raw bytes), for use
 * with `Staged`. Encodes `string` sides as UTF-8, marker-merges, and
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
