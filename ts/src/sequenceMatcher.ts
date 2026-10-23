/**
 * SequenceMatcher: longest-contiguous-subsequence matching over two
 * sequences, yielding opcodes (`equal` / `replace` / `delete` /
 * `insert`) that turn one into the other.
 *
 * Vendored from the `SequenceMatcher` opcode path of qiao/difflib.js
 * (itself a port of Python's `difflib`), via `difflib-ts`, translated
 * to typed TypeScript. Only the opcode path is taken here —
 * `Differ`, `getCloseMatches`, and their `heap` / `assert` helpers are
 * omitted, so this file has no dependencies. The element index uses a
 * `Map` rather than a plain object, which additionally keeps elements
 * like `"__proto__"` or `"constructor"` behaving as ordinary values.
 *
 * Original license terms (from qiao/difflib.js README, retained per
 * its grant): PSF LICENSE AGREEMENT FOR PYTHON 2.7.2 — Copyright ©
 * 2001-2012 Python Software Foundation; All Rights Reserved. Changes
 * made to Python 2.7.2 for this file: ported to JavaScript by
 * qiao/difflib.js, re-ported to TypeScript by difflib-ts, translated
 * here to typed generics with a `Map`-based index; opcode behavior
 * verified byte-identical against CPython's
 * `SequenceMatcher(..., autojunk=False).get_opcodes()`.
 */

export type OpcodeTag = 'replace' | 'delete' | 'insert' | 'equal'

export interface Opcode {
  tag: OpcodeTag
  /** Start/end in the first sequence. */
  aStart: number
  aEnd: number
  /** Start/end in the second sequence. */
  bStart: number
  bEnd: number
}

/**
 * Indexing helper: all indices in this file are in-bounds by
 * construction (loop bounds and match arithmetic mirror the Python
 * original, where indexing cannot fail). One cast here instead of
 * `undefined` guards scattered through the algorithm.
 */
function at<T>(arr: readonly T[], i: number): T {
  return arr[i] as T
}

export class SequenceMatcher<T> {
  private a: readonly T[] = []
  private b: readonly T[] = []
  private readonly isjunk: ((elt: T) => boolean) | null
  private readonly autojunk: boolean
  private b2j = new Map<T, number[]>()
  private isbjunk: (elt: T) => boolean = () => false
  private matchingBlocks: Array<readonly [number, number, number]> | null = null
  private opcodes: Opcode[] | null = null

  constructor(
    isjunk: ((elt: T) => boolean) | null = null,
    a: readonly T[] = [],
    b: readonly T[] = [],
    autojunk = true,
  ) {
    this.isjunk = isjunk ?? null
    this.autojunk = autojunk
    this.setSeqs(a, b)
  }

  setSeqs(a: readonly T[], b: readonly T[]): void {
    this.setSeq1(a)
    this.setSeq2(b)
  }

  setSeq1(a: readonly T[]): void {
    if (a === this.a) return
    this.a = a
    this.matchingBlocks = null
    this.opcodes = null
  }

  setSeq2(b: readonly T[]): void {
    if (b === this.b) return
    this.b = b
    this.matchingBlocks = null
    this.opcodes = null
    this.chainB()
  }

  private chainB(): void {
    const b = this.b
    this.b2j = new Map<T, number[]>()
    const b2j = this.b2j
    for (const [i, elt] of b.entries()) {
      let indices = b2j.get(elt)
      if (indices === undefined) {
        indices = []
        b2j.set(elt, indices)
      }
      indices.push(i)
    }
    const { isjunk } = this
    const junk = new Set<T>()
    if (isjunk) {
      for (const elt of b2j.keys()) {
        if (isjunk(elt)) {
          junk.add(elt)
          b2j.delete(elt)
        }
      }
    }
    const popular = new Set<T>()
    const n = b.length
    if (this.autojunk && n >= 200) {
      const ntest = Math.floor(n / 100) + 1
      for (const [elt, idxs] of b2j) {
        if (idxs.length > ntest) {
          popular.add(elt)
          b2j.delete(elt)
        }
      }
    }
    this.isbjunk = (elt) => junk.has(elt) || popular.has(elt)
  }

  findLongestMatch(
    alo: number,
    ahi: number,
    blo: number,
    bhi: number,
  ): readonly [number, number, number] {
    const { a, b, b2j, isbjunk } = this
    let besti = alo
    let bestj = blo
    let bestsize = 0
    let j2len = new Map<number, number>()
    for (let i = alo; i < ahi; i++) {
      const newj2len = new Map<number, number>()
      const jarray = b2j.get(at(a, i)) ?? []
      for (const j of jarray) {
        if (j < blo) continue
        if (j >= bhi) break
        const k = (j2len.get(j - 1) ?? 0) + 1
        newj2len.set(j, k)
        if (k > bestsize) {
          besti = i - k + 1
          bestj = j - k + 1
          bestsize = k
        }
      }
      j2len = newj2len
    }
    while (
      besti > alo &&
      bestj > blo &&
      !isbjunk(at(b, bestj - 1)) &&
      at(a, besti - 1) === at(b, bestj - 1)
    ) {
      besti--
      bestj--
      bestsize++
    }
    while (
      besti + bestsize < ahi &&
      bestj + bestsize < bhi &&
      !isbjunk(at(b, bestj + bestsize)) &&
      at(a, besti + bestsize) === at(b, bestj + bestsize)
    ) {
      bestsize++
    }
    while (
      besti > alo &&
      bestj > blo &&
      isbjunk(at(b, bestj - 1)) &&
      at(a, besti - 1) === at(b, bestj - 1)
    ) {
      besti--
      bestj--
      bestsize++
    }
    while (
      besti + bestsize < ahi &&
      bestj + bestsize < bhi &&
      isbjunk(at(b, bestj + bestsize)) &&
      at(a, besti + bestsize) === at(b, bestj + bestsize)
    ) {
      bestsize++
    }
    return [besti, bestj, bestsize]
  }

  getMatchingBlocks(): Array<readonly [number, number, number]> {
    if (this.matchingBlocks) return this.matchingBlocks
    const la = this.a.length
    const lb = this.b.length
    const queue: Array<[number, number, number, number]> = [[0, la, 0, lb]]
    const matchingBlocks: Array<readonly [number, number, number]> = []
    while (queue.length > 0) {
      const [alo, ahi, blo, bhi] = queue.pop() as [number, number, number, number]
      const [i, j, k] = this.findLongestMatch(alo, ahi, blo, bhi)
      if (k > 0) {
        matchingBlocks.push([i, j, k])
        if (alo < i && blo < j) queue.push([alo, i, blo, j])
        if (i + k < ahi && j + k < bhi) queue.push([i + k, ahi, j + k, bhi])
      }
    }
    matchingBlocks.sort((x, y) => x[0] - y[0] || x[1] - y[1] || x[2] - y[2])
    const nonAdjacent: Array<readonly [number, number, number]> = []
    let [i1, j1, k1] = [0, 0, 0]
    for (const [i2, j2, k2] of matchingBlocks) {
      if (i1 + k1 === i2 && j1 + k1 === j2) {
        k1 += k2
      } else {
        if (k1 > 0) nonAdjacent.push([i1, j1, k1])
        i1 = i2
        j1 = j2
        k1 = k2
      }
    }
    if (k1 > 0) nonAdjacent.push([i1, j1, k1])
    nonAdjacent.push([la, lb, 0])
    this.matchingBlocks = nonAdjacent
    return nonAdjacent
  }

  getOpcodes(): Opcode[] {
    if (this.opcodes) return this.opcodes
    let i = 0
    let j = 0
    const answer: Opcode[] = []
    for (const [ai, bj, size] of this.getMatchingBlocks()) {
      let tag: OpcodeTag | '' = ''
      if (i < ai && j < bj) tag = 'replace'
      else if (i < ai) tag = 'delete'
      else if (j < bj) tag = 'insert'
      if (tag !== '') {
        answer.push({ tag, aStart: i, aEnd: ai, bStart: j, bEnd: bj })
      }
      i = ai + size
      j = bj + size
      if (size > 0) {
        answer.push({ tag: 'equal', aStart: ai, aEnd: i, bStart: bj, bEnd: j })
      }
    }
    this.opcodes = answer
    return answer
  }
}
