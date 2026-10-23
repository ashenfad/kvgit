/**
 * JSON as Python's `json.dumps` writes it, and a parse that keeps what
 * Python would keep.
 *
 * kvgit's storage format is defined by the bytes Python kvgit writes:
 * HAMT nodes and commit hashes come from `json.dumps(sort_keys=True,
 * separators=(",", ":"))`, records from the same without `sort_keys`.
 * `JSON.stringify` differs from it in three ways, and each would change
 * a hash:
 *
 * - **Escaping.** Python escapes every character outside printable
 *   ASCII (`ensure_ascii`), DEL included, as `\uXXXX` with lowercase
 *   hex, a character outside the basic plane as its surrogate pair.
 * - **Key order.** `sort_keys` orders by code point; JS's default sort
 *   orders by UTF-16 code unit, which disagrees once a key holds a
 *   character outside the basic plane.
 * - **Floats.** Python writes a float with `repr`: `1.0` rather than
 *   `1`, and `1e+16` where JS writes `10000000000000000`.
 *
 * JS has one number type, so a value marks a float with {@link PyFloat}
 * when it must be written as one (a commit time); a plain number is
 * written as an integer when it is one and as a float otherwise.
 * {@link pyLoads} parses floats into `PyFloat`, so text Python wrote
 * re-encodes to the same bytes.
 */

/** A number Python holds as a float, written as `repr` writes it. */
export class PyFloat {
  constructor(readonly value: number) {}

  valueOf(): number {
    return this.value
  }
}

export interface PyDumpsOptions {
  /** Order object keys by code point, as `sort_keys=True` does. */
  readonly sortKeys?: boolean
}

/** `json.dumps(value, separators=(",", ":"))`, with `sort_keys` when
 *  asked: compact, `ensure_ascii`, Python's float `repr`. */
export function pyDumps(value: unknown, opts: PyDumpsOptions = {}): string {
  return encode(value, opts.sortKeys === true)
}

const _encoder = new TextEncoder()

/** {@link pyDumps} as UTF-8 bytes (all ASCII, by construction). */
export function pyDumpsBytes(value: unknown, opts: PyDumpsOptions = {}): Uint8Array {
  return _encoder.encode(pyDumps(value, opts))
}

function encode(value: unknown, sortKeys: boolean): string {
  if (value === null || value === undefined) return 'null'
  if (value === true) return 'true'
  if (value === false) return 'false'
  if (typeof value === 'string') return pyString(value)
  if (typeof value === 'bigint') return value.toString()
  if (value instanceof PyFloat) return pyFloat(value.value)
  if (typeof value === 'number') {
    return Number.isInteger(value) && !Object.is(value, -0) ? String(value) : pyFloat(value)
  }
  if (Array.isArray(value)) return `[${value.map((v) => encode(v, sortKeys)).join(',')}]`
  if (value instanceof Map) return encodeEntries([...value.entries()], sortKeys)
  if (typeof value === 'object') return encodeEntries(Object.entries(value), sortKeys)
  throw new TypeError(`Object of type ${typeof value} is not JSON serializable`)
}

function encodeEntries(entries: Array<[unknown, unknown]>, sortKeys: boolean): string {
  const pairs = entries.map(([k, v]) => [String(k), v] as const)
  if (sortKeys) pairs.sort(([a], [b]) => compareCodePoints(a, b))
  return `{${pairs.map(([k, v]) => `${pyString(k)}:${encode(v, sortKeys)}`).join(',')}}`
}

/** Order two strings by code point, as Python compares `str`. */
export function compareCodePoints(a: string, b: string): number {
  const ia = a[Symbol.iterator]()
  const ib = b[Symbol.iterator]()
  for (;;) {
    const x = ia.next()
    const y = ib.next()
    if (x.done === true) return y.done === true ? 0 : -1
    if (y.done === true) return 1
    const cx = (x.value as string).codePointAt(0) as number
    const cy = (y.value as string).codePointAt(0) as number
    if (cx !== cy) return cx < cy ? -1 : 1
  }
}

const SHORT_ESCAPES: Readonly<Record<string, string>> = {
  '"': '\\"',
  '\\': '\\\\',
  '\n': '\\n',
  '\r': '\\r',
  '\t': '\\t',
  '\b': '\\b',
  '\f': '\\f',
}

/** A JSON string as Python writes it with `ensure_ascii`: printable
 *  ASCII as itself, everything else escaped, code unit by code unit. */
function pyString(s: string): string {
  let out = '"'
  for (let i = 0; i < s.length; i++) {
    const ch = s[i] as string
    const short = SHORT_ESCAPES[ch]
    if (short !== undefined) {
      out += short
      continue
    }
    const code = s.charCodeAt(i)
    out += code >= 0x20 && code <= 0x7e ? ch : `\\u${code.toString(16).padStart(4, '0')}`
  }
  return `${out}"`
}

/**
 * A float as Python's `repr` writes it: the shortest digits that read
 * back to the same double (as JS finds them), in fixed notation with at
 * least one digit after the point, or in exponent notation when the
 * decimal exponent is below -4 or at least 16, with a sign and at
 * least two exponent digits.
 */
export function pyFloat(x: number): string {
  if (Number.isNaN(x)) return 'NaN'
  if (x === Number.POSITIVE_INFINITY) return 'Infinity'
  if (x === Number.NEGATIVE_INFINITY) return '-Infinity'
  if (x === 0) return Object.is(x, -0) ? '-0.0' : '0.0'
  const sign = x < 0 ? '-' : ''
  // toExponential() gives the shortest round-tripping digits: d.ddde±n
  const [mantissa, expText] = Math.abs(x).toExponential().split('e') as [string, string]
  const digits = mantissa.replace('.', '')
  const exp = Number.parseInt(expText, 10)
  if (exp < -4 || exp >= 16) {
    const frac = digits.slice(1)
    const expAbs = String(Math.abs(exp)).padStart(2, '0')
    return `${sign}${digits[0]}${frac ? `.${frac}` : ''}e${exp < 0 ? '-' : '+'}${expAbs}`
  }
  if (exp < 0) return `${sign}0.${'0'.repeat(-exp - 1)}${digits}`
  const whole = digits.slice(0, exp + 1).padEnd(exp + 1, '0')
  const frac = digits.slice(exp + 1)
  return `${sign}${whole}.${frac || '0'}`
}

/**
 * Parse JSON text, keeping what Python's `json.loads` would keep: a
 * number written with a fraction or an exponent is a {@link PyFloat},
 * an integer past the safe range a `bigint`, `NaN` and `Infinity` are
 * accepted. Re-encoded with {@link pyDumps}, the value gives the bytes
 * Python would.
 */
export function pyLoads(text: string): unknown {
  const parser = new Parser(text)
  const value = parser.value()
  parser.end()
  return value
}

const _decoder = new TextDecoder()

/** {@link pyLoads} over UTF-8 bytes. */
export function pyLoadsBytes(raw: Uint8Array): unknown {
  return pyLoads(_decoder.decode(raw))
}

class Parser {
  #i = 0

  constructor(private readonly text: string) {}

  value(): unknown {
    this.#space()
    const ch = this.text[this.#i]
    if (ch === '{') return this.#object()
    if (ch === '[') return this.#array()
    if (ch === '"') return this.#string()
    if (this.#word('true')) return true
    if (this.#word('false')) return false
    if (this.#word('null')) return null
    if (this.#word('NaN')) return new PyFloat(Number.NaN)
    if (this.#word('Infinity')) return new PyFloat(Number.POSITIVE_INFINITY)
    if (this.#word('-Infinity')) return new PyFloat(Number.NEGATIVE_INFINITY)
    return this.#number()
  }

  end(): void {
    this.#space()
    if (this.#i !== this.text.length) this.#fail('extra data')
  }

  #space(): void {
    while (/[ \t\n\r]/.test(this.text[this.#i] ?? '')) this.#i++
  }

  #word(word: string): boolean {
    if (!this.text.startsWith(word, this.#i)) return false
    this.#i += word.length
    return true
  }

  #fail(what: string): never {
    throw new SyntaxError(`pyLoads: ${what} at position ${this.#i}`)
  }

  #object(): Record<string, unknown> {
    this.#i++
    const out: Record<string, unknown> = {}
    this.#space()
    if (this.text[this.#i] === '}') {
      this.#i++
      return out
    }
    for (;;) {
      this.#space()
      if (this.text[this.#i] !== '"') this.#fail('expected a key')
      const key = this.#string()
      this.#space()
      if (this.text[this.#i] !== ':') this.#fail("expected ':'")
      this.#i++
      Object.defineProperty(out, key, {
        value: this.value(),
        enumerable: true,
        writable: true,
        configurable: true,
      })
      this.#space()
      const ch = this.text[this.#i++]
      if (ch === '}') return out
      if (ch !== ',') this.#fail("expected ',' or '}'")
    }
  }

  #array(): unknown[] {
    this.#i++
    const out: unknown[] = []
    this.#space()
    if (this.text[this.#i] === ']') {
      this.#i++
      return out
    }
    for (;;) {
      out.push(this.value())
      this.#space()
      const ch = this.text[this.#i++]
      if (ch === ']') return out
      if (ch !== ',') this.#fail("expected ',' or ']'")
    }
  }

  #string(): string {
    const start = this.#i
    this.#i++
    for (;;) {
      const ch = this.text[this.#i]
      if (ch === undefined) this.#fail('unterminated string')
      if (ch === '\\') this.#i += 2
      else if (ch === '"') break
      else this.#i++
    }
    this.#i++
    return JSON.parse(this.text.slice(start, this.#i)) as string
  }

  #number(): number | bigint | PyFloat {
    const m = /^-?(0|[1-9]\d*)(\.\d+)?([eE][-+]?\d+)?/.exec(this.text.slice(this.#i))
    if (m === null) this.#fail('expected a value')
    this.#i += m[0].length
    if (m[2] !== undefined || m[3] !== undefined) return new PyFloat(Number(m[0]))
    const n = Number(m[0])
    return Number.isSafeInteger(n) ? n : BigInt(m[0])
  }
}
