/**
 * How a repository turns values into stored bytes and back.
 *
 * A codec is a setting of the `Repo`, not of the store: nothing records
 * which one wrote a value, so every reader of a store has to use the
 * codec its writers did. Three are named: `'bytes'` (values are bytes,
 * stored as they are), `'json'`, and `'cbor'`, the polymorphic one.
 */

import { Encoder } from 'cbor-x'

/** Values to bytes and back. `decode(encode(v))` must give back `v`. */
export interface Codec<V> {
  encode(value: V): Uint8Array
  decode(bytes: Uint8Array): V
}

/** Values are bytes, stored as they are. */
export const bytesCodec: Codec<Uint8Array> = {
  encode: (value) => value,
  decode: (bytes) => bytes,
}

const _encoder = new TextEncoder()
const _decoder = new TextDecoder()

/** Values are anything `JSON.stringify` writes and `JSON.parse` reads
 *  back the same: plain objects, arrays, strings, finite numbers,
 *  booleans and null. */
export const jsonCodec: Codec<unknown> = {
  encode: (value) => {
    const text = JSON.stringify(value)
    if (text === undefined) throw new TypeError(`jsonCodec cannot encode ${typeof value}`)
    return _encoder.encode(text)
  },
  decode: (bytes) => JSON.parse(_decoder.decode(bytes)) as unknown,
}

const _cbor = new Encoder({
  // Plain CBOR only: no cbor-x record extension, so any CBOR reader
  // (Python's cbor2, say) reads what this writes.
  useRecords: false,
  // A plain object is a CBOR map with string keys; a JS Map is tagged
  // (259) so it comes back a Map, with keys of any type.
  mapsAsObjects: true,
  // A Uint8Array is a CBOR byte string, the type every reader has.
  tagUint8Array: false,
})

/**
 * Values are anything CBOR (RFC 8949) carries, round-tripping in JS
 * with their types: objects, arrays, strings, numbers, booleans, null
 * and undefined, `Uint8Array` (a byte string, no base64), bigint, `Map`
 * (tag 259, any keys), `Set` (tag 258), `Date` (tag 1) and the other
 * typed arrays (RFC 8746 tags).
 *
 * The polymorphic codec, as `pickle` is in Python kvgit, but a standard
 * one: Python reads it with `cbor2` (a `Map` arrives as a tagged map,
 * typed arrays other than bytes as tagged values). An integer Python
 * writes past 32 bits comes back a bigint.
 */
export const cborCodec: Codec<unknown> = {
  // cbor-x returns a view into its scratch buffer (a Node `Buffer` where
  // there is one), several times the value's size; copy it out so a
  // backend that keeps or stores the array doesn't keep the scratch too.
  encode: (value) => new Uint8Array(_cbor.encode(value)),
  decode: (bytes) => plainBytes(_cbor.decode(bytes)),
}

/**
 * Give every byte string and typed array its own memory. cbor-x decodes
 * them as views into the encoded input, which a backend may hand out
 * as the very bytes it holds; a caller changing a value it read must
 * not change a stored commit. Node's `Buffer` comes back a plain
 * `Uint8Array`, so a value reads the same in every runtime.
 */
function plainBytes(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value
  if (isBuffer(value)) return new Uint8Array(value)
  if (ArrayBuffer.isView(value)) {
    return value instanceof DataView
      ? new DataView(value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength))
      : (value as unknown as { slice(): unknown }).slice()
  }
  if (value instanceof Date) return value
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = plainBytes(value[i])
    return value
  }
  if (value instanceof Map) {
    const out = new Map()
    for (const [k, v] of value) out.set(plainBytes(k), plainBytes(v))
    return out
  }
  if (value instanceof Set) return new Set([...value].map(plainBytes))
  const record = value as Record<string, unknown>
  for (const k of Object.keys(record)) record[k] = plainBytes(record[k])
  return value
}

const BufferCtor = (globalThis as { Buffer?: { isBuffer(v: unknown): boolean } }).Buffer

function isBuffer(value: object): value is Uint8Array {
  return BufferCtor?.isBuffer(value) === true
}

/** The codecs a `Repo` can name instead of passing one. */
export type CodecName = 'bytes' | 'json' | 'cbor'

/** The codec a name, or a codec, stands for. */
export function resolveCodec<V>(spec: Codec<V> | CodecName): Codec<V> {
  if (typeof spec !== 'string') return spec
  const named: Record<CodecName, Codec<unknown>> = {
    bytes: bytesCodec as Codec<unknown>,
    json: jsonCodec,
    cbor: cborCodec,
  }
  const codec = named[spec]
  if (codec === undefined) throw new RangeError(`Unknown codec '${String(spec)}'`)
  return codec as Codec<V>
}
