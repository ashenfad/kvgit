/**
 * Storage layout and commit identity (storage v4), as Python kvgit
 * writes them.
 *
 * The single source of truth for the reserved keys, how blobs and
 * commits are named, and how records are encoded. `VersionedKV` writes
 * commits through it and the sync layer moves them through it, so a
 * layout change breaks loudly in one place.
 *
 *   `__kvgit_version__`              — storage version sentinel (4)
 *   `__branch_head__<branch>`        — current HEAD commit hash
 *   `__branch_head_prev__<branch>`   — previous HEAD (recovery backup)
 *   `__commit_root__<commit>`        — keyset HAMT root hash
 *   `__parent_commit__<commit>`      — list of parent commit hashes
 *   `__parent_gens__<commit>`        — each parent's generation
 *   `__commit_time__<commit>`        — wall time, in seconds (a float)
 *   `__info__<commit>`               — optional caller-supplied info
 *   `__tag_info__<tag>`              — a tag's creation time and info
 *   `__gc_lease__`                   — the lease a sweep runs under
 *   `__inflight__<commit>`           — a written, not yet published commit
 *   `kvgit:keyset:<node_hash>`       — HAMT node bytes (via Keyset)
 *   `kvgit:blob:<sha256>`            — blob bytes, keyed by content
 *
 * Everything below a commit is keyed by content: a blob by the SHA-256
 * of its bytes, a HAMT node by the entries it holds, and the commit hash
 * is computed last, over the parents, the keyset root, the time and the
 * info. Equal bytes are stored once, and two sides of a merge agree
 * about a key exactly when they point at the same blob.
 *
 * Records are encoded as Python's `json.dumps` writes them
 * (`../pyjson`), so a store either language writes reads and verifies
 * the same in the other.
 */

import { core } from '../core'
import { StorageVersionError } from '../errors'
import { PyFloat, pyDumps, pyDumpsBytes, pyLoadsBytes } from '../pyjson'
import type { KVStore } from '../types'

export const STORAGE_VERSION = 4
export const STORAGE_VERSION_KEY = '__kvgit_version__'

export const BRANCH_HEAD = (branch: string): string => `__branch_head__${branch}`
export const BRANCH_HEAD_PREV = (branch: string): string => `__branch_head_prev__${branch}`
export const COMMIT_ROOT = (commit: string): string => `__commit_root__${commit}`
export const PARENT_COMMIT = (commit: string): string => `__parent_commit__${commit}`
export const PARENT_GENS = (commit: string): string => `__parent_gens__${commit}`
export const COMMIT_TIME = (commit: string): string => `__commit_time__${commit}`
export const INFO_KEY = (commit: string): string => `__info__${commit}`
export const TAG_INFO_KEY = (tag: string): string => `__tag_info__${tag}`
export const BRANCH_HEAD_PREFIX = '__branch_head__'
export const BLOB_PREFIX = 'kvgit:blob:'

/** The store-wide lease every sweep runs under:
 *  `{"owner": <opaque id>, "expires": <seconds>}`. Absent, unreadable
 *  or expired all mean "no live lease". */
export const GC_LEASE_KEY = '__gc_lease__'

/** Marker for a commit whose batch has landed but whose head has not:
 *  its value is the time (seconds) the protection lapses. */
export const IN_FLIGHT_KEY = (commit: string): string => `__inflight__${commit}`
export const IN_FLIGHT_PREFIX = '__inflight__'

/**
 * The reserved branch-name prefix a tag's commit pointer lives under: a
 * tag is `__branch_head__refs/tags/<name>`, a branch head in every
 * respect except that the branch API hides it. Reachability is decided
 * by walking branch heads, so a tagged commit stays alive under any
 * kvgit, including one that knows nothing of tags.
 */
export const TAG_BRANCH_PREFIX = 'refs/tags/'

/** Length of a commit hash in hex chars (sha-256 truncated to 40). */
export const COMMIT_HASH_LEN = 40

/** Hash of every branch's initial empty commit. Fixed rather than
 *  derived, so any two branches share it as an ancestor whichever
 *  kvgit minted them. */
export const ROOT_COMMIT = '821bf06b4dcb406ea508a4a992eadc22f29850cd'

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/** A record's bytes: compact JSON, as Python's `json.dumps` writes it. */
export function dumps(value: unknown): Uint8Array {
  return pyDumpsBytes(value)
}

const _decoder = new TextDecoder()

/** A record's value, as plain JSON (floats are numbers). */
export function loads(raw: Uint8Array): unknown {
  return JSON.parse(_decoder.decode(raw))
}

/** {@link loads}, or `null` for bytes that don't parse. */
export function safeLoads(raw: Uint8Array): unknown {
  try {
    return loads(raw)
  } catch {
    return null
  }
}

/** A record's value with floats kept as `PyFloat`, for re-encoding to
 *  exactly the bytes it was read from (a commit's time and info, when a
 *  commit hash is checked). */
export function loadsExact(raw: Uint8Array): unknown {
  return pyLoadsBytes(raw)
}

/** A record's value with integers kept exact (`bigint` past the safe
 *  range) and floats as plain numbers: commit info, as callers read it. */
export function loadsInfo(raw: Uint8Array): unknown {
  return unwrapFloats(pyLoadsBytes(raw))
}

function unwrapFloats(value: unknown): unknown {
  if (value instanceof PyFloat) return value.value
  if (Array.isArray(value)) return value.map(unwrapFloats)
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value)) {
      Object.defineProperty(out, k, {
        value: unwrapFloats(v),
        enumerable: true,
        writable: true,
        configurable: true,
      })
    }
    return out
  }
  return value
}

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

export async function sha256Hex(data: Uint8Array): Promise<string> {
  const buf = await globalThis.crypto.subtle.digest('SHA-256', data as Uint8Array<ArrayBuffer>)
  const bytes = new Uint8Array(buf)
  let hex = ''
  for (let i = 0; i < bytes.length; i++) {
    const b = bytes[i] as number
    hex += (b < 16 ? '0' : '') + b.toString(16)
  }
  return hex
}

/** A blob's storage key: the SHA-256 of its bytes. */
export async function blobKey(value: Uint8Array): Promise<string> {
  return BLOB_PREFIX + (await sha256Hex(value))
}

/**
 * The 40-hex hash naming a commit: SHA-256 over
 * `["kvgit/4", parents, root, created, info]`, encoded as Python's
 * `json.dumps(sort_keys=True)` writes it.
 *
 * `created` is the commit time in seconds, always written as a float. A
 * caller re-checking a stored commit passes the time and info read with
 * {@link loadsExact}, so they re-encode to what was hashed.
 */
export async function commitHash(
  parents: readonly string[],
  root: string,
  created: number | PyFloat,
  info: unknown,
): Promise<string> {
  const time = created instanceof PyFloat ? created.value : created
  const infoText = info === null || info === undefined ? undefined : pyDumps(info)
  return (await core()).commitHash(JSON.stringify(parents), root, time, infoText)
}

/** A commit's generation is one more than its highest parent's, and 0
 *  for a commit with none. */
export function generationOf(parentGens: readonly number[]): number {
  return parentGens.length === 0 ? 0 : 1 + Math.max(...parentGens)
}

// ---------------------------------------------------------------------------
// Heads and the version stamp
// ---------------------------------------------------------------------------

/**
 * Read a branch's HEAD commit hash, or null if absent/malformed.
 *
 * Plain read — no prev-HEAD recovery (that's `VersionedKV.open`'s
 * job). For the sync layer, a corrupt head reads as "no head" and the
 * orchestration surfaces it rather than repairing it.
 */
export async function readBranchHead(store: KVStore, branch: string): Promise<string | null> {
  const raw = await store.get(BRANCH_HEAD(branch))
  if (raw === null) return null
  const parsed = safeLoads(raw)
  return typeof parsed === 'string' ? parsed : null
}
