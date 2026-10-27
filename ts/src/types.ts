/**
 * @agex-ts/kvgit public type contracts.
 *
 * Canonical source for the public types of @agex-ts/kvgit. Implementations
 * (`Repo`, `Worktree`, `Namespaced`, the backends) live in sibling files
 * and import from here.
 */

import type { PyFloat } from './pyjson'

export {
  BranchExistsError,
  BranchMovedError,
  ConcurrencyError,
  CorruptHeadError,
  GcBusy,
  IntegrityError,
  KvgitError,
  MergeConflict,
  StorageVersionError,
  TagExistsError,
  UnknownBranchError,
  UnknownCommitError,
  UnknownTagError,
} from './errors'

// ---------------------------------------------------------------------------
// KV store
// ---------------------------------------------------------------------------

/**
 * Bytes-only key-value store. Serialization happens at higher layers
 * (a repository's codec).
 *
 * All operations are async to accommodate IndexedDB / OPFS / remote
 * backends. In-process backends (`Memory`) wrap sync operations in
 * resolved promises so user code is uniform across backends.
 *
 * `cas` is correctness-critical — it underpins every branch HEAD
 * advancement. Backends must guarantee atomicity (read + conditional
 * write within a single transaction or equivalent).
 */
export interface KVStore {
  get(key: string): Promise<Uint8Array | null>
  set(key: string, value: Uint8Array): Promise<void>
  remove(key: string): Promise<void>
  has(key: string): Promise<boolean>

  /** Returns only keys that exist; missing keys are silently omitted. */
  getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>>
  setMany(items: Iterable<readonly [string, Uint8Array]>): Promise<void>
  removeMany(keys: Iterable<string>): Promise<void>

  /**
   * Atomic compare-and-swap.
   *
   * Sets `value` only if the current value equals `expected` (compared
   * bytewise). `expected = null` means "key must not exist".
   *
   * Returns `true` if the swap succeeded.
   */
  cas(key: string, value: Uint8Array, expected: Uint8Array | null): Promise<boolean>
  /**
   * Apply `writes` and `removes` atomically, if and only if every key in
   * `expected` currently holds its value (`null`: the key must be
   * absent). Either every write and removal lands or none does, and no
   * other writer's change lands between the check and the write. A key
   * may be both expected and written; a key both written and removed is
   * a caller error. Resolves `true` when the batch landed, `false` when
   * an expectation failed (and nothing changed).
   */
  casMany(
    expected: ReadonlyMap<string, Uint8Array | null>,
    writes: ReadonlyMap<string, Uint8Array>,
    removes?: Iterable<string>,
  ): Promise<boolean>

  /**
   * Iterate keys, optionally restricted to a string prefix.
   *
   * Backends with native prefix support (IDB key ranges, SQLite
   * range scans) implement this in O(matching keys). Memory falls
   * back to filter-after-iterate, which is still O(N) but correct.
   * Pass `undefined` (or no argument) to iterate all keys.
   */
  keys(prefix?: string): AsyncIterable<string>

  /** Iterate `(key, value)` pairs, optionally restricted to a prefix. */
  items(prefix?: string): AsyncIterable<readonly [string, Uint8Array]>

  clear(): Promise<void>
}

// ---------------------------------------------------------------------------
// HAMT
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Versioned
// ---------------------------------------------------------------------------

/** Key-level differences between two commits. */
export interface DiffResult {
  readonly added: ReadonlySet<string>
  readonly removed: ReadonlySet<string>
  readonly modified: ReadonlySet<string>
}

/**
 * What a commit, merge or applied change did.
 *
 * `strategy` says how it landed:
 * - `'no_op'`: nothing to do (no changes to commit, or the branch
 *   already contains what was merged or applied).
 * - `'fast_forward'`: a commit on a branch that had not moved, or a
 *   merge that moved the branch to theirs without a merge commit.
 * - `'three_way'`: a merge commit, from a merge or from a commit that
 *   found the branch had moved.
 * - `'apply'`: a change applied as a single-parent commit (`apply`,
 *   `cherryPick`, `revert`).
 */
export interface MergeResult {
  /** Whether the call landed; false when it abandoned
   *  (`onConflict: 'abandon'`). */
  readonly merged: boolean
  /** The commit the branch is at afterwards; null when abandoned. */
  readonly commit: string | null
  readonly strategy: 'no_op' | 'fast_forward' | 'three_way' | 'apply'
  /** Keys a merge rule decided: keys both sides changed that a merge
   *  function (or the `MergeChoice` it returned) resolved, and keys
   *  under a registered `MergeChoice` that either side changed. */
  readonly autoMergedKeys: readonly string[]
  /** Keys the other side changed that the merge took as they were: the
   *  other writer's changes on a lost race, the merged branch's on a
   *  merge or fast-forward, the picked change on `apply`. Empty for a
   *  commit with no other side and for `no_op`. A key both sides
   *  changed alike is in neither list, unless a registered
   *  `MergeChoice` covers it. */
  readonly carriedKeys: readonly string[]
}

/**
 * One side of a merge, named as the answer for a key. Two uses, with
 * deliberately different reach:
 *
 * - **Returned by a merge function** in place of a value: the merged
 *   value of that contested key is that side's, unchanged (no new blob
 *   is written; a side that removed the key removes it).
 * - **Registered in place of a merge function** (by key, by prefix, or
 *   as the default): a standing policy handing one side every key it
 *   covers that *either* side changed, so `OURS` also drops a key the
 *   other side added and keeps one it removed. Nothing is read.
 */
export class MergeChoice {
  static readonly OURS = new MergeChoice('ours')
  static readonly THEIRS = new MergeChoice('theirs')

  private constructor(readonly side: 'ours' | 'theirs') {}

  toString(): string {
    return `MergeChoice.${this.side.toUpperCase()}`
  }
}

/**
 * Three-way merge function over raw bytes. Any argument may be `null`
 * (key absent or removed on that side). Return the merged bytes, or a
 * {@link MergeChoice} to keep one side's value as it is.
 *
 * Throw to signal an unresolvable conflict: the merge files the key as
 * conflicted, with any other contested keys, in one `MergeConflict`.
 */
export type BytesMergeFn = (
  old: Uint8Array | null,
  ours: Uint8Array | null,
  theirs: Uint8Array | null,
) => Uint8Array | MergeChoice

/** What a merge registration holds: a function, consulted for keys both
 *  sides changed, or a {@link MergeChoice}, which gives one side every
 *  key it covers. */
export type MergePolicy = BytesMergeFn | MergeChoice

/**
 * Accept or refuse a value a merge function produced: `(key, merged)`.
 * Returning false files the key as conflicted, as if no merge function
 * had resolved it. Doesn't run for a key whose merge function answered
 * with a `MergeChoice`, since no new value was produced.
 */
export type PostCheck = (key: string, merged: Uint8Array) => boolean

/**
 * User-level merge function. Receives decoded values; the encoding
 * layer wraps these into a `BytesMergeFn` at commit time. May return a
 * {@link MergeChoice} to keep one side's value.
 */
export type MergeFn<T = unknown> = (
  old: T | null,
  ours: T | null,
  theirs: T | null,
) => T | MergeChoice

/** Optional commit-level metadata, as a commit records it. */
export type CommitInfo = Record<string, unknown>

// ---------------------------------------------------------------------------
// Sync / wire
// ---------------------------------------------------------------------------

/**
 * One kvgit commit in wire form — the unit of transfer between two
 * kvgit histories (sync remotes, bundles).
 *
 * A `WireCommit` carries what a replayer needs to reproduce the commit
 * byte for byte: its parents, its records as stored, and the values
 * whose entries differ from its first parent's. HAMT nodes never cross
 * (they are rebuilt locally), and blob keys are derived from the bytes.
 *
 * Replay must reproduce `hash`: rebuilding the keyset from the first
 * parent's and recomputing the commit hash is the sync layer's
 * integrity check.
 */
export interface WireCommit {
  /** kvgit commit hash (40-hex). */
  readonly hash: string
  /** Parent hashes, order-significant (order participates in `hash`).
   *  For three-way merges, `parents[0]` is the head the merge landed
   *  on — the CAS winner for `commit()`, our own head for
   *  `mergeHeads()` — and `updates`/`removals` are relative to it.
   *  See `VersionedBase`. */
  readonly parents: readonly string[]
  /** Each parent's generation, as recorded (`__parent_gens__`); null
   *  when the commit records none (the root commit). */
  readonly parentGens: readonly number[] | null
  /** Commit time in seconds (`__commit_time__`), as recorded: a
   *  `PyFloat`, so it re-encodes to the bytes that were hashed. */
  readonly time: PyFloat
  /** Caller-supplied info (`__info__`) as recorded, floats as
   *  `PyFloat`, or null. */
  readonly info: unknown
  /** Keys whose entry differs from `parents[0]`'s: key → value bytes.
   *  A merge's entries adopted from another parent are here too; their
   *  blob is the same bytes, so the same key. */
  readonly updates: ReadonlyMap<string, Uint8Array>
  /** Keys present in `parents[0]`'s keyset but absent here. */
  readonly removals: ReadonlySet<string>
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------
