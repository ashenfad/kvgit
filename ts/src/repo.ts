/**
 * Repo: a kvgit repository over one `KVStore`, as Python kvgit 0.4
 * shapes it.
 *
 * The repository owns the store and the settings every worktree
 * shares (the codec, default merge rules, HEAD recovery, the clock);
 * none of them is written to the store. From it come worktrees (a
 * branch checked out for work), snapshots (a read-only view at one
 * commit), commit records, history, and the branch and tag
 * collections.
 */

import { type Codec, type CodecName, bytesCodec, resolveCodec } from './codec'
import { core } from './core'
import { BranchExistsError, UnknownCommitError, UnknownTagError } from './errors'
import { NodeCache } from './hamt'
import { Keyset } from './keyset'
import type { CommitInfo, DiffResult, KVStore } from './types'
import { walkHistory } from './versioned/helpers'
import {
  type CorruptHeadRecoverer,
  type GcOptions,
  type LazyKeysetOptions,
  VersionedKV,
  createBranchHead,
  deleteBranchHead,
  gcStore,
  headOf,
  loadParents,
  mergeBase,
  repairHead,
} from './versioned/kv'
import {
  COMMIT_ROOT,
  COMMIT_TIME,
  INFO_KEY,
  PARENT_COMMIT,
  ROOT_COMMIT,
  checkStorageVersion,
  loads,
  loadsInfo,
  safeLoads,
} from './versioned/layout'
import {
  type TagInfo,
  createTag,
  deleteTag,
  listBranches,
  listTags,
  rejectReservedBranch,
  resolveTag,
  tagInfo,
} from './versioned/refs'
import { type MergeRule, Worktree } from './worktree'

/** One commit: its parents, when it was made, what the caller said
 *  about it, and its keyset root (equal roots mean equal contents). */
export interface Commit {
  readonly hash: string
  readonly parents: readonly string[]
  /** Seconds; null if the record is missing. */
  readonly time: number | null
  readonly info: CommitInfo | null
  readonly root: string
}

/** Exactly one of a commit, a branch or a tag. */
export type Ref = { commit: string } | { branch: string } | { tag: string }

export interface RepoOptions<V> {
  /** How values become stored bytes: a codec, or the name of one
   *  (`'bytes'`, `'json'`, `'cbor'`). Default `'bytes'`: values are
   *  `Uint8Array`, stored as they are, so any reader can take them. */
  codec?: Codec<V> | CodecName
  /** Merge rules by key that every worktree inherits: a merge
   *  function over decoded values, or a `MergeChoice`. */
  mergeFns?: ReadonlyMap<string, MergeRule<V>>
  /** Merge rules by key prefix that every worktree inherits. */
  mergePrefixes?: ReadonlyMap<string, MergeRule<V>>
  /** The merge rule for keys no other rule covers. */
  defaultMerge?: MergeRule<V>
  /** Last-resort recovery for a damaged HEAD with no usable backup. */
  recoverFromCorruptHead?: CorruptHeadRecoverer
  /** The clock commits and tags are stamped with, in seconds. Default:
   *  wall time. */
  now?: () => number
}

const wallClock = (): number => Date.now() / 1000

/**
 * A kvgit repository: every branch, tag and commit in one `KVStore`.
 *
 * Open one with {@link Repo.open}, which checks the store's storage
 * version (and stamps a fresh store).
 */
export class Repo<V = Uint8Array> {
  readonly store: KVStore
  readonly codec: Codec<V>
  /** Every branch, as a live collection of name → tip commit. */
  readonly branches: Branches
  /** Every tag, as a live collection of name → commit. */
  readonly tags: Tags
  /** @internal */
  readonly mergeFns: ReadonlyMap<string, MergeRule<V>>
  /** @internal */
  readonly mergePrefixes: ReadonlyMap<string, MergeRule<V>>
  /** @internal */
  readonly defaultMerge: MergeRule<V> | undefined
  /** @internal */
  readonly recover: CorruptHeadRecoverer | undefined
  /** @internal */
  readonly now: () => number
  #nodeCache: NodeCache | null = null

  private constructor(store: KVStore, opts: RepoOptions<V>) {
    this.store = store
    this.codec =
      opts.codec === undefined ? (bytesCodec as unknown as Codec<V>) : resolveCodec(opts.codec)
    this.mergeFns = new Map(opts.mergeFns ?? [])
    this.mergePrefixes = new Map(opts.mergePrefixes ?? [])
    this.defaultMerge = opts.defaultMerge
    this.recover = opts.recoverFromCorruptHead
    this.now = opts.now ?? wallClock
    this.branches = new Branches(this as Repo<unknown>)
    this.tags = new Tags(this as Repo<unknown>)
  }

  /**
   * Open the repository in `store`.
   *
   * @throws StorageVersionError if the store is stamped with a layout
   *   this code does not read.
   */
  static open(
    store: KVStore,
    opts?: RepoOptions<Uint8Array> & { codec?: 'bytes' | Codec<Uint8Array> },
  ): Promise<Repo<Uint8Array>>
  /** A named JSON or CBOR codec: values are `unknown` unless the caller
   *  names their type (`Repo.open<State>(store, { codec: 'cbor' })`). */
  static open<V = unknown>(
    store: KVStore,
    opts: RepoOptions<V> & { codec: 'json' | 'cbor' },
  ): Promise<Repo<V>>
  static open<V>(store: KVStore, opts: RepoOptions<V> & { codec: Codec<V> }): Promise<Repo<V>>
  static async open<V>(store: KVStore, opts: RepoOptions<V> = {}): Promise<Repo<V>> {
    await Promise.all([core(), checkStorageVersion(store)])
    return new Repo(store, opts)
  }

  // --- Branches ---

  /**
   * The node cache lazy worktrees share unless given their own. HAMT
   * nodes are content addressed, so sharing is safe across branches.
   */
  get nodeCache(): NodeCache {
    this.#nodeCache ??= new NodeCache()
    return this.#nodeCache
  }

  /**
   * Check out branch `name` for work.
   *
   * With `lazy`, the worktree reads its keyset on demand rather than
   * loading all of it: opening reads only the head's root, reads walk
   * the tree through a bounded node cache (the repository's, unless
   * `lazy.cache` names another), and a commit rewrites only the paths
   * it changes. For keysets of tens of thousands of keys and up. A lazy
   * worktree keeps no cache of decoded values; `keys()` reads the
   * whole tree on every call.
   *
   * @throws UnknownBranchError if there is no such branch, unless
   *   `create` is set, which creates it at the empty root commit.
   * @throws CorruptHeadError if its HEAD is damaged beyond recovery.
   */
  async worktree(
    name: string,
    opts: { create?: boolean; lazy?: boolean | LazyKeysetOptions } = {},
  ): Promise<Worktree<V>> {
    await rejectReservedBranch(name)
    if (opts.create === true && !(await this.branches.has(name))) {
      try {
        await createBranchHead(this.store, name, ROOT_COMMIT, this.now)
      } catch (e) {
        // Created by someone else in the meantime.
        if (!(e instanceof BranchExistsError)) throw e
      }
    }
    const lazy =
      opts.lazy === undefined || opts.lazy === false
        ? undefined
        : { cache: (opts.lazy === true ? undefined : opts.lazy.cache) ?? this.nodeCache }
    const engine = await VersionedKV.open(this.store, {
      branch: name,
      create: false,
      now: this.now,
      ...(this.recover !== undefined && { recoverFromCorruptHead: this.recover }),
      ...(lazy !== undefined && { lazy }),
    })
    return new Worktree(this, engine)
  }

  /**
   * Persist a recovered HEAD for a damaged branch. Reads recover in
   * memory only; this is the call that makes a recovery durable.
   *
   * @returns The commit the branch now names, or null if nothing was
   *   recoverable.
   */
  async repairHead(name: string): Promise<string | null> {
    await rejectReservedBranch(name)
    return repairHead(this.store, name, {
      ...(this.recover !== undefined && { recoverFromCorruptHead: this.recover }),
    })
  }

  // --- Commits and history ---

  /**
   * One commit's record.
   *
   * @throws UnknownCommitError if it is not in the store.
   */
  async getCommit(commit: string): Promise<Commit> {
    const keys = [COMMIT_ROOT(commit), PARENT_COMMIT(commit), COMMIT_TIME(commit), INFO_KEY(commit)]
    const found = await this.store.getMany(keys)
    const rootRaw = found.get(keys[0] as string)
    if (rootRaw === undefined) throw new UnknownCommitError(commit)
    const parentsRaw = found.get(keys[1] as string)
    const parsed = parentsRaw === undefined ? [] : safeLoads(parentsRaw)
    const parents =
      typeof parsed === 'string'
        ? [parsed]
        : Array.isArray(parsed)
          ? parsed.filter((p): p is string => typeof p === 'string')
          : []
    const timeRaw = found.get(keys[2] as string)
    const time = timeRaw === undefined ? null : safeLoads(timeRaw)
    const infoRaw = found.get(keys[3] as string)
    return {
      hash: commit,
      parents,
      time: typeof time === 'number' ? time : null,
      info: infoRaw === undefined ? null : (loadsInfo(infoRaw) as CommitInfo),
      root: loads(rootRaw) as string,
    }
  }

  /**
   * Commits reachable from a commit, branch or tag, newest first.
   * `firstParent` follows only each commit's first parent: the line of
   * history made on the branch itself, skipping what merges brought in.
   */
  async *log(
    ref: Ref,
    opts: { limit?: number; firstParent?: boolean } = {},
  ): AsyncIterable<Commit> {
    const start = await this.resolveRef(ref)
    let count = 0
    for await (const hash of walkHistory(start, (h) => loadParents(this.store, h), {
      allParents: opts.firstParent !== true,
    })) {
      if (opts.limit !== undefined && count >= opts.limit) return
      count++
      yield await this.getCommit(hash)
    }
  }

  /**
   * Keys added, removed and modified going from commit `a` to `b`. A
   * structural diff: the cost follows the size of the change, not of
   * the two keysets.
   *
   * @throws UnknownCommitError if either commit is not in the store.
   */
  async diff(a: string, b: string): Promise<DiffResult> {
    const [rootA, rootB] = await Promise.all([this.rootOf(a), this.rootOf(b)])
    const changes = await Keyset.fromRoot(this.store, rootA).diff(
      Keyset.fromRoot(this.store, rootB),
    )
    return {
      added: new Set(changes.added.keys()),
      removed: new Set(changes.removed.keys()),
      modified: new Set(changes.modified.keys()),
    }
  }

  /**
   * The lowest common ancestor of two commits, or null if they share no
   * history: the base a merge of the two would use.
   *
   * @throws UnknownCommitError if either commit is not in the store.
   */
  async mergeBase(a: string, b: string): Promise<string | null> {
    await Promise.all([this.rootOf(a), this.rootOf(b)])
    return mergeBase(this.store, a, b)
  }

  /**
   * A read-only view of the state at a commit, branch or tag, pinned to
   * the commit resolved now: a branch that moves later does not move
   * it.
   */
  async snapshot(ref: Ref): Promise<Snapshot<V>> {
    const commit = await this.resolveRef(ref)
    return new Snapshot(this, commit, await this.rootOf(commit))
  }

  // --- Maintenance ---

  /**
   * Reclaim what no branch, tag or in-flight commit reaches; return how
   * many commits were removed. Orphans younger than `minAge` seconds
   * (default an hour) are kept: how long abandoned work lingers, and any
   * value is safe beside writers. `deep` also scans for content no
   * commit references (crash leftovers). A sweep already running is
   * waited out, or with `wait: false` throws `GcBusy`; writers wait
   * while a sweep runs.
   */
  async gc(opts: Omit<GcOptions, 'now'> = {}): Promise<number> {
    return gcStore(this.store, { ...opts, now: this.now })
  }

  // --- Internal ---

  /** @internal The commit a ref names. */
  async resolveRef(ref: Ref): Promise<string> {
    const given = Object.keys(ref).filter((k) => (ref as Record<string, unknown>)[k] !== undefined)
    if (given.length !== 1) throw new TypeError('name exactly one of commit, branch or tag')
    if ('branch' in ref) return this.branches.get(ref.branch)
    if ('tag' in ref) {
      const tagged = await resolveTag(this.store, ref.tag)
      if (tagged === null) throw new UnknownTagError(ref.tag)
      await this.rootOf(tagged)
      return tagged
    }
    await this.rootOf(ref.commit)
    return ref.commit
  }

  private async rootOf(commit: string): Promise<string> {
    const raw = await this.store.get(COMMIT_ROOT(commit))
    if (raw === null) throw new UnknownCommitError(commit)
    return loads(raw) as string
  }
}

/**
 * Every branch of a repository, mapped to the commit it points at. A
 * live view: each read goes to the store, so it sees branches other
 * writers create, move and delete. Tags are not branches.
 */
export class Branches {
  constructor(private readonly repo: Repo<unknown>) {}

  /**
   * The branch's tip commit.
   *
   * @throws UnknownBranchError if there is no such branch.
   * @throws CorruptHeadError if its HEAD is damaged beyond recovery.
   */
  async get(name: string): Promise<string> {
    return headOf(this.repo.store, name, this.repo.recover)
  }

  /** Whether the branch exists. Never writes; a damaged HEAD counts. */
  async has(name: string): Promise<boolean> {
    return VersionedKV.exists(this.repo.store, name)
  }

  /** Every branch name, sorted. */
  async list(): Promise<string[]> {
    return listBranches(this.repo.store)
  }

  /**
   * Create branch `name` at commit `at` (default: the empty root
   * commit); return the commit it points at.
   *
   * @throws BranchExistsError if the name is taken.
   * @throws UnknownCommitError if `at` is not in the store.
   */
  async create(name: string, at?: string): Promise<string> {
    const target = at ?? ROOT_COMMIT
    await createBranchHead(this.repo.store, name, target, this.repo.now)
    return target
  }

  /**
   * Delete a branch. Its commits become collectable at the next `gc`;
   * worktrees still holding it read from their heads and throw
   * `UnknownBranchError` on commit.
   *
   * @throws UnknownBranchError if there is no such branch.
   */
  async delete(name: string): Promise<void> {
    await deleteBranchHead(this.repo.store, name)
  }
}

/**
 * Every tag of a repository, mapped to the commit it names. A live
 * view, like {@link Branches}. A tag never moves: creating one over a
 * taken name throws, and pointing a name elsewhere is `delete` then
 * `create`. A tag keeps its commit, and everything it descends from,
 * alive.
 */
export class Tags {
  constructor(private readonly repo: Repo<unknown>) {}

  /**
   * The commit a tag names, which may be dangling ({@link info} says).
   *
   * @throws UnknownTagError if there is no such tag.
   */
  async get(name: string): Promise<string> {
    const commit = await resolveTag(this.repo.store, name)
    if (commit === null) throw new UnknownTagError(name)
    return commit
  }

  async has(name: string): Promise<boolean> {
    return (await resolveTag(this.repo.store, name)) !== null
  }

  /** Every tag name, sorted. */
  async list(): Promise<string[]> {
    return [...(await listTags(this.repo.store)).keys()]
  }

  /** Every tag and its commit, sorted by name, read in one pass. */
  async entries(): Promise<Map<string, string>> {
    return listTags(this.repo.store)
  }

  /**
   * Name `commit` permanently.
   *
   * @throws TagExistsError if the name is taken.
   * @throws UnknownCommitError if `commit` is not in the store.
   */
  async create(name: string, commit: string, opts: { info?: CommitInfo } = {}): Promise<void> {
    await createTag(this.repo.store, name, commit, {
      ...(opts.info !== undefined && { info: opts.info }),
      now: this.repo.now,
    })
  }

  /**
   * Delete a tag. A commit it alone kept alive becomes collectable.
   *
   * @throws UnknownTagError if there is no such tag.
   */
  async delete(name: string): Promise<void> {
    await deleteTag(this.repo.store, name)
  }

  /**
   * A tag's commit, creation time, info, and whether its commit is
   * missing from the store.
   *
   * @throws UnknownTagError if there is no such tag.
   */
  async info(name: string): Promise<TagInfo> {
    const found = await tagInfo(this.repo.store, name)
    if (found === null) throw new UnknownTagError(name)
    return found
  }
}

/**
 * The state of the repository at one commit, read-only, as decoded
 * values. {@link raw} is the same state as stored bytes.
 *
 * Reads look keys up in the tree a batch at a time, so reading a few
 * keys stays cheap however many the commit holds; listing the keys
 * reads the whole keyset once.
 */
export class Snapshot<V = Uint8Array> {
  readonly commit: string
  readonly raw: RawSnapshot
  readonly #repo: Repo<V>
  readonly #keyset: Keyset
  /** The whole keyset (key → blob), once something has listed it. */
  #index: Map<string, string> | null = null
  /** Keys looked up one batch at a time (null: absent). */
  readonly #lookedUp = new Map<string, string | null>()

  /** @internal Made by {@link Repo.snapshot}. */
  constructor(repo: Repo<V>, commit: string, root: string) {
    this.#repo = repo
    this.commit = commit
    this.#keyset = Keyset.fromRoot(repo.store, root)
    this.raw = new RawSnapshot(this)
  }

  /** The value of `key`, or undefined if absent. */
  async get(key: string): Promise<V | undefined> {
    const raw = (await this.rawMany([key])).get(key)
    return raw === undefined ? undefined : this.#repo.codec.decode(raw)
  }

  /** The values of the given keys that exist here. */
  async getMany(keys: Iterable<string>): Promise<Map<string, V>> {
    const out = new Map<string, V>()
    for (const [k, raw] of await this.rawMany(keys)) out.set(k, this.#repo.codec.decode(raw))
    return out
  }

  async has(key: string): Promise<boolean> {
    return (await this.pointersOf([key])).has(key)
  }

  /** Every key, sorted. */
  async keys(): Promise<string[]> {
    return [...(await this.index()).keys()].sort()
  }

  async size(): Promise<number> {
    return (await this.index()).size
  }

  /** @internal */
  async rawMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    const wanted = new Map<string, string[]>()
    for (const [key, pointer] of await this.pointersOf(keys)) {
      const at = wanted.get(pointer) ?? []
      at.push(key)
      wanted.set(pointer, at)
    }
    const out = new Map<string, Uint8Array>()
    if (wanted.size === 0) return out
    for (const [pointer, raw] of await this.#repo.store.getMany(wanted.keys())) {
      for (const key of wanted.get(pointer) ?? []) out.set(key, raw)
    }
    return out
  }

  private async pointersOf(keys: Iterable<string>): Promise<Map<string, string>> {
    const asked = [...new Set(keys)]
    const out = new Map<string, string>()
    if (this.#index !== null) {
      for (const k of asked) {
        const p = this.#index.get(k)
        if (p !== undefined) out.set(k, p)
      }
      return out
    }
    const unknown = asked.filter((k) => !this.#lookedUp.has(k))
    if (unknown.length > 0) {
      const found = await this.#keyset.getMany(unknown)
      for (const k of unknown) this.#lookedUp.set(k, found.get(k)?.blob ?? null)
    }
    for (const k of asked) {
      const p = this.#lookedUp.get(k)
      if (p !== undefined && p !== null) out.set(k, p)
    }
    return out
  }

  private async index(): Promise<Map<string, string>> {
    if (this.#index === null) {
      const index = new Map<string, string>()
      for (const [k, entry] of await this.#keyset.materialize()) index.set(k, entry.blob)
      this.#index = index
    }
    return this.#index
  }
}

/** What a {@link RawSnapshot} reads through. */
interface RawSource {
  rawMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>>
  has(key: string): Promise<boolean>
  keys(): Promise<string[]>
}

/** A snapshot's stored bytes, undecoded. */
export class RawSnapshot {
  readonly #snapshot: RawSource

  /** @internal */
  constructor(snapshot: RawSource) {
    this.#snapshot = snapshot
  }

  async get(key: string): Promise<Uint8Array | undefined> {
    return (await this.#snapshot.rawMany([key])).get(key)
  }

  async getMany(keys: Iterable<string>): Promise<Map<string, Uint8Array>> {
    return this.#snapshot.rawMany(keys)
  }

  async has(key: string): Promise<boolean> {
    return this.#snapshot.has(key)
  }

  async keys(): Promise<string[]> {
    return this.#snapshot.keys()
  }
}
