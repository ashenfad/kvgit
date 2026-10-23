/**
 * kvgit's errors.
 *
 * Everything kvgit throws about the state of a store derives from
 * {@link KvgitError}. Arguments that are invalid on their face — a
 * malformed name, two refs where one is expected — throw `TypeError` or
 * `RangeError`.
 */

/** Base class for kvgit's errors. */
export class KvgitError extends Error {
  override readonly name: string = 'KvgitError'
}

/**
 * A concurrent write won a race this call cannot merge past: another
 * writer moved the branch between when this call read it and when it
 * tried to publish. Nothing was changed; refresh and retry.
 */
export class ConcurrencyError extends KvgitError {
  override readonly name: string = 'ConcurrencyError'
}

/**
 * A commit made with `onMoved: 'raise'` found its branch moved: the
 * branch's head is no longer the commit the handle is based on. Nothing
 * was published and pending changes are kept; reload from `head` (or
 * refresh) and retry, or give up.
 */
export class BranchMovedError extends ConcurrencyError {
  override readonly name: string = 'BranchMovedError'
  readonly branch: string
  /** The commit the handle is based on, which the branch no longer names. */
  readonly base: string
  /** The branch's head as the commit found it. */
  readonly head: string

  constructor(branch: string, base: string, head: string) {
    super(`Branch '${branch}' moved from ${base} to ${head}; nothing was committed`)
    this.branch = branch
    this.base = base
    this.head = head
  }
}

/**
 * One or more contested keys had no merge function (or their merge fn
 * threw). The merge attempt is aborted; pending changes remain intact
 * for inspection.
 */
export class MergeConflict extends KvgitError {
  override readonly name: string = 'MergeConflict'
  readonly keys: ReadonlySet<string>
  readonly causes: ReadonlyMap<string, unknown>

  constructor(keys: Iterable<string>, causes?: ReadonlyMap<string, unknown>) {
    const keySet = new Set(keys)
    super(`Merge conflict on ${keySet.size} key(s): ${[...keySet].sort().join(', ')}`)
    this.keys = keySet
    this.causes = causes ?? new Map()
  }
}

/**
 * A branch that does not exist: opening it (with `create: false`),
 * reading its head, deleting it, or committing to it after it was
 * deleted.
 */
export class UnknownBranchError extends KvgitError {
  override readonly name: string = 'UnknownBranchError'
  readonly branch: string

  constructor(branch: string, createHint = false) {
    super(
      createHint
        ? `Branch '${branch}' does not exist (open with create: true to create it)`
        : `Branch '${branch}' does not exist`,
    )
    this.branch = branch
  }
}

/** A tag that does not exist. */
export class UnknownTagError extends KvgitError {
  override readonly name: string = 'UnknownTagError'
  readonly tag: string

  constructor(tag: string) {
    super(`Tag '${tag}' does not exist`)
    this.tag = tag
  }
}

/** A commit that is not in the store. */
export class UnknownCommitError extends KvgitError {
  override readonly name: string = 'UnknownCommitError'
  readonly commit: string

  constructor(commit: string) {
    super(`Commit '${commit}' does not exist`)
    this.commit = commit
  }
}

/** Creating a branch under a name already taken. */
export class BranchExistsError extends KvgitError {
  override readonly name: string = 'BranchExistsError'
  readonly branch: string

  constructor(branch: string) {
    super(`Branch '${branch}' already exists`)
    this.branch = branch
  }
}

/** Creating a tag under a name already taken. Tags never move: delete
 *  the tag first to point the name elsewhere. */
export class TagExistsError extends KvgitError {
  override readonly name: string = 'TagExistsError'
  readonly tag: string

  constructor(tag: string) {
    super(`Tag '${tag}' already exists`)
    this.tag = tag
  }
}

/**
 * A branch whose HEAD is present but unusable and could not be
 * recovered from its backup. `repairHead` is the way back.
 */
export class CorruptHeadError extends KvgitError {
  override readonly name: string = 'CorruptHeadError'
  readonly branch: string

  constructor(branch: string) {
    super(`Branch '${branch}' HEAD is corrupt and unrecoverable`)
    this.branch = branch
  }
}

/** A store stamped with a storage layout this code does not read, or
 *  one older than any it supports. Nothing is written to it. */
export class StorageVersionError extends KvgitError {
  override readonly name: string = 'StorageVersionError'
}

/**
 * A sweep couldn't take the store's GC lease because another sweep
 * holds an unexpired one. `gc({ wait: false })` throws it; by default
 * `gc` waits instead. The lease carries an expiry, so a holder that
 * dies without releasing it blocks nothing past that point.
 */
export class GcBusy extends KvgitError {
  override readonly name: string = 'GcBusy'
}
