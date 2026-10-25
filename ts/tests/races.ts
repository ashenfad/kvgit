/**
 * Races staged in the store: a branch moved just before a writer's
 * publish, a read that fails once.
 */

import type { KVStore } from '../src/types'
import { BRANCH_HEAD } from '../src/versioned/layout'

/**
 * Run `advance` just before each of the next `times` writes that move
 * `branch`'s head (every one, for `null`), so each publish finds the
 * branch moved. Writes `advance` makes itself go through untouched.
 */
export function racePublish(
  store: KVStore,
  branch: string,
  advance: () => Promise<unknown>,
  times: number | null = 1,
): void {
  const real = store.casMany.bind(store)
  const head = BRANCH_HEAD(branch)
  let left = times
  let racing = false
  store.casMany = async (expected, writes, removes = []) => {
    if (writes.has(head) && !racing && (left === null || left > 0)) {
      if (left !== null) left--
      racing = true
      try {
        await advance()
      } finally {
        racing = false
      }
    }
    return real(expected, writes, removes)
  }
}

/** Throw `error` from the first `getMany` whose keys `when` holds for,
 *  then read normally. */
export function failReads(store: KVStore, when: (keys: string[]) => boolean, error: Error): void {
  const real = store.getMany.bind(store)
  let armed = true
  store.getMany = async (keys) => {
    const list = [...keys]
    if (armed && when(list)) {
      armed = false
      throw error
    }
    return real(list)
  }
}
