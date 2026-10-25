/**
 * The kvgit core, compiled to WebAssembly. Its bytes ship inside the
 * package, so it loads the same way under Node, bundlers and workers,
 * without a URL to resolve or a file to fetch.
 */

import init, * as wasm from '../wasm/kvgit_wasm.js'
import { wasmBytes } from '../wasm/kvgit_wasm_bytes.js'

export type Core = typeof wasm

let loading: Promise<Core> | null = null
let loaded: Core | null = null

/** The core, loaded on first use. */
export function core(): Promise<Core> {
  loading ??= init({ module_or_path: wasmBytes() }).then(() => {
    loaded = wasm
    return wasm
  })
  return loading
}

/**
 * Load the core ahead of a call that can't wait for it: the text merge
 * helpers are synchronous. `Repo.open` loads it, so only code that
 * merges text before opening any repository needs this.
 */
export async function loadCore(): Promise<void> {
  await core()
}

/** The core, for synchronous callers; it must have loaded. */
export function loadedCore(): Core {
  if (loaded === null) {
    throw new Error('The kvgit core is not loaded yet: open a Repo, or await loadCore(), first')
  }
  return loaded
}
