/**
 * The kvgit core, compiled to WebAssembly. Its bytes ship inside the
 * package, so it loads the same way under Node, bundlers and workers,
 * without a URL to resolve or a file to fetch.
 */

import init, * as wasm from '../wasm/kvgit_wasm.js'
import { wasmBytes } from '../wasm/kvgit_wasm_bytes.js'

export type Core = typeof wasm

let loading: Promise<Core> | null = null

/** The core, loaded on first use. */
export function core(): Promise<Core> {
  loading ??= init({ module_or_path: wasmBytes() }).then(() => wasm)
  return loading
}
