#!/usr/bin/env node
/**
 * Build the core's WebAssembly binding into `wasm/`: compile
 * `crates/wasm` for wasm32, then generate its JS glue with the
 * wasm-bindgen CLI, whose version must equal the `wasm-bindgen` crate's.
 *
 * Needs a Rust toolchain with the `wasm32-unknown-unknown` target and
 * `wasm-bindgen-cli` installed. Run via `pnpm build:wasm`.
 */

import { spawnSync } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const pkgRoot = join(dirname(fileURLToPath(import.meta.url)), '..')
const repoRoot = join(pkgRoot, '..')
const target = 'wasm32-unknown-unknown'

function run(command, args) {
  const result = spawnSync(command, args, { cwd: repoRoot, stdio: 'inherit' })
  if (result.error) throw result.error
  if (result.status !== 0) process.exit(result.status ?? 1)
}

run('cargo', ['build', '--release', '--target', target, '-p', 'kvgit-wasm'])
run('wasm-bindgen', [
  '--target',
  'web',
  '--out-dir',
  join(pkgRoot, 'wasm'),
  join(repoRoot, 'target', target, 'release', 'kvgit_wasm.wasm'),
])
