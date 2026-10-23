import { readFile } from 'node:fs/promises'
import { describe, expect, it } from 'vitest'
import { initSync, version } from '../wasm/kvgit_wasm.js'
import { coreVersion } from './expected'

describe('the core in Node', () => {
  it('loads from bytes and reports its version', async () => {
    const bytes = await readFile(new URL('../wasm/kvgit_wasm_bg.wasm', import.meta.url))
    initSync({ module: bytes })
    expect(version()).toBe(coreVersion)
  })
})
