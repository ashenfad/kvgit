/**
 * Vitest config for the WebAssembly smoke test in Node: the core's
 * binding, built by `pnpm build:wasm`, loads and answers. Run via
 * `pnpm test:wasm`, which also runs `vitest.wasm-browser.config.ts`.
 */
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['wasm-smoke/node.test.ts'],
    globals: false,
  },
})
