/**
 * Vitest config for the WebAssembly smoke test in Chromium: the core's
 * binding, built by `pnpm build:wasm`, loads in a Vite-served page, in
 * the main thread and in a module worker. Run via `pnpm test:wasm`.
 */
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['wasm-smoke/browser.test.ts'],
    globals: false,
    browser: {
      enabled: true,
      provider: 'playwright',
      name: 'chromium',
      headless: true,
    },
  },
})
