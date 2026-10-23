/**
 * Vitest browser-mode config for IndexedDB tests.
 *
 * Standalone (does NOT extend vitest.config.ts) so the
 * fake-indexeddb setup file isn't loaded — browser mode uses real
 * IDB. Run via `pnpm test:browser` (locally requires
 * `pnpm exec playwright install chromium` once).
 */
import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/idb.test.ts'],
    globals: false,
    browser: {
      enabled: true,
      provider: 'playwright',
      name: 'chromium',
      headless: true,
    },
  },
})
