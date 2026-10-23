import { defineConfig } from 'vitest/config'

export default defineConfig({
  // node:sqlite is a recent Node built-in (Node 22.5+) that Vite 5's
  // dep-optimizer can mishandle by stripping the `node:` prefix; tell
  // it not to touch the import so Node's native resolver handles it.
  optimizeDeps: {
    exclude: ['node:sqlite'],
  },
  ssr: {
    external: ['node:sqlite'],
  },
  test: {
    include: ['tests/**/*.test.ts'],
    globals: false,
    passWithNoTests: true,
    // The GitHub live suite (tests/github-pack-live.test.ts) runs real
    // API calls; give it room when its env gate is set.
    // Everything else keeps the snappy default.
    testTimeout: process.env.KVGIT_GH_TOKEN ? 30_000 : 5_000,
    setupFiles: ['./tests/setup-fake-indexeddb.ts'],
    server: {
      deps: {
        external: [/^node:/],
      },
    },
  },
})
