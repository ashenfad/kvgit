/**
 * Vitest Node-mode setup: install the fake-indexeddb shim on globalThis.
 *
 * Browser-mode runs (Vitest browser provider) skip this file via
 * vitest.browser.config.ts and use the real IDB.
 */
import 'fake-indexeddb/auto'
