import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'
export default defineConfig({
  resolve: {
    // Tests run against @workos/sync source, not its last build.
    alias: { '@workos/sync': fileURLToPath(new URL('../sync/src/index.ts', import.meta.url)) },
  },
  test: {
    include: ['test/**/*.test.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    pool: 'forks',
  },
})
