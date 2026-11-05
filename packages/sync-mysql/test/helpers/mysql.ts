/**
 * A real MySQL for the tests, one throwaway database per test.
 *
 * There is no embedded MySQL the way PGlite embeds Postgres, so the suite
 * needs a server: `TEST_MYSQL_URL=mysql://root@127.0.0.1:3307` with a user
 * that may create and drop databases. Without it the database-backed tests
 * skip themselves (`describeDb`), and `pnpm test:database` refuses to run.
 *
 * Each test gets a database named after a random suffix and drops it on
 * `close()`, so tests can run in parallel workers against one server.
 */
import { randomBytes } from 'node:crypto'
import { describe } from 'vitest'
import { connect, type SqlClient } from '../../src/db/client.js'

export const TEST_MYSQL_URL = process.env['TEST_MYSQL_URL']

/** `describe` when a server is configured, `describe.skip` otherwise. */
export const describeDb = (name: string, fn: () => void): void => {
  if (TEST_MYSQL_URL) describe(name, fn)
  else describe.skip(name, fn)
}

export interface TestDb extends SqlClient {
  /** The throwaway database this test owns; pass it as the destination's `schema`. */
  schema: string
}

export async function createTestDb(): Promise<TestDb> {
  if (!TEST_MYSQL_URL) throw new Error('TEST_MYSQL_URL is not set')
  const schema = `workos_test_${randomBytes(6).toString('hex')}`
  const client = await connect(TEST_MYSQL_URL)
  return {
    schema,
    query: client.query.bind(client),
    exec: client.exec.bind(client),
    transaction: client.transaction.bind(client),
    pinned: client.pinned.bind(client),
    async close() {
      try {
        await client.exec(`drop database if exists \`${schema}\``)
      } finally {
        await client.close()
      }
    },
  }
}
