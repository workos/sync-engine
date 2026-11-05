/**
 * A real ClickHouse for the tests, one throwaway database per test.
 *
 * There is no embedded ClickHouse with a Keeper in it, and KeeperMap needs
 * one, so the suite needs a server: `TEST_CLICKHOUSE_URL=http://default@127.0.0.1:8123`
 * against a server started with `test/clickhouse/config.xml`. Without it the
 * database-backed tests skip themselves (`describeDb`), and
 * `pnpm test:database` refuses to run.
 *
 * Each test gets a database named after a random suffix and drops it on
 * `close()`, so tests can run in parallel workers against one server. The
 * KeeperMap tables keep their Keeper paths under the database's name, so
 * two tests never share a lease.
 */
import { randomBytes } from 'node:crypto'
import { describe } from 'vitest'
import { connect, type ChClient } from '../../src/db/client.js'

export const TEST_CLICKHOUSE_URL = process.env['TEST_CLICKHOUSE_URL']

/** `describe` when a server is configured, `describe.skip` otherwise. */
export const describeDb = (name: string, fn: () => void): void => {
  if (TEST_CLICKHOUSE_URL) describe(name, fn)
  else describe.skip(name, fn)
}

export interface TestDb extends ChClient {
  /** The throwaway database this test owns; pass it as the destination's `schema`. */
  schema: string
}

export async function createTestDb(): Promise<TestDb> {
  if (!TEST_CLICKHOUSE_URL) throw new Error('TEST_CLICKHOUSE_URL is not set')
  const schema = `workos_test_${randomBytes(6).toString('hex')}`
  const client = await connect(TEST_CLICKHOUSE_URL)
  return {
    schema,
    query: client.query.bind(client),
    command: client.command.bind(client),
    insert: client.insert.bind(client),
    async close() {
      try {
        await client.command(`DROP DATABASE IF EXISTS \`${schema}\` SYNC`)
      } finally {
        await client.close()
      }
    },
  }
}
