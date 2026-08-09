/**
 * PGlite-backed SqlClient. PGlite is real Postgres compiled to WebAssembly, so
 * the tests exercise actual generated columns, `xmax`, jsonb operators and the
 * immutability rules for generated expressions — not a mock's approximation.
 */
import { PGlite } from '@electric-sql/pglite'
import type { SqlClient } from '../../src/db/client.js'

export async function createTestDb(): Promise<SqlClient & { raw: PGlite }> {
  const pg = new PGlite()
  await pg.waitReady

  const client: SqlClient & { raw: PGlite } = {
    raw: pg,
    async query<T>(sql: string, params?: unknown[]) {
      const res = await pg.query(sql, params as never[])
      return { rows: res.rows as T[] }
    },
    async exec(sql: string) {
      await pg.exec(sql)
    },
    async transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T> {
      await pg.exec('begin')
      try {
        const out = await fn(client)
        await pg.exec('commit')
        return out
      } catch (err) {
        await pg.exec('rollback')
        throw err
      }
    },
    async close() {
      await pg.close()
    },
  }
  return client
}
