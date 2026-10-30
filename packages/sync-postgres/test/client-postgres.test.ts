import { Pool } from 'pg'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { connect, type SqlClient } from '../src/db/client.js'

// PGlite has one session and no server to close it; this needs a backend to terminate.
describe.skipIf(!process.env.TEST_DATABASE_URL)('an idle connection closed by the server, on PostgreSQL', () => {
  let db: SqlClient | undefined
  let admin: Pool

  beforeEach(() => {
    admin = new Pool({ connectionString: process.env.TEST_DATABASE_URL, max: 1 })
  })

  afterEach(async () => {
    await Promise.allSettled([db?.close(), admin.end()])
  })

  it('is logged, and the next query gets a new connection', async () => {
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() }
    db = await connect(process.env.TEST_DATABASE_URL!, { logger })

    // Leaves one connection idle in the pool, and learns which backend it is.
    const before = await db.query<{ pid: number }>('select pg_backend_pid() as pid')
    const pid = before.rows[0]!.pid

    // What a restart, a failover or a proxy's idle timeout does to it.
    await admin.query('select pg_terminate_backend($1)', [pid])
    await vi.waitFor(() => expect(logger.warn).toHaveBeenCalledTimes(1), { timeout: 10_000 })
    expect(logger.warn).toHaveBeenCalledWith(
      'idle database connection closed by the server; the pool will reconnect',
      { error: expect.stringContaining('terminat') },
    )

    const after = await db.query<{ pid: number }>('select pg_backend_pid() as pid')
    expect(after.rows[0]!.pid).not.toBe(pid)
    expect(logger.error).not.toHaveBeenCalled()
  })
})
