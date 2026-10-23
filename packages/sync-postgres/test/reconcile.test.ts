/**
 * The reconcile set difference, computed in Postgres rather than in Node.
 *
 * These exercise the database half on its own, against PGlite, with the
 * remote side handed in as pages. The engine half — pagination, repair — is
 * covered by the drift and repair cases in engine.test.ts.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName, STREAMS } from '@workos/sync-engine'
import { createTestDb } from './helpers/pg.js'
import { migrate } from '../src/schema/migrate.js'
import { upsertRows, deleteRows } from '../src/db/upsert.js'
import { diffIdsInSql } from '../src/db/reconcile.js'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'

const users = streamByName('users')!
const opts = { schema: 'workos' }

// Zero-padded so lexical order (what the SQL sorts by) is numeric order.
const user = (n: number, over: Record<string, unknown> = {}) => ({
  object: 'user',
  id: `user_${String(n).padStart(5, '0')}`,
  email: `u${n}@example.com`,
  created_at: '2026-01-01T00:00:00.000Z',
  updated_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => user(from + i))

async function* pages(rows: Record<string, unknown>[], size = 100) {
  for (let i = 0; i < rows.length; i += size) yield rows.slice(i, i + size)
}

describe('diff in sql', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
    await migrate(db, STREAMS, 'workos')
  })
  afterEach(async () => {
    await db.close()
  })

  const seed = (rows: Record<string, unknown>[], env = 'prod') =>
    upsertRows(
      db,
      opts,
      users,
      env,
      rows.map((data) => ({ data, updatedAt: data['updated_at'] as string })),
      'replace',
    )

  it('reports matched, localOnly and remoteOnly, with the payload intact', async () => {
    await seed([user(1), user(2), user(3)])
    const four = user(4, { email: 'four@example.com', metadata: { plan: 'pro' } })

    const diff = await diffIdsInSql(db, opts, users, 'prod', pages([user(2), user(3), four]))

    expect(diff.matched).toBe(2)
    expect(diff.localOnly).toEqual(['user_00001'])
    expect(diff.remoteOnly).toEqual([four])
    expect(diff.skipped).toBe(0)
  })

  it('stages in batches of 500 across pages, keeping the first copy of a repeated id', async () => {
    await seed(range(1, 2000))
    // Pages of 1200 so a page splits into batches of 500, 500 and 200, and the
    // batch boundary never lines up with the page boundary.
    const remote = [...range(11, 2010), user(11, { email: 'again@example.com' })]

    const diff = await diffIdsInSql(db, opts, users, 'prod', pages(remote, 1200))

    expect(diff.matched).toBe(1990)
    expect(diff.localOnly).toEqual(range(1, 10).map((u) => u.id))
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual(range(2001, 2010).map((u) => u.id))
    expect(diff.skipped).toBe(0)
  })

  it('reports a tombstoned local row that still exists upstream as remoteOnly', async () => {
    await seed([user(1), user(2)])
    await deleteRows(db, opts, users, 'prod', [
      { data: { id: user(2).id }, updatedAt: '2026-01-02T00:00:00.000Z' },
    ])

    const diff = await diffIdsInSql(db, opts, users, 'prod', pages([user(1), user(2)]))

    // A tombstone is not live, so the upstream copy looks like a missed create.
    // Repair upserts it, and the tombstone stays: deletion is terminal.
    expect(diff.matched).toBe(1)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])
  })

  it('skips remote rows without a key instead of throwing', async () => {
    await seed([user(1)])
    const remote = [user(1), { object: 'user', email: 'no-id@example.com' }, user(2, { id: '' })]

    const diff = await diffIdsInSql(db, opts, users, 'prod', pages(remote))

    expect(diff.matched).toBe(1)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly).toEqual([])
    expect(diff.skipped).toBe(2)
  })

  it('only looks at the requested environment', async () => {
    await seed([user(1)], 'prod')
    await seed([user(2)], 'staging')

    const diff = await diffIdsInSql(db, opts, users, 'prod', pages([user(1), user(2)]))

    expect(diff.matched).toBe(1)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])
  })

  it('leaves no staging table behind, so the destination can be asked again', async () => {
    const destination = new PostgresDestination({ db, schema: 'workos' })
    await destination.init([users])
    await seed([user(1)])

    const first = await destination.diffIds(users, 'prod', pages([user(1), user(2)]))
    expect(first.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])

    const leftover = await db.query<{ oid: string | null }>(
      `select to_regclass('pg_temp._reconcile_remote')::text as oid`,
    )
    expect(leftover.rows[0]!.oid).toBeNull()

    const second = await destination.diffIds(users, 'prod', pages([user(3)]))
    expect(second.matched).toBe(0)
    expect(second.localOnly).toEqual([user(1).id])
    expect(second.remoteOnly.map((r) => r['id'])).toEqual([user(3).id])
  })
})
