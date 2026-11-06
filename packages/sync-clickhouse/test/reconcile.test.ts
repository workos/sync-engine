/**
 * The reconcile set difference, computed in ClickHouse rather than in Node.
 *
 * These exercise the database half on its own, with the remote side handed
 * in as pages. The engine half — pagination, repair — is covered by the
 * drift and repair cases in engine.test.ts.
 */
import { it, expect, beforeEach, afterEach } from 'vitest'
import { diffIdsInMemory, diffStream, streamByName, WorkOSClient } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { migrate } from '../src/schema/migrate.js'
import { upsertRows, deleteRows } from '../src/db/upsert.js'
import { diffIdsInClickHouse } from '../src/db/reconcile.js'
import { ClickHouseDestination } from '../src/destination.js'

const users = streamByName('users')!
const STREAMS = ['users', 'organizations', 'organization_roles'].map((n) => streamByName(n)!)

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

describeDb('diff in sql', () => {
  let db: TestDb
  let opts: { schema: string }

  beforeEach(async () => {
    db = await createTestDb()
    opts = { schema: db.schema }
    await migrate(db, STREAMS, db.schema)
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
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', pages([user(2), user(3), four]))
    expect(diff.matched).toBe(2)
    expect(diff.localOnly).toEqual(['user_00001'])
    expect(diff.remoteOnly).toEqual([four])
    expect(diff.skipped).toBe(0)
  })

  it('compares complete composite keys in SQL and in memory', async () => {
    const destination = new ClickHouseDestination({ db, schema: db.schema })
    const roles = streamByName('organization_roles')!
    const removed = { organization_id: 'org_1', slug: 'admin' }
    const kept = { organization_id: 'org_1', slug: 'auditor' }
    const elsewhere = { organization_id: 'org_2', slug: 'admin' }
    const added = { organization_id: 'org_1', slug: 'member', permissions: ['read'] }
    await destination.upsert(
      roles,
      'prod',
      [removed, kept, elsewhere].map((data) => ({ data, updatedAt: '2026-01-01T00:00:00.000Z' })),
      'replace',
    )
    const remote = [
      kept,
      elsewhere,
      added,
      { ...added, permissions: [] },
      { organization_id: 'org_1' },
      { slug: 'admin' },
      { organization_id: 'org_1', slug: '' },
    ]
    const diff = await diffIdsInClickHouse(db, opts, roles, 'prod', pages(remote, 1))
    expect(diff).toEqual({ matched: 2, localOnly: ['["org_1","admin"]'], remoteOnly: [added], skipped: 3 })
    expect(await destination.liveIds(roles, 'prod')).toEqual([
      '["org_1","admin"]',
      '["org_1","auditor"]',
      '["org_2","admin"]',
    ])
    expect(await diffIdsInMemory(destination, roles, 'prod', pages(remote, 1))).toEqual({
      matched: diff.matched,
      localOnly: diff.localOnly,
      remoteOnly: diff.remoteOnly,
    })
  })

  it('repairs a missing role and a missed deletion without touching sibling roles', async () => {
    const destination = new ClickHouseDestination({ db, schema: db.schema })
    const roles = streamByName('organization_roles')!
    const removed = { organization_id: 'org_1', slug: 'admin' }
    const kept = { organization_id: 'org_1', slug: 'auditor' }
    const added = { organization_id: 'org_1', slug: 'member' }
    const updatedAt = '2026-01-01T00:00:00.000Z'
    await destination.upsert(streamByName('organizations')!, 'prod', [{ data: { id: 'org_1' }, updatedAt }], 'replace')
    await destination.upsert(
      roles,
      'prod',
      [removed, kept].map((data) => ({ data, updatedAt })),
      'replace',
    )
    const api = new WorkOSClient({
      apiKey: 'sk_test_1',
      fetchImpl: async (input) => {
        expect(new URL(String(input)).pathname).toBe('/authorization/organizations/org_1/roles')
        return Response.json({ data: [kept, added] })
      },
    })
    const result = await diffStream(destination, api, 'prod', roles, { repair: true })
    expect(result).toMatchObject({
      matched: 1,
      localOnly: ['["org_1","admin"]'],
      remoteOnly: ['["org_1","member"]'],
      repaired: { deleted: 1, inserted: 1 },
    })
    const rows = await db.query<{ slug: string; deleted: number }>(
      `SELECT slug, \`_deleted_at\` IS NOT NULL AS deleted FROM \`${db.schema}\`.organization_roles FINAL ORDER BY slug`,
    )
    // Comparisons come back as UInt8, not Bool.
    expect(rows).toEqual([
      { slug: 'admin', deleted: 1 },
      { slug: 'auditor', deleted: 0 },
      { slug: 'member', deleted: 0 },
    ])
    expect(await diffStream(destination, api, 'prod', roles)).toMatchObject({
      matched: 2,
      localOnly: [],
      remoteOnly: [],
    })
  })

  it('stages in batches of 500 across pages, keeping the first copy of a repeated id', async () => {
    await seed(range(1, 2000))
    const remote = [...range(11, 2010), user(11, { email: 'again@example.com' })]
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', pages(remote, 1200))
    expect(diff.matched).toBe(1990)
    expect(diff.localOnly).toEqual(range(1, 10).map((u) => u.id))
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual(range(2001, 2010).map((u) => u.id))
    expect(diff.skipped).toBe(0)
  })

  it('keeps the first copy of a remote-only id that arrives twice', async () => {
    const diff = await diffIdsInClickHouse(
      db,
      opts,
      users,
      'prod',
      pages([user(1, { email: 'first@example.com' }), user(1, { email: 'second@example.com' })], 1),
    )
    expect(diff.remoteOnly).toEqual([user(1, { email: 'first@example.com' })])
  })

  it('reports a tombstoned local row that still exists upstream as remoteOnly', async () => {
    await seed([user(1), user(2)])
    await deleteRows(db, opts, users, 'prod', [{ data: { id: user(2).id }, updatedAt: '2026-01-02T00:00:00.000Z' }])
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', pages([user(1), user(2)]))
    expect(diff.matched).toBe(1)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])
  })

  it('is not fooled by a row the tail writes while the list is in progress', async () => {
    await seed([user(1), user(2)])
    async function* remote() {
      yield [user(1)]
      await seed([user(3)])
      yield [user(2), user(4)]
    }
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', remote())
    expect(diff.matched).toBe(2)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual([user(4).id])
  })

  it('skips remote rows without a key instead of throwing', async () => {
    await seed([user(1)])
    const remote = [user(1), { object: 'user', email: 'no-id@example.com' }, user(2, { id: '' })]
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', pages(remote))
    expect(diff).toMatchObject({ matched: 1, localOnly: [], remoteOnly: [], skipped: 2 })
  })

  it('only looks at the requested environment', async () => {
    await seed([user(1)], 'prod')
    await seed([user(2)], 'staging')
    const diff = await diffIdsInClickHouse(db, opts, users, 'prod', pages([user(1), user(2)]))
    expect(diff.matched).toBe(1)
    expect(diff.localOnly).toEqual([])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])
  })

  it('leaves no staging table behind, even when the listing throws', async () => {
    const destination = new ClickHouseDestination({ db, schema: db.schema })
    await destination.init([users])
    await seed([user(1)])
    const staging = async () =>
      (
        await db.query<{ name: string }>(
          `SELECT name FROM system.tables WHERE database = {schema:String} AND name LIKE '_reconcile_%'`,
          { schema: db.schema },
        )
      ).map((r) => r.name)
    const first = await destination.diffIds(users, 'prod', pages([user(1), user(2)]))
    expect(first.remoteOnly.map((r) => r['id'])).toEqual([user(2).id])
    expect(await staging()).toEqual([])
    async function* failing() {
      yield [user(1)]
      throw new Error('upstream went away')
    }
    await expect(destination.diffIds(users, 'prod', failing())).rejects.toThrow('upstream went away')
    expect(await staging()).toEqual([])
    const again = await destination.diffIds(users, 'prod', pages([user(3)]))
    expect(again.matched).toBe(0)
    expect(again.localOnly).toEqual([user(1).id])
    expect(again.remoteOnly.map((r) => r['id'])).toEqual([user(3).id])
  })
})
