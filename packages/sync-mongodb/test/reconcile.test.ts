/**
 * The reconcile set difference, computed by the server rather than in Node.
 *
 * These exercise the database half on its own, with the remote side handed in
 * as pages. The engine half — pagination, repair — is covered by the drift
 * and repair cases in engine.test.ts.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { diffIdsInMemory, streamByName, STREAMS } from '@workos/sync-engine'
import type { Collection } from 'mongodb'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { migrate } from '../src/schema/migrate.js'
import { Names } from '../src/schema/names.js'
import { upsertRows, deleteRows, type MirrorDoc } from '../src/db/upsert.js'
import { diffIdsInMongo } from '../src/db/reconcile.js'
import { MongoDestination } from '../src/destination.js'

const users = streamByName('users')!
const names = new Names()
const opts = {}

// Zero-padded so lexical order (what the server sorts by) is numeric order.
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

describe('diff in mongodb', () => {
  let t: TestDb
  let coll: Collection<MirrorDoc>

  beforeEach(async () => {
    t = await createTestDb()
    await migrate(t.db, STREAMS, names)
    coll = t.db.collection<MirrorDoc>(names.mirror(users))
  })
  afterEach(async () => {
    await t.close()
  })

  const seed = (rows: Record<string, unknown>[], env = 'prod') =>
    upsertRows(
      coll,
      opts,
      users,
      env,
      rows.map((data) => ({ data, updatedAt: data['updated_at'] as string })),
      'replace',
    )

  const stagingLeft = async () => (await t.db.listCollections({ name: /_reconcile_/ }).toArray()).map((c) => c.name)

  it('reports matched, localOnly and remoteOnly, with the payload intact', async () => {
    await seed([user(1), user(2), user(3)])
    const four = user(4, { email: 'four@example.com', metadata: { plan: 'pro', 'a.b': '$x' } })

    const diff = await diffIdsInMongo(t.db, names, opts, users, 'prod', pages([user(2), user(3), four]))

    expect(diff.matched).toBe(2)
    expect(diff.localOnly).toEqual(['user_00001'])
    expect(diff.remoteOnly).toEqual([four])
    expect(diff.skipped).toBe(0)
    // The staging collections are gone once the answer is out.
    expect(await stagingLeft()).toEqual([])
  })

  it('compares complete composite keys on the server and in memory', async () => {
    const destination = new MongoDestination({ client: t.client, database: t.name })
    await destination.init(STREAMS)
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

    const diff = await diffIdsInMongo(t.db, names, opts, roles, 'prod', pages(remote, 1))

    expect(diff).toEqual({
      matched: 2,
      localOnly: ['["org_1","admin"]'],
      remoteOnly: [added],
      skipped: 3,
    })
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

  it('ignores tombstoned rows and other environments, and keeps the first of a duplicated id', async () => {
    await seed(range(1, 5))
    await seed([user(1, { email: 'staging@example.com' })], 'staging')
    await deleteRows(coll, opts, users, 'prod', [{ data: user(5), updatedAt: '2026-01-02T00:00:00.000Z' }])

    // user 3 arrives twice across a page boundary, as it would when a write
    // moves the boundary under the listing; the first copy is what is kept.
    const remote = [
      user(1),
      user(2),
      user(3, { email: 'first@example.com' }),
      user(3, { email: 'second@example.com' }),
      user(6),
    ]
    const diff = await diffIdsInMongo(t.db, names, opts, users, 'prod', pages(remote, 3))

    expect(diff.matched).toBe(3)
    expect(diff.localOnly).toEqual(['user_00004'])
    expect(diff.remoteOnly).toEqual([user(6)])
    expect(remote.filter((r) => r['id'] === 'user_00003')).toHaveLength(2)
  })

  it('scales with the drift, not the stream, and holds pages one at a time', async () => {
    await seed(range(1, 1_200))
    let served = 0
    async function* counted() {
      for await (const page of pages(
        range(1, 1_210).filter((u) => u.id !== 'user_00007'),
        100,
      )) {
        served++
        yield page
      }
    }
    const diff = await diffIdsInMongo(t.db, names, { batchSize: 50 }, users, 'prod', counted())
    expect(served).toBe(13)
    expect(diff.matched).toBe(1_199)
    expect(diff.localOnly).toEqual(['user_00007'])
    expect(diff.remoteOnly.map((r) => r['id'])).toEqual(range(1_201, 1_210).map((u) => u.id))
  })

  it('cleans up its staging collections when the listing fails midway', async () => {
    await seed(range(1, 3))
    async function* failing() {
      yield [user(1)]
      throw new Error('listing failed')
    }
    await expect(diffIdsInMongo(t.db, names, opts, users, 'prod', failing())).rejects.toThrow('listing failed')
    expect(await stagingLeft()).toEqual([])
  })

  it('reports an empty mirror and an empty listing without choking', async () => {
    expect(await diffIdsInMongo(t.db, names, opts, users, 'prod', pages([]))).toEqual({
      matched: 0,
      localOnly: [],
      remoteOnly: [],
      skipped: 0,
    })
    expect(await diffIdsInMongo(t.db, names, opts, users, 'prod', pages([user(1)]))).toMatchObject({
      remoteOnly: [user(1)],
    })
  })
})
