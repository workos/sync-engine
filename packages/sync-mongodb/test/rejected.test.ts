/**
 * The quarantine store on its own, plus the two things that reach into it
 * from outside: `redact`, which must scrub it, and `freshness`, which the
 * status command reports alongside its count.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName, STREAMS, type WorkOSEvent } from '@workos/sync-engine'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { MongoDestination } from '../src/destination.js'
import type { RejectedDoc } from '../src/db/rejected.js'

const users = streamByName('users')!
const events = streamByName('events')!
const auth = streamByName('auth_attempts')!

const event = (n: number, data: Record<string, unknown>, over: Partial<WorkOSEvent> = {}): WorkOSEvent => ({
  id: `event_${String(n).padStart(3, '0')}`,
  event: 'user.updated',
  data,
  created_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describe('quarantine', () => {
  let t: TestDb
  let dest: MongoDestination

  beforeEach(async () => {
    t = await createTestDb()
    dest = new MongoDestination({ client: t.client, database: t.name })
    await dest.init(STREAMS)
  })
  afterEach(async () => {
    await t.close()
  })

  it('stores whole events, lists them oldest first, counts without reading them, and forgets on request', async () => {
    const e2 = event(2, { id: 'user_2', name: '$ref', nested: { 'a.b': 1 } })
    const e1 = event(1, { id: 'user_1' })
    await dest.reject('prod', [
      { event: e2, error: 'boom' },
      { event: e1, error: 'bang' },
    ])
    await dest.reject('staging', [{ event: event(3, { id: 'user_3' }), error: 'elsewhere' }])

    expect(await dest.rejectedCount('prod')).toBe(2)
    const listed = await dest.rejected('prod')
    expect(listed.map((r) => r.eventId)).toEqual(['event_001', 'event_002'])
    expect(listed[1]).toMatchObject({
      eventId: 'event_002',
      eventType: 'user.updated',
      objectId: 'user_2',
      createdAt: '2026-01-01T00:00:00.000Z',
      error: 'boom',
      raw: e2,
    })
    expect(listed[1]!.rejectedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/)
    expect(await dest.rejected('prod', { ids: ['event_002'] })).toHaveLength(1)
    expect(await dest.rejected('prod', { ids: [] })).toEqual([])

    expect(await dest.clearRejected('prod', ['event_001', 'event_999'])).toBe(1)
    expect(await dest.clearRejected('prod', [])).toBe(0)
    expect(await dest.rejectedCount('prod')).toBe(1)
    expect(await dest.rejectedCount('staging')).toBe(1)
  })

  it('replaces the error and the timestamp when an id is rejected again', async () => {
    const e = event(1, { id: 'user_1' })
    await dest.reject('prod', [{ event: e, error: 'first' }])
    const before = (await dest.rejected('prod'))[0]!
    await new Promise((r) => setTimeout(r, 5))
    await dest.reject('prod', [{ event: { ...e, data: { id: 'user_1', fixed: true } }, error: 'second' }])
    const after = (await dest.rejected('prod'))[0]!
    expect(await dest.rejectedCount('prod')).toBe(1)
    expect(after.error).toBe('second')
    expect(after.raw.data).toEqual({ id: 'user_1', fixed: true })
    expect(Date.parse(after.rejectedAt)).toBeGreaterThanOrEqual(Date.parse(before.rejectedAt))
  })

  it('never fails on the timestamp that may have caused the rejection', async () => {
    const e = event(1, { id: 'user_1' }, { created_at: '2026-02-30T12:00:00Z' })
    await dest.reject('prod', [{ event: e, error: 'out of range' }])
    const stored = await t.db.collection<RejectedDoc>('workos__rejected').findOne({ event_id: 'event_001' })
    expect(stored!.created_at).toBeNull()
    expect((await dest.rejected('prod'))[0]!.createdAt).toBe('2026-02-30T12:00:00Z')
    // A membership event has no data.id; object_id is null rather than invented.
    await dest.reject('prod', [{ event: event(2, { group_id: 'g', organization_membership_id: 'om' }), error: 'x' }])
    expect((await dest.rejected('prod', { ids: ['event_002'] }))[0]!.objectId).toBeNull()
  })

  it('is scoped to the sync id', async () => {
    await dest.reject('prod', [{ event: event(1, { id: 'user_1' }), error: 'x' }])
    const other = new MongoDestination({ client: t.client, database: t.name, syncId: 'reporting' })
    await other.init(STREAMS)
    expect(await other.rejectedCount('prod')).toBe(0)
    expect(await other.clearRejected('prod', ['event_001'])).toBe(0)
    expect(await dest.rejectedCount('prod')).toBe(1)
  })
})

describe('redact', () => {
  let t: TestDb
  let dest: MongoDestination

  beforeEach(async () => {
    t = await createTestDb()
    dest = new MongoDestination({ client: t.client, database: t.name })
    await dest.init(STREAMS)
  })
  afterEach(async () => {
    await t.close()
  })

  it('erases a subject from the mirror, the archive and the quarantine, and reports each', async () => {
    const at = '2026-01-01T00:00:00.000Z'
    await dest.upsert(
      users,
      'prod',
      [
        { data: { id: 'user_1', email: 'forget@example.com' }, updatedAt: at },
        { data: { id: 'user_2', email: 'keep@example.com' }, updatedAt: at },
      ],
      'replace',
    )
    await dest.upsert(
      auth,
      'prod',
      [
        { data: { event_id: 'event_a', user_id: 'user_1', status: 'failed' }, updatedAt: at },
        { data: { event_id: 'event_b', user_id: 'user_2', status: 'failed' }, updatedAt: at },
      ],
      'replace',
    )
    await dest.upsert(
      events,
      'prod',
      [
        { data: event(1, { id: 'user_1' }) as unknown as Record<string, unknown>, updatedAt: at },
        {
          data: event(2, { id: 'session_1', user_id: 'user_1' }, { event: 'session.created' }) as unknown as Record<
            string,
            unknown
          >,
          updatedAt: at,
        },
        { data: event(3, { id: 'user_2' }) as unknown as Record<string, unknown>, updatedAt: at },
      ],
      'replace',
    )
    await dest.reject('prod', [
      { event: event(4, { id: 'user_1' }), error: 'x' },
      { event: event(5, { id: 'om_1', user_id: 'user_1' }, { event: 'organization_membership.created' }), error: 'x' },
      { event: event(6, { id: 'user_2' }), error: 'x' },
    ])
    // Another environment's rows are not this environment's to erase.
    await dest.upsert(
      users,
      'staging',
      [{ data: { id: 'user_1', email: 'staging@example.com' }, updatedAt: at }],
      'replace',
    )

    const removed = await dest.redact(STREAMS, 'prod', 'user_1')
    expect(removed).toEqual(
      expect.arrayContaining([
        { table: 'users', rows: 1 },
        { table: 'auth_attempts', rows: 1 },
        { table: 'events', rows: 2 },
        { table: '_rejected', rows: 2 },
      ]),
    )
    expect(removed.find((r) => r.table === 'organizations')).toBeUndefined()
    expect(await t.db.collection('workos_users').countDocuments({ _env: 'prod' })).toBe(1)
    expect(await t.db.collection('workos_users').countDocuments({ _env: 'staging' })).toBe(1)
    expect(await t.db.collection('workos_events').countDocuments()).toBe(1)
    expect(await dest.rejectedCount('prod')).toBe(1)
    expect(await dest.redact(STREAMS, 'prod', 'user_1')).toEqual([])
  })
})

describe('freshness', () => {
  let t: TestDb
  let dest: MongoDestination

  beforeEach(async () => {
    t = await createTestDb()
    dest = new MongoDestination({ client: t.client, database: t.name })
    await dest.init(STREAMS)
  })
  afterEach(async () => {
    await t.close()
  })

  it('reports counts, tombstones, the last write and the lag inside the window', async () => {
    expect(await dest.freshness(users, 'prod', 24)).toEqual({
      stream: 'users',
      rows: 0,
      deleted: 0,
      lastSyncedAt: null,
      stalenessSeconds: null,
      lagSeconds: null,
      lagSampleSize: 0,
    })
    const recent = new Date(Date.now() - 30_000).toISOString()
    await dest.upsert(
      users,
      'prod',
      [
        { data: { id: 'user_1' }, updatedAt: recent },
        { data: { id: 'user_2' }, updatedAt: '2025-01-01T00:00:00.000Z' },
      ],
      'replace',
    )
    await dest.delete(users, 'prod', [{ data: { id: 'user_2' }, updatedAt: '2025-01-02T00:00:00.000Z' }])

    const f = await dest.freshness(users, 'prod', 24)
    expect(f).toMatchObject({ stream: 'users', rows: 2, deleted: 1, lagSampleSize: 1 })
    expect(Date.parse(f.lastSyncedAt!)).toBeGreaterThan(Date.now() - 60_000)
    expect(f.stalenessSeconds).toBeGreaterThanOrEqual(29)
    expect(f.stalenessSeconds).toBeLessThan(120)
    // Only the recent row is in the lag average; the year-old one would make a current mirror look months behind.
    expect(f.lagSeconds).toBeGreaterThanOrEqual(29)
    expect(f.lagSeconds).toBeLessThan(120)
    expect((await dest.freshness(users, 'prod', 0)).lagSeconds).toBeNull()
    expect((await dest.freshness(users, 'staging', 24)).rows).toBe(0)
    // Append-only streams have no tombstones to count.
    expect((await dest.freshness(events, 'prod', 24)).deleted).toBe(0)
  })
})
