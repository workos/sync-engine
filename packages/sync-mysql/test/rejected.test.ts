/**
 * The quarantine store on its own, against a real MySQL. How the engine
 * fills it is covered by the poison-event cases in engine.test.ts.
 */
import { it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName, type WorkOSEvent } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { MysqlDestination } from '../src/destination.js'

const event = (n: number, over: Partial<WorkOSEvent> = {}): WorkOSEvent => ({
  id: `event_${String(n).padStart(3, '0')}`,
  event: 'user.created',
  data: { id: `user_${n}`, email: `u${n}@example.com`, nested: { 'odd key': [1, null, 'x'] } },
  created_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describeDb('quarantine store', () => {
  let db: TestDb
  let dest: MysqlDestination

  beforeEach(async () => {
    db = await createTestDb()
    dest = new MysqlDestination({ db, schema: db.schema })
    await dest.init([streamByName('users')!, streamByName('events')!])
  })
  afterEach(async () => {
    await db.close()
  })

  it('stores whole events, lists them oldest first by id, and counts without reading them', async () => {
    await dest.reject('prod', [
      { event: event(2), error: 'second' },
      { event: event(1), error: 'first' },
    ])
    expect(await dest.rejectedCount('prod')).toBe(2)
    expect(await dest.rejectedCount('staging')).toBe(0)
    const rows = await dest.rejected('prod')
    expect(rows.map((r) => r.eventId)).toEqual(['event_001', 'event_002'])
    expect(rows[0]).toMatchObject({
      eventType: 'user.created',
      objectId: 'user_1',
      createdAt: '2026-01-01T00:00:00.000Z',
      error: 'first',
      raw: event(1),
    })
    expect(rows[0]!.rejectedAt).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    expect(Date.parse(rows[0]!.rejectedAt)).toBeGreaterThan(Date.now() - 60_000)
  })

  it('replaces the error and timestamp when an id is rejected again', async () => {
    await dest.reject('prod', [{ event: event(1), error: 'first try' }])
    const before = (await dest.rejected('prod'))[0]!
    await new Promise((r) => setTimeout(r, 5))
    await dest.reject('prod', [{ event: event(1, { data: { id: 'user_1', retried: true } }), error: 'second try' }])
    const rows = await dest.rejected('prod')
    expect(rows).toHaveLength(1)
    expect(rows[0]!.error).toBe('second try')
    expect(rows[0]!.raw.data).toEqual({ id: 'user_1', retried: true })
    expect(Date.parse(rows[0]!.rejectedAt)).toBeGreaterThanOrEqual(Date.parse(before.rejectedAt))
  })

  it('filters by id and forgets what was replayed', async () => {
    await dest.reject(
      'prod',
      [1, 2, 3].map((n) => ({ event: event(n), error: 'e' })),
    )
    expect((await dest.rejected('prod', { ids: ['event_002'] })).map((r) => r.eventId)).toEqual(['event_002'])
    expect(await dest.rejected('prod', { ids: [] })).toEqual([])
    expect(await dest.clearRejected('prod', ['event_001', 'event_003', 'event_nope'])).toBe(2)
    expect(await dest.clearRejected('prod', [])).toBe(0)
    expect((await dest.rejected('prod')).map((r) => r.eventId)).toEqual(['event_002'])
  })

  it('never fails on the event timestamp it is quarantining', async () => {
    await dest.reject('prod', [{ event: event(1, { created_at: '2026-13-01T00:00:00Z' }), error: 'bad month' }])
    const rows = await dest.rejected('prod')
    expect(rows[0]!.createdAt).toBe('2026-13-01T00:00:00Z')
    const stored = await db.query<{ created_at: Date | null }>(`select created_at from \`${db.schema}\`.\`_rejected\``)
    expect(stored.rows[0]!.created_at).toBeNull()
  })

  it('keeps membership events, which carry no object id', async () => {
    const link = event(1, { event: 'group.member_added', data: { group_id: 'g', organization_membership_id: 'om' } })
    await dest.reject('prod', [{ event: link, error: 'e' }])
    expect((await dest.rejected('prod'))[0]!.objectId).toBeNull()
  })

  it('keeps syncs sharing a database apart', async () => {
    const other = new MysqlDestination({ db, schema: db.schema, syncId: 'reporting' })
    await dest.reject('prod', [{ event: event(1), error: 'e' }])
    await other.reject('prod', [{ event: event(2), error: 'e' }])
    expect((await dest.rejected('prod')).map((r) => r.eventId)).toEqual(['event_001'])
    expect((await other.rejected('prod')).map((r) => r.eventId)).toEqual(['event_002'])
  })
})
