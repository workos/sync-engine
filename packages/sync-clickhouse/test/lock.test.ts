import { it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { migrate } from '../src/schema/migrate.js'
import { acquireStreamLock, releaseStreamLock, withStreamLock, lockKey } from '../src/db/lock.js'
import { upsertRows, deleteRows } from '../src/db/upsert.js'
import { isTransientClickHouseError } from '../src/db/errors.js'

const users = streamByName('users')!
const events = streamByName('events')!

describeDb('per-stream write lock', () => {
  let db: TestDb
  const key = lockKey('users', 'prod')

  beforeEach(async () => {
    db = await createTestDb()
    await migrate(db, [users, events], db.schema)
  })
  afterEach(async () => {
    await db.close()
  })

  it('admits one holder at a time, and the next once it is released', async () => {
    const a = { schema: db.schema, holder: 'a' }
    const b = { schema: db.schema, holder: 'b', waitMs: 300 }
    const token = await acquireStreamLock(db, a, key)
    const err = await withStreamLock(db, b, key, async () => 'ran').catch((e: unknown) => e)
    expect(err).toMatchObject({ code: 'LOCK_TIMEOUT' })
    expect(isTransientClickHouseError(err)).toBe(true)
    await releaseStreamLock(db, a, key, token)
    expect(await withStreamLock(db, b, key, async () => 'ran')).toBe('ran')
  })

  it("is taken over once a crashed holder's lock expires, and not before", async () => {
    await acquireStreamLock(db, { schema: db.schema, holder: 'crashed', ttlMs: 400 }, key)
    const started = Date.now()
    expect(await withStreamLock(db, { schema: db.schema, holder: 'next', waitMs: 5_000 }, key, async () => 'ran')).toBe(
      'ran',
    )
    expect(Date.now() - started).toBeGreaterThanOrEqual(300)
  })

  it('is per table and per environment', async () => {
    const a = { schema: db.schema, holder: 'a', waitMs: 300 }
    await acquireStreamLock(db, a, key)
    expect(await withStreamLock(db, a, lockKey('users', 'staging'), async () => 'ran')).toBe('ran')
    expect(await withStreamLock(db, a, lockKey('events', 'prod'), async () => 'ran')).toBe('ran')
  })

  it('never lets two critical sections overlap, even within one process', async () => {
    let active = 0
    let overlaps = 0
    let runs = 0
    await Promise.all(
      Array.from({ length: 8 }, () =>
        withStreamLock(db, { schema: db.schema, holder: 'same' }, key, async () => {
          if (active++ > 0) overlaps++
          await new Promise((r) => setTimeout(r, 20))
          active--
          runs++
        }),
      ),
    )
    expect(runs).toBe(8)
    expect(overlaps).toBe(0)
  })

  it('serialises concurrent writers so an append-only event lands once', async () => {
    const rows = Array.from({ length: 20 }, (_, i) => ({
      data: { id: `event_${i}`, event: 'user.created', created_at: '2026-01-01T00:00:00.000Z', data: {} },
      updatedAt: '2026-01-01T00:00:00.000Z',
    }))
    // Six polls delivering the same page at once: a lease takeover's worst case.
    await Promise.all(
      Array.from({ length: 6 }, (_, p) =>
        upsertRows(db, { schema: db.schema, lockHolder: `poll${p}` }, events, 'prod', rows, 'merge'),
      ),
    )
    const n = await db.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM \`${db.schema}\`.events WHERE \`_env\` = 'prod'`,
    )
    expect(Number(n[0]!.n)).toBe(20)
  })

  it('keeps a tombstone that a concurrent snapshot write would otherwise outrank', async () => {
    const payload = { object: 'user', id: 'user_1', email: 'ada@example.com', updated_at: '2026-01-01T00:00:00.000Z' }
    const at = '2026-01-01T00:00:00.000Z'
    await upsertRows(db, { schema: db.schema }, users, 'prod', [{ data: payload, updatedAt: at }], 'replace')
    // A refresh re-listing the same snapshot with a field events omit, while
    // the tail deletes the user. Both versions carry the same `_updated_at`,
    // so unserialised the tie would go to whichever insert landed last.
    await Promise.all([
      deleteRows(db, { schema: db.schema, lockHolder: 'tail' }, users, 'prod', [
        { data: { id: 'user_1' }, updatedAt: '2026-01-01T00:00:00.500Z' },
      ]),
      upsertRows(
        db,
        { schema: db.schema, lockHolder: 'refresh' },
        users,
        'prod',
        [{ data: { ...payload, last_sign_in_at: at }, updatedAt: at }],
        'replace',
      ),
    ])
    expect(await db.query(`SELECT id FROM \`${db.schema}\`.users_live`)).toEqual([])
  })
})
