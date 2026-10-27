import { randomUUID } from 'node:crypto'
import { Pool, type PoolClient } from 'pg'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { fromPgPool } from '../src/db/client.js'
import { PostgresDestination } from '../src/destination.js'

// PGlite has only one session; these races need independent PostgreSQL connections.
describe.skipIf(!process.env.TEST_DATABASE_URL)('checkpoint fencing on PostgreSQL', () => {
  let writer: Pool
  let observer: Pool
  let dest: PostgresDestination
  let other: PostgresDestination
  let schema: string
  let blocker: PoolClient | undefined
  let checkpoint: Promise<boolean> | undefined

  beforeEach(async () => {
    schema = `lease_test_${randomUUID().replaceAll('-', '')}`
    const options = { connectionString: process.env.TEST_DATABASE_URL, statement_timeout: 10_000 }
    writer = new Pool({ ...options, max: 1, application_name: schema })
    observer = new Pool({ ...options, max: 4 })
    dest = new PostgresDestination({ db: fromPgPool(writer), schema })
    other = new PostgresDestination({ db: fromPgPool(observer), schema })
    blocker = undefined
    checkpoint = undefined
    await dest.init([streamByName('users')!])
    await dest.acquireLease('prod', 'old', 60_000)
    await dest.setState('prod', '__tail__', { cursor: 'c1' })
  })

  afterEach(async () => {
    try {
      if (blocker) {
        await blocker.query('rollback')
        blocker.release()
      }
      await Promise.allSettled(checkpoint ? [checkpoint] : [])
      await observer.query(`drop schema if exists "${schema}" cascade`)
    } finally {
      await Promise.all([writer.end(), observer.end()])
    }
  })

  async function lockRow(table: '_sync_state' | '_sync_lease'): Promise<void> {
    blocker = await observer.connect()
    await blocker.query('begin')
    await blocker.query(`select 1 from "${schema}".${table} for update`)
  }

  function startCheckpoint(expected: unknown = { cursor: 'c1' }): void {
    checkpoint = dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'old', expected)
  }

  async function insertCursor(): Promise<void> {
    await observer.query(`delete from "${schema}"._sync_state`)
    blocker = await observer.connect()
    await blocker.query('begin')
    await blocker.query(`insert into "${schema}"._sync_state (sync_id, env, stream, state)
      values ('default', 'prod', '__tail__', '{"cursor":"recaptured"}')`)
  }

  async function waitForExpiry(): Promise<void> {
    await expect.poll(async () => {
      const result = await observer.query(`select expires_at <= clock_timestamp() as expired from "${schema}"._sync_lease`)
      return result.rows[0]?.expired
    }, { timeout: 5_000, interval: 20 }).toBe(true)
  }

  async function waitForLock(): Promise<void> {
    await expect.poll(async () => {
      const result = await observer.query(
        `select wait_event_type from pg_stat_activity where application_name = $1`, [schema],
      )
      return result.rows[0]?.wait_event_type
    }, { timeout: 5_000, interval: 10 }).toBe('Lock')
  }

  it('refuses a blocked checkpoint after takeover without delaying the replacement', async () => {
    await lockRow('_sync_state')
    startCheckpoint()
    await waitForLock()
    await observer.query(`update "${schema}"._sync_lease set expires_at = now() - interval '1 second'`)
    expect((await other.acquireLease('prod', 'new', 60_000)).acquired).toBe(true)
    expect(await other.getState('prod', '__tail__')).toEqual({ cursor: 'c1' })
    await blocker!.query('commit')
    expect(await checkpoint).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'c1' })
    expect(await other.setStateIfHeld('prod', '__tail__', { cursor: 'c3' }, 'new', { cursor: 'c1' })).toBe(true)
  })

  it.each(['_sync_state', '_sync_lease'] as const)('checks the current clock after waiting on %s', async (table) => {
    await dest.acquireLease('prod', 'old', 2_000)
    await lockRow(table)
    startCheckpoint()
    await waitForLock()
    await waitForExpiry()
    await blocker!.query('commit')
    expect(await checkpoint).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'c1' })
  })

  it('rechecks a lease changed while its row lock was unavailable', async () => {
    blocker = await observer.connect()
    await blocker.query('begin')
    await blocker.query(`update "${schema}"._sync_lease set holder = 'new'`)
    startCheckpoint()
    await waitForLock()
    await blocker.query('commit')
    expect(await checkpoint).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'c1' })
  })

  it('refuses a checkpoint when another writer changes the locked cursor', async () => {
    await lockRow('_sync_state')
    await blocker!.query(`update "${schema}"._sync_state set state = '{"cursor":"recaptured"}'::jsonb`)
    startCheckpoint()
    await waitForLock()
    await blocker!.query('commit')
    expect(await checkpoint).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'recaptured' })
  })

  it.each(['commit', 'rollback'])('allows takeover while the first checkpoint waits for an insert to %s', async (end) => {
    await dest.acquireLease('prod', 'old', 2_000)
    await insertCursor()
    startCheckpoint(null)
    await waitForLock()
    await waitForExpiry()
    await observer.query(`select 1 from "${schema}"._sync_lease for update nowait`)
    expect((await other.acquireLease('prod', 'new', 60_000)).acquired).toBe(true)
    await blocker!.query(end)
    expect(await checkpoint).toBe(false)
    const expected = end === 'commit' ? { cursor: 'recaptured' } : null
    expect(await dest.getState('prod', '__tail__')).toEqual(expected)
    expect(await other.setStateIfHeld('prod', '__tail__', { cursor: 'c3' }, 'new', expected)).toBe(true)
  })

  it('refuses an expired first checkpoint after a competing insert rolls back', async () => {
    await dest.acquireLease('prod', 'old', 2_000)
    await insertCursor()
    startCheckpoint(null)
    await waitForLock()
    await waitForExpiry()
    await blocker!.query('rollback')
    expect(await checkpoint).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toBeNull()
  })

  it('saves the first checkpoint after a competing insert rolls back while the lease is valid', async () => {
    await insertCursor()
    startCheckpoint(null)
    await waitForLock()
    await blocker!.query('rollback')
    expect(await checkpoint).toBe(true)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'c2' })
  })

  it.each([null, { cursor: 'c1' }])('rolls back a refused checkpoint inside a caller transaction (%j)', async (expected) => {
    if (expected === null) await observer.query(`delete from "${schema}"._sync_state`)
    await fromPgPool(writer).transaction(async (tx) => {
      const nested = new PostgresDestination({ db: tx, schema })
      await nested.setState('prod', 'unrelated', { keep: true })
      expect(await nested.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'wrong', expected)).toBe(false)
    })
    expect(await dest.getState('prod', '__tail__')).toEqual(expected)
    expect(await dest.getState('prod', 'unrelated')).toEqual({ keep: true })
  })

  it('preserves compare-and-swap for competing inserts and updates', async () => {
    await observer.query(`delete from "${schema}"._sync_state`)
    for (let round = 0; round < 2; round++) {
      const expected = await dest.getState('prod', '__tail__')
      const writes = await Promise.all(Array.from({ length: 8 }, (_, i) =>
        other.setStateIfHeld('prod', '__tail__', { cursor: `${round}_${i}` }, 'old', expected),
      ))
      expect(writes.filter(Boolean)).toHaveLength(1)
    }
  })
})
