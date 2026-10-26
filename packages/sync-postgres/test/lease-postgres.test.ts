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

  function startCheckpoint(): void {
    checkpoint = dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'old', { cursor: 'c1' })
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
    await expect.poll(async () => {
      const result = await observer.query(`select expires_at <= clock_timestamp() as expired from "${schema}"._sync_lease`)
      return result.rows[0]?.expired
    }, { timeout: 5_000, interval: 20 }).toBe(true)
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
