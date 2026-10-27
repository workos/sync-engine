/**
 * Lease semantics against PGlite. Multi-session races are exercised against
 * standalone PostgreSQL in lease-postgres.test.ts.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { createTestDb } from './helpers/pg.js'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'

describe('events tail lease', () => {
  let db: SqlClient
  let dest: PostgresDestination

  beforeEach(async () => {
    db = await createTestDb()
    dest = new PostgresDestination({ db, schema: 'workos' })
    await dest.init([streamByName('users')!])
  })
  afterEach(async () => {
    await db.close()
  })

  it('is taken by the first process to ask', async () => {
    const r = await dest.acquireLease('prod', 'a', 60_000)
    expect(r.acquired).toBe(true)
    expect(r.holder).toBe('a')
    expect(Date.parse(r.expiresAt!)).toBeGreaterThan(Date.now() + 30_000)
    expect(await dest.getLease('prod')).toEqual({ holder: 'a', expiresAt: r.expiresAt })
  })

  it('refuses a second process and tells it who holds the lease', async () => {
    const first = await dest.acquireLease('prod', 'a', 60_000)
    const second = await dest.acquireLease('prod', 'b', 60_000)
    expect(second).toEqual({ acquired: false, holder: 'a', expiresAt: first.expiresAt })
    // The refusal changed nothing.
    expect(await dest.getLease('prod')).toEqual({ holder: 'a', expiresAt: first.expiresAt })
  })

  it('renews for the holder without changing hands', async () => {
    const first = await dest.acquireLease('prod', 'a', 1_000)
    const renewed = await dest.acquireLease('prod', 'a', 60_000)
    expect(renewed.acquired).toBe(true)
    expect(renewed.holder).toBe('a')
    expect(Date.parse(renewed.expiresAt!)).toBeGreaterThan(Date.parse(first.expiresAt!))

    // acquired_at marks the original takeover, not the latest heartbeat.
    const row = await db.query<{ same: boolean }>(
      `select acquired_at < expires_at - interval '30 seconds' as same from workos."_sync_lease"`,
    )
    expect(row.rows[0]!.same).toBe(true)
  })

  it('passes to a new process once it has expired', async () => {
    // A TTL of zero expires on arrival: the holder has, in effect, crashed.
    const stale = await dest.acquireLease('prod', 'a', 0)
    expect(stale.acquired).toBe(true)
    expect(await dest.getLease('prod')).toBeNull()

    const takeover = await dest.acquireLease('prod', 'b', 60_000)
    expect(takeover.acquired).toBe(true)
    expect(takeover.holder).toBe('b')
    // And the original holder is now the one refused.
    expect((await dest.acquireLease('prod', 'a', 60_000)).holder).toBe('b')
  })

  it('judges expiry by the database clock', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await db.query(`update workos."_sync_lease" set expires_at = now() - interval '1 second'`)
    expect((await dest.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
  })

  it('is freed by release', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await dest.releaseLease('prod', 'a')
    expect(await dest.getLease('prod')).toBeNull()
    expect((await dest.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
  })

  it('ignores a release from anyone but the holder', async () => {
    const r = await dest.acquireLease('prod', 'a', 60_000)
    await dest.releaseLease('prod', 'b')
    await dest.releaseLease('prod', 'a-but-older')
    expect(await dest.getLease('prod')).toEqual({ holder: 'a', expiresAt: r.expiresAt })
  })

  it('is scoped to the sync id and the environment', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    // Same database, same environment label, a different sync: no contention.
    const other = new PostgresDestination({ db, schema: 'workos', syncId: 'reporting' })
    expect((await other.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
    // Same sync, another environment: also independent.
    expect((await dest.acquireLease('staging', 'c', 60_000)).acquired).toBe(true)
    expect(await dest.getLease('prod')).toMatchObject({ holder: 'a' })
  })

  describe('fences the state write', () => {
    const stored = () => dest.getState<Record<string, unknown>>('prod', '__tail__')

    it('writes for the holder, and refuses anyone else', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'b', null)).toBe(false)
      expect(await stored()).toBeNull()
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)).toBe(true)
      expect(await stored()).toEqual({ cursor: 'c1' })
    })

    it('refuses a holder whose lease has expired or been taken over', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      await db.query(`update workos."_sync_lease" set expires_at = now() - interval '1 second'`)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { cursor: 'c1' })).toBe(false)
      // The replacement is now the writer; the former holder stays refused.
      await dest.acquireLease('prod', 'b', 60_000)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { cursor: 'c1' })).toBe(false)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'b', { cursor: 'c1' })).toBe(true)
      expect(await stored()).toEqual({ cursor: 'c2' })
    })

    it('refuses a write whose expected state is no longer the stored one', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', { cursor: 'c0' })).toBe(false)
      expect(await stored()).toBeNull()
      await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      // A row appeared where none was expected, and a stale expectation: both
      // are how a holder would rewind a position someone else moved.
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', null)).toBe(false)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { cursor: 'c0' })).toBe(false)
      expect(await stored()).toEqual({ cursor: 'c1' })
      // Equality is semantic, not textual: key order does not matter.
      await dest.setState('prod', '__tail__', { cursor: 'c1', lastEventAt: null })
      expect(
        await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { lastEventAt: null, cursor: 'c1' }),
      ).toBe(true)
      expect(await stored()).toEqual({ cursor: 'c2' })
    })
  })
})
