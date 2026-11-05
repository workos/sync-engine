/**
 * Lease semantics against a real MySQL, including the multi-connection race
 * the Postgres package keeps in a separate file: the pool here is real, so
 * two attempts can be in flight at once.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { MysqlDestination } from '../src/destination.js'

describeDb('events tail lease', () => {
  let db: TestDb
  let dest: MysqlDestination

  beforeEach(async () => {
    db = await createTestDb()
    dest = new MysqlDestination({ db, schema: db.schema })
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
    expect(r.expiresAt).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/)
    expect(await dest.getLease('prod')).toEqual({ holder: 'a', expiresAt: r.expiresAt })
  })

  it('refuses a second process and tells it who holds the lease', async () => {
    const first = await dest.acquireLease('prod', 'a', 60_000)
    const second = await dest.acquireLease('prod', 'b', 60_000)
    expect(second).toEqual({ acquired: false, holder: 'a', expiresAt: first.expiresAt })
    expect(await dest.getLease('prod')).toEqual({ holder: 'a', expiresAt: first.expiresAt })
  })

  it('renews for the holder without changing hands', async () => {
    const first = await dest.acquireLease('prod', 'a', 1_000)
    const renewed = await dest.acquireLease('prod', 'a', 60_000)
    expect(renewed.acquired).toBe(true)
    expect(renewed.holder).toBe('a')
    expect(Date.parse(renewed.expiresAt!)).toBeGreaterThan(Date.parse(first.expiresAt!))

    // acquired_at marks the original takeover, not the latest heartbeat.
    const row = await db.query<{ same: number }>(
      `select acquired_at < timestampadd(second, -30, expires_at) as same from \`${db.schema}\`.\`_sync_lease\``,
    )
    expect(row.rows[0]!.same).toBe(1)
  })

  it('passes to a new process once it has expired', async () => {
    const stale = await dest.acquireLease('prod', 'a', 0)
    expect(stale.acquired).toBe(true)
    expect(await dest.getLease('prod')).toBeNull()

    const takeover = await dest.acquireLease('prod', 'b', 60_000)
    expect(takeover.acquired).toBe(true)
    expect(takeover.holder).toBe('b')
    expect((await dest.acquireLease('prod', 'a', 60_000)).holder).toBe('b')
  })

  it('judges expiry by the database clock', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await db.query(`update \`${db.schema}\`.\`_sync_lease\` set expires_at = timestampadd(second, -1, now(6))`)
    expect(await dest.getLease('prod')).toBeNull()
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
    const other = new MysqlDestination({ db, schema: db.schema, syncId: 'reporting' })
    expect((await other.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
    expect((await dest.acquireLease('staging', 'c', 60_000)).acquired).toBe(true)
    expect(await dest.getLease('prod')).toMatchObject({ holder: 'a' })
  })

  it('gives exactly one of many simultaneous contenders the lease', async () => {
    const holders = Array.from({ length: 8 }, (_, i) => `p${i}`)
    const results = await Promise.all(holders.map((h) => dest.acquireLease('prod', h, 60_000)))
    const winners = results.filter((r) => r.acquired)
    expect(winners).toHaveLength(1)
    const winner = winners[0]!.holder
    for (const r of results) if (!r.acquired) expect(r.holder).toBe(winner)
    expect(await dest.getLease('prod')).toMatchObject({ holder: winner })
  })

  describe('fences the state write', () => {
    const stored = () => dest.getState<Record<string, unknown>>('prod', '__tail__')

    /**
     * Poll until a transaction is waiting on a lock, so a blocked checkpoint
     * is known to be blocked. Polled gently: a tight loop of pool queries
     * keeps grabbing the free connection and the checkpoint never gets one.
     */
    const waitForLockWait = async () => {
      const deadline = Date.now() + 5_000
      for (;;) {
        const r = await db.query<{ n: number }>(
          `select count(*) as n from information_schema.innodb_trx where trx_state = 'LOCK WAIT'`,
        )
        if (Number(r.rows[0]!.n) > 0) return
        if (Date.now() > deadline) throw new Error('no transaction is waiting on a lock')
        await new Promise((r) => setTimeout(r, 100))
      }
    }

    it('checks the current clock after waiting on the lease row', async () => {
      // The lease is valid when the checkpoint starts and expired by the time
      // its row lock is granted. `NOW(6)` is fixed per statement, so a check
      // folded into the locking read would see the clock from before the
      // wait and let an expired holder save its cursor.
      await dest.acquireLease('prod', 'a', 1_000)
      let release!: () => void
      const released = new Promise<void>((r) => (release = r))
      const blocker = db.pinned(async (conn) => {
        await conn.exec('begin')
        await conn.query(`select 1 from \`${db.schema}\`.\`_sync_lease\` for update`)
        await released
        await conn.exec('commit')
      })
      const checkpoint = dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      await waitForLockWait()
      await new Promise((r) => setTimeout(r, 1_200))
      release()
      await blocker
      expect(await checkpoint).toBe(false)
      expect(await stored()).toBeNull()
    })

    it('rechecks a lease changed while its row lock was unavailable', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      let release!: () => void
      const released = new Promise<void>((r) => (release = r))
      const blocker = db.pinned(async (conn) => {
        await conn.exec('begin')
        await conn.query(`update \`${db.schema}\`.\`_sync_lease\` set holder = 'b'`)
        await released
        await conn.exec('commit')
      })
      const checkpoint = dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      await waitForLockWait()
      release()
      await blocker
      // The takeover committed first; the former holder is judged against it.
      expect(await checkpoint).toBe(false)
      expect(await stored()).toBeNull()
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'b', null)).toBe(true)
    })

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
      await db.query(`update \`${db.schema}\`.\`_sync_lease\` set expires_at = timestampadd(second, -1, now(6))`)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { cursor: 'c1' })).toBe(false)
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

    it('writes the same state again without reporting a refusal', async () => {
      // FOUND_ROWS: an update to identical values still counts as matched.
      await dest.acquireLease('prod', 'a', 60_000)
      await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', { cursor: 'c1' })).toBe(true)
    })

    it('leaves a refused write out of an enclosing transaction', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      await dest.setState('prod', 'other', { n: 1 })
      await db.transaction(async (tx) => {
        const inner = new MysqlDestination({ db: tx, schema: db.schema })
        await inner.setState('prod', 'other', { n: 2 })
        expect(await inner.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'nobody', null)).toBe(false)
      })
      // The transaction committed its own write; the refused one rolled back
      // to its savepoint and nothing else.
      expect(await dest.getState('prod', 'other')).toEqual({ n: 2 })
      expect(await stored()).toBeNull()
    })
  })

  describe('plain state', () => {
    it('round-trips JSON and overwrites in place', async () => {
      expect(await dest.getState('prod', 'users')).toBeNull()
      await dest.setState('prod', 'users', { kind: 'backfill', status: 'running', after: null })
      await dest.setState('prod', 'users', { kind: 'backfill', status: 'complete', count: 3 })
      expect(await dest.getState('prod', 'users')).toEqual({ kind: 'backfill', status: 'complete', count: 3 })
      expect(await dest.getState('staging', 'users')).toBeNull()
    })
  })
})
