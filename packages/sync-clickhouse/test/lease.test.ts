/**
 * Lease and cursor semantics against a real ClickHouse Keeper, through
 * KeeperMap: the compare-and-swap the engine's fenced checkpoint relies on,
 * exercised with real races.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { ClickHouseDestination } from '../src/destination.js'

describeDb('events tail lease', () => {
  let db: TestDb
  let dest: ClickHouseDestination

  beforeEach(async () => {
    db = await createTestDb()
    dest = new ClickHouseDestination({ db, schema: db.schema })
    await dest.init([streamByName('users')!])
  })
  afterEach(async () => {
    await db.close()
  })

  const expire = () =>
    db.command(
      `ALTER TABLE \`${db.schema}\`.\`_sync\` UPDATE expires_at = now64(3, 'UTC') - toIntervalSecond(1) WHERE holder != ''`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )

  it('retries a checkpoint that lost a Keeper version race, and still refuses a cursor that moved', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await dest.setState('prod', '__tail__', { cursor: 1 })
    // Refuses the first checkpoint write the way Keeper does when the row's
    // version moved under it — the holder's own renewal is enough — then
    // passes everything through.
    let refused = 0
    const racing: TestDb = {
      ...db,
      async command(sql, params, settings) {
        if (refused === 0 && /UPDATE states =/.test(sql)) {
          refused++
          throw Object.assign(new Error('Code: 999. DB::Exception: Bad version. (KEEPER_EXCEPTION)'), { code: '999' })
        }
        return db.command(sql, params, settings)
      },
    }
    const viaRace = new ClickHouseDestination({ db: racing, schema: db.schema })
    expect(await viaRace.setStateIfHeld('prod', '__tail__', { cursor: 2 }, 'a', { cursor: 1 })).toBe(true)
    expect(refused).toBe(1)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 2 })

    refused = 0
    await dest.setState('prod', '__tail__', { cursor: 5 })
    expect(await viaRace.setStateIfHeld('prod', '__tail__', { cursor: 3 }, 'a', { cursor: 2 })).toBe(false)
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 5 })
  })

  it('checkpoints while the holder renews, without a refusal', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    let cursor = 0
    await dest.setState('prod', '__tail__', { cursor })
    for (let i = 0; i < 10; i++) {
      const [, ok] = await Promise.all([
        dest.acquireLease('prod', 'a', 60_000),
        dest.setStateIfHeld('prod', '__tail__', { cursor: cursor + 1 }, 'a', { cursor }),
      ])
      expect(ok).toBe(true)
      cursor++
    }
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 10 })
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
      `SELECT acquired_at < expires_at - toIntervalSecond(30) AS same FROM \`${db.schema}\`.\`_sync\``,
    )
    expect(Number(row[0]!.same)).toBe(1)
  })

  it('passes to a new process once it has expired', async () => {
    const stale = await dest.acquireLease('prod', 'a', 0)
    expect(stale.acquired).toBe(true)
    await new Promise((r) => setTimeout(r, 20))
    expect(await dest.getLease('prod')).toBeNull()
    const takeover = await dest.acquireLease('prod', 'b', 60_000)
    expect(takeover.acquired).toBe(true)
    expect(takeover.holder).toBe('b')
    expect((await dest.acquireLease('prod', 'a', 60_000)).holder).toBe('b')
  })

  it('judges expiry by the database clock', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await expire()
    expect(await dest.getLease('prod')).toBeNull()
    expect((await dest.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
  })

  it('is freed by release, and the cursors survive it', async () => {
    await dest.acquireLease('prod', 'a', 60_000)
    await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
    await dest.releaseLease('prod', 'a')
    expect(await dest.getLease('prod')).toBeNull()
    expect(await dest.getState('prod', '__tail__')).toEqual({ cursor: 'c1' })
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
    const other = new ClickHouseDestination({ db, schema: db.schema, syncId: 'reporting' })
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

  it('gives exactly one of many simultaneous contenders an expired lease', async () => {
    await dest.acquireLease('prod', 'old', 60_000)
    await expire()
    const results = await Promise.all(Array.from({ length: 8 }, (_, i) => dest.acquireLease('prod', `p${i}`, 60_000)))
    expect(results.filter((r) => r.acquired)).toHaveLength(1)
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
      await expire()
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

    it('refuses a checkpoint that races a cursor moved by someone else', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)
      // Two writers with the same expectation: only one can be right.
      const [x, y] = await Promise.all([
        dest.setStateIfHeld('prod', '__tail__', { cursor: 'x' }, 'a', { cursor: 'c1' }),
        dest.setStateIfHeld('prod', '__tail__', { cursor: 'y' }, 'a', { cursor: 'c1' }),
      ])
      expect([x, y].filter(Boolean)).toHaveLength(1)
      expect(await stored()).toEqual({ cursor: x ? 'x' : 'y' })
    })

    it('keeps other streams’ cursors when writing one', async () => {
      await dest.acquireLease('prod', 'a', 60_000)
      await dest.setState('prod', 'users', { kind: 'backfill', status: 'complete' })
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c1' }, 'a', null)).toBe(true)
      expect(await dest.getState('prod', 'users')).toEqual({ kind: 'backfill', status: 'complete' })
      expect(await stored()).toEqual({ cursor: 'c1' })
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

    it('survives concurrent writers to different streams', async () => {
      await Promise.all(Array.from({ length: 6 }, (_, i) => dest.setState('prod', `stream_${i}`, { i })))
      for (let i = 0; i < 6; i++) expect(await dest.getState('prod', `stream_${i}`)).toEqual({ i })
    })
  })
})
