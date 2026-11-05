/**
 * Lease and checkpoint semantics against a real server. The lease and the
 * cursors share one document, so every check here is one atomic update.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { MongoDestination } from '../src/destination.js'
import { canonicalJson, type SyncDoc } from '../src/db/lease.js'

describe('events tail lease', () => {
  let t: TestDb
  let dest: MongoDestination

  beforeEach(async () => {
    t = await createTestDb()
    dest = new MongoDestination({ client: t.client, database: t.name })
    await dest.init([streamByName('users')!])
  })
  afterEach(async () => {
    await t.close()
  })

  const sync = () => t.db.collection<SyncDoc>('workos__sync')
  const expire = () =>
    sync().updateOne({ sync_id: 'default', env: 'prod' }, { $set: { 'lease.expires_at': new Date(Date.now() - 1000) } })

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
    const doc = (await sync().findOne({ sync_id: 'default', env: 'prod' }))!
    expect(doc.lease!.acquired_at.getTime()).toBeLessThan(doc.lease!.expires_at.getTime() - 30_000)
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
    await expire()
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
    // Same database, same environment label, a different sync: no contention.
    const other = new MongoDestination({ client: t.client, database: t.name, syncId: 'reporting' })
    await other.init([streamByName('users')!])
    expect((await other.acquireLease('prod', 'b', 60_000)).acquired).toBe(true)
    // Same sync, another environment: also independent.
    expect((await dest.acquireLease('staging', 'c', 60_000)).acquired).toBe(true)
    expect(await dest.getLease('prod')).toMatchObject({ holder: 'a' })
  })

  it('stores a sync id, an environment and a holder that start with "$" as text', async () => {
    // Inside the update pipeline a bare "$reporting" would be read as a
    // field path, and the document would be keyed by nothing.
    const dollar = new MongoDestination({ client: t.client, database: t.name, syncId: '$reporting' })
    await dollar.init([streamByName('users')!])
    const r = await dollar.acquireLease('$production', '$host', 60_000)
    expect(r).toMatchObject({ acquired: true, holder: '$host' })
    expect(await dollar.getLease('$production')).toMatchObject({ holder: '$host' })
    const doc = await sync().findOne({ sync_id: '$reporting', env: '$production' })
    expect(doc?.lease?.holder).toBe('$host')
    // A renewal still recognises the holder, and the fenced write finds the document.
    expect((await dollar.acquireLease('$production', '$host', 60_000)).acquired).toBe(true)
    expect(await dollar.setStateIfHeld('$production', '__tail__', { cursor: 'c1' }, '$host', null)).toBe(true)
    expect(await dollar.getState('$production', '__tail__')).toEqual({ cursor: 'c1' })
  })

  it('survives two processes racing to create the document', async () => {
    const results = await Promise.all(['a', 'b', 'c', 'd'].map((h) => dest.acquireLease('prod', h, 60_000)))
    expect(results.filter((r) => r.acquired)).toHaveLength(1)
    const holder = results.find((r) => r.acquired)!.holder
    expect(results.every((r) => r.holder === holder)).toBe(true)
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
      // A cursor appeared where none was expected, and a stale expectation: both
      // are how a holder would rewind a position someone else moved.
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', null)).toBe(false)
      expect(await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', { cursor: 'c0' })).toBe(false)
      expect(await stored()).toEqual({ cursor: 'c1' })
      // Equality is semantic, not textual: key order does not matter.
      await dest.setState('prod', '__tail__', {
        cursor: 'c1',
        lastEventAt: null,
        nested: { b: 1, a: [2, { d: 1, c: 2 }] },
      })
      expect(
        await dest.setStateIfHeld('prod', '__tail__', { cursor: 'c2' }, 'a', {
          nested: { a: [2, { c: 2, d: 1 }], b: 1 },
          lastEventAt: null,
          cursor: 'c1',
        }),
      ).toBe(true)
      expect(await stored()).toEqual({ cursor: 'c2' })
    })

    it('keeps cursors per stream and per environment in the one document', async () => {
      await dest.setState('prod', 'users', { kind: 'backfill', status: 'running' })
      await dest.setState('prod', '__reconcile__:users', { kind: 'reconcile', status: 'complete' })
      await dest.setState('staging', 'users', { kind: 'backfill', status: 'complete' })
      expect(await dest.getState('prod', 'users')).toEqual({ kind: 'backfill', status: 'running' })
      expect(await dest.getState('prod', '__reconcile__:users')).toEqual({ kind: 'reconcile', status: 'complete' })
      expect(await dest.getState('staging', 'users')).toEqual({ kind: 'backfill', status: 'complete' })
      expect(await dest.getState('prod', 'organizations')).toBeNull()
      expect(await sync().countDocuments()).toBe(2)
      // Setting the state again replaces it whole.
      await dest.setState('prod', 'users', { kind: 'backfill', status: 'complete' })
      expect(await dest.getState('prod', 'users')).toEqual({ kind: 'backfill', status: 'complete' })
    })
  })
})

describe('canonicalJson', () => {
  it('sorts keys at every depth and drops undefined, so equal values serialise equally', () => {
    expect(canonicalJson({ b: 1, a: { d: [{ f: 1, e: 2 }], c: null }, u: undefined })).toBe(
      '{"a":{"c":null,"d":[{"e":2,"f":1}]},"b":1}',
    )
    expect(canonicalJson([3, { b: 1, a: 2 }])).toBe('[3,{"a":2,"b":1}]')
    expect(canonicalJson('x')).toBe('"x"')
  })
})
