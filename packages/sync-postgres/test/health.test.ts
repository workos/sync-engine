/**
 * `engine.health()` driven by real ticks: the fake WorkOS API on one side, a
 * real Postgres on the other, and the in-memory health state in between. The
 * HTTP surface itself is unit-tested in sync-engine/test/health.test.ts; here
 * it is only wired to a live engine to prove the two agree.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { SyncEngine, WorkOSClient, silentLogger, startHealthServer, type HealthServer } from '@workos/sync-engine'
import { createTestDb } from './helpers/pg.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'

const STREAMS = ['users', 'organizations', 'events']

function makeEngine(db: SqlClient, fetchImpl: typeof fetch, apiKey = 'sk_test_1') {
  return new SyncEngine(
    {
      destination: new PostgresDestination({ db, schema: 'workos' }),
      environments: [{ name: 'prod', apiKey }],
      streams: STREAMS,
      logger: silentLogger,
      lagWindowMs: 0,
    },
    {
      clientFor: () =>
        new WorkOSClient({ apiKey, apiUrl: 'http://fake.workos.local', fetchImpl, maxRetries: 0, pageSize: 100 }),
    },
  )
}

describe('engine health', () => {
  let db: SqlClient
  let fake: FakeWorkOS
  let server: HealthServer | undefined

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await server?.close()
    server = undefined
    await db.close()
  })

  it('is `starting` with nothing recorded before the first tick', () => {
    const h = makeEngine(db, fake.fetchImpl).health()
    expect(h.state).toBe('starting')
    expect(h.fatal).toBeNull()
    expect(Date.parse(h.startedAt)).not.toBeNaN()
    expect(h.environments['prod']).toMatchObject({
      lastPolledAt: null,
      lastEventAt: null,
      cursor: null,
      lastError: null,
      counters: { ticks: 0, events: 0, errors: 0 },
    })
  })

  it('advances counters and the cursor as ticks process events', async () => {
    const u = fake.createUser()
    fake.createOrganization()
    const engine = makeEngine(db, fake.fetchImpl)
    await engine.backfill()

    await engine.tick()
    let h = engine.health()
    expect(h.state).toBe('listening')
    let e = h.environments['prod']!
    expect(e.counters.ticks).toBe(1)
    expect(e.counters.events).toBe(2)
    // Backfill already wrote both rows, so the tail's replay updates them.
    expect(e.counters.created + e.counters.updated + e.counters.skipped).toBeGreaterThan(0)
    expect(e.cursor).toMatch(/^event_/)
    expect(e.lastEventAt).not.toBeNull()
    expect(e.lastPolledAt).not.toBeNull()
    expect(e.lastError).toBeNull()

    // A quiet poll still counts as a poll, and still moves lastPolledAt.
    const before = e.lastPolledAt!
    await new Promise((r) => setTimeout(r, 5))
    await engine.tick()
    e = engine.health().environments['prod']!
    expect(e.counters.ticks).toBe(2)
    expect(e.counters.events).toBe(2)
    expect(e.lastPolledAt! >= before).toBe(true)

    fake.updateUser(u['id'] as string, { first_name: 'Grace' })
    await engine.tick()
    h = engine.health()
    e = h.environments['prod']!
    expect(e.counters.events).toBe(3)
    expect(e.counters.updated).toBeGreaterThanOrEqual(1)
  })

  it('records a retryable failure as lastError and keeps going', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake.fetchImpl)
    await engine.backfill()
    await engine.tick()

    // Make the events endpoint fail with a 500. With no retry budget the
    // client surfaces it on the first attempt, and the engine treats it as
    // transient: logged, counted, and polled again next tick.
    let broken = true
    const flaky = ((input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input.toString()
      if (broken && url.includes('/events')) {
        return Promise.resolve(new Response('{"message":"boom"}', { status: 500 }))
      }
      return fake.fetchImpl(input, init)
    }) as typeof fetch
    const failing = makeEngine(db, flaky)
    await failing.tick()
    let e = failing.health().environments['prod']!
    expect(e.counters.errors).toBe(1)
    expect(e.counters.ticks).toBe(1)
    expect(e.lastError?.message).toMatch(/500/)
    expect(e.lastPolledAt).toBeNull()
    expect(failing.health().fatal).toBeNull()

    broken = false
    await failing.tick()
    e = failing.health().environments['prod']!
    expect(e.counters.ticks).toBe(2)
    expect(e.lastPolledAt).not.toBeNull()
    // The record of the failure stays; the counters say it is not ongoing.
    expect(e.lastError?.message).toMatch(/500/)
  })

  it('records a rejected credential as fatal and stopped', async () => {
    const strict = new FakeWorkOS({ apiKey: 'sk_test_1' })
    strict.createUser()
    const engine = makeEngine(db, strict.fetchImpl, 'sk_test_WRONG')
    await expect(engine.tick()).rejects.toThrow(/401/)
    const h = engine.health()
    expect(h.state).toBe('stopped')
    expect(h.fatal).toMatch(/401/)
  })

  it('marks the engine stopped once listen() returns', async () => {
    const engine = makeEngine(db, fake.fetchImpl)
    const listening = engine.listen()
    // Stop after the first tick has had a chance to run.
    await new Promise((r) => setTimeout(r, 50))
    engine.stop()
    await listening
    const h = engine.health()
    expect(h.state).toBe('stopped')
    expect(h.environments['prod']!.counters.ticks).toBeGreaterThanOrEqual(1)
  })

  it('serves the same state over HTTP, and the status code follows it', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake.fetchImpl)
    let clock = Date.now()
    server = await startHealthServer(engine, { port: 0, host: '127.0.0.1', now: () => clock })
    const base = `http://127.0.0.1:${server.port}`

    let res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(200)
    expect(((await res.json()) as { state: string }).state).toBe('starting')

    await engine.backfill()
    await engine.tick()
    clock = Date.now()
    res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(200)
    const body = (await res.json()) as {
      state: string
      healthy: boolean
      environments: Record<string, { pollAgeSeconds: number; counters: { events: number } }>
    }
    expect(body.state).toBe('listening')
    expect(body.healthy).toBe(true)
    expect(body.environments['prod']!.counters.events).toBe(1)
    expect(body.environments['prod']!.pollAgeSeconds).toBeLessThan(5)

    const metrics = await (await fetch(`${base}/metrics`)).text()
    expect(metrics).toMatch(/^workos_sync_poll_age_seconds\{env="prod"\} \d/m)
    expect(metrics).toContain('workos_sync_events_total{env="prod"} 1\n')
    expect(metrics).toContain('workos_sync_up 1\n')

    // Age the last poll past the window without waiting for it.
    clock += 61_000
    res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(503)
  })
})
