import { describe, it, expect } from 'vitest'
import { fetchEvents, CursorDirectionError, cursorAgeDays, checkCursorAge } from '../src/workos/events.js'
import { WorkOSClient, TokenBucket, WorkOSApiError } from '../src/workos/client.js'
import { FakeWorkOS, resetIds } from './helpers/fake-workos.js'
import type { Logger } from '../src/types.js'

const clientFor = (fake: FakeWorkOS) =>
  new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl })

/**
 * `GET /events` requires an `events` filter — it returns 400 without one, even
 * though the published OpenAPI spec marks the parameter optional. Every call
 * therefore has to name the types it wants.
 */
const EVENTS = [
  'user.created',
  'user.updated',
  'user.deleted',
  'organization.created',
  'authentication.password_succeeded',
  'authentication.password_failed',
  'quantum_widget.entangled',
]

const collectLogs = () => {
  const lines: { level: string; msg: string }[] = []
  const logger: Logger = {
    debug: (msg) => lines.push({ level: 'debug', msg }),
    info: (msg) => lines.push({ level: 'info', msg }),
    warn: (msg) => lines.push({ level: 'warn', msg }),
    error: (msg) => lines.push({ level: 'error', msg }),
  }
  return { lines, logger }
}

describe('events tail', () => {
  it('returns events after the cursor and reports the next position', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    const u = fake.createUser()
    fake.updateUser(u['id'] as string, { first_name: 'Grace' })

    const res = await fetchEvents(clientFor(fake), { lagWindowMs: 0, events: EVENTS })
    expect(res.events.map((e) => e.event)).toEqual(['user.created', 'user.updated'])
    expect(res.cursor).toBe(res.events.at(-1)!.id)

    const next = await fetchEvents(clientFor(fake), { after: res.cursor, lagWindowMs: 0, events: EVENTS })
    expect(next.events).toHaveLength(0)
  })

  it('holds back events inside the lag window', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    fake.createUser()
    // The fake stamps events ~60s in the past; a 10-minute window covers them.
    const res = await fetchEvents(clientFor(fake), { lagWindowMs: 600_000, events: EVENTS })
    expect(res.events).toHaveLength(0)
    expect(res.withheld).toBe(1)
    // The cursor must not move past an event we declined to process.
    expect(res.cursor).toBeNull()
  })

  it('never advances the cursor past a withheld event', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    fake.createUser()
    fake.createUser()
    const all = await fetchEvents(clientFor(fake), { lagWindowMs: 0, events: EVENTS })
    const firstId = all.events[0]!.id

    // Place the cutoff exactly on the first event so only the newer one is
    // withheld: events strictly newer than the cutoff are held back.
    const cutoff = Date.parse(all.events[0]!.created_at)
    expect(Date.parse(all.events[1]!.created_at)).toBeGreaterThan(cutoff)
    const res = await fetchEvents(clientFor(fake), { lagWindowMs: 0, now: () => cutoff, events: EVENTS })
    expect(res.events).toHaveLength(1)
    expect(res.withheld).toBe(1)
    expect(res.cursor).toBe(firstId)
  })

  it('refuses to page backwards through history', async () => {
    // The direction of `after` under each order mode is ambiguous in the docs.
    // Guessing wrong would replay history while looking perfectly healthy, so
    // the engine asserts instead of assuming.
    const fake = new FakeWorkOS()
    fake.createUser()
    const backwards = {
      ...fake,
      fetchImpl: (async () =>
        new Response(
          JSON.stringify({
            data: [
              { id: 'event_00000000000000000001', event: 'user.created', data: {}, created_at: '2020-01-01T00:00:00.000Z' },
            ],
            list_metadata: { after: null },
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )) as unknown as typeof fetch,
    }
    const client = new WorkOSClient({
      apiKey: 'sk_test_1',
      apiUrl: 'http://fake.workos.local',
      fetchImpl: backwards.fetchImpl,
    })

    await expect(
      fetchEvents(client, { after: 'event_99999999999999999999', lagWindowMs: 0, events: EVENTS }),
    ).rejects.toBeInstanceOf(CursorDirectionError)
  })

  it('is rejected by the API when no event filter is supplied', async () => {
    // Verified against the live API: GET /events returns
    // 400 invalid_events_request without `events`, even though the published
    // OpenAPI spec marks the parameter optional. The engine therefore always
    // enumerates its subscription — see subscribedEventTypes().
    const fake = new FakeWorkOS()
    fake.createUser()
    await expect(fetchEvents(clientFor(fake), { lagWindowMs: 0 })).rejects.toThrow(
      /invalid_events_request|events parameter/,
    )
  })

  it('filters by event type when asked', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    const u = fake.createUser()
    fake.emitAuthAttempt(u['id'] as string, false)
    const res = await fetchEvents(clientFor(fake), {
      lagWindowMs: 0,
      events: ['authentication.password_failed'],
    })
    expect(res.events).toHaveLength(1)
    expect(res.events[0]!.event).toBe('authentication.password_failed')
  })
})

describe('cursor age', () => {
  it('measures the age of the last processed event', () => {
    const now = Date.parse('2026-03-01T00:00:00.000Z')
    expect(cursorAgeDays('2026-02-28T00:00:00.000Z', now)).toBeCloseTo(1)
    expect(cursorAgeDays(null, now)).toBeNull()
  })

  it('warns before the retention cliff and errors past it', () => {
    const now = Date.parse('2026-06-01T00:00:00.000Z')
    const fresh = collectLogs()
    checkCursorAge('2026-05-31T00:00:00.000Z', fresh.logger, now)
    expect(fresh.lines).toHaveLength(0)

    // 80 days: still recoverable, but the window is closing.
    const aging = collectLogs()
    checkCursorAge('2026-03-13T00:00:00.000Z', aging.logger, now)
    expect(aging.lines[0]?.level).toBe('warn')

    // Past 90 days the events are gone and only a backfill can repair it.
    const expired = collectLogs()
    checkCursorAge('2026-01-01T00:00:00.000Z', expired.logger, now)
    expect(expired.lines[0]?.level).toBe('error')
  })
})

describe('api client', () => {
  it('asks for the maximum page size, since the API default is 10', async () => {
    const fake = new FakeWorkOS()
    await clientFor(fake).list('/organizations')
    expect(fake.requestLog[0]).toContain('limit=100')
  })

  it('paginates through every page', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    for (let i = 0; i < 250; i++) fake.createOrganization()
    const client = new WorkOSClient({
      apiKey: 'sk_test_1',
      apiUrl: 'http://fake.workos.local',
      fetchImpl: fake.fetchImpl,
      pageSize: 100,
    })
    const seen: string[] = []
    for await (const page of client.paginate<{ id: string }>('/organizations')) {
      seen.push(...page.rows.map((r) => r.id))
    }
    expect(seen).toHaveLength(250)
    expect(new Set(seen).size).toBe(250)
  })

  it('retries a rate-limited request', async () => {
    const fake = new FakeWorkOS({ rateLimitFirst: 2 })
    fake.createOrganization()
    const res = await clientFor(fake).list<{ id: string }>('/organizations')
    expect(res.data).toHaveLength(1)
    // Two 429s plus the success.
    expect(fake.requestLog).toHaveLength(3)
  })

  it('returns null rather than throwing on a 404, which is how deletes are detected', async () => {
    const fake = new FakeWorkOS()
    const missing = await clientFor(fake).get('/user_management/users/user_nope')
    expect(missing).toBeNull()
  })

  it('surfaces other errors', async () => {
    const fake = new FakeWorkOS()
    // The memberships endpoint requires a filter; without one it is a 422.
    await expect(clientFor(fake).list('/user_management/organization_memberships')).rejects.toBeInstanceOf(
      WorkOSApiError,
    )
  })

  it('handles the unpaginated roles envelope', async () => {
    const fake = new FakeWorkOS()
    const res = await clientFor(fake).list('/authorization/roles')
    expect(res.data).toEqual([])
    expect(res.listMetadata.after).toBeNull()
  })
})

describe('token bucket', () => {
  it('allows a burst up to capacity then paces refill', () => {
    let now = 0
    const bucket = new TokenBucket(4, 4, () => now)
    for (let i = 0; i < 4; i++) expect(bucket.take()).toBe(0)
    // Fifth request in the same instant must wait for a refill.
    expect(bucket.take()).toBeGreaterThan(0)
    now += 1000
    expect(bucket.take()).toBe(0)
  })
})
