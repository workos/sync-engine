/**
 * The health surface in isolation: the tracker's state machine, the probe
 * verdict, the metrics text, and the HTTP server against a stub engine. The
 * engine-driven path (real ticks moving these numbers) is covered in
 * sync-postgres/test/health.test.ts, where a destination exists.
 */
import { describe, it, expect, afterEach } from 'vitest'
import {
  HealthTracker,
  healthReport,
  renderMetrics,
  startHealthServer,
  type EngineHealth,
  type HealthServer,
} from '../src/health.js'

const T0 = Date.parse('2026-10-23T12:00:00.000Z')

/** A tracker whose clock the test owns, so poll age is a number and not a race. */
function tracker(envs = ['prod']) {
  let t = T0
  const h = new HealthTracker(envs, () => t)
  return { h, now: () => t, advance: (ms: number) => (t += ms) }
}

function polled(h: HealthTracker, now: () => number, env = 'prod', events = 0) {
  h.polled(env, {
    polledAt: new Date(now()).toISOString(),
    cursor: events ? 'event_01' : null,
    lastEventAt: events ? new Date(now() - 1_000).toISOString() : null,
    events,
    stats: { created: events, updated: 0, skipped: 0, deleted: 0 },
  })
}

describe('HealthTracker', () => {
  it('starts in `starting` with every environment present and empty', () => {
    const { h } = tracker(['prod', 'staging'])
    const s = h.snapshot()
    expect(s.state).toBe('starting')
    expect(s.fatal).toBeNull()
    expect(Object.keys(s.environments)).toEqual(['prod', 'staging'])
    expect(s.environments['staging']).toEqual({
      lastPolledAt: null,
      lastEventAt: null,
      cursor: null,
      lastError: null,
      counters: { ticks: 0, events: 0, created: 0, updated: 0, skipped: 0, deleted: 0, errors: 0 },
    })
  })

  it('becomes `listening` once a tick has completed, and `stopped` when listen returns', () => {
    const { h } = tracker()
    h.listening() // a first listen() changes nothing: the first poll is still pending
    expect(h.snapshot().state).toBe('starting')
    h.tickCompleted()
    expect(h.snapshot().state).toBe('listening')
    h.stopped()
    expect(h.snapshot().state).toBe('stopped')
    // A restart after stop() is listening again; there is no first poll to wait for.
    h.listening()
    expect(h.snapshot().state).toBe('listening')
  })

  it('records a fatal error as stopped, and stays stopped', () => {
    const { h } = tracker()
    h.fatalError(new Error('401 on /events'))
    const s = h.snapshot()
    expect(s.state).toBe('stopped')
    expect(s.fatal).toMatch(/401/)
    h.listening()
    expect(h.snapshot().state).toBe('stopped')
  })

  it('accumulates counters and keeps the last error after a later success', () => {
    const { h, now, advance } = tracker()
    polled(h, now, 'prod', 3)
    h.failed('prod', new Error('500 on /events'))
    advance(1_000)
    polled(h, now, 'prod', 2)
    const e = h.snapshot().environments['prod']!
    expect(e.counters).toEqual({ ticks: 3, events: 5, created: 5, updated: 0, skipped: 0, deleted: 0, errors: 1 })
    expect(e.lastError?.message).toMatch(/500/)
    expect(e.lastPolledAt).toBe(new Date(now()).toISOString())
  })

  it('hands out copies, not its own state', () => {
    const { h, now } = tracker()
    polled(h, now)
    const s = h.snapshot()
    s.environments['prod']!.counters.ticks = 99
    expect(h.snapshot().environments['prod']!.counters.ticks).toBe(1)
  })
})

describe('healthReport', () => {
  it('is healthy while starting, whatever the timestamps say', () => {
    const { h, now } = tracker()
    expect(healthReport(h.snapshot(), { now }).healthy).toBe(true)
  })

  it('needs every environment polled inside the window once listening', () => {
    const { h, now, advance } = tracker(['prod', 'staging'])
    polled(h, now, 'prod')
    h.tickCompleted()
    // staging never polled: unhealthy even though prod is fresh.
    expect(healthReport(h.snapshot(), { now }).healthy).toBe(false)
    polled(h, now, 'staging')
    expect(healthReport(h.snapshot(), { now }).healthy).toBe(true)

    advance(60_001)
    const r = healthReport(h.snapshot(), { now, maxPollAgeMs: 60_000 })
    expect(r.healthy).toBe(false)
    expect(r.environments['prod']!.pollAgeSeconds).toBeCloseTo(60.001, 3)
  })

  it('is unhealthy after a fatal error or once stopped, even with a fresh poll', () => {
    const { h, now } = tracker()
    polled(h, now)
    h.tickCompleted()
    h.stopped()
    expect(healthReport(h.snapshot(), { now }).healthy).toBe(false)

    const other = tracker()
    polled(other.h, other.now)
    other.h.fatalError(new Error('403'))
    expect(healthReport(other.h.snapshot(), { now: other.now }).healthy).toBe(false)
  })
})

describe('renderMetrics', () => {
  it('emits every family with HELP and TYPE, and omits gauges with nothing to report', () => {
    const { h, now } = tracker(['prod'])
    const text = renderMetrics(h.snapshot(), now())
    for (const name of [
      'workos_sync_up',
      'workos_sync_poll_age_seconds',
      'workos_sync_cursor_age_days',
      'workos_sync_ticks_total',
      'workos_sync_events_total',
      'workos_sync_writes_total',
      'workos_sync_errors_total',
    ]) {
      expect(text).toContain(`# HELP ${name} `)
      expect(text).toMatch(new RegExp(`^# TYPE ${name} (gauge|counter)$`, 'm'))
    }
    expect(text).toContain('workos_sync_up 1\n')
    // Never polled: no sample, rather than NaN, so an alert can use absent().
    expect(text).not.toMatch(/^workos_sync_poll_age_seconds\{/m)
    expect(text).not.toMatch(/^workos_sync_cursor_age_days\{/m)
    expect(text).toContain('workos_sync_ticks_total{env="prod"} 0\n')
    expect(text.endsWith('\n')).toBe(true)
  })

  it('reports ages and counters per environment and kind', () => {
    const { h, now, advance } = tracker(['prod'])
    polled(h, now, 'prod', 4)
    advance(2_500)
    const text = renderMetrics(h.snapshot(), now())
    expect(text).toContain('workos_sync_poll_age_seconds{env="prod"} 2.5\n')
    // 3.5s of the 86,400s in a day.
    expect(text).toMatch(/^workos_sync_cursor_age_days\{env="prod"\} 0\.0000405\d*$/m)
    expect(text).toContain('workos_sync_events_total{env="prod"} 4\n')
    expect(text).toContain('workos_sync_writes_total{env="prod",kind="created"} 4\n')
    expect(text).toContain('workos_sync_writes_total{env="prod",kind="skipped"} 0\n')
    expect(text).toContain('workos_sync_errors_total{env="prod"} 0\n')
  })

  it('escapes label values and reports 0 for up once stopped', () => {
    const { h, now } = tracker(['we"ird\\env'])
    h.fatalError(new Error('x'))
    const text = renderMetrics(h.snapshot(), now())
    expect(text).toContain('workos_sync_up 0\n')
    expect(text).toContain('workos_sync_ticks_total{env="we\\"ird\\\\env"} 0\n')
  })
})

describe('startHealthServer', () => {
  let server: HealthServer | undefined
  afterEach(async () => {
    await server?.close()
    server = undefined
  })

  /** A stub engine the test can mutate between requests. */
  function stub() {
    const { h, now, advance } = tracker()
    const engine = { health: (): EngineHealth => h.snapshot() }
    return { h, now, advance, engine }
  }

  it('serves /healthz with the verdict in the status code and the state in the body', async () => {
    const { h, now, advance, engine } = stub()
    server = await startHealthServer(engine, { port: 0, host: '127.0.0.1', now })
    const base = `http://127.0.0.1:${server.port}`

    let res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(200)
    let body = (await res.json()) as { state: string; healthy: boolean }
    expect(body.state).toBe('starting')
    expect(body.healthy).toBe(true)

    polled(h, now)
    h.tickCompleted()
    res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(200)
    expect(res.headers.get('cache-control')).toBe('no-store')

    // The injected clock ages the poll without the test waiting a minute.
    advance(60_001)
    res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(503)
    body = (await res.json()) as { state: string; healthy: boolean }
    expect(body.healthy).toBe(false)

    h.fatalError(new Error('401 on /events'))
    res = await fetch(`${base}/healthz`)
    expect(res.status).toBe(503)
    expect(((await res.json()) as { fatal: string }).fatal).toMatch(/401/)
  })

  it('honours maxPollAgeMs', async () => {
    const { h, now, advance, engine } = stub()
    server = await startHealthServer(engine, { port: 0, host: '127.0.0.1', now, maxPollAgeMs: 1_000 })
    polled(h, now)
    h.tickCompleted()
    advance(999)
    expect((await fetch(`http://127.0.0.1:${server.port}/healthz`)).status).toBe(200)
    advance(2)
    expect((await fetch(`http://127.0.0.1:${server.port}/healthz`)).status).toBe(503)
  })

  it('serves /metrics as Prometheus text and 404s everything else', async () => {
    const { h, now, engine } = stub()
    server = await startHealthServer(engine, { port: 0, host: '127.0.0.1', now })
    polled(h, now, 'prod', 1)
    const base = `http://127.0.0.1:${server.port}`

    const res = await fetch(`${base}/metrics`)
    expect(res.status).toBe(200)
    expect(res.headers.get('content-type')).toMatch(/^text\/plain; version=0\.0\.4/)
    const text = await res.text()
    expect(text).toContain('# TYPE workos_sync_poll_age_seconds gauge')
    expect(text).toContain('workos_sync_events_total{env="prod"} 1\n')

    expect((await fetch(`${base}/`)).status).toBe(404)
    expect((await fetch(`${base}/health`)).status).toBe(404)
    expect((await fetch(`${base}/healthz`, { method: 'POST' })).status).toBe(405)
  })

  it('closes promptly even with a keep-alive connection open', async () => {
    const { engine } = stub()
    server = await startHealthServer(engine, { port: 0, host: '127.0.0.1' })
    const port = server.port
    await fetch(`http://127.0.0.1:${port}/healthz`)
    const started = Date.now()
    await server.close()
    server = undefined
    expect(Date.now() - started).toBeLessThan(1_000)
    await expect(fetch(`http://127.0.0.1:${port}/healthz`)).rejects.toThrow()
  })
})
