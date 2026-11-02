/**
 * Health of a running engine, kept in memory and served over HTTP.
 *
 * `status()` is the thorough view: it opens the destination and queries every
 * stream. That is the wrong shape for a probe, which fires every few seconds
 * and has to answer even when the database is the thing that is broken.
 * Everything here is bookkeeping `tick()` already does, recorded in memory
 * as it happens and read synchronously, so a probe never touches the
 * destination and never waits on it.
 *
 * Two readers are served from the same state:
 *
 *   GET /healthz   200 or 503 with a JSON body, for Docker and Kubernetes
 *   GET /metrics   Prometheus text exposition, for scraping
 *
 * The number that matters is poll age, for the reason the operations doc
 * gives: a stalled sync is silent, and the only thing that reliably moves
 * while it is healthy is the timestamp of the last successful poll.
 */
import { createServer } from 'node:http'
import { cursorAgeDays } from './workos/events.js'
import type { WriteStats } from './types.js'

export interface HealthCounters {
  /** Polls attempted for this environment, successful or not. */
  ticks: number
  /** Events applied. */
  events: number
  created: number
  updated: number
  /** Rows rejected by the staleness gate or unchanged. Graph this. */
  skipped: number
  deleted: number
  /** Polls that failed with a retryable error. Fatal ones end the process instead. */
  errors: number
}

export interface EnvironmentHealth {
  /** When the last successful poll finished, event or no event. Null before the first. */
  lastPolledAt: string | null
  /** `created_at` of the newest event applied. Null until the first event. */
  lastEventAt: string | null
  /** Last processed event id. Null until the first event. */
  cursor: string | null
  /**
   * The most recent poll failure. It is not cleared by a later success: the
   * counters say whether failures are ongoing, and the message says what the
   * last one was, which is the question an operator asks first.
   */
  lastError: { at: string; message: string } | null
  counters: HealthCounters
}

export interface EngineHealth {
  /**
   * `starting` until the first `tick()` completes, `listening` while polls
   * are running, `stopped` once `listen()` has returned or the engine gave
   * up. A probe treats `starting` as healthy so a slow schema init or a long
   * first page does not get the process killed before it has done anything.
   */
  state: 'starting' | 'listening' | 'stopped'
  /**
   * Set when the engine stopped on an error that retrying would not fix: a
   * rejected credential or a cursor that pages backwards. The process is
   * expected to exit shortly after; until it does, the probe reports 503.
   */
  fatal: string | null
  /** When the engine was constructed. */
  startedAt: string
  environments: Record<string, EnvironmentHealth>
}

const emptyCounters = (): HealthCounters => ({
  ticks: 0,
  events: 0,
  created: 0,
  updated: 0,
  skipped: 0,
  deleted: 0,
  errors: 0,
})

/**
 * The bookkeeping behind `SyncEngine.health()`. It only ever records what the
 * engine tells it, so it can be unit-tested without a destination, and the
 * engine's own edits stay down to a handful of one-line calls.
 */
export class HealthTracker {
  private state: EngineHealth['state'] = 'starting'
  private fatal: string | null = null
  private readonly startedAt: string
  private readonly envs = new Map<string, EnvironmentHealth>()

  constructor(
    environments: string[],
    private readonly now: () => number = Date.now,
  ) {
    this.startedAt = new Date(now()).toISOString()
    // Pre-register every environment so a never-polled one is visible as
    // such, rather than silently absent from the report.
    for (const name of environments) this.envs.set(name, emptyEnvironment())
  }

  private env(name: string): EnvironmentHealth {
    let e = this.envs.get(name)
    if (!e) {
      e = emptyEnvironment()
      this.envs.set(name, e)
    }
    return e
  }

  /** A poll completed without error. `events` and `stats` are zero when nothing arrived. */
  polled(
    name: string,
    result: {
      polledAt: string
      cursor: string | null
      lastEventAt: string | null
      events?: number
      stats?: WriteStats
    },
  ): void {
    const e = this.env(name)
    e.lastPolledAt = result.polledAt
    e.cursor = result.cursor
    e.lastEventAt = result.lastEventAt
    e.counters.ticks++
    e.counters.events += result.events ?? 0
    if (result.stats) {
      e.counters.created += result.stats.created
      e.counters.updated += result.stats.updated
      e.counters.skipped += result.stats.skipped
      e.counters.deleted += result.stats.deleted
    }
  }

  /** A poll failed with something the next poll may recover from. */
  failed(name: string, err: unknown): void {
    const e = this.env(name)
    e.counters.ticks++
    e.counters.errors++
    e.lastError = { at: new Date(this.now()).toISOString(), message: String(err) }
  }

  /** The engine is stopping on an error it will not retry. */
  fatalError(err: unknown): void {
    this.fatal = String(err)
    this.state = 'stopped'
  }

  /** A whole `tick()` finished: the engine is past its first poll. */
  tickCompleted(): void {
    if (this.state === 'starting') this.state = 'listening'
  }

  /** `listen()` was called. Only a restart after `stop()` changes anything. */
  listening(): void {
    if (this.state === 'stopped' && this.fatal === null) this.state = 'listening'
  }

  /** `listen()` returned, for whatever reason. A fatal error already set this. */
  stopped(): void {
    this.state = 'stopped'
  }

  /** A copy, so a caller holding the result cannot alter the running state. */
  snapshot(): EngineHealth {
    const environments: Record<string, EnvironmentHealth> = {}
    for (const [name, e] of this.envs) {
      environments[name] = {
        ...e,
        lastError: e.lastError ? { ...e.lastError } : null,
        counters: { ...e.counters },
      }
    }
    return { state: this.state, fatal: this.fatal, startedAt: this.startedAt, environments }
  }
}

function emptyEnvironment(): EnvironmentHealth {
  return { lastPolledAt: null, lastEventAt: null, cursor: null, lastError: null, counters: emptyCounters() }
}

// ------------------------------------------------------------- evaluation

export interface HealthCheckOptions {
  /**
   * Oldest `lastPolledAt` that still counts as healthy. The default is
   * twelve idle poll intervals at the default interval, so a transient API
   * error that the client retries through never flips the probe.
   */
  maxPollAgeMs?: number
  /** Injectable clock, so a test can age the last poll without sleeping. */
  now?: () => number
}

/** What `/healthz` returns: the health state plus the verdict and the number it was based on. */
export interface HealthReport extends EngineHealth {
  healthy: boolean
  environments: Record<string, EnvironmentHealth & { pollAgeSeconds: number | null }>
}

export const DEFAULT_MAX_POLL_AGE_MS = 60_000

function pollAgeMs(e: EnvironmentHealth, now: number): number | null {
  if (!e.lastPolledAt) return null
  const t = Date.parse(e.lastPolledAt)
  return Number.isFinite(t) ? Math.max(0, now - t) : null
}

/**
 * The probe verdict. Healthy while starting, because the process is alive
 * and has not had a chance to poll; after that, every environment has to
 * have polled successfully inside the window. A fatal error or a finished
 * `listen()` is unhealthy whatever the timestamps say, because nothing will
 * move them again.
 */
export function healthReport(health: EngineHealth, opts: HealthCheckOptions = {}): HealthReport {
  const now = opts.now ? opts.now() : Date.now()
  const max = opts.maxPollAgeMs ?? DEFAULT_MAX_POLL_AGE_MS
  const environments: HealthReport['environments'] = {}
  let fresh = true
  for (const [name, e] of Object.entries(health.environments)) {
    const age = pollAgeMs(e, now)
    environments[name] = { ...e, pollAgeSeconds: age === null ? null : age / 1000 }
    if (age === null || age > max) fresh = false
  }
  const healthy = health.fatal === null && health.state !== 'stopped' && (health.state === 'starting' || fresh)
  return { ...health, healthy, environments }
}

// ---------------------------------------------------------------- metrics

/**
 * Prometheus text exposition (version 0.0.4), hand-written because it is a
 * dozen lines and a client library would be the project's second runtime
 * dependency. A gauge with nothing to report is omitted rather than emitted
 * as NaN: `NaN > 300` is false, so a NaN sample would silence the very alert
 * the metric exists for, while an absent series is at least visible as such.
 */
export function renderMetrics(health: EngineHealth, now: number = Date.now()): string {
  const lines: string[] = []
  const family = (name: string, type: 'gauge' | 'counter', help: string) => {
    lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`)
  }
  const sample = (name: string, labels: Record<string, string>, value: number) => {
    const l = Object.entries(labels)
      .map(([k, v]) => `${k}="${escapeLabel(v)}"`)
      .join(',')
    lines.push(`${name}${l ? `{${l}}` : ''} ${value}`)
  }
  const envs = Object.entries(health.environments)

  family('workos_sync_up', 'gauge', '1 while the engine is running; 0 once it has stopped or hit a fatal error.')
  sample('workos_sync_up', {}, health.fatal === null && health.state !== 'stopped' ? 1 : 0)

  family(
    'workos_sync_poll_age_seconds',
    'gauge',
    'Seconds since the last successful poll of the Events API. Alert on this. Absent until the first successful poll.',
  )
  for (const [env, e] of envs) {
    const age = pollAgeMs(e, now)
    if (age !== null) sample('workos_sync_poll_age_seconds', { env }, age / 1000)
  }

  family(
    'workos_sync_cursor_age_days',
    'gauge',
    'Age of the newest event applied, in days: distance from the 90-day retention cliff, not liveness. Absent until the first event.',
  )
  for (const [env, e] of envs) {
    const age = cursorAgeDays(e.lastEventAt, now)
    if (age !== null) sample('workos_sync_cursor_age_days', { env }, age)
  }

  family('workos_sync_ticks_total', 'counter', 'Polls attempted, successful or not.')
  for (const [env, e] of envs) sample('workos_sync_ticks_total', { env }, e.counters.ticks)

  family('workos_sync_events_total', 'counter', 'Events applied to the mirror.')
  for (const [env, e] of envs) sample('workos_sync_events_total', { env }, e.counters.events)

  family(
    'workos_sync_writes_total',
    'counter',
    'Rows written by kind. A spike in skipped means events are arriving out of order or a backfill is colliding with the tail.',
  )
  for (const [env, e] of envs) {
    for (const kind of ['created', 'updated', 'skipped', 'deleted'] as const) {
      sample('workos_sync_writes_total', { env, kind }, e.counters[kind])
    }
  }

  family('workos_sync_errors_total', 'counter', 'Polls that failed with a retryable error.')
  for (const [env, e] of envs) sample('workos_sync_errors_total', { env }, e.counters.errors)

  // The exposition format wants a trailing newline on the last line.
  return lines.join('\n') + '\n'
}

/** Label values may hold anything an operator put in WORKOS_ENVIRONMENT_NAME. */
function escapeLabel(v: string): string {
  return v.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')
}

// ----------------------------------------------------------------- server

export interface HealthServerOptions extends HealthCheckOptions {
  /** 0 picks a free port, which the returned handle reports. */
  port: number
  /**
   * Defaults to every interface, because the common caller is a container
   * whose probe arrives from outside the process's own loopback namespace.
   */
  host?: string
}

export interface HealthServer {
  port: number
  close(): Promise<void>
}

/**
 * Serve `/healthz` and `/metrics` for an engine. Anything with a `health()`
 * is accepted rather than `SyncEngine` itself so this module does not import
 * the engine that imports it, and so a test can hand it a plain object.
 */
export function startHealthServer(
  engine: { health(): EngineHealth },
  opts: HealthServerOptions,
): Promise<HealthServer> {
  const host = opts.host ?? '0.0.0.0'
  const check: HealthCheckOptions = { maxPollAgeMs: opts.maxPollAgeMs, now: opts.now }
  const now = () => (opts.now ? opts.now() : Date.now())

  const server = createServer((req, res) => {
    const path = (req.url ?? '/').split('?')[0]
    if (path !== '/healthz' && path !== '/metrics') {
      res.writeHead(404, { 'content-type': 'text/plain' }).end('not found\n')
      return
    }
    if (req.method !== 'GET' && req.method !== 'HEAD') {
      res.writeHead(405, { allow: 'GET, HEAD' }).end()
      return
    }
    if (path === '/healthz') {
      const report = healthReport(engine.health(), check)
      res.writeHead(report.healthy ? 200 : 503, {
        'content-type': 'application/json',
        // A probe must never be answered from a cache in front of the process.
        'cache-control': 'no-store',
      })
      res.end(JSON.stringify(report))
      return
    }
    res.writeHead(200, {
      'content-type': 'text/plain; version=0.0.4; charset=utf-8',
      'cache-control': 'no-store',
    })
    res.end(renderMetrics(engine.health(), now()))
  })
  // Probes reconnect every time; holding idle sockets open only delays shutdown.
  server.keepAliveTimeout = 1_000

  return new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(opts.port, host, () => {
      server.off('error', reject)
      const addr = server.address()
      const port = typeof addr === 'object' && addr ? addr.port : opts.port
      resolve({
        port,
        close: () =>
          new Promise<void>((done) => {
            // Without this, close() waits for every keep-alive connection to
            // time out, and a SIGTERM takes seconds longer than it should.
            server.closeAllConnections()
            server.close(() => done())
          }),
      })
    })
  })
}
