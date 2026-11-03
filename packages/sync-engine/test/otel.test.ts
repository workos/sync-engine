/**
 * The OpenTelemetry surface in isolation: the bridge from `HealthTracker` to
 * observable instruments, the client's request instruments, what happens
 * when the SDK arrives late, and that the Prometheus exporter names the
 * series the way the built-in `/metrics` does. Driving the engine's own
 * instruments takes a destination; sync-postgres/test/otel.test.ts has one.
 */
import { describe, it, expect, afterEach } from 'vitest'
import { metrics, type Attributes } from '@opentelemetry/api'
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
  type MetricData,
  type ResourceMetrics,
} from '@opentelemetry/sdk-metrics'
import { PrometheusSerializer } from '@opentelemetry/exporter-prometheus'
import { EngineInstruments, METER_NAME } from '../src/otel.js'
import { HealthTracker, renderMetrics } from '../src/health.js'
import { WorkOSClient } from '../src/workos/client.js'
import { FakeWorkOS, resetIds } from './helpers/fake-workos.js'

const T0 = Date.parse('2026-10-23T12:00:00.000Z')

/** An SDK in memory, installed as the global meter provider until `uninstall()`. */
function install() {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)
  const provider = new MeterProvider({
    readers: [new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 })],
  })
  expect(metrics.setGlobalMeterProvider(provider)).toBe(true)
  const resourceMetrics = async (): Promise<ResourceMetrics> => {
    exporter.reset()
    await provider.forceFlush()
    return exporter.getMetrics().at(-1)!
  }
  return {
    resourceMetrics,
    /** Export now and return this package's metrics. */
    async collect(): Promise<MetricData[]> {
      const rm = await resourceMetrics()
      return rm.scopeMetrics.find((s) => s.scope.name === METER_NAME)?.metrics ?? []
    },
    async uninstall() {
      metrics.disable()
      await provider.shutdown()
    },
  }
}

type Sdk = ReturnType<typeof install>

const metric = (ms: MetricData[], name: string) => ms.find((m) => m.descriptor.name === name)

/** The data point carrying exactly these attributes, or undefined. */
function point(m: MetricData | undefined, attrs: Attributes) {
  return m?.dataPoints.find((p) => {
    const keys = new Set([...Object.keys(p.attributes), ...Object.keys(attrs)])
    return [...keys].every((k) => p.attributes[k] === attrs[k])
  })
}

const value = (m: MetricData | undefined, attrs: Attributes = {}) => point(m, attrs)?.value as number | undefined
const histogram = (m: MetricData | undefined, attrs: Attributes) =>
  point(m, attrs)?.value as { count: number; sum: number } | undefined

function tracker(envs = ['prod']) {
  let t = T0
  const h = new HealthTracker(envs, () => t)
  return { h, now: () => t, advance: (ms: number) => (t += ms) }
}

function polled(h: HealthTracker, now: () => number, env = 'prod', events = 0) {
  h.polled(env, {
    polledAt: new Date(now()).toISOString(),
    cursor: events ? 'event_01' : null,
    lastEventAt: events ? new Date(now() - 86_400_000).toISOString() : null,
    events,
    stats: { created: events, updated: 0, skipped: 0, deleted: 0 },
  })
}

describe('engine instruments', () => {
  let sdk: Sdk | undefined
  afterEach(async () => {
    await sdk?.uninstall()
    sdk = undefined
  })

  it('bridges the health tracker, with gauges absent until they have something to say', async () => {
    sdk = install()
    const { h, now, advance } = tracker()
    const instruments = new EngineInstruments(() => h.snapshot(), now)
    instruments.register()

    // Before the first poll: up, and counters at zero, so a never-polled
    // environment is visible; no poll age and no cursor age.
    let ms = await sdk.collect()
    expect(value(metric(ms, 'workos.sync.up'))).toBe(1)
    expect(value(metric(ms, 'workos.sync.ticks'), { env: 'prod' })).toBe(0)
    expect(value(metric(ms, 'workos.sync.errors'), { env: 'prod' })).toBe(0)
    expect(metric(ms, 'workos.sync.poll_age_seconds')?.dataPoints ?? []).toHaveLength(0)
    expect(metric(ms, 'workos.sync.cursor_age_days')?.dataPoints ?? []).toHaveLength(0)

    polled(h, now, 'prod', 3)
    advance(2_500)
    ms = await sdk.collect()
    expect(value(metric(ms, 'workos.sync.poll_age_seconds'), { env: 'prod' })).toBeCloseTo(2.5)
    expect(value(metric(ms, 'workos.sync.cursor_age_days'), { env: 'prod' })).toBeCloseTo(1, 2)
    expect(value(metric(ms, 'workos.sync.ticks'), { env: 'prod' })).toBe(1)
    expect(value(metric(ms, 'workos.sync.events'), { env: 'prod' })).toBe(3)
    expect(value(metric(ms, 'workos.sync.writes'), { env: 'prod', kind: 'created' })).toBe(3)
    expect(value(metric(ms, 'workos.sync.writes'), { env: 'prod', kind: 'skipped' })).toBe(0)
    expect(metric(ms, 'workos.sync.writes')?.dataPoints).toHaveLength(4)

    // The counters are cumulative, as an observable counter must report them.
    h.failed('prod', new Error('500 on /events'))
    ms = await sdk.collect()
    expect(value(metric(ms, 'workos.sync.ticks'), { env: 'prod' })).toBe(2)
    expect(value(metric(ms, 'workos.sync.errors'), { env: 'prod' })).toBe(1)

    h.fatalError(new Error('401 on /events'))
    ms = await sdk.collect()
    expect(value(metric(ms, 'workos.sync.up'))).toBe(0)
  })

  it('records the synchronous instruments with their attributes', async () => {
    sdk = install()
    const { h, now } = tracker()
    const instruments = new EngineInstruments(() => h.snapshot(), now)
    instruments.tick('prod', 'ok', 0.3)
    instruments.tick('prod', 'empty', 0.02)
    instruments.tick('prod', 'ok', 0.7)
    instruments.rejected('prod', 0)
    instruments.rejected('prod', 2)

    const ms = await sdk.collect()
    const duration = metric(ms, 'workos.sync.tick.duration_seconds')
    expect(duration?.descriptor.unit).toBe('s')
    expect(histogram(duration, { env: 'prod', outcome: 'ok' })).toMatchObject({ count: 2, sum: 1 })
    expect(histogram(duration, { env: 'prod', outcome: 'empty' })).toMatchObject({ count: 1 })
    expect(value(metric(ms, 'workos.sync.events.rejected'), { env: 'prod' })).toBe(2)
  })

  it('makes its instruments again when the global provider changes', async () => {
    // No SDK yet: the engine is built first, as a host that configures
    // OpenTelemetry after its own startup would do.
    const { h, now } = tracker()
    const instruments = new EngineInstruments(() => h.snapshot(), now)
    instruments.register()
    instruments.tick('prod', 'ok', 1)

    sdk = install()
    instruments.tick('prod', 'ok', 1)
    let ms = await sdk.collect()
    // The record made before the SDK existed went nowhere; the one after did,
    // and the observable series came along without another register().
    expect(histogram(metric(ms, 'workos.sync.tick.duration_seconds'), { env: 'prod', outcome: 'ok' })?.count).toBe(1)
    expect(value(metric(ms, 'workos.sync.up'))).toBe(1)

    // A second provider, as a test suite installs one per case, gets its own.
    await sdk.uninstall()
    sdk = install()
    instruments.tick('prod', 'ok', 1)
    ms = await sdk.collect()
    expect(histogram(metric(ms, 'workos.sync.tick.duration_seconds'), { env: 'prod', outcome: 'ok' })?.count).toBe(1)
  })

  it('names the series the way the built-in /metrics does', async () => {
    sdk = install()
    const { h, now } = tracker()
    polled(h, now, 'prod', 3)
    const at = now() + 1_000
    const instruments = new EngineInstruments(
      () => h.snapshot(),
      () => at,
    )
    instruments.register()

    // No prefix, no timestamps, no target_info, no scope labels: the bare series.
    const text = new PrometheusSerializer(undefined, false, undefined, true, true).serialize(
      await sdk.resourceMetrics(),
    )
    const builtin = renderMetrics(h.snapshot(), at)
    const families = (s: string) => [...s.matchAll(/^# TYPE (\S+) (\S+)$/gm)].map((m) => `${m[1]} ${m[2]}`)
    expect(families(builtin).length).toBeGreaterThan(0)
    for (const family of families(builtin)) expect(families(text)).toContain(family)

    // Same labels, same values, down to the absent series.
    expect(text).toMatch(/^workos_sync_up 1$/m)
    expect(text).toMatch(/^workos_sync_poll_age_seconds\{env="prod"\} 1$/m)
    expect(text).toMatch(/^workos_sync_ticks_total\{env="prod"\} 1$/m)
    expect(text).toMatch(/^workos_sync_writes_total\{env="prod",kind="created"\} 3$/m)
    expect(builtin).toMatch(/^workos_sync_writes_total\{env="prod",kind="created"\} 3$/m)
  })
})

describe('client instruments', () => {
  let sdk: Sdk | undefined
  afterEach(async () => {
    await sdk?.uninstall()
    sdk = undefined
  })

  const clientFor = (fetchImpl: typeof fetch) =>
    new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl })

  it('records every attempt by status or error, the retries, and the throttle waits', async () => {
    sdk = install()
    resetIds()
    const fake = new FakeWorkOS({ rateLimitFirst: 1 })
    fake.createOrganization()
    // One request fails on the socket before any reaches the fake.
    let dropped = false
    const fetchImpl: typeof fetch = (input, init) => {
      if (!dropped) {
        dropped = true
        return Promise.reject(Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNRESET' } }))
      }
      return fake.fetchImpl(input, init)
    }
    const client = clientFor(fetchImpl)

    // The socket error, then the 429, then the page: three attempts, two retries.
    await client.withPhase('backfill').list('/organizations')
    // Five on a directory bucket that holds four: the last one waits.
    for (let i = 0; i < 5; i++) await client.withPhase('reconcile').list('/organizations', {}, 'directory:dir_1')

    const ms = await sdk.collect()
    const duration = metric(ms, 'workos.sync.api.request.duration_seconds')
    expect(duration?.descriptor.unit).toBe('s')
    const global = { phase: 'backfill', rate_key: 'global' }
    expect(histogram(duration, { ...global, 'error.type': 'ECONNRESET' })?.count).toBe(1)
    expect(histogram(duration, { ...global, 'http.response.status_code': 429 })?.count).toBe(1)
    expect(histogram(duration, { ...global, 'http.response.status_code': 200 })?.count).toBe(1)
    expect(
      histogram(duration, { phase: 'reconcile', rate_key: 'directory:dir_1', 'http.response.status_code': 200 })?.count,
    ).toBe(5)

    const retries = metric(ms, 'workos.sync.api.retries')
    expect(value(retries, { ...global, reason: 'network' })).toBe(1)
    expect(value(retries, { ...global, reason: 'rate_limited' })).toBe(1)
    expect(point(retries, { ...global, reason: 'server_error' })).toBeUndefined()

    const throttle = histogram(metric(ms, 'workos.sync.api.throttle.duration_seconds'), {
      phase: 'reconcile',
      rate_key: 'directory:dir_1',
    })
    expect(throttle?.count).toBeGreaterThanOrEqual(1)
    expect(throttle?.sum).toBeGreaterThan(0)
  })

  it('labels a request from an unphased client with the bucket alone', async () => {
    sdk = install()
    resetIds()
    const fake = new FakeWorkOS()
    fake.createOrganization()
    await clientFor(fake.fetchImpl).list('/organizations')
    const ms = await sdk.collect()
    const p = point(metric(ms, 'workos.sync.api.request.duration_seconds'), {
      rate_key: 'global',
      'http.response.status_code': 200,
    })
    expect(p).toBeDefined()
    expect(p?.attributes).not.toHaveProperty('phase')
  })

  it('shares one set of instruments across phases, made again when the provider changes', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    fake.createOrganization()
    const client = clientFor(fake.fetchImpl)
    await client.list('/organizations') // no SDK: nowhere to go
    sdk = install()
    await client.withPhase('tail').list('/organizations')
    await client.withPhase('hydrate').list('/organizations')
    const ms = await sdk.collect()
    const duration = metric(ms, 'workos.sync.api.request.duration_seconds')
    expect(duration?.dataPoints).toHaveLength(2)
    expect(histogram(duration, { phase: 'tail', rate_key: 'global', 'http.response.status_code': 200 })?.count).toBe(1)
    expect(histogram(duration, { phase: 'hydrate', rate_key: 'global', 'http.response.status_code': 200 })?.count).toBe(
      1,
    )
  })
})
