/**
 * The engine's own instruments driven by real ticks against a real MongoDB
 * (the bridge and the client instruments are unit-tested in
 * sync-engine/test/otel.test.ts), and the CLI's SDK bootstrap.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { context, metrics, propagation, type Attributes } from '@opentelemetry/api'
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
  type MetricData,
} from '@opentelemetry/sdk-metrics'
import { SyncEngine, WorkOSClient, silentLogger, METER_NAME } from '@workos/sync-engine'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'
import { FaultyDestination } from './helpers/faulty.js'

import { DEFAULT_SERVICE_NAME, otelEnabled, startOtel, type SdkModule } from '../src/otel.js'

const STREAMS = ['users', 'organizations', 'events']

/** An SDK in memory, installed as the global meter provider until `uninstall()`. */
function install() {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE)
  const provider = new MeterProvider({
    readers: [new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 })],
  })
  expect(metrics.setGlobalMeterProvider(provider)).toBe(true)
  return {
    async collect(): Promise<MetricData[]> {
      exporter.reset()
      await provider.forceFlush()
      const rm = exporter.getMetrics().at(-1)
      return rm?.scopeMetrics.find((s) => s.scope.name === METER_NAME)?.metrics ?? []
    },
    async uninstall() {
      metrics.disable()
      await provider.shutdown()
    },
  }
}

const metric = (ms: MetricData[], name: string) => ms.find((m) => m.descriptor.name === name)

function point(m: MetricData | undefined, attrs: Attributes) {
  return m?.dataPoints.find((p) => {
    const keys = new Set([...Object.keys(p.attributes), ...Object.keys(attrs)])
    return [...keys].every((k) => p.attributes[k] === attrs[k])
  })
}

const value = (m: MetricData | undefined, attrs: Attributes = {}) => point(m, attrs)?.value as number | undefined
const histogram = (m: MetricData | undefined, attrs: Attributes) =>
  point(m, attrs)?.value as { count: number; sum: number } | undefined

describe('engine metrics', () => {
  let t: TestDb
  let fake: FakeWorkOS
  let sdk: ReturnType<typeof install>

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
    sdk = install()
  })
  afterEach(async () => {
    await sdk.uninstall()
    await t.close()
  })

  function makeEngine(destination = new FaultyDestination({ client: t.client, database: t.name })) {
    return new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: STREAMS,
        hydrate: 'off',
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: () =>
          new WorkOSClient({
            apiKey: 'sk_test_1',
            apiUrl: 'http://fake.workos.local',
            fetchImpl: fake.fetchImpl,
            maxRetries: 0,
            pageSize: 100,
          }),
      },
    )
  }

  it("times each environment's poll by outcome, with the bridge and the client following along", async () => {
    fake.createUser()
    fake.createOrganization()
    const engine = makeEngine()
    await engine.backfill()
    await engine.tick() // two events: ok
    await engine.tick() // nothing new: empty

    const ms = await sdk.collect()
    const duration = metric(ms, 'workos.sync.tick.duration_seconds')
    expect(histogram(duration, { env: 'prod', outcome: 'ok' })?.count).toBe(1)
    expect(histogram(duration, { env: 'prod', outcome: 'empty' })?.count).toBe(1)
    expect(duration?.dataPoints).toHaveLength(2)

    // The observable series were made at construction and read live state.
    expect(value(metric(ms, 'workos.sync.up'))).toBe(1)
    expect(value(metric(ms, 'workos.sync.ticks'), { env: 'prod' })).toBe(2)
    expect(value(metric(ms, 'workos.sync.events'), { env: 'prod' })).toBe(2)
    expect(value(metric(ms, 'workos.sync.poll_age_seconds'), { env: 'prod' })).toBeGreaterThanOrEqual(0)

    // The client the engine built is instrumented too, by phase and by the
    // rate-limit bucket the endpoint draws on: users are under /user_management.
    const requests = metric(ms, 'workos.sync.api.request.duration_seconds')
    const ok = (phase: string, rate_key: string) =>
      histogram(requests, { phase, rate_key, 'http.response.status_code': 200 })?.count ?? 0
    expect(ok('tail', 'global')).toBeGreaterThanOrEqual(2)
    expect(ok('backfill', 'global')).toBe(1)
    expect(ok('backfill', 'user_management')).toBe(1)
  })

  it('counts a failed poll as an error and the events a later one quarantines', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const destination = new FaultyDestination({ client: t.client, database: t.name })
    destination.poison = bad['id'] as string
    const engine = makeEngine(destination)

    await engine.tick() // first sighting: the poll fails and the cursor stays
    await engine.tick() // second: the poison event is set aside, the rest written

    const ms = await sdk.collect()
    const duration = metric(ms, 'workos.sync.tick.duration_seconds')
    expect(histogram(duration, { env: 'prod', outcome: 'error' })?.count).toBe(1)
    expect(histogram(duration, { env: 'prod', outcome: 'ok' })?.count).toBe(1)
    expect(value(metric(ms, 'workos.sync.events.rejected'), { env: 'prod' })).toBe(1)
    expect(value(metric(ms, 'workos.sync.errors'), { env: 'prod' })).toBe(1)
    expect(value(metric(ms, 'workos.sync.ticks'), { env: 'prod' })).toBe(2)
  })
})

describe('WORKOS_SYNC_OTEL', () => {
  afterEach(() => {
    vi.unstubAllEnvs()
    // The real SDK registers these and leaves them on shutdown.
    metrics.disable()
    context.disable()
    propagation.disable()
  })

  it('is off unless asked, and refuses what it cannot read', async () => {
    expect(otelEnabled({})).toBe(false)
    expect(otelEnabled({ WORKOS_SYNC_OTEL: '' })).toBe(false)
    expect(otelEnabled({ WORKOS_SYNC_OTEL: '0' })).toBe(false)
    expect(otelEnabled({ WORKOS_SYNC_OTEL: 'false' })).toBe(false)
    expect(otelEnabled({ WORKOS_SYNC_OTEL: '1' })).toBe(true)
    expect(otelEnabled({ WORKOS_SYNC_OTEL: ' True ' })).toBe(true)
    expect(() => otelEnabled({ WORKOS_SYNC_OTEL: 'maybe' })).toThrow(/invalid WORKOS_SYNC_OTEL/)
    await expect(startOtel({})).resolves.toBeNull()
  })

  it('starts the SDK with a service name of its own unless OTEL_SERVICE_NAME is set, and stops it', async () => {
    const configs: unknown[] = []
    let started = 0
    let stopped = 0
    const sdkNode: SdkModule = {
      NodeSDK: class {
        constructor(configuration?: { serviceName?: string }) {
          configs.push(configuration)
        }
        start() {
          started++
        }
        async shutdown() {
          stopped++
        }
      },
    }
    const load = async () => sdkNode

    const handle = await startOtel({ WORKOS_SYNC_OTEL: '1' }, load)
    expect(configs).toEqual([{ serviceName: DEFAULT_SERVICE_NAME }])
    expect(started).toBe(1)
    await handle!.shutdown()
    expect(stopped).toBe(1)

    await startOtel({ WORKOS_SYNC_OTEL: 'yes', OTEL_SERVICE_NAME: 'mirror' }, load)
    expect(configs[1]).toEqual({})
  })

  it('says what is missing when the SDK is not installed, and passes other failures through', async () => {
    const missing = async () => {
      throw Object.assign(new Error("Cannot find package '@opentelemetry/sdk-node'"), { code: 'ERR_MODULE_NOT_FOUND' })
    }
    await expect(startOtel({ WORKOS_SYNC_OTEL: '1' }, missing)).rejects.toThrow(/not installed/)
    const broken = async () => {
      throw new Error('boom')
    }
    await expect(startOtel({ WORKOS_SYNC_OTEL: '1' }, broken)).rejects.toThrow('boom')
  })

  it('installs the real SDK as the global meter provider, configured from OTEL_* variables', async () => {
    vi.stubEnv('OTEL_METRICS_EXPORTER', 'console')
    vi.stubEnv('OTEL_TRACES_EXPORTER', 'none')
    vi.stubEnv('OTEL_LOGS_EXPORTER', 'none')
    const before = metrics.getMeterProvider()
    const handle = await startOtel({ WORKOS_SYNC_OTEL: '1' })
    expect(handle).not.toBeNull()
    const provider = metrics.getMeterProvider()
    expect(provider).not.toBe(before)
    expect(typeof (provider as { forceFlush?: unknown }).forceFlush).toBe('function')
    await handle!.shutdown()
  })
})
