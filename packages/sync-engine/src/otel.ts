/**
 * OpenTelemetry metrics.
 *
 * The engine depends on `@opentelemetry/api` and nothing else of
 * OpenTelemetry, the way a library should: the API is a set of no-op
 * singletons until the host process installs an SDK, so a program that never
 * sets one up pays nothing, and one that does gets every instrument here
 * without configuring anything in the engine.
 *
 * Two kinds of instrument live here. The observable ones are read from the
 * same `HealthTracker` that serves `/healthz` and `/metrics`, once per export,
 * so the three can never disagree and nothing in the engine has to remember
 * to update them. The synchronous ones record what the tracker does not
 * keep: how long a poll took, how long each request to WorkOS took and how
 * many were retried or held back by the rate limiter, and how many events
 * were quarantined.
 *
 * Names carry their unit (`poll_age_seconds`, not `poll_age` with unit `s`)
 * so that the OpenTelemetry Prometheus exporter, which appends `_total` to a
 * counter but no unit suffix, produces exactly the series `renderMetrics`
 * does, and the alert rules in the operations doc fit either.
 *
 * The API has no proxy for meters: an instrument created before an SDK is
 * registered is a no-op for good. So instruments are made from whichever
 * meter provider is global at the moment of use and made again when that
 * changes. A host that builds the engine first and installs its SDK before
 * the first poll still gets everything, and a test can install a fresh
 * provider per case.
 */
import {
  metrics,
  type Attributes,
  type Counter,
  type Histogram,
  type Meter,
  type MeterProvider,
} from '@opentelemetry/api'
import { pollAgeMs, type EngineHealth } from './health.js'
import { cursorAgeDays } from './workos/events.js'
import type { RequestPhase } from './workos/client.js'
import { VERSION } from './version.js'

/** The instrumentation scope every instrument in this package is created under. */
export const METER_NAME = '@workos/sync-engine'

/** Request latencies, in seconds: the buckets the HTTP semantic conventions recommend. */
const REQUEST_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10]
/** A poll fetches up to `maxEventsPerTick` events over several pages and writes them, so minutes are possible. */
const TICK_BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300]
/** Waits imposed by the token bucket. The directory bucket refills four a second, so a quarter second is the common case. */
const THROTTLE_BUCKETS = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

/**
 * Instruments made from the global meter provider at the time of use, and
 * made again if it changes. See the module comment for why.
 */
abstract class Instruments<T> {
  private current: { provider: MeterProvider; set: T } | undefined

  protected get set(): T {
    const provider = metrics.getMeterProvider()
    if (!this.current || this.current.provider !== provider) {
      this.current = { provider, set: this.create(provider.getMeter(METER_NAME, VERSION)) }
    }
    return this.current.set
  }

  protected abstract create(meter: Meter): T

  /** Make the instruments now rather than at first use, so observable series exist from the start. */
  register(): void {
    void this.set
  }
}

// ----------------------------------------------------------------- engine

/**
 * How one environment's share of a poll ended. `refused` is a poll whose
 * checkpoint the destination refused because this process had lost the lease.
 */
export type TickOutcome = 'ok' | 'empty' | 'refused' | 'error' | 'fatal'

const WRITE_KINDS = ['created', 'updated', 'skipped', 'deleted'] as const

interface EngineSet {
  tickDuration: Histogram
  rejected: Counter
}

export class EngineInstruments extends Instruments<EngineSet> {
  constructor(
    private readonly health: () => EngineHealth,
    private readonly now: () => number = Date.now,
  ) {
    super()
  }

  protected create(meter: Meter): EngineSet {
    const up = meter.createObservableGauge('workos.sync.up', {
      description: '1 while the engine is running; 0 once it has stopped or hit a fatal error.',
    })
    const pollAge = meter.createObservableGauge('workos.sync.poll_age_seconds', {
      unit: 's',
      description:
        'Seconds since the last successful poll of the Events API. Alert on this. Absent until the first successful poll.',
    })
    const cursorAge = meter.createObservableGauge('workos.sync.cursor_age_days', {
      unit: 'd',
      description:
        'Age of the newest event applied, in days: distance from the 90-day retention cliff, not liveness. Absent until the first event.',
    })
    const ticks = meter.createObservableCounter('workos.sync.ticks', {
      unit: '{poll}',
      description: 'Polls attempted, successful or not.',
    })
    const events = meter.createObservableCounter('workos.sync.events', {
      unit: '{event}',
      description: 'Events applied to the mirror.',
    })
    const writes = meter.createObservableCounter('workos.sync.writes', {
      unit: '{row}',
      description:
        'Rows written by kind. A spike in skipped means events are arriving out of order or a backfill is colliding with the tail.',
    })
    const errors = meter.createObservableCounter('workos.sync.errors', {
      unit: '{poll}',
      description: 'Polls that failed with a retryable error.',
    })

    // One callback for the lot, so every series in an export comes from the
    // same snapshot. A gauge with nothing to report is not observed, for the
    // reason `renderMetrics` gives: an absent series is visible as such, a
    // NaN would silence the alert the metric exists for.
    meter.addBatchObservableCallback(
      (result) => {
        const h = this.health()
        const now = this.now()
        result.observe(up, h.fatal === null && h.state !== 'stopped' ? 1 : 0)
        for (const [env, e] of Object.entries(h.environments)) {
          const age = pollAgeMs(e, now)
          if (age !== null) result.observe(pollAge, age / 1000, { env })
          const days = cursorAgeDays(e.lastEventAt, now)
          if (days !== null) result.observe(cursorAge, days, { env })
          result.observe(ticks, e.counters.ticks, { env })
          result.observe(events, e.counters.events, { env })
          for (const kind of WRITE_KINDS) result.observe(writes, e.counters[kind], { env, kind })
          result.observe(errors, e.counters.errors, { env })
        }
      },
      [up, pollAge, cursorAge, ticks, events, writes, errors],
    )

    return {
      tickDuration: meter.createHistogram('workos.sync.tick.duration_seconds', {
        unit: 's',
        description:
          "How long one environment's share of a poll took, by outcome: ok, empty, refused (the lease was lost), error, fatal.",
        advice: { explicitBucketBoundaries: TICK_BUCKETS },
      }),
      rejected: meter.createCounter('workos.sync.events.rejected', {
        unit: '{event}',
        description: 'Events quarantined by the tail because they failed deterministically twice.',
      }),
    }
  }

  /** One environment's share of a poll finished, however it ended. */
  tick(env: string, outcome: TickOutcome, seconds: number): void {
    this.set.tickDuration.record(seconds, { env, outcome })
  }

  /** A poll set `count` events aside. */
  rejected(env: string, count: number): void {
    if (count > 0) this.set.rejected.add(count, { env })
  }
}

// ----------------------------------------------------------------- client

export type RetryReason = 'network' | 'rate_limited' | 'server_error'

interface RequestAttrs {
  phase: RequestPhase | undefined
  rateKey: string
}

interface ClientSet {
  requestDuration: Histogram
  retries: Counter
  throttle: Histogram
}

export class ClientInstruments extends Instruments<ClientSet> {
  protected create(meter: Meter): ClientSet {
    return {
      requestDuration: meter.createHistogram('workos.sync.api.request.duration_seconds', {
        unit: 's',
        description:
          'Time per HTTP request to the WorkOS API, response body included, one sample per attempt. Labelled with the phase, the rate-limit bucket, and the status code or the error type.',
        advice: { explicitBucketBoundaries: REQUEST_BUCKETS },
      }),
      retries: meter.createCounter('workos.sync.api.retries', {
        unit: '{retry}',
        description: 'Requests retried, by reason: network, rate_limited (429), server_error (5xx).',
      }),
      throttle: meter.createHistogram('workos.sync.api.throttle.duration_seconds', {
        unit: 's',
        description: 'Time a request waited for the local rate limiter before it was sent. Only waits are recorded.',
        advice: { explicitBucketBoundaries: THROTTLE_BUCKETS },
      }),
    }
  }

  /** One attempt finished: with a status, or with the error that stopped it. */
  request(seconds: number, attrs: RequestAttrs & { status?: number; errorType?: string }): void {
    this.set.requestDuration.record(seconds, {
      ...base(attrs),
      ...(attrs.status !== undefined ? { 'http.response.status_code': attrs.status } : {}),
      ...(attrs.errorType !== undefined ? { 'error.type': attrs.errorType } : {}),
    })
  }

  retry(attrs: RequestAttrs & { reason: RetryReason }): void {
    this.set.retries.add(1, { ...base(attrs), reason: attrs.reason })
  }

  throttled(seconds: number, attrs: RequestAttrs): void {
    this.set.throttle.record(seconds, base(attrs))
  }
}

/** The phase is a label only when the client has one; the bucket always is. */
function base(attrs: RequestAttrs): Attributes {
  return { ...(attrs.phase ? { phase: attrs.phase } : {}), rate_key: attrs.rateKey }
}
