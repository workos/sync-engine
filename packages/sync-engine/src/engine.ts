/**
 * The sync engine.
 *
 * Lifecycle, in the order that keeps the mirror correct:
 *
 *   1. capture   — record the current position in the event stream
 *   2. backfill  — page the REST list endpoints for everything that exists
 *   3. drain     — replay events from the captured cursor
 *   4. tail      — poll forever, checkpointing after each durable write
 *
 * Steps 2 and 3 overlap on purpose. Anything that changes mid-backfill is
 * replayed in step 3, and the staleness gate decides which version wins.
 */
import type { Destination, FreshnessRow, RejectedEvent } from './destination.js'
import { StateStore } from './state.js'
import { STREAMS, streamByName, defaultStreams, subscribedEventTypes } from './catalog.js'
import { WorkOSClient, WorkOSApiError } from './workos/client.js'
import {
  fetchEvents,
  checkCursorAge,
  cursorAgeDays,
  CursorDirectionError,
  EVENT_RETENTION_DAYS,
} from './workos/events.js'
import { applyEvents, HydrationError } from './sync/apply.js'
import { applyWithQuarantine, eventObjectId, isTransientError, type SuspectMap } from './sync/quarantine.js'
import { backfillStream, type BackfillStreamResult } from './sync/backfill.js'
import { diffStream, freshness, refreshStream, type DiffResult } from './sync/reconcile.js'
import { createLogger } from './logger.js'
import { redacting } from './redact.js'
import type {
  EnvironmentConfig,
  HydrateMode,
  Logger,
  StreamDef,
  SyncEngineConfig,
  WorkOSEvent,
  WriteStats,
} from './types.js'
import { addStats, emptyStats, DEFAULT_HYDRATE } from './types.js'

export interface EnvironmentStatus {
  /** Last processed event id. Null until the first event is applied. */
  cursor: string | null
  /** Bootstrap position recorded by captureCursor, used until a cursor exists. */
  rangeStart: string | null
  /** Whether a starting position has been recorded at all. */
  started: boolean
  /**
   * Age of the newest event applied, in days. This is the distance from the
   * 90-day retention cliff, not a liveness signal: a quiet environment makes
   * it grow while the sync is healthy. Null until the first event is applied.
   */
  cursorAgeDays: number | null
  /** When the last successful poll finished, event or no event. Null before the first poll. */
  lastPolledAt: string | null
  /** Seconds since `lastPolledAt`. This is the number to alert on for a stalled sync. */
  pollAgeSeconds: number | null
  /**
   * Events the tail set aside because they failed deterministically. The
   * cursor has moved past them, so nothing else is stalled, but they are not
   * in the mirror until `replayRejected` succeeds. Alert on this being non-zero.
   */
  rejected: number
  streams: FreshnessRow[]
}

export interface EngineDeps {
  /** Injectable for tests; one client per environment. */
  clientFor?: (env: EnvironmentConfig) => WorkOSClient
}

export class SyncEngine {
  readonly streams: StreamDef[]
  private destination: Destination
  private state: StateStore
  private logger: Logger
  private clients = new Map<string, WorkOSClient>()
  private stopping = false
  /**
   * Events that failed on the previous poll, per environment. In memory on
   * purpose: a restart forgets them, and the worst that costs is one more
   * poll before a poison event is quarantined.
   */
  private suspects = new Map<string, SuspectMap>()
  /** Event types passed to `GET /events`, which requires them. */
  readonly eventTypes: string[]
  /** Effective hydration setting, after defaults. */
  readonly hydrate: HydrateMode | Record<string, HydrateMode>

  constructor(
    private config: SyncEngineConfig,
    private deps: EngineDeps = {},
  ) {
    if (!config.destination) throw new Error('a destination is required')
    // Credential paths a stream declares are stripped before any row reaches
    // the destination, whichever database is behind it.
    this.destination = redacting(config.destination)
    this.state = new StateStore(this.destination)
    this.logger = config.logger ?? createLogger('info')
    // Every event is archived to `events` before it is routed, so that table
    // has to exist whatever else is selected.
    const names = new Set([...(config.streams ?? defaultStreams()), 'events'])
    for (const n of names) {
      if (!streamByName(n)) throw new Error(`unknown stream: ${n}`)
    }
    const extra = config.extraColumns ?? {}
    this.streams = STREAMS.filter((s) => names.has(s.name)).map((s) =>
      extra[s.name]?.length ? { ...s, columns: [...s.columns, ...extra[s.name]!] } : s,
    )
    for (const name of Object.keys(extra)) {
      if (!names.has(name)) throw new Error(`extraColumns names a stream that is not selected: ${name}`)
    }
    if (this.streams.length === 0) throw new Error('no streams selected')
    this.hydrate = config.hydrate ?? DEFAULT_HYDRATE
    // The events endpoint rejects a request with no `events` filter, so the
    // subscription is derived from whichever streams are being synced.
    this.eventTypes = config.eventTypes ?? subscribedEventTypes(this.streams)
    if (this.eventTypes.length === 0) {
      throw new Error(
        'no event types to subscribe to: GET /events requires at least one, ' +
          'and none of the selected streams declare any',
      )
    }
    if (!config.environments?.length) throw new Error('at least one environment is required')
    // A mirror row is only meaningful with the environment it came from: ids
    // are unique per environment and there is no environment discriminator in
    // the payloads, so the label has to be supplied by configuration.
    const seen = new Set<string>()
    for (const env of config.environments) {
      if (seen.has(env.name)) throw new Error(`duplicate environment name: ${env.name}`)
      seen.add(env.name)
    }
  }

  private clientFor(env: EnvironmentConfig): WorkOSClient {
    let c = this.clients.get(env.name)
    if (!c) {
      c = this.deps.clientFor
        ? this.deps.clientFor(env)
        : new WorkOSClient({
            apiKey: env.apiKey,
            apiUrl: env.apiUrl,
            logger: this.logger,
            pageSize: this.config.pageSize ?? 100,
          })
      this.clients.set(env.name, c)
    }
    return c
  }

  /** Connect and apply the schema. Idempotent. */
  async init(): Promise<void> {
    await this.destination.init(this.streams, this.logger)
  }

  /**
   * Record where the tail should begin, before backfill starts, so that
   * nothing changing mid-backfill is missed.
   *
   * There is deliberately no API call here. `GET /events` always returns
   * oldest-first — verified against the live API, where `order=asc`,
   * `order=desc` and omitting the parameter return byte-identical results — so
   * asking for "the newest event id" would mean paging the entire retention
   * window. Recording the wall clock and bootstrapping with `range_start`
   * achieves the same thing for free.
   */
  async captureCursor(env: EnvironmentConfig): Promise<string | null> {
    const existing = await this.state.getTail(env.name)
    if (existing?.cursor || existing?.rangeStart) {
      // A cursor older than retention points at events that no longer exist.
      // Replaying from it is impossible, and this backfill is re-establishing
      // current state anyway, so start the tail from now instead.
      const age = cursorAgeDays(existing.lastEventAt)
      if (age !== null && age >= EVENT_RETENTION_DAYS) {
        this.logger.warn('events cursor is past retention; re-capturing the starting position', {
          env: env.name,
          cursor: existing.cursor,
          ageDays: Math.round(age),
        })
        await this.state.clearTail(env.name)
      } else {
        return existing.cursor ?? null
      }
    }
    const rangeStart = new Date().toISOString()
    await this.state.setTail(env.name, { cursor: null, rangeStart })
    this.logger.info('captured starting position', { env: env.name, rangeStart })
    return null
  }

  async backfill(options: { resume?: boolean } = {}): Promise<BackfillStreamResult[]> {
    await this.init()
    const results: BackfillStreamResult[] = []
    for (const env of this.config.environments) {
      await this.captureCursor(env)
      const api = this.clientFor(env)
      // Parents before children: memberships need organizations, directory
      // users need directories.
      const ordered = [...this.streams].sort((a, b) => order(a) - order(b))
      for (const stream of ordered) {
        if (stream.mode === 'append') continue
        try {
          const r = await backfillStream(this.destination, api, this.state, stream, {
            env: env.name,
            logger: this.logger,
            resume: options.resume,
          })
          results.push(r)
        } catch (err) {
          if (isFatal(err)) throw err
          // Stream-scoped failure: other streams continue, and this stream's
          // state is left where it was so the next run resumes.
          this.logger.error('backfill failed', {
            env: env.name,
            stream: stream.name,
            error: String(err),
          })
          results.push({
            stream: stream.name,
            stats: emptyStats(),
            pages: 0,
            skippedReason: `failed: ${String(err)}`,
          })
        }
      }
    }
    return results
  }

  private suspectsFor(env: string): SuspectMap {
    let m = this.suspects.get(env)
    if (!m) {
      m = new Map()
      this.suspects.set(env, m)
    }
    return m
  }

  /** One poll of the events tail across every environment. */
  async tick(): Promise<{ processed: number; stats: WriteStats; rejected: number }> {
    await this.init()
    let processed = 0
    let stats = emptyStats()
    let rejected = 0

    for (const env of this.config.environments) {
      const api = this.clientFor(env)
      const tail = await this.state.getTail(env.name)
      checkCursorAge(tail?.lastEventAt, this.logger)

      try {
        const limit = this.config.pageSize ?? 100
        const max = this.config.maxEventsPerTick ?? limit
        const events: WorkOSEvent[] = []
        let cursor = tail?.cursor ?? null
        let pages = 0
        let hasMore = true
        // Pull pages until the budget is met or the stream is caught up, then
        // apply them as one batch so the destination sees one write per
        // stream and the cursor is checkpointed once.
        while (hasMore && events.length < max) {
          const batch = await fetchEvents(
            api,
            {
              after: cursor,
              // Only used until the first real cursor arrives; sending both is a
              // 400 ("Only one of range_start and after may be provided").
              ...(cursor ? {} : { rangeStart: tail?.rangeStart ?? undefined }),
              limit,
              lagWindowMs: this.config.lagWindowMs ?? 5_000,
              events: this.eventTypes,
            },
            this.logger,
          )
          pages++
          events.push(...batch.events)
          cursor = batch.cursor
          hasMore = batch.hasMore
        }
        if (events.length === 0) {
          // Nothing to apply, but the poll succeeded: record that, so a quiet
          // upstream is distinguishable from a stalled sync.
          await this.state.setTail(env.name, {
            cursor: tail?.cursor ?? null,
            lastEventAt: tail?.lastEventAt ?? null,
            rangeStart: tail?.rangeStart ?? null,
            lastPolledAt: new Date().toISOString(),
          })
          continue
        }

        // Applies the batch as before; on a deterministic failure it isolates
        // the event responsible and, once it has failed twice, quarantines it
        // so the cursor can move. See sync/quarantine.ts.
        const result = await applyWithQuarantine(
          this.destination,
          events,
          { env: env.name, logger: this.logger, hydrate: this.hydrate, api },
          this.suspectsFor(env.name),
        )
        for (const row of result.rejected) {
          this.logger.error('event quarantined; the cursor is moving past it', {
            env: env.name,
            eventId: row.event.id,
            eventType: row.event.event,
            objectId: eventObjectId(row.event),
            error: row.error,
            hint: 'inspect with the `rejected` command; fix the cause, then `rejected --replay`',
          })
        }
        rejected += result.rejected.length

        // Checkpoint only after the writes above are durable.
        const last = events[events.length - 1]!
        await this.state.setTail(env.name, {
          cursor,
          lastEventAt: last.created_at,
          rangeStart: tail?.rangeStart ?? null,
          lastPolledAt: new Date().toISOString(),
        })

        processed += events.length
        stats = addStats(stats, result.stats)
        this.logger.info('processed events', {
          env: env.name,
          events: events.length,
          pages,
          cursor,
          ...result.stats,
        })
      } catch (err) {
        if (err instanceof CursorDirectionError) {
          // Loud and fatal: continuing would rewrite the mirror with history.
          this.logger.error('cursor direction check failed; stopping', { error: String(err) })
          throw err
        }
        if (isFatal(err)) {
          // A rejected credential will not fix itself; retrying every poll
          // interval would only hide the problem behind a healthy-looking process.
          this.logger.error('authentication failed; stopping', { env: env.name, error: String(err) })
          throw err
        }
        this.logger.error('tick failed', { env: env.name, error: String(err) })
      }
    }
    return { processed, stats, rejected }
  }

  /** Poll until `stop()`. */
  async listen(): Promise<void> {
    await this.init()
    this.stopping = false
    const interval = this.config.pollIntervalMs ?? 5_000
    this.logger.info('listening', {
      environments: this.config.environments.map((e) => e.name),
      streams: this.streams.length,
      pollIntervalMs: interval,
    })
    while (!this.stopping) {
      const { processed } = await this.tick()
      // Drain fast when there is a backlog; idle politely when there is not.
      if (!this.stopping && processed === 0) await sleep(interval)
    }
  }

  stop(): void {
    this.stopping = true
  }

  async reconcile(options: { repair?: boolean; streams?: string[] } = {}): Promise<{
    freshness: Record<string, FreshnessRow[]>
    diffs: Record<string, DiffResult[]>
  }> {
    await this.init()
    const chosen = options.streams
      ? this.streams.filter((s) => options.streams!.includes(s.name))
      : this.streams
    const out = {
      freshness: {} as Record<string, FreshnessRow[]>,
      diffs: {} as Record<string, DiffResult[]>,
    }
    for (const env of this.config.environments) {
      out.freshness[env.name] = await freshness(this.destination, env.name, chosen)
      const api = this.clientFor(env)
      const diffs: DiffResult[] = []
      for (const stream of chosen) {
        try {
          diffs.push(
            await diffStream(this.destination, api, env.name, stream, {
              repair: options.repair,
              logger: this.logger,
            }),
          )
        } catch (err) {
          if (isFatal(err)) throw err
          this.logger.error('reconcile failed', { stream: stream.name, error: String(err) })
        }
      }
      out.diffs[env.name] = diffs
    }
    return out
  }

  /** Re-list every stream, refreshing fields that event payloads omit. */
  async refresh(): Promise<{ stream: string; refreshed: number }[]> {
    await this.init()
    const out: { stream: string; refreshed: number }[] = []
    for (const env of this.config.environments) {
      const api = this.clientFor(env)
      for (const stream of this.streams) {
        if (!stream.list || stream.list.parent) continue
        out.push(await refreshStream(this.destination, api, env.name, stream))
      }
    }
    return out
  }

  async status(): Promise<Record<string, EnvironmentStatus>> {
    await this.init()
    const out: Record<string, EnvironmentStatus> = {}
    for (const env of this.config.environments) {
      const tail = await this.state.getTail(env.name)
      const polled = tail?.lastPolledAt ? Date.parse(tail.lastPolledAt) : NaN
      out[env.name] = {
        cursor: tail?.cursor ?? null,
        // Set before the first event arrives; the tail bootstraps from here.
        rangeStart: tail?.rangeStart ?? null,
        started: Boolean(tail?.cursor || tail?.rangeStart),
        cursorAgeDays: cursorAgeDays(tail?.lastEventAt),
        lastPolledAt: tail?.lastPolledAt ?? null,
        pollAgeSeconds: Number.isFinite(polled) ? (Date.now() - polled) / 1000 : null,
        // A count, not a listing: `status` runs on every monitoring poll, and
        // each rejected row carries a whole event payload.
        rejected: await this.destination.rejectedCount(env.name),
        streams: await freshness(this.destination, env.name, this.streams),
      }
    }
    return out
  }

  /** Quarantined events per environment, oldest first. */
  async rejected(options: { ids?: string[] } = {}): Promise<Record<string, RejectedEvent[]>> {
    await this.init()
    const out: Record<string, RejectedEvent[]> = {}
    for (const env of this.config.environments) {
      out[env.name] = await this.destination.rejected(env.name, options)
    }
    return out
  }

  /**
   * Put quarantined events back through the normal write path, oldest first,
   * and forget the ones that succeed. Run it after fixing whatever rejected
   * them: a release with the router fix, the generated column altered, the
   * payload corrected upstream. One that fails again stays quarantined with
   * its error updated. The staleness gate still applies, so a replayed event
   * older than the row it targets is a no-op — correct, because later events
   * already carried the newer state.
   */
  async replayRejected(options: { ids?: string[] } = {}): Promise<{
    replayed: number
    stillRejected: { eventId: string; error: string }[]
  }> {
    await this.init()
    let replayed = 0
    const stillRejected: { eventId: string; error: string }[] = []
    for (const env of this.config.environments) {
      const api = this.clientFor(env)
      const rows = await this.destination.rejected(env.name, options)
      for (const row of rows) {
        try {
          await applyEvents(this.destination, [row.raw], {
            env: env.name,
            logger: this.logger,
            hydrate: this.hydrate,
            api,
          })
        } catch (err) {
          // An outage mid-replay is not a verdict on the event. Stop and say
          // so, rather than stamping every remaining row with a connection error.
          if (isFatal(err) || isTransientError(err, this.destination)) throw err
          const error = String(err)
          await this.destination.reject(env.name, [{ event: row.raw, error }])
          stillRejected.push({ eventId: row.eventId, error })
          this.logger.warn('replayed event failed again', { env: env.name, eventId: row.eventId, error })
          continue
        }
        await this.destination.clearRejected(env.name, [row.eventId])
        replayed++
        this.logger.info('replayed rejected event', {
          env: env.name,
          eventId: row.eventId,
          eventType: row.eventType,
        })
      }
    }
    return { replayed, stillRejected }
  }

  /**
   * Erase every trace of one subject: mirror rows and archived event payloads.
   * A mirror inherits the deletion obligations of any other replica of the data.
   */
  async redact(userId: string): Promise<{ table: string; rows: number }[]> {
    await this.init()
    const out: { table: string; rows: number }[] = []
    for (const env of this.config.environments) {
      out.push(...(await this.destination.redact(this.streams, env.name, userId)))
    }
    return out
  }

  /** A verified webhook delivery is a nudge to poll now. */
  async processWebhook(): Promise<{ processed: number }> {
    const { processed } = await this.tick()
    return { processed }
  }

  async close(): Promise<void> {
    this.stop()
    await this.destination.close()
  }
}

/**
 * Errors that no retry and no amount of waiting will fix. The engine stops on
 * these rather than looping, so the failure is visible instead of being a
 * process that looks alive while writing nothing.
 */
function isFatal(err: unknown): boolean {
  // A hydration GET wraps whatever the API answered. A denied credential is
  // no less fatal for having been met on an object GET than on the events
  // page, and without unwrapping it the tail would retry it forever.
  if (err instanceof HydrationError) return isFatal(err.cause)
  return err instanceof WorkOSApiError && (err.status === 401 || err.status === 403)
}

const ORDERING = ['organizations', 'users', 'directories', 'connections']
function order(s: StreamDef): number {
  const i = ORDERING.indexOf(s.name)
  return i === -1 ? 100 : i
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export { streamByName }
