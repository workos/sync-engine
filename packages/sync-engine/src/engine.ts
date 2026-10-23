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
 *
 * Step 4 runs under a single-writer lease held in the destination. Without
 * it, a rolling deploy has the old and the new process both advancing the
 * same cursor for as long as they overlap. Everything else — backfill,
 * reconcile, refresh, status, redact — is idempotent and runs alongside the
 * tail without taking it.
 */
import os from 'node:os'
import { randomBytes } from 'node:crypto'
import type { Destination, FreshnessRow, Lease, RejectedEvent } from './destination.js'
import { StateStore, type TailState, type TailUpdate } from './state.js'
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
import { HealthTracker, type EngineHealth } from './health.js'
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
  /** Which process is tailing this environment right now. Null when nothing is. */
  lease: Lease | null
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

/**
 * Thrown by `tick()` when another process holds the events-tail lease. A
 * cron-driven tick must not wait — the next run is minutes away anyway — so
 * it fails fast and names the holder, which is usually a `listen` that
 * someone forgot was running.
 */
export class LeaseHeldError extends Error {
  constructor(
    readonly env: string,
    readonly holder: string | null,
    readonly expiresAt: string | null,
  ) {
    super(
      `events tail lease for environment "${env}" is held by ${holder ?? 'another process'}` +
        (expiresAt ? ` until ${expiresAt}` : ''),
    )
    this.name = 'LeaseHeldError'
  }
}

const DEFAULT_LEASE_TTL_MS = 60_000
/** Waiting on a lease is the normal state during a deploy, not an incident; one line a minute is plenty. */
const LEASE_WARN_INTERVAL_MS = 60_000

/** Why an acquisition pass stopped: the environment refused, and who has it. */
interface LeaseRefusal {
  env: string
  holder: string | null
  expiresAt: string | null
}

export class SyncEngine {
  readonly streams: StreamDef[]
  private destination: Destination
  private state: StateStore
  private logger: Logger
  private clients = new Map<string, WorkOSClient>()
  private stopping = false
  /** In-memory bookkeeping behind `health()`; see health.ts. */
  private healthTracker: HealthTracker
  /**
   * The identity leases are held under. Host and pid make it recognisable in
   * logs; the random suffix keeps two engines in one process (or a recycled
   * pid on a restarted host) from passing as the same holder.
   */
  private readonly holder = `${os.hostname()}:${process.pid}:${randomBytes(3).toString('hex')}`
  /**
   * Environments whose lease this engine currently holds, through `listen()`
   * or a standalone `tick()`. The heartbeat renews exactly these.
   */
  private held = new Set<string>()
  private heartbeat: NodeJS.Timeout | undefined
  /** The heartbeat renewal in flight, so a release can wait for it rather than race it. */
  private renewal: Promise<void> | undefined
  /**
   * Serialises everything in this engine that takes, polls under, or gives
   * back a lease. `tick()` and `listen()` share one `holder`, so the database
   * cannot tell their claims apart: without this, a `tick()` overlapping
   * `listen()`'s startup would acquire the same lease, poll alongside the
   * listener, and then release the listener's claim on return.
   */
  private leaseWork: Promise<unknown> = Promise.resolve()
  private lastLeaseWarn = 0
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
    this.healthTracker = new HealthTracker([...seen])
    // A zero TTL expires on arrival, so every acquire would succeed and the
    // lease would exclude nobody; NaN fails in SQL on the first poll instead.
    // Both are caught here, before anything runs, rather than at either end.
    const ttl = config.leaseTtlMs
    if (ttl !== undefined && !(Number.isFinite(ttl) && Math.floor(ttl) >= 1)) {
      throw new Error(`leaseTtlMs must be a number of milliseconds of at least 1, got ${String(ttl)}`)
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

  /**
   * One poll of the events tail across every environment, under the lease.
   * Throws `LeaseHeldError` rather than waiting when another process has it.
   * The lease is released on return unless `listen()` in this engine already
   * holds it, so a webhook nudge never drops the tail's own lease. A tick
   * that overlaps the listener runs after its current poll, never alongside.
   */
  async tick(): Promise<{ processed: number; stats: WriteStats; rejected: number }> {
    await this.init()
    return this.serialized(async () => {
      const taken: string[] = []
      // A standalone tick has no loop renewing for it, and the checkpoint is
      // refused once the lease lapses, so it runs the heartbeat itself: a
      // cron tick slower than the TTL would otherwise replay the same batch
      // forever. One running under the listener is covered by the listener's.
      const own = this.heartbeat ? undefined : this.startHeartbeat()
      try {
        for (const env of this.config.environments) {
          if (this.held.has(env.name)) continue
          const r = await this.destination.acquireLease(env.name, this.holder, this.leaseTtlMs)
          if (!r.acquired) throw new LeaseHeldError(env.name, r.holder, r.expiresAt)
          taken.push(env.name)
          this.held.add(env.name)
        }
        return await this.poll()
      } finally {
        if (own) this.stopHeartbeat(own)
        await this.release(taken)
      }
    })
  }

  /** Run `fn` after every lease-holding section queued before it. See `leaseWork`. */
  private serialized<T>(fn: () => Promise<T>): Promise<T> {
    const run = this.leaseWork.then(fn, fn)
    // The chain itself must never reject, or every later section would be skipped.
    this.leaseWork = run.catch(() => undefined)
    return run
  }

  private get leaseTtlMs(): number {
    return this.config.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS
  }

  /** The poll itself, with no lease handling: the caller guarantees exclusivity. */
  private async poll(): Promise<{ processed: number; stats: WriteStats; rejected: number }> {
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
          const polledAt = new Date().toISOString()
          const saved = await this.checkpoint(env.name, tail, {
            cursor: tail?.cursor ?? null,
            lastEventAt: tail?.lastEventAt ?? null,
            rangeStart: tail?.rangeStart ?? null,
            lastPolledAt: polledAt,
          })
          // A refused checkpoint means this process is no longer the writer,
          // which is not a successful poll of its own.
          if (saved) {
            this.healthTracker.polled(env.name, {
              polledAt,
              cursor: tail?.cursor ?? null,
              lastEventAt: tail?.lastEventAt ?? null,
            })
          }
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

        // Checkpoint only after the writes above are durable. A batch whose
        // checkpoint was refused is not counted: its rows are written, but
        // the replacement that now owns the cursor will apply them again.
        const last = events[events.length - 1]!
        const polledAt = new Date().toISOString()
        const saved = await this.checkpoint(env.name, tail, {
          cursor,
          lastEventAt: last.created_at,
          rangeStart: tail?.rangeStart ?? null,
          lastPolledAt: polledAt,
        })
        if (!saved) continue
        this.healthTracker.polled(env.name, {
          polledAt,
          cursor,
          lastEventAt: last.created_at,
          events: events.length,
          stats: result.stats,
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
          this.healthTracker.fatalError(err)
          this.logger.error('cursor direction check failed; stopping', { error: String(err) })
          throw err
        }
        if (isFatal(err)) {
          // A rejected credential will not fix itself; retrying every poll
          // interval would only hide the problem behind a healthy-looking process.
          this.healthTracker.fatalError(err)
          this.logger.error('authentication failed; stopping', { env: env.name, error: String(err) })
          throw err
        }
        this.healthTracker.failed(env.name, err)
        this.logger.error('tick failed', { env: env.name, error: String(err) })
      }
    }
    this.healthTracker.tickCompleted()
    return { processed, stats, rejected }
  }

  /**
   * Save the tail position, unless this process stopped being the writer
   * while the poll ran. Mirror writes are idempotent and staleness-gated, so
   * a former holder re-applying a batch is harmless; a former holder saving
   * its older cursor is not, because it rewinds the replacement and makes it
   * apply the same events again. The destination refuses the write, in the
   * same statement that makes it, unless this process holds the lease and the
   * stored position is still the one this poll started from. The second
   * condition catches a replacement that advanced the cursor and has since
   * let go, and a backfill that re-captured the position. Nothing is acquired
   * here: taking a lease is the loop's job, and the heartbeat's. Returns
   * false when nothing was written.
   */
  private async checkpoint(env: string, before: TailState | null, next: TailUpdate): Promise<boolean> {
    if (await this.state.setTailIfHeld(env, next, this.holder, before)) return true
    // Refused. The two reasons mean different things to an operator, so look
    // up which it was; this path is rare enough that one extra read is fine.
    const lease = await this.destination.getLease(env)
    if (lease?.holder === this.holder) {
      this.logger.warn('events cursor moved during this poll; not checkpointing', {
        env,
        cursor: before?.cursor ?? null,
      })
    } else {
      this.lostLease(env, lease?.holder ?? null)
    }
    return false
  }

  /**
   * Poll until `stop()`, as the sole writer. While another process holds the
   * lease this waits rather than failing: that is the rolling-deploy case,
   * where the new process must idle until the old one stops.
   */
  async listen(): Promise<void> {
    this.healthTracker.listening()
    try {
      await this.run()
    } finally {
      // However the loop ended, nothing will move the poll timestamps again,
      // and a probe has to say so rather than report the last good poll.
      this.healthTracker.stopped()
    }
  }

  /** The listen loop: take the lease, poll, idle, until `stop()`. */
  private async run(): Promise<void> {
    await this.init()
    this.stopping = false
    const interval = this.config.pollIntervalMs ?? 5_000
    const ttl = this.leaseTtlMs
    this.logger.info('listening', {
      environments: this.config.environments.map((e) => e.name),
      streams: this.streams.length,
      pollIntervalMs: interval,
      holder: this.holder,
      leaseTtlMs: ttl,
    })
    // The loop renews before every poll, but a single poll can run longer
    // than the TTL (a backlog, a slow database), so a background heartbeat
    // keeps the lease alive mid-tick as well. It is not cleared by `stop()`
    // on purpose: the final batch after a SIGTERM must stay covered until it
    // is written.
    const own = this.startHeartbeat()
    try {
      while (!this.stopping) {
        // Waiting happens outside the serialised section so a webhook nudge
        // is not held up by a listener that is itself waiting on a lease.
        const outcome = await this.serialized(async () => {
          if (this.stopping) return { refused: undefined, processed: 0 }
          const refused = await this.acquirePass()
          if (refused) return { refused, processed: 0 }
          return { refused: undefined, processed: (await this.poll()).processed }
        })
        if (outcome.refused) {
          this.warnRefused(outcome.refused)
          await sleep(interval)
          continue
        }
        // Drain fast when there is a backlog; idle politely when there is not.
        if (!this.stopping && outcome.processed === 0) await sleep(interval)
      }
    } finally {
      // Inside the serialised section, so a `tick()` queued while the loop
      // slept — it borrowed this heartbeat rather than starting its own —
      // finishes under a live lease before the timer stops. Then hand over
      // immediately: without the release a replacement waits a full TTL.
      await this.serialized(async () => {
        this.stopHeartbeat(own)
        await this.releaseHeld()
      })
    }
  }

  stop(): void {
    this.stopping = true
  }

  /**
   * The in-memory view of this engine: state, counters, last poll and last
   * error per environment. Synchronous and free of I/O, unlike `status()`,
   * so it is what a liveness probe or a metrics scrape should read. Serve it
   * with `startHealthServer`.
   */
  health(): EngineHealth {
    return this.healthTracker.snapshot()
  }

  /**
   * Renew everything in `held` every third of the TTL, in the background.
   * Unref'd so it never keeps a finished process alive. One renewal at a
   * time: a slow database must not pile them up behind itself. Returns the
   * timer so the caller stops only the one it started: a listener starting
   * while a standalone tick runs replaces the tick's timer and must outlive it.
   */
  private startHeartbeat(): NodeJS.Timeout {
    clearInterval(this.heartbeat)
    const timer = setInterval(
      () => {
        this.renewal ??= this.renewHeld().finally(() => {
          this.renewal = undefined
        })
      },
      Math.max(1, Math.floor(this.leaseTtlMs / 3)),
    )
    timer.unref()
    this.heartbeat = timer
    return timer
  }

  private stopHeartbeat(timer: NodeJS.Timeout): void {
    clearInterval(timer)
    if (this.heartbeat === timer) this.heartbeat = undefined
  }

  /**
   * One attempt to take or renew every environment's lease. Returns the first
   * refusal, or undefined once all are held. Renewing the ones already held
   * on every pass means nothing we hold lapses while we wait for the rest.
   *
   * Environments are claimed in name order, and a pass stops at the first
   * refusal and gives back whatever it holds. Two embedded engines whose
   * environment lists overlap could otherwise each hold the lease the other
   * is waiting for, with the heartbeat keeping both half-claims alive forever.
   */
  private async acquirePass(): Promise<LeaseRefusal | undefined> {
    const ordered = this.config.environments.map((e) => e.name).sort()
    for (const env of ordered) {
      const r = await this.destination.acquireLease(env, this.holder, this.leaseTtlMs)
      if (r.acquired) {
        this.held.add(env)
        continue
      }
      // We had it and it lapsed: this process stalled for longer than the
      // TTL. The other process is now the writer; fall back to waiting.
      if (this.held.has(env)) this.lostLease(env, r.holder)
      await this.releaseHeld()
      return { env, holder: r.holder, expiresAt: r.expiresAt }
    }
    return undefined
  }

  private warnRefused(refusal: LeaseRefusal): void {
    if (Date.now() - this.lastLeaseWarn < LEASE_WARN_INTERVAL_MS) return
    this.lastLeaseWarn = Date.now()
    this.logger.warn('events tail lease is held by another process; waiting', { ...refusal })
  }

  /** Heartbeat body. Failures are logged, not thrown: the next loop pass renews again. */
  private async renewHeld(): Promise<void> {
    for (const env of [...this.held]) {
      try {
        const r = await this.destination.acquireLease(env, this.holder, this.leaseTtlMs)
        if (!r.acquired) this.lostLease(env, r.holder)
      } catch (err) {
        this.logger.warn('lease renewal failed', { env, error: String(err) })
      }
    }
  }

  private lostLease(env: string, holder: string | null): void {
    this.held.delete(env)
    this.logger.error('lost the events tail lease; another process took over', { env, holder })
  }

  private releaseHeld(): Promise<void> {
    return this.release([...this.held])
  }

  private async release(envs: string[]): Promise<void> {
    // Acquiring is an upsert, so a renewal still in flight would put back a
    // row deleted here and the replacement would wait a full TTL after a
    // clean stop. Let it finish first. Then forget everything before the
    // first release, so a heartbeat firing mid-release finds nothing to renew.
    await this.renewal
    for (const env of envs) this.held.delete(env)
    for (const env of envs) {
      try {
        await this.destination.releaseLease(env, this.holder)
      } catch (err) {
        // Not worth failing a shutdown over: the lease expires by itself.
        this.logger.warn('lease release failed; it will expire on its own', { env, error: String(err) })
      }
    }
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
        lease: await this.destination.getLease(env.name),
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
    await this.serialized(() => this.releaseHeld())
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
