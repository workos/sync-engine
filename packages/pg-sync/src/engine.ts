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
import { connect, type SqlClient } from './db/client.js'
import { StateStore } from './db/state.js'
import { migrate } from './schema/migrate.js'
import { STREAMS, streamByName, defaultStreams, subscribedEventTypes } from './catalog.js'
import { WorkOSClient } from './workos/client.js'
import { fetchEvents, checkCursorAge, CursorDirectionError } from './workos/events.js'
import { applyEvents } from './sync/apply.js'
import { backfillStream, type BackfillStreamResult } from './sync/backfill.js'
import { diffStream, freshness, refreshStream, type DiffResult, type FreshnessRow } from './sync/reconcile.js'
import { createLogger } from './logger.js'
import type {
  EnvironmentConfig,
  Logger,
  StreamDef,
  SyncEngineConfig,
  WriteStats,
} from './types.js'
import { addStats, emptyStats } from './types.js'

export interface EnvironmentStatus {
  /** Last processed event id. Null until the first event is applied. */
  cursor: string | null
  /** Bootstrap position recorded by captureCursor, used until a cursor exists. */
  rangeStart: string | null
  /** Whether a starting position has been recorded at all. */
  started: boolean
  cursorAgeDays: number | null
  streams: FreshnessRow[]
}

export interface EngineDeps {
  db?: SqlClient
  /** Injectable for tests; one client per environment. */
  clientFor?: (env: EnvironmentConfig) => WorkOSClient
}

export class SyncEngine {
  readonly schema: string
  readonly streams: StreamDef[]
  private db!: SqlClient
  private state!: StateStore
  private logger: Logger
  private clients = new Map<string, WorkOSClient>()
  private stopping = false
  private ownsDb = false
  /** Event types passed to `GET /events`, which requires them. */
  readonly eventTypes: string[]

  constructor(
    private config: SyncEngineConfig,
    private deps: EngineDeps = {},
  ) {
    this.schema = config.schema ?? 'workos'
    this.logger = config.logger ?? createLogger('info')
    const names = config.streams ?? defaultStreams()
    this.streams = STREAMS.filter((s) => names.includes(s.name))
    if (this.streams.length === 0) throw new Error('no streams selected')
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
    if (!this.db) {
      if (this.deps.db) {
        this.db = this.deps.db
      } else if (this.config.databaseUrl) {
        this.db = await connect(this.config.databaseUrl)
        this.ownsDb = true
      } else {
        throw new Error('databaseUrl or a db dependency is required')
      }
      this.state = new StateStore(this.db, this.schema, this.config.syncId ?? 'default')
    }
    await migrate(this.db, this.streams, this.schema, this.logger)
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
    if (existing?.cursor || existing?.rangeStart) return existing.cursor ?? null
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
          const r = await backfillStream(this.db, api, this.state, stream, {
            schema: this.schema,
            env: env.name,
            batchSize: this.config.batchSize,
            logger: this.logger,
            resume: options.resume,
          })
          results.push(r)
        } catch (err) {
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

  /** One poll of the events tail across every environment. */
  async tick(): Promise<{ processed: number; stats: WriteStats }> {
    await this.init()
    let processed = 0
    let stats = emptyStats()

    for (const env of this.config.environments) {
      const api = this.clientFor(env)
      const tail = await this.state.getTail(env.name)
      checkCursorAge(tail?.lastEventAt, this.logger)

      try {
        const batch = await fetchEvents(
          api,
          {
            after: tail?.cursor ?? null,
            // Only used until the first real cursor arrives; sending both is a
            // 400 ("Only one of range_start and after may be provided").
            ...(tail?.cursor ? {} : { rangeStart: tail?.rangeStart ?? undefined }),
            limit: this.config.pageSize ?? 100,
            lagWindowMs: this.config.lagWindowMs ?? 5_000,
            events: this.eventTypes,
          },
          this.logger,
        )
        if (batch.events.length === 0) continue

        const result = await applyEvents(this.db, batch.events, {
          schema: this.schema,
          env: env.name,
          batchSize: this.config.batchSize,
          logger: this.logger,
          hydrate: this.config.hydrate,
          api,
        })

        // Checkpoint only after the writes above are durable.
        const last = batch.events[batch.events.length - 1]!
        await this.state.setTail(env.name, {
          cursor: batch.cursor,
          lastEventAt: last.created_at,
          rangeStart: tail?.rangeStart ?? null,
        })

        processed += batch.events.length
        stats = addStats(stats, result.stats)
        this.logger.info('processed events', {
          env: env.name,
          events: batch.events.length,
          cursor: batch.cursor,
          ...result.stats,
        })
      } catch (err) {
        if (err instanceof CursorDirectionError) {
          // Loud and fatal: continuing would rewrite the mirror with history.
          this.logger.error('cursor direction check failed; stopping', { error: String(err) })
          throw err
        }
        this.logger.error('tick failed', { env: env.name, error: String(err) })
      }
    }
    return { processed, stats }
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
      out.freshness[env.name] = await freshness(this.db, this.schema, env.name, chosen)
      const api = this.clientFor(env)
      const diffs: DiffResult[] = []
      for (const stream of chosen) {
        try {
          diffs.push(
            await diffStream(this.db, api, this.schema, env.name, stream, {
              repair: options.repair,
              logger: this.logger,
            }),
          )
        } catch (err) {
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
        out.push(await refreshStream(this.db, api, this.schema, env.name, stream))
      }
    }
    return out
  }

  async status(): Promise<Record<string, EnvironmentStatus>> {
    await this.init()
    const out: Record<string, EnvironmentStatus> = {}
    for (const env of this.config.environments) {
      const tail = await this.state.getTail(env.name)
      const age = tail?.lastEventAt
        ? (Date.now() - Date.parse(tail.lastEventAt)) / 86_400_000
        : null
      out[env.name] = {
        cursor: tail?.cursor ?? null,
        // Set before the first event arrives; the tail bootstraps from here.
        rangeStart: tail?.rangeStart ?? null,
        started: Boolean(tail?.cursor || tail?.rangeStart),
        cursorAgeDays: age,
        streams: await freshness(this.db, this.schema, env.name, this.streams),
      }
    }
    return out
  }

  /**
   * Erase every trace of one subject: mirror rows and archived event payloads.
   * A mirror inherits the deletion obligations of any other replica of the data.
   */
  async redact(userId: string): Promise<{ table: string; rows: number }[]> {
    await this.init()
    const out: { table: string; rows: number }[] = []
    for (const env of this.config.environments) {
      for (const stream of this.streams) {
        const col = stream.columns.find(
          (c) => c.name === 'user_id' || (c.name === 'id' && stream.name === 'users'),
        )
        if (!col) continue
        const r = await this.db.query(
          `delete from "${this.schema}"."${stream.name}"
           where "_env" = $1 and "${col.name}" = $2 returning 1`,
          [env.name, userId],
        )
        if (r.rows.length > 0) out.push({ table: stream.name, rows: r.rows.length })
      }
      // Archived events embed the payload, so they must be scrubbed too.
      const ev = await this.db.query(
        `delete from "${this.schema}"."events"
         where "_env" = $1 and ("object_id" = $2 or "_raw"->'data'->>'user_id' = $2) returning 1`,
        [env.name, userId],
      )
      if (ev.rows.length > 0) out.push({ table: 'events', rows: ev.rows.length })
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
    if (this.ownsDb && this.db) await this.db.close()
  }
}

const ORDERING = ['organizations', 'users', 'directories', 'connections']
function order(s: StreamDef): number {
  const i = ORDERING.indexOf(s.name)
  return i === -1 ? 100 : i
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export { streamByName }
