/**
 * The Postgres bookkeeping store: cursors in `_sync_state`, the single-writer
 * lease in `_sync_lease`, the quarantine in `_rejected`. Every row is keyed by
 * sync id as well, so several syncs can share one database without seeing
 * each other's cursors, leases or rejects.
 *
 * `PostgresDestination` keeps one of these on its own connection and offers
 * it as `destination.bookkeeping`, which is why the single-database
 * deployment needs no extra configuration. Built on its own, with a
 * `databaseUrl` or a `db`, it is the store for a destination that cannot make
 * an atomic conditional write — a spreadsheet, an object store, a queue — and
 * is paired with it through `SyncEngineConfig.bookkeeping`.
 */
import type { Bookkeeping, Lease, LeaseResult, Logger, RejectedEvent, WorkOSEvent } from '@workos/sync-engine'
import { connect, type SqlClient } from './db/client.js'
import { acquireLease, releaseLease, getLease, setStateIfHeld } from './db/lease.js'
import { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
import { bookkeepingSql, createSchemaSql, parseIso8601Function, quoteIdent } from './schema/ddl.js'

export interface PostgresBookkeepingOptions {
  /** Connection string. Alternatively pass a pre-built `db`. */
  databaseUrl?: string
  /** A pre-built client, e.g. `fromPgPool(pool)` to share the app's pool. Not closed by `close()`. */
  db?: SqlClient
  /** Schema the bookkeeping tables live in. Defaults to `workos`. */
  schema?: string
  /** Identifier when several syncs share one database. Defaults to `default`. */
  syncId?: string
}

export class PostgresBookkeeping implements Bookkeeping {
  readonly schema: string
  private db: SqlClient | undefined
  private ownsDb = false
  private readonly syncId: string
  /**
   * The tables are created once per instance. `init()` runs before every
   * engine operation, and a locked DDL transaction on each would be the
   * single most expensive thing a `tick` does.
   */
  private ready = false
  /**
   * The initialization in flight, shared by every caller until it settles.
   * Two first calls racing — `status()` from a monitor while a poll starts —
   * would otherwise each open a pool and keep only the last, leaking the first.
   */
  private initializing: Promise<void> | undefined

  constructor(private options: PostgresBookkeepingOptions) {
    if (!options.db && !options.databaseUrl) throw new Error('databaseUrl or db is required')
    this.schema = options.schema ?? 'workos'
    this.syncId = options.syncId ?? 'default'
    this.db = options.db
  }

  private get client(): SqlClient {
    if (!this.db) throw new Error('bookkeeping store not initialised: call init() first')
    return this.db
  }

  private get opts() {
    return { schema: this.schema, syncId: this.syncId }
  }

  async init(logger?: Logger): Promise<void> {
    if (this.ready) return
    // A failed attempt is forgotten, so the next call can try again.
    this.initializing ??= this.initialize(logger).finally(() => {
      this.initializing = undefined
    })
    return this.initializing
  }

  private async initialize(logger?: Logger): Promise<void> {
    if (!this.db) {
      this.db = await connect(this.options.databaseUrl!)
      this.ownsDb = true
    }
    await this.db.transaction(async (tx) => {
      // The same lock the mirror's migration takes, because `if not exists`
      // does not serialise catalog writes: two processes starting together
      // would otherwise race to create the same table and one would fail.
      await tx.query(
        `select pg_advisory_xact_lock(hashtext('workos-sync-postgres:migrate'), hashtext($1::name::text))`,
        [this.schema],
      )
      await tx.exec(createSchemaSql(this.schema))
      // The quarantine insert parses `created_at` through this function in
      // the store's own schema, so a store apart from the mirror needs its
      // own copy; the mirror's, when they share a schema, is the same DDL.
      await tx.exec(parseIso8601Function(this.schema))
      await tx.exec(bookkeepingSql(this.schema))
    })
    this.ready = true
    logger?.debug('bookkeeping tables ready', { schema: this.schema, syncId: this.syncId })
  }

  async close(): Promise<void> {
    if (this.ownsDb && this.db) await this.db.close()
  }

  async getState<T>(env: string, stream: string): Promise<T | null> {
    const r = await this.client.query<{ state: T }>(
      `select state from ${quoteIdent(this.schema)}."_sync_state"
       where sync_id = $1 and env = $2 and stream = $3`,
      [this.syncId, env, stream],
    )
    return r.rows[0]?.state ?? null
  }

  async setState(env: string, stream: string, state: unknown): Promise<void> {
    await this.client.query(
      `insert into ${quoteIdent(this.schema)}."_sync_state" (sync_id, env, stream, state, updated_at)
       values ($1, $2, $3, $4::jsonb, now())
       on conflict (sync_id, env, stream) do update
         set state = excluded.state, updated_at = now()`,
      [this.syncId, env, stream, JSON.stringify(state)],
    )
  }

  setStateIfHeld(env: string, stream: string, state: unknown, holder: string, expected: unknown): Promise<boolean> {
    return setStateIfHeld(this.client, this.schema, this.syncId, env, stream, state, holder, expected)
  }

  acquireLease(env: string, holder: string, ttlMs: number): Promise<LeaseResult> {
    return acquireLease(this.client, this.schema, this.syncId, env, holder, ttlMs)
  }

  releaseLease(env: string, holder: string): Promise<void> {
    return releaseLease(this.client, this.schema, this.syncId, env, holder)
  }

  getLease(env: string): Promise<Lease | null> {
    return getLease(this.client, this.schema, this.syncId, env)
  }

  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void> {
    return rejectEvents(this.client, this.opts, env, rows)
  }

  rejected(env: string, opts: { ids?: string[] } = {}): Promise<RejectedEvent[]> {
    return listRejected(this.client, this.opts, env, opts.ids)
  }

  rejectedCount(env: string): Promise<number> {
    return countRejected(this.client, this.opts, env)
  }

  clearRejected(env: string, ids: string[]): Promise<number> {
    return clearRejected(this.client, this.opts, env, ids)
  }

  /**
   * Quarantined events embed the payload, and may be the only copy of it: an
   * event rejected before its archive write succeeded is nowhere in `events`.
   */
  async redact(env: string, userId: string): Promise<{ table: string; rows: number }[]> {
    const r = await this.client.query(
      `delete from ${quoteIdent(this.schema)}."_rejected"
       where sync_id = $1 and env = $2 and ("object_id" = $3 or "_raw"->'data'->>'user_id' = $3) returning 1`,
      [this.syncId, env, userId],
    )
    return r.rows.length > 0 ? [{ table: '_rejected', rows: r.rows.length }] : []
  }
}
