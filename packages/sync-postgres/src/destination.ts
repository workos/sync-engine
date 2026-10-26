/**
 * The Postgres destination: everything `@workos/sync-engine` needs to know about a
 * database, implemented with `pg` in production and PGlite in tests.
 */
import type {
  Destination,
  FreshnessRow,
  IdDiff,
  Lease,
  LeaseResult,
  Logger,
  RejectedEvent,
  Row,
  StreamDef,
  WorkOSEvent,
  WriteMode,
  WriteStats,
} from '@workos/sync-engine'
import { encodeKey } from '@workos/sync-engine'
import { connect, type SqlClient } from './db/client.js'
import { upsertRows, deleteRows, relinkRows, unlinkRows } from './db/upsert.js'
import { diffIdsInSql } from './db/reconcile.js'
import { acquireLease, releaseLease, getLease, setStateIfHeld } from './db/lease.js'
import { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
import { isTransientPgError } from './db/errors.js'
import { migrate, type MigrateResult } from './schema/migrate.js'
import { quoteIdent, safeIdentifier } from './schema/ddl.js'

const ident = (name: string) => quoteIdent(safeIdentifier(name))

export interface PostgresDestinationOptions {
  /** Connection string. Alternatively pass a pre-built `db`. */
  databaseUrl?: string
  /** A pre-built client, e.g. `fromPgPool(pool)` to share the app's pool. Not closed by `close()`. */
  db?: SqlClient
  /** Schema the mirror lives in. Applies to DDL *and* DML. Defaults to `workos`. */
  schema?: string
  /** Identifier when several syncs share one database. Defaults to `default`. */
  syncId?: string
  /** Rows per upsert statement. Defaults to 500. */
  batchSize?: number
  /**
   * On `init()`, rebuild a generated `_live` view that `create or replace
   * view` refuses because its shape changed between releases, and carry its
   * grants onto the new definition. Off, `init()` fails and lists the grants
   * it would have carried instead. Turning this on is the operator consenting
   * to widen those grantees to the generated definition — `select * ... where
   * "_deleted_at" is null` — whatever the old view showed them. `init
   * --repair` on the CLI. Views without the generated comment, or with it but
   * not the generated shape, are never rebuilt. See docs/schema.md.
   */
  repairViews?: boolean
  /**
   * During `init()`, rebuild from `_raw` any generated column whose expression
   * or type no longer matches the catalog. Off by default: drift is then
   * reported at error level and the schema fingerprint withheld until it is
   * fixed. Key columns, columns carrying your own indexes or constraints, and
   * orphans are never touched either way; see docs/schema.md, "Changing a
   * column". `init --repair` on the CLI sets this together with `repairViews`.
   */
  repairColumns?: boolean
}

export class PostgresDestination implements Destination {
  readonly schema: string
  private db: SqlClient | undefined
  private ownsDb = false
  private readonly syncId: string
  private readonly batchSize: number | undefined
  /**
   * What the most recent `init()` did, including any column drift it found.
   * The engine's `init()` returns nothing, so this is how a caller — the CLI's
   * `init` command — gets the drift report back.
   */
  lastMigration: MigrateResult | undefined

  constructor(private options: PostgresDestinationOptions) {
    if (!options.db && !options.databaseUrl) throw new Error('databaseUrl or db is required')
    this.schema = options.schema ?? 'workos'
    this.syncId = options.syncId ?? 'default'
    this.batchSize = options.batchSize
    this.db = options.db
  }

  private get client(): SqlClient {
    if (!this.db) throw new Error('destination not initialised: call init() first')
    return this.db
  }

  private get writeOpts() {
    return { schema: this.schema, batchSize: this.batchSize }
  }

  private get bookkeepingOpts() {
    return { schema: this.schema, syncId: this.syncId }
  }

  private table(stream: StreamDef): string {
    return `${quoteIdent(this.schema)}.${ident(stream.name)}`
  }

  async init(streams: StreamDef[], logger?: Logger): Promise<void> {
    if (!this.db) {
      this.db = await connect(this.options.databaseUrl!)
      this.ownsDb = true
    }
    this.lastMigration = await migrate(this.db, streams, this.schema, logger, {
      repair: this.options.repairColumns,
      repairViews: this.options.repairViews,
    })
  }

  async close(): Promise<void> {
    if (this.ownsDb && this.db) await this.db.close()
  }

  upsert(stream: StreamDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats> {
    return upsertRows(this.client, this.writeOpts, stream, env, rows, mode)
  }

  delete(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return deleteRows(this.client, this.writeOpts, stream, env, rows)
  }

  relink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return relinkRows(this.client, this.writeOpts, stream, env, rows)
  }

  unlink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return unlinkRows(this.client, this.writeOpts, stream, env, rows)
  }

  async liveIds(stream: StreamDef, env: string): Promise<string[]> {
    const keys = stream.key.map((name) => `${ident(name)}::text`).join(', ')
    const r = await this.client.query<{ id: string[] }>(
      `select array[${keys}] as id from ${this.table(stream)}
       where "_env" = $1 and "_deleted_at" is null order by id`,
      [env],
    )
    return r.rows.map((row) => encodeKey(row.id))
  }

  async rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]> {
    const r = await this.client.query<{ raw: Record<string, unknown> }>(
      `select "_raw" as raw from ${this.table(stream)} where "_env" = $1`,
      [env],
    )
    return r.rows.map((row) => row.raw)
  }

  /** Staged into a temp table and diffed in SQL; see `db/reconcile.ts`. */
  diffIds(
    stream: StreamDef,
    env: string,
    remote: AsyncIterable<Record<string, unknown>[]>,
  ): Promise<IdDiff> {
    return diffIdsInSql(this.client, this.writeOpts, stream, env, remote)
  }

  async freshness(stream: StreamDef, env: string, lagWindowHours: number): Promise<FreshnessRow> {
    const hasDeleted = stream.mode !== 'append'
    const r = await this.client.query<{
      rows: string
      deleted: string | null
      last_synced_at: string | null
      staleness: string | null
      lag: string | null
      lag_n: string
    }>(
      `select count(*)::text as rows,
              ${hasDeleted ? `count(*) filter (where "_deleted_at" is not null)::text` : `'0'`} as deleted,
              max("_synced_at")::text as last_synced_at,
              extract(epoch from (now() - max("_updated_at")))::text as staleness,
              avg(extract(epoch from ("_synced_at" - "_updated_at")))
                filter (where "_updated_at" > now() - ($2 || ' hours')::interval)::text as lag,
              count(*) filter (where "_updated_at" > now() - ($2 || ' hours')::interval)::text as lag_n
       from ${this.table(stream)} where "_env" = $1`,
      [env, String(lagWindowHours)],
    )
    const row = r.rows[0]
    const lagN = Number(row?.lag_n ?? 0)
    return {
      stream: stream.name,
      rows: Number(row?.rows ?? 0),
      deleted: Number(row?.deleted ?? 0),
      lastSyncedAt: row?.last_synced_at ?? null,
      stalenessSeconds: row?.staleness ? Number(row.staleness) : null,
      lagSeconds: lagN > 0 && row?.lag ? Number(row.lag) : null,
      lagSampleSize: lagN,
    }
  }

  async redact(
    streams: StreamDef[],
    env: string,
    userId: string,
  ): Promise<{ table: string; rows: number }[]> {
    const out: { table: string; rows: number }[] = []
    for (const stream of streams) {
      const col = stream.columns.find(
        (c) => c.name === 'user_id' || (c.name === 'id' && stream.name === 'users'),
      )
      if (!col) continue
      const r = await this.client.query(
        `delete from ${this.table(stream)}
         where "_env" = $1 and ${ident(col.name)} = $2 returning 1`,
        [env, userId],
      )
      if (r.rows.length > 0) out.push({ table: stream.name, rows: r.rows.length })
    }
    // Archived events embed the payload, so they must be scrubbed too.
    const ev = await this.client.query(
      `delete from ${quoteIdent(this.schema)}."events"
       where "_env" = $1 and ("object_id" = $2 or "_raw"->'data'->>'user_id' = $2) returning 1`,
      [env, userId],
    )
    if (ev.rows.length > 0) out.push({ table: 'events', rows: ev.rows.length })
    // Quarantined events embed the payload too, and they may be the only copy
    // of it: an event rejected before its archive write succeeded is nowhere
    // in `events`.
    const rj = await this.client.query(
      `delete from ${quoteIdent(this.schema)}."_rejected"
       where sync_id = $1 and env = $2 and ("object_id" = $3 or "_raw"->'data'->>'user_id' = $3) returning 1`,
      [this.syncId, env, userId],
    )
    if (rj.rows.length > 0) out.push({ table: '_rejected', rows: rj.rows.length })
    return out
  }

  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void> {
    return rejectEvents(this.client, this.bookkeepingOpts, env, rows)
  }

  rejected(env: string, opts: { ids?: string[] } = {}): Promise<RejectedEvent[]> {
    return listRejected(this.client, this.bookkeepingOpts, env, opts.ids)
  }

  rejectedCount(env: string): Promise<number> {
    return countRejected(this.client, this.bookkeepingOpts, env)
  }

  clearRejected(env: string, ids: string[]): Promise<number> {
    return clearRejected(this.client, this.bookkeepingOpts, env, ids)
  }

  isTransientError(err: unknown): boolean {
    return isTransientPgError(err)
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
}
