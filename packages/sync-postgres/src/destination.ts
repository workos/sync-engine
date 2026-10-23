/**
 * The Postgres destination: everything `@workos/sync-engine` needs to know about a
 * database, implemented with `pg` in production and PGlite in tests.
 */
import type {
  Destination,
  FreshnessRow,
  Lease,
  LeaseResult,
  Logger,
  Row,
  StreamDef,
  WriteMode,
  WriteStats,
} from '@workos/sync-engine'
import { connect, type SqlClient } from './db/client.js'
import { upsertRows, deleteRows, relinkRows } from './db/upsert.js'
import { acquireLease, releaseLease, getLease } from './db/lease.js'
import { migrate } from './schema/migrate.js'
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
}

export class PostgresDestination implements Destination {
  readonly schema: string
  private db: SqlClient | undefined
  private ownsDb = false
  private readonly syncId: string
  private readonly batchSize: number | undefined

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

  private table(stream: StreamDef): string {
    return `${quoteIdent(this.schema)}.${ident(stream.name)}`
  }

  async init(streams: StreamDef[], logger?: Logger): Promise<void> {
    if (!this.db) {
      this.db = await connect(this.options.databaseUrl!)
      this.ownsDb = true
    }
    await migrate(this.db, streams, this.schema, logger)
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

  async liveIds(stream: StreamDef, env: string): Promise<string[]> {
    const key = ident(stream.key[0]!)
    const r = await this.client.query<{ id: string }>(
      `select ${key} as id from ${this.table(stream)}
       where "_env" = $1 and "_deleted_at" is null order by ${key}`,
      [env],
    )
    return r.rows.map((row) => row.id)
  }

  async rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]> {
    const r = await this.client.query<{ raw: Record<string, unknown> }>(
      `select "_raw" as raw from ${this.table(stream)} where "_env" = $1`,
      [env],
    )
    return r.rows.map((row) => row.raw)
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
    return out
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
