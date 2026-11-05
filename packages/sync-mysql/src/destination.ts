/**
 * The MySQL destination: everything `@workos/sync-engine` needs to know
 * about a database, implemented with `mysql2`.
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
  TableDef,
  WorkOSEvent,
  WriteMode,
  WriteStats,
} from '@workos/sync-engine'
import { encodeKey } from '@workos/sync-engine'
import { connect, type SqlClient } from './db/client.js'
import { upsertRows, deleteRows, relinkRows, unlinkRows } from './db/upsert.js'
import { diffIdsInSql } from './db/reconcile.js'
import { acquireLease, releaseLease, getLease, setStateIfHeld, iso } from './db/lease.js'
import { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
import { isTransientMysqlError } from './db/errors.js'
import { migrate, type MigrateResult } from './schema/migrate.js'
import { jsonPath, quoteIdent, safeIdentifier, KEY_LENGTH } from './schema/ddl.js'

const ident = (name: string) => quoteIdent(safeIdentifier(name))

export interface MysqlDestinationOptions {
  /** Connection string, `mysql://user:password@host:3306/db`. Alternatively pass a pre-built `db`. */
  databaseUrl?: string
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to,
   * for a database whose certificate a public CA did not issue. TLS is then
   * required and the certificate verified against this CA and the host
   * name. Only with `databaseUrl`: a pre-built `db` brings its own TLS
   * settings. `WORKOS_SYNC_MYSQL_SSL_CA` on the CLI.
   */
  sslCa?: string
  /** A pre-built client, e.g. `fromPool(pool)` to share the app's pool. Not closed by `close()`. */
  db?: SqlClient
  /**
   * The database (MySQL calls it a schema too) the mirror lives in, created
   * if missing. Applies to DDL *and* DML, and every statement qualifies its
   * tables with it, so the connection needs no default database. Defaults
   * to `workos`. Cross-database joins are ordinary SQL in MySQL, so the
   * mirror can sit beside the application's own database on one server.
   */
  schema?: string
  /** Identifier when several syncs share one database. Defaults to `default`. */
  syncId?: string
  /** Rows per statement. Defaults to 500. */
  batchSize?: number
  /**
   * On `init()`, replace a `_live` view under a generated name that is not
   * the generated shape. Off, `init()` fails and names it instead. `init
   * --repair` on the CLI.
   */
  repairViews?: boolean
  /**
   * During `init()`, rebuild from `_raw` any generated column whose
   * definition no longer matches the catalog. Off by default: drift is then
   * reported at error level and the schema fingerprint withheld until it is
   * fixed. Key columns, columns carrying your own indexes, non-generated
   * columns and orphans are never touched either way. `init --repair` on the
   * CLI sets this together with `repairViews`.
   */
  repairColumns?: boolean
}

export class MysqlDestination implements Destination {
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

  constructor(private options: MysqlDestinationOptions) {
    if (!options.db && !options.databaseUrl) throw new Error('databaseUrl or db is required')
    if (options.sslCa !== undefined && (options.db || !options.databaseUrl)) {
      throw new Error('sslCa applies to databaseUrl; a pre-built db carries its own TLS settings')
    }
    this.schema = options.schema ?? 'workos'
    if (this.schema.length === 0 || this.schema.length > 64) {
      throw new Error('schema must be between 1 and 64 characters')
    }
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

  private table(stream: TableDef): string {
    return `${quoteIdent(this.schema)}.${ident(stream.name)}`
  }

  async init(streams: TableDef[], logger?: Logger): Promise<void> {
    if (!this.db) {
      this.db = await connect(this.options.databaseUrl!, { sslCa: this.options.sslCa, logger })
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

  upsert(stream: TableDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats> {
    return upsertRows(this.client, this.writeOpts, stream, env, rows, mode)
  }

  delete(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return deleteRows(this.client, this.writeOpts, stream, env, rows)
  }

  relink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return relinkRows(this.client, this.writeOpts, stream, env, rows)
  }

  unlink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return unlinkRows(this.client, this.writeOpts, stream, env, rows)
  }

  async liveIds(stream: TableDef, env: string): Promise<string[]> {
    const keys = stream.key.map((name) => ident(name))
    const live = stream.mode === 'append' ? '' : ' and `_deleted_at` is null'
    const r = await this.client.query<Record<string, unknown>>(
      `select ${keys.join(', ')} from ${this.table(stream)} where \`_env\` = ?${live} order by ${keys.join(', ')}`,
      [env],
    )
    return r.rows.map((row) => encodeKey(stream.key.map((name) => String(row[safeIdentifier(name)]))))
  }

  async rawRows(stream: TableDef, env: string): Promise<Record<string, unknown>[]> {
    const r = await this.client.query<{ raw: Record<string, unknown> }>(
      `select \`_raw\` as raw from ${this.table(stream)} where \`_env\` = ?`,
      [env],
    )
    return r.rows.map((row) => row.raw)
  }

  /** Staged into temporary tables and diffed in SQL; see `db/reconcile.ts`. */
  diffIds(stream: TableDef, env: string, remote: AsyncIterable<Record<string, unknown>[]>): Promise<IdDiff> {
    return diffIdsInSql(this.client, this.writeOpts, stream, env, remote)
  }

  async freshness(stream: TableDef, env: string, lagWindowHours: number): Promise<FreshnessRow> {
    const hasDeleted = stream.mode !== 'append'
    const recent = 'timestampadd(hour, ?, now(6))'
    const r = await this.client.query<{
      n: number
      deleted: string | number | null
      last_synced_at: string | null
      staleness: string | number | null
      lag_s: string | number | null
      lag_n: string | number | null
    }>(
      `select count(*) as n,
              ${hasDeleted ? 'sum(`_deleted_at` is not null)' : '0'} as deleted,
              ${iso('max(`_synced_at`)')} as last_synced_at,
              timestampdiff(microsecond, max(\`_updated_at\`), now(6)) / 1000000 as staleness,
              avg(if(\`_updated_at\` > ${recent}, timestampdiff(microsecond, \`_updated_at\`, \`_synced_at\`) / 1000000, null)) as lag_s,
              sum(\`_updated_at\` > ${recent}) as lag_n
       from ${this.table(stream)} where \`_env\` = ?`,
      [-lagWindowHours, -lagWindowHours, env],
    )
    const row = r.rows[0]
    const lagN = Number(row?.lag_n ?? 0)
    const rows = Number(row?.n ?? 0)
    return {
      stream: stream.name,
      rows,
      deleted: Number(row?.deleted ?? 0),
      lastSyncedAt: rows > 0 ? (row?.last_synced_at ?? null) : null,
      stalenessSeconds: row?.staleness != null ? Number(row.staleness) : null,
      lagSeconds: lagN > 0 && row?.lag_s != null ? Number(row.lag_s) : null,
      lagSampleSize: lagN,
    }
  }

  async redact(streams: TableDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]> {
    const out: { table: string; rows: number }[] = []
    for (const stream of streams) {
      const col = stream.columns.find((c) => c.name === 'user_id' || (c.name === 'id' && stream.name === 'users'))
      if (!col) continue
      const r = await this.client.query(
        `delete from ${this.table(stream)} where \`_env\` = ? and ${ident(col.name)} = ?`,
        [env, userId],
      )
      if (r.affectedRows > 0) out.push({ table: stream.name, rows: r.affectedRows })
    }
    const userIdInData = `json_value(\`_raw\`, ${jsonPath(['data', 'user_id'])} returning char(${KEY_LENGTH}) null on error)`
    // Archived events embed the payload, so they must be scrubbed too.
    const ev = await this.client.query(
      `delete from ${quoteIdent(this.schema)}.\`events\`
       where \`_env\` = ? and (\`object_id\` = ? or ${userIdInData} = ?)`,
      [env, userId, userId],
    )
    if (ev.affectedRows > 0) out.push({ table: 'events', rows: ev.affectedRows })
    // Quarantined events embed the payload too, and they may be the only copy
    // of it: an event rejected before its archive write succeeded is nowhere
    // in `events`.
    const rj = await this.client.query(
      `delete from ${quoteIdent(this.schema)}.\`_rejected\`
       where sync_id = ? and env = ? and (object_id = ? or ${userIdInData} = ?)`,
      [this.syncId, env, userId, userId],
    )
    if (rj.affectedRows > 0) out.push({ table: '_rejected', rows: rj.affectedRows })
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
    return isTransientMysqlError(err)
  }

  async getState<T>(env: string, stream: string): Promise<T | null> {
    const r = await this.client.query<{ state: T }>(
      `select state from ${quoteIdent(this.schema)}.\`_sync_state\` where sync_id = ? and env = ? and stream = ?`,
      [this.syncId, env, stream],
    )
    return r.rows[0]?.state ?? null
  }

  async setState(env: string, stream: string, state: unknown): Promise<void> {
    await this.client.query(
      `insert into ${quoteIdent(this.schema)}.\`_sync_state\` (sync_id, env, stream, state, updated_at)
       values (?, ?, ?, cast(? as json), now(6)) as new
       on duplicate key update state = new.state, updated_at = now(6)`,
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
