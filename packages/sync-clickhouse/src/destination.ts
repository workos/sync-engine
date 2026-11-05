/**
 * The ClickHouse destination: everything `@workos/sync-engine` needs to know
 * about a database, implemented over the official HTTP client.
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
import { connect, isoOf, type ChClient } from './db/client.js'
import { upsertRows, deleteRows, relinkRows, unlinkRows, keyExpression, tableRef, KEY_SEPARATOR } from './db/upsert.js'
import { diffIdsInClickHouse } from './db/reconcile.js'
import { acquireLease, releaseLease, getLease, getState, setState, setStateIfHeld } from './db/lease.js'
import { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
import { isTransientClickHouseError } from './db/errors.js'
import { migrate, type MigrateResult } from './schema/migrate.js'
import { jsonPathArgs, quoteIdent, safeIdentifier } from './schema/ddl.js'

const ident = (name: string) => quoteIdent(safeIdentifier(name))

export interface ClickHouseDestinationOptions {
  /**
   * The HTTP interface, `https://user:password@host:8443`. Alternatively
   * pass a pre-built `db`. A path in the URL is ignored: every statement
   * qualifies its tables with `schema`.
   */
  databaseUrl?: string
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to,
   * for a server whose certificate a public CA did not issue. Only with an
   * `https://` `databaseUrl`; a pre-built `db` brings its own TLS settings.
   * `WORKOS_SYNC_CLICKHOUSE_TLS_CA` on the CLI.
   */
  sslCa?: string
  /** A pre-built client, e.g. `fromClient(client)` to share the app's. Not closed by `close()`. */
  db?: ChClient
  /** The database the mirror lives in, created if missing. Defaults to `workos`. */
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
   * During `init()`, redefine any projected column whose definition no
   * longer matches the catalog. Off by default: drift is then reported at
   * error level and the schema fingerprint withheld until it is fixed. Key
   * columns, non-generated columns and orphans are never touched either
   * way. `init --repair` on the CLI sets this together with `repairViews`.
   */
  repairColumns?: boolean
}

export class ClickHouseDestination implements Destination {
  readonly schema: string
  private db: ChClient | undefined
  private ownsDb = false
  private readonly syncId: string
  private readonly batchSize: number | undefined
  /**
   * What the most recent `init()` did, including any column drift it found.
   * The engine's `init()` returns nothing, so this is how a caller — the CLI's
   * `init` command — gets the drift report back.
   */
  lastMigration: MigrateResult | undefined

  constructor(private options: ClickHouseDestinationOptions) {
    if (!options.db && !options.databaseUrl) throw new Error('databaseUrl or db is required')
    if (options.sslCa !== undefined && (options.db || !options.databaseUrl)) {
      throw new Error('sslCa applies to databaseUrl; a pre-built db carries its own TLS settings')
    }
    this.schema = options.schema ?? 'workos'
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(this.schema)) {
      throw new Error('schema must be a plain identifier: letters, digits and underscores, up to 63 characters')
    }
    this.syncId = options.syncId ?? 'default'
    this.batchSize = options.batchSize
    this.db = options.db
  }

  private get client(): ChClient {
    if (!this.db) throw new Error('destination not initialised: call init() first')
    return this.db
  }

  private get writeOpts() {
    return { schema: this.schema, batchSize: this.batchSize }
  }

  private get bookkeepingOpts() {
    return { schema: this.schema, syncId: this.syncId }
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

  /** `FINAL` for the mirror tables, where a key may have several versions; append tables have one row per key. */
  private final(stream: TableDef): string {
    return stream.mode === 'append' ? '' : ' FINAL'
  }

  async liveIds(stream: TableDef, env: string): Promise<string[]> {
    const live = stream.mode === 'append' ? '' : ' AND `_deleted_at` IS NULL'
    const rows = await this.client.query<{ k: string }>(
      `SELECT ${keyExpression(stream)} AS k FROM ${tableRef(this.schema, stream)}${this.final(stream)}
       WHERE \`_env\` = {env:String}${live} ORDER BY k`,
      { env },
    )
    return rows.map((r) => encodeKey(stream.key.length === 1 ? [r.k] : r.k.split(KEY_SEPARATOR)))
  }

  async rawRows(stream: TableDef, env: string): Promise<Record<string, unknown>[]> {
    const rows = await this.client.query<{ raw: string }>(
      `SELECT \`_raw\` AS raw FROM ${tableRef(this.schema, stream)}${this.final(stream)} WHERE \`_env\` = {env:String}`,
      { env },
    )
    return rows.map((r) => JSON.parse(r.raw) as Record<string, unknown>)
  }

  /** Staged into tables of their own and diffed in SQL; see `db/reconcile.ts`. */
  diffIds(stream: TableDef, env: string, remote: AsyncIterable<Record<string, unknown>[]>): Promise<IdDiff> {
    return diffIdsInClickHouse(this.client, this.writeOpts, stream, env, remote)
  }

  async freshness(stream: TableDef, env: string, lagWindowHours: number): Promise<FreshnessRow> {
    const hasDeleted = stream.mode !== 'append'
    const recent = `\`_updated_at\` > now64(3, 'UTC') - toIntervalHour({hours:UInt64})`
    const ms = (col: string) => `toUnixTimestamp64Milli(${col})`
    const rows = await this.client.query<{
      n: string
      deleted: string
      last_synced_at: string
      staleness: string
      lag_s: string | null
      lag_n: string
    }>(
      `SELECT toString(count()) AS n,
              ${hasDeleted ? 'toString(countIf(`_deleted_at` IS NOT NULL))' : "'0'"} AS deleted,
              max(\`_synced_at\`) AS last_synced_at,
              toString((${ms(`now64(3, 'UTC')`)} - ${ms('max(`_updated_at`)')}) / 1000) AS staleness,
              toString(avgIf((${ms('`_synced_at`')} - ${ms('`_updated_at`')}) / 1000, ${recent})) AS lag_s,
              toString(countIf(${recent})) AS lag_n
       FROM ${tableRef(this.schema, stream)}${this.final(stream)} WHERE \`_env\` = {env:String}`,
      { env, hours: Math.max(0, Math.floor(lagWindowHours)) },
    )
    const row = rows[0]
    const n = Number(row?.n ?? 0)
    const lagN = Number(row?.lag_n ?? 0)
    // Aggregates over no rows read as epoch and NaN here, not NULL.
    return {
      stream: stream.name,
      rows: n,
      deleted: Number(row?.deleted ?? 0),
      lastSyncedAt: n > 0 && row ? isoOf(row.last_synced_at) : null,
      stalenessSeconds: n > 0 && row ? Number(row.staleness) : null,
      lagSeconds: lagN > 0 && row?.lag_s != null && Number.isFinite(Number(row.lag_s)) ? Number(row.lag_s) : null,
      lagSampleSize: lagN,
    }
  }

  async redact(streams: TableDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]> {
    const out: { table: string; rows: number }[] = []
    // A delete reports no count, so each table is counted first.
    const erase = async (
      label: string,
      table: string,
      final: string,
      where: string,
      params: Record<string, unknown>,
    ) => {
      const counted = await this.client.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM ${table}${final} WHERE ${where}`,
        params,
      )
      const n = Number(counted[0]?.n ?? 0)
      if (n === 0) return
      await this.client.command(`DELETE FROM ${table} WHERE ${where}`, params)
      out.push({ table: label, rows: n })
    }
    for (const stream of streams) {
      const col = stream.columns.find((c) => c.name === 'user_id' || (c.name === 'id' && stream.name === 'users'))
      if (!col) continue
      await erase(
        stream.name,
        tableRef(this.schema, stream),
        this.final(stream),
        `\`_env\` = {env:String} AND ${ident(col.name)} = {id:String}`,
        { env, id: userId },
      )
    }
    const userIdInData = `JSONExtractString(\`_raw\`, ${jsonPathArgs(['data', 'user_id'])})`
    // Archived events embed the payload, so they must be scrubbed too.
    await erase(
      'events',
      `${quoteIdent(this.schema)}.\`events\``,
      '',
      `\`_env\` = {env:String} AND (\`object_id\` = {id:String} OR ${userIdInData} = {id:String})`,
      { env, id: userId },
    )
    // Quarantined events embed the payload too, and they may be the only copy
    // of it: an event rejected before its archive write succeeded is nowhere
    // in `events`.
    await erase(
      '_rejected',
      `${quoteIdent(this.schema)}.\`_rejected\``,
      ' FINAL',
      `sync_id = {syncId:String} AND env = {env:String} AND (object_id = {id:String} OR ${userIdInData} = {id:String})`,
      { syncId: this.syncId, env, id: userId },
    )
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
    return isTransientClickHouseError(err)
  }

  getState<T>(env: string, stream: string): Promise<T | null> {
    return getState<T>(this.client, this.schema, this.syncId, env, stream)
  }

  setState(env: string, stream: string, state: unknown): Promise<void> {
    return setState(this.client, this.schema, this.syncId, env, stream, state)
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
