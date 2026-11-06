/**
 * The MongoDB destination: everything `@workos/sync-engine` needs to know
 * about a database, implemented with the official driver.
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
import type { Collection, Db, Document, MongoClient } from 'mongodb'
import { connect, databaseFromUrl } from './db/client.js'
import { isTransientMongoError } from './db/errors.js'
import { acquireLease, getLease, getState, releaseLease, setState, setStateIfHeld, type SyncDoc } from './db/lease.js'
import { diffIdsInMongo } from './db/reconcile.js'
import { clearRejected, countRejected, listRejected, rejectEvents, type RejectedDoc } from './db/rejected.js'
import {
  deleteRows,
  readPath,
  relinkRows,
  unlinkRows,
  upsertRows,
  type MirrorDoc,
  type WriteOptions,
} from './db/upsert.js'
import { migrate, type MigrateResult } from './schema/migrate.js'
import { fieldPath, keyPath, Names } from './schema/names.js'

export interface MongoDestinationOptions {
  /** Connection string (`mongodb://` or `mongodb+srv://`). Alternatively pass a pre-built `client`. */
  url?: string
  /** A pre-built client, e.g. the application's own. Not closed by `close()`. */
  client?: MongoClient
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to,
   * for a database whose certificate a public CA did not issue. TLS is then
   * required, whatever the connection string carries short of `tls=false`,
   * which is refused as a contradiction. Only with `url`: a pre-built
   * `client` brings its own TLS settings. `WORKOS_SYNC_MONGODB_TLS_CA` on the CLI.
   */
  sslCa?: string
  /**
   * Database the mirror lives in. Defaults to the one the connection string
   * names, or the pre-built client's default database; `workos` when
   * neither names one. The mirror shares the application's database on
   * purpose: `$lookup` cannot cross databases.
   */
  database?: string
  /** Collection prefix, applied to the mirror, the views and the bookkeeping. Defaults to `workos_`. */
  prefix?: string
  /** Identifier when several syncs share one database. Defaults to `default`. */
  syncId?: string
  /** Rows per bulk write. Defaults to 500. */
  batchSize?: number
  /**
   * On `init()`, redefine a generated `_live` view whose pipeline is not the
   * generated one. Off, `init()` fails and names the view instead. A view
   * over some other collection, or a plain collection under the name, is
   * never touched either way. `init --repair` on the CLI.
   */
  repairViews?: boolean
}

export class MongoDestination implements Destination {
  readonly names: Names
  readonly prefix: string
  private client: MongoClient | undefined
  private ownsClient = false
  private db: Db | undefined
  private readonly database: string | undefined
  private readonly syncId: string
  private readonly batchSize: number | undefined
  /**
   * What the most recent `init()` did, including any orphan indexes it found.
   * The engine's `init()` returns nothing, so this is how a caller — the
   * CLI's `init` command — gets the report back.
   */
  lastMigration: MigrateResult | undefined

  constructor(private options: MongoDestinationOptions) {
    if (!options.client && !options.url) throw new Error('url or client is required')
    // With both `client` and `url`, `client` is what gets used and `init()`
    // never connects, so a CA given alongside would be silently dropped.
    if (options.sslCa !== undefined && (options.client || !options.url)) {
      throw new Error('sslCa applies to url; a pre-built client carries its own TLS settings')
    }
    this.names = new Names(options.prefix)
    this.prefix = this.names.prefix
    this.syncId = options.syncId ?? 'default'
    this.batchSize = options.batchSize
    this.client = options.client
    this.database = options.database ?? (options.url ? databaseFromUrl(options.url) : undefined)
  }

  /** The database the mirror lives in. Known once `init()` has run, or sooner when configured. */
  get databaseName(): string | undefined {
    return this.db?.databaseName ?? this.database
  }

  private get store(): Db {
    if (!this.db) throw new Error('destination not initialised: call init() first')
    return this.db
  }

  private get writeOpts(): WriteOptions {
    return { batchSize: this.batchSize }
  }

  private get bookkeepingOpts() {
    return { syncId: this.syncId }
  }

  private mirror(stream: TableDef): Collection<MirrorDoc> {
    return this.store.collection<MirrorDoc>(this.names.mirror(stream))
  }

  private get sync(): Collection<SyncDoc> {
    return this.store.collection<SyncDoc>(this.names.sync)
  }

  private get quarantine(): Collection<RejectedDoc> {
    return this.store.collection<RejectedDoc>(this.names.rejected)
  }

  async init(streams: TableDef[], logger?: Logger): Promise<void> {
    if (!this.client) {
      this.client = await connect(this.options.url!, { sslCa: this.options.sslCa, logger })
      this.ownsClient = true
    }
    this.db ??= this.client.db(this.database ?? (this.options.client ? this.client.db().databaseName : 'workos'))
    this.lastMigration = await migrate(this.db, streams, this.names, logger, {
      repairViews: this.options.repairViews,
    })
  }

  async close(): Promise<void> {
    if (this.ownsClient && this.client) await this.client.close()
  }

  upsert(stream: TableDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats> {
    return upsertRows(this.mirror(stream), this.writeOpts, stream, env, rows, mode)
  }

  delete(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return deleteRows(this.mirror(stream), this.writeOpts, stream, env, rows)
  }

  relink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return relinkRows(this.mirror(stream), this.writeOpts, stream, env, rows)
  }

  unlink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats> {
    return unlinkRows(this.mirror(stream), this.writeOpts, stream, env, rows)
  }

  async liveIds(stream: TableDef, env: string): Promise<string[]> {
    const paths = stream.key.map((k) => keyPath(stream, k))
    const fields = paths.map((p) => fieldPath(p))
    const filter: Document = stream.mode === 'append' ? { _env: env } : { _env: env, _deleted_at: null }
    const docs = await this.mirror(stream)
      .find(filter, {
        projection: { _id: 0, ...Object.fromEntries(fields.map((f) => [f, 1])) },
        sort: Object.fromEntries(fields.map((f) => [f, 1])),
      })
      .toArray()
    return docs.map((doc) => encodeKey(paths.map((p) => String(readPath(doc._raw, p)))))
  }

  async rawRows(stream: TableDef, env: string): Promise<Record<string, unknown>[]> {
    const docs = await this.mirror(stream)
      .find({ _env: env }, { projection: { _id: 0, _raw: 1 } })
      .toArray()
    return docs.map((doc) => doc._raw)
  }

  /** Staged into collections of their own and diffed by the server; see `db/reconcile.ts`. */
  diffIds(stream: TableDef, env: string, remote: AsyncIterable<Record<string, unknown>[]>): Promise<IdDiff> {
    return diffIdsInMongo(this.store, this.names, this.writeOpts, stream, env, remote)
  }

  async freshness(stream: TableDef, env: string, lagWindowHours: number): Promise<FreshnessRow> {
    const windowMs = lagWindowHours * 3_600_000
    const recent = { $gt: ['$_updated_at', { $subtract: ['$$NOW', windowMs] }] }
    const [row] = await this.mirror(stream)
      .aggregate<{
        rows: number
        deleted: number
        lastSynced: Date | null
        staleness: number | null
        lag: number | null
        lagN: number
      }>([
        { $match: { _env: env } },
        {
          $group: {
            _id: null,
            rows: { $sum: 1 },
            deleted: { $sum: { $cond: [{ $ne: [{ $ifNull: ['$_deleted_at', null] }, null] }, 1, 0] } },
            lastSynced: { $max: '$_synced_at' },
            lastUpdated: { $max: '$_updated_at' },
            // `$avg` ignores nulls, so only rows inside the window count.
            lag: { $avg: { $cond: [recent, { $subtract: ['$_synced_at', '$_updated_at'] }, null] } },
            lagN: { $sum: { $cond: [recent, 1, 0] } },
          },
        },
        {
          $project: {
            _id: 0,
            rows: 1,
            deleted: 1,
            lastSynced: 1,
            lag: 1,
            lagN: 1,
            staleness: { $subtract: ['$$NOW', '$lastUpdated'] },
          },
        },
      ])
      .toArray()
    const lagN = row?.lagN ?? 0
    return {
      stream: stream.name,
      rows: row?.rows ?? 0,
      deleted: row?.deleted ?? 0,
      lastSyncedAt: row?.lastSynced ? row.lastSynced.toISOString() : null,
      stalenessSeconds: row?.staleness != null ? row.staleness / 1000 : null,
      lagSeconds: lagN > 0 && row?.lag != null ? row.lag / 1000 : null,
      lagSampleSize: lagN,
    }
  }

  async redact(streams: TableDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]> {
    const out: { table: string; rows: number }[] = []
    for (const stream of streams) {
      const col = stream.columns.find((c) => c.name === 'user_id' || (c.name === 'id' && stream.name === 'users'))
      if (!col) continue
      const r = await this.mirror(stream).deleteMany({ _env: env, [fieldPath(col.path)]: userId })
      if (r.deletedCount > 0) out.push({ table: stream.name, rows: r.deletedCount })
    }
    // Archived events embed the payload, so they must be scrubbed too.
    const ev = await this.store
      .collection<MirrorDoc>(this.names.mirror('events'))
      .deleteMany({ _env: env, $or: [{ '_raw.data.id': userId }, { '_raw.data.user_id': userId }] })
    if (ev.deletedCount > 0) out.push({ table: 'events', rows: ev.deletedCount })
    // Quarantined events embed the payload too, and they may be the only copy
    // of it: an event rejected before its archive write succeeded is nowhere
    // in `events`.
    const rj = await this.quarantine.deleteMany({
      sync_id: this.syncId,
      env,
      $or: [{ object_id: userId }, { '_raw.data.user_id': userId }],
    })
    if (rj.deletedCount > 0) out.push({ table: '_rejected', rows: rj.deletedCount })
    return out
  }

  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void> {
    return rejectEvents(this.quarantine, this.bookkeepingOpts, env, rows)
  }

  rejected(env: string, opts: { ids?: string[] } = {}): Promise<RejectedEvent[]> {
    return listRejected(this.quarantine, this.bookkeepingOpts, env, opts.ids)
  }

  rejectedCount(env: string): Promise<number> {
    return countRejected(this.quarantine, this.bookkeepingOpts, env)
  }

  clearRejected(env: string, ids: string[]): Promise<number> {
    return clearRejected(this.quarantine, this.bookkeepingOpts, env, ids)
  }

  isTransientError(err: unknown): boolean {
    return isTransientMongoError(err)
  }

  getState<T>(env: string, stream: string): Promise<T | null> {
    return getState<T>(this.sync, this.syncId, env, stream)
  }

  setState(env: string, stream: string, state: unknown): Promise<void> {
    return setState(this.sync, this.syncId, env, stream, state)
  }

  setStateIfHeld(env: string, stream: string, state: unknown, holder: string, expected: unknown): Promise<boolean> {
    return setStateIfHeld(this.sync, this.syncId, env, stream, state, holder, expected)
  }

  acquireLease(env: string, holder: string, ttlMs: number): Promise<LeaseResult> {
    return acquireLease(this.sync, this.syncId, env, holder, ttlMs)
  }

  releaseLease(env: string, holder: string): Promise<void> {
    return releaseLease(this.sync, this.syncId, env, holder)
  }

  getLease(env: string): Promise<Lease | null> {
    return getLease(this.sync, this.syncId, env)
  }
}
