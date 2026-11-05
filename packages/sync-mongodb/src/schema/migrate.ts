/**
 * Schema setup.
 *
 * MongoDB has no columns to declare, so "schema" here is the collections,
 * their indexes and the `_live` views, all derived from the stream catalog
 * rather than hand-written per field. A comparable sync engine accumulated 42
 * hand-written migration files, one per upstream schema change, each needing
 * a release; deriving the indexes instead means promoting a field to an
 * index is a catalog edit.
 *
 * Every step is idempotent — `createCollection` tolerates an existing one,
 * `createIndexes` is a no-op for an index that already exists with the same
 * specification, a view is compared before it is touched — so two processes
 * running `init` at once need no lock. A fingerprint of the catalog gates the
 * whole pass, so an unchanged catalog costs one query on every start.
 *
 * There are no typed columns to drift, so there is no repair pass. What can
 * linger is an index for a column the catalog no longer indexes; it is
 * reported as an orphan and never dropped, since it may be doing work for
 * somebody's query.
 */
import { createHash } from 'node:crypto'
import type { Logger, TableDef } from '@workos/sync-engine'
import type { Db, Document, IndexDescription } from 'mongodb'
import { fieldPath, keyPath, type Names } from './names.js'
import { ensureLiveView, type ApplyViewOptions } from './views.js'

/**
 * Bumped whenever what is generated for an unchanged catalog changes: a new
 * kind of index, a reworked view. The fingerprint otherwise hashes only the
 * catalog, so without this a database whose streams did not change would
 * never see the new work on upgrade.
 */
export const DDL_VERSION = 1

export function fingerprint(streams: TableDef[], prefix: string): string {
  const canonical = JSON.stringify({
    ddl: DDL_VERSION,
    prefix,
    streams: streams
      .map((s) => ({
        n: s.name,
        k: s.key,
        m: s.mode,
        c: s.columns.map((c) => [c.name, c.type, c.path, c.index ?? false]),
      }))
      .sort((a, b) => a.n.localeCompare(b.n)),
  })
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16)
}

/**
 * The indexes a stream's collection carries: the unique key, one per
 * `index: true` column, and the two the engine's own queries scan. Every
 * index key leads with `_env`, since every query does.
 */
export function indexSpecs(stream: TableDef): IndexDescription[] {
  const specs: IndexDescription[] = []
  const key: Record<string, 1> = { _env: 1 }
  for (const k of stream.key) key[fieldPath(keyPath(stream, k))] = 1
  specs.push({ key, unique: true })
  const seen = new Set([indexName(key)])
  for (const c of stream.columns) {
    if (!c.index) continue
    const spec = { _env: 1 as const, [fieldPath(c.path)]: 1 as const }
    if (seen.has(indexName(spec))) continue
    seen.add(indexName(spec))
    specs.push({ key: spec })
  }
  // Freshness and reconciliation scan these.
  specs.push({ key: { _synced_at: 1 } })
  if (stream.mode !== 'append') {
    specs.push({ key: { _deleted_at: 1 }, partialFilterExpression: { _deleted_at: { $type: 'date' } } })
  }
  return specs
}

/** The name MongoDB gives an index left unnamed: fields and directions joined by `_`. */
export function indexName(key: Document): string {
  return Object.entries(key)
    .map(([field, direction]) => `${field}_${String(direction)}`)
    .join('_')
}

export interface OrphanIndex {
  collection: string
  index: string
  key: Document
}

export interface MigrateOptions extends ApplyViewOptions {}

export interface MigrateResult {
  /** Work was done. `false` means the fingerprint was already recorded. */
  applied: boolean
  fingerprint: string
  collections: number
  indexes: number
  /** `_live` views created or redefined by this run. */
  views: string[]
  /**
   * Indexes on the engine's collections that the catalog does not call for:
   * a column no longer indexed, or an `extraColumns` path that changed.
   * Reported, never dropped.
   */
  orphans: OrphanIndex[]
}

interface MigrationDoc extends Document {
  _id: string
  applied_at: Date
  detail: Document
}

const NAMESPACE_EXISTS = 48
const DUPLICATE_KEY = 11000

async function ensureCollection(db: Db, name: string): Promise<boolean> {
  try {
    await db.createCollection(name)
    return true
  } catch (err) {
    if ((err as { code?: unknown }).code !== NAMESPACE_EXISTS) throw err
    return false
  }
}

/**
 * Engine bookkeeping: the lease-and-cursors document, the quarantine, and the
 * applied fingerprints. Ensured on every `migrate` before the fingerprint
 * check, so an index added here reaches existing deployments without a new
 * fingerprint.
 */
export async function ensureBookkeeping(db: Db, names: Names): Promise<void> {
  await db.collection(names.sync).createIndexes([{ key: { sync_id: 1, env: 1 }, unique: true }])
  await db.collection(names.rejected).createIndexes([{ key: { sync_id: 1, env: 1, event_id: 1 }, unique: true }])
}

export async function migrate(
  db: Db,
  streams: TableDef[],
  names: Names,
  logger?: Logger,
  options: MigrateOptions = {},
): Promise<MigrateResult> {
  const fp = fingerprint(streams, names.prefix)
  await ensureBookkeeping(db, names)

  const migrations = db.collection<MigrationDoc>(names.migrations)
  if (await migrations.findOne({ _id: fp }, { projection: { _id: 1 } })) {
    logger?.debug('schema up to date', { fingerprint: fp })
    return { applied: false, fingerprint: fp, collections: 0, indexes: 0, views: [], orphans: [] }
  }

  let collections = 0
  let indexes = 0
  const views: string[] = []
  const orphans: OrphanIndex[] = []
  for (const stream of streams) {
    const name = names.mirror(stream)
    if (await ensureCollection(db, name)) collections++
    const coll = db.collection(name)
    const specs = indexSpecs(stream)
    await coll.createIndexes(specs)
    indexes += specs.length

    const expected = new Set(['_id_', ...specs.map((s) => s.name ?? indexName(s.key))])
    for (const existing of await coll.listIndexes().toArray()) {
      if (expected.has(existing['name'] as string)) continue
      const orphan = { collection: name, index: existing['name'] as string, key: existing['key'] as Document }
      orphans.push(orphan)
      // Still serving whatever query it was made for, so it is harmless to
      // leave; reported so a renamed or removed column does not linger unnoticed.
      logger?.warn('orphan index', orphan)
    }

    // Append-only streams are immutable logs: no tombstones, nothing to hide.
    if (stream.mode !== 'append') {
      const outcome = await ensureLiveView(db, names, stream, logger, options)
      if (outcome !== 'kept') views.push(names.liveView(stream))
    }
  }

  try {
    await migrations.insertOne({
      _id: fp,
      applied_at: new Date(),
      detail: { streams: streams.map((s) => s.name), collections, indexes },
    })
  } catch (err) {
    // A concurrent init recorded the same fingerprint first; the work was the same.
    if ((err as { code?: unknown }).code !== DUPLICATE_KEY) throw err
  }

  logger?.info('schema applied', { fingerprint: fp, collections, indexes, views: views.length })
  return { applied: true, fingerprint: fp, collections, indexes, views, orphans }
}
