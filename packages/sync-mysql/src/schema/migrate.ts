/**
 * Schema setup.
 *
 * The DDL is derived from the stream catalog rather than hand-written per
 * field, and a fingerprint gates the per-stream work, so an unchanged
 * catalog costs one query. A named lock (`GET_LOCK`) serialises concurrent
 * setups, held on one pinned connection for the whole run because MySQL
 * DDL commits implicitly and so cannot be wrapped in a transaction.
 *
 * Every statement is idempotent — `CREATE ... IF NOT EXISTS`, and columns
 * and indexes added only after `information_schema` says they are missing —
 * so a run that dies halfway is simply finished by the next one.
 *
 * The DDL is additive, which cannot change a column that already exists. So
 * after it runs, and only when the fingerprint is new, the live tables are
 * compared against the catalog (see `drift.ts`). The fingerprint is withheld
 * while any projection is wrong: recording it would turn the next `init`
 * into a silent no-op on a database that needs attention.
 */
import { createHash } from 'node:crypto'
import type { Logger, TableDef } from '@workos/sync-engine'
import type { SqlClient } from '../db/client.js'
import {
  addColumnSql,
  addIndexSql,
  bookkeepingSql,
  createSchemaSql,
  createTableSql,
  generatedColumnSql,
  indexSpecs,
  quoteIdent,
  safeIdentifier,
} from './ddl.js'
import { applyLiveView, type ApplyViewOptions } from './views.js'
import { detectColumnDrift, recordColumn, repairColumnDrift, unrepairableReason, type ColumnDrift } from './drift.js'

/**
 * Bumped whenever the DDL generated for an unchanged catalog changes: a new
 * kind of object, a reworked index, a view. The fingerprint otherwise hashes
 * only the catalog, so without this a database whose streams did not change
 * would never run the new statements on upgrade.
 */
export const DDL_VERSION = 1

export function fingerprint(streams: TableDef[], schema: string): string {
  const canonical = JSON.stringify({
    ddl: DDL_VERSION,
    schema,
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

export interface MigrateOptions extends ApplyViewOptions {
  /**
   * Rebuild drifted generated columns from `_raw` instead of only reporting
   * them. Key columns, non-generated columns, columns with your own indexes
   * on them, and orphans are still only reported; see `repairColumnDrift`.
   */
  repair?: boolean
}

export interface MigrateResult {
  /** DDL was run. `false` means the fingerprint was already recorded. */
  applied: boolean
  fingerprint: string
  statements: number
  /**
   * Generated columns that still disagree with the catalog after this run.
   * Any `expression` or `type` item here means the fingerprint was *not*
   * recorded, so the next run detects and reports again. Orphans are
   * reported but do not hold the fingerprint back.
   */
  drift: ColumnDrift[]
  /** Columns rebuilt from `_raw` by this run's repair pass. Only with `repair: true`. */
  repaired: ColumnDrift[]
}

/** `GET_LOCK` names are limited to 64 characters. */
export function lockName(schema: string): string {
  const name = `workos-sync-mysql:migrate:${schema}`
  if (name.length <= 64) return name
  return `workos-sync-mysql:migrate:${createHash('sha256').update(schema).digest('hex').slice(0, 32)}`
}

export async function migrate(
  db: SqlClient,
  streams: TableDef[],
  schema: string,
  logger?: Logger,
  options: MigrateOptions = {},
): Promise<MigrateResult> {
  return db.pinned(async (conn) => {
    const lock = lockName(schema)
    // Read the value by position, not by alias: Vitess (PlanetScale) answers a
    // lock function itself and names the column after the expression, so `l`
    // would be undefined there even though the lock was granted.
    const got = await conn.query<Record<string, number | null>>(`select get_lock(?, 60) as l`, [lock])
    const taken = Object.values(got.rows[0] ?? {})[0]
    if (Number(taken) !== 1) throw new Error(`could not take the migration lock for ${schema} within 60s`)
    try {
      return await applySchema(conn, streams, schema, logger, options)
    } finally {
      await conn.query(`select release_lock(?)`, [lock])
    }
  })
}

async function applySchema(
  db: SqlClient,
  streams: TableDef[],
  schema: string,
  logger: Logger | undefined,
  options: MigrateOptions,
): Promise<MigrateResult> {
  const fp = fingerprint(streams, schema)

  // `CREATE DATABASE IF NOT EXISTS` still needs the CREATE privilege when
  // the database is already there, so only ask when it is not.
  const exists = await db.query(`select 1 from information_schema.schemata where schema_name = ?`, [schema])
  if (exists.rows.length === 0) await db.exec(createSchemaSql(schema))
  // The bookkeeping tables must exist before we can check whether they say
  // this fingerprint is already applied. Only the missing ones are created:
  // a host that gates DDL (PlanetScale's safe migrations) refuses even
  // `CREATE TABLE IF NOT EXISTS`, and once the schema is in place `init`
  // must get by on DML alone.
  const bookkeeping = await db.query<{ table_name: string }>(
    `select table_name as table_name from information_schema.tables where table_schema = ? and table_name like '\\_%'`,
    [schema],
  )
  const have = new Set(bookkeeping.rows.map((r) => r.table_name))
  for (const [name, sql] of bookkeepingSql(schema)) if (!have.has(name)) await db.exec(sql)

  const existing = await db.query(`select 1 from ${quoteIdent(schema)}.\`_migrations\` where fingerprint = ?`, [fp])
  if (existing.rows.length > 0) {
    logger?.debug('schema up to date', { fingerprint: fp })
    return { applied: false, fingerprint: fp, statements: 0, drift: [], repaired: [] }
  }

  let statements = 0
  for (const stream of streams) {
    const table = safeIdentifier(stream.name)
    const live = await db.query<{ column_name: string }>(
      `select column_name as column_name from information_schema.columns where table_schema = ? and table_name = ?`,
      [schema, table],
    )
    if (live.rows.length === 0) {
      await db.exec(createTableSql(stream, schema))
      statements++
      for (const col of stream.columns) {
        await recordColumn(db, schema, table, safeIdentifier(col.name), generatedColumnSql(col, stream))
      }
    } else {
      const present = new Set(live.rows.map((r) => r.column_name))
      for (const col of stream.columns) {
        if (present.has(safeIdentifier(col.name))) continue
        // Computed from the rows already present. No backfill, no downtime.
        await db.exec(addColumnSql(stream, col, schema))
        await recordColumn(db, schema, table, safeIdentifier(col.name), generatedColumnSql(col, stream))
        statements++
      }
      const indexes = await db.query<{ index_name: string }>(
        `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = ?`,
        [schema, table],
      )
      const have = new Set(indexes.rows.map((r) => r.index_name))
      for (const idx of indexSpecs(stream)) {
        if (have.has(idx.name)) continue
        await db.exec(addIndexSql(stream, idx, schema))
        statements++
      }
    }
    // Last, so the view's `select *` is expanded after any column was added.
    if (await applyLiveView(db, stream, schema, logger, options)) statements++
  }

  // Detection runs only on this path — once per catalog change, not on every
  // restart — because a recorded fingerprint means it already came back clean.
  let drift = await detectColumnDrift(db, streams, schema)
  let repaired: ColumnDrift[] = []
  const failures = new Map<ColumnDrift, string>()
  if (options.repair && drift.some((d) => unrepairableReason(d) === null)) {
    // A recorded fingerprint means "the live columns were checked against
    // this catalog". Rebuilding a column breaks that promise for every
    // catalog recorded before now, so every other fingerprint is forgotten
    // and a rollback re-runs detection. Forgotten *before* the first DDL,
    // not after the last: MySQL commits each statement on its own, so a
    // repair that dies between dropping a column and adding it back has
    // already changed the table, and an older fingerprint must not vouch
    // for what is left.
    await db.query(`delete from ${quoteIdent(schema)}.\`_migrations\` where fingerprint <> ?`, [fp])
    const r = await repairColumnDrift(db, drift, streams, schema, logger)
    repaired = r.repaired
    for (const f of r.failed) failures.set(f.drift, f.error)
    drift = drift.filter((d) => !repaired.includes(d))
  }

  for (const d of drift) {
    const fields = { stream: d.stream, column: d.column, expected: d.expected, actual: d.actual, key: d.key }
    if (d.kind === 'orphan') {
      logger?.warn('orphan generated column', fields)
      continue
    }
    const reason = failures.get(d) ?? unrepairableReason(d) ?? 'run `init --repair`, or apply the DDL by hand'
    logger?.error('column drift: projection does not match the catalog', { ...fields, kind: d.kind, reason })
  }

  const unresolved = drift.filter((d) => d.kind !== 'orphan')
  if (unresolved.length > 0) {
    logger?.error('schema fingerprint not recorded: generated columns have drifted', {
      fingerprint: fp,
      columns: unresolved.length,
    })
    return { applied: true, fingerprint: fp, statements, drift, repaired }
  }

  await db.query(
    `insert ignore into ${quoteIdent(schema)}.\`_migrations\` (fingerprint, detail) values (?, cast(? as json))`,
    [fp, JSON.stringify({ streams: streams.map((s) => s.name), statements })],
  )

  logger?.info('schema applied', { fingerprint: fp, statements, repaired: repaired.length })
  return { applied: true, fingerprint: fp, statements, drift, repaired }
}
