/**
 * Schema setup.
 *
 * The DDL is derived from the stream catalog rather than hand-written per
 * field, and a fingerprint gates the per-stream work, so an unchanged
 * catalog costs one query. Every statement is idempotent — `CREATE ... IF
 * NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, `ADD INDEX IF NOT EXISTS` — so
 * two processes applying the schema at once, or a run that dies halfway,
 * leave the same database as one clean run; there is no lock to take.
 *
 * The DDL is additive, which cannot change a column that already exists. So
 * after it runs, and only when the fingerprint is new, the live tables are
 * compared against the catalog (see `drift.ts`). The fingerprint is withheld
 * while any projection is wrong: recording it would turn the next `init`
 * into a silent no-op on a database that needs attention.
 */
import { createHash } from 'node:crypto'
import type { Logger, TableDef } from '@workos/sync-engine'
import { KEEPER_SETTINGS, type ChClient } from '../db/client.js'
import {
  addColumnSql,
  addIndexSql,
  materializeColumnSql,
  bookkeepingSql,
  createSchemaSql,
  createTableSql,
  generatedColumnSql,
  indexSpecs,
  quoteIdent,
  safeIdentifier,
} from './ddl.js'
import { applyLiveView, isGeneratedDefinition, liveViewName, type ApplyViewOptions } from './views.js'
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
   * Redefine drifted columns instead of only reporting them. Key columns,
   * non-generated columns and orphans are still only reported; see
   * `repairColumnDrift`.
   */
  repair?: boolean
}

export interface MigrateResult {
  /** DDL was run. `false` means the fingerprint was already recorded. */
  applied: boolean
  fingerprint: string
  statements: number
  /**
   * Columns that still disagree with the catalog after this run. Any
   * `expression` or `type` item here means the fingerprint was *not*
   * recorded, so the next run detects and reports again. Orphans are
   * reported but do not hold the fingerprint back.
   */
  drift: ColumnDrift[]
  /** Columns redefined by this run's repair pass. Only with `repair: true`. */
  repaired: ColumnDrift[]
}

const names = async (db: ChClient, sql: string, params: Record<string, unknown>) =>
  new Set((await db.query<{ name: string }>(sql, params)).map((r) => r.name))

const typedColumns = async (db: ChClient, schema: string, table: string) =>
  new Map(
    (
      await db.query<{ name: string; type: string }>(
        `SELECT name, type FROM system.columns WHERE database = {schema:String} AND table = {table:String}`,
        { schema, table },
      )
    ).map((r) => [r.name, r.type]),
  )

export async function migrate(
  db: ChClient,
  streams: TableDef[],
  schema: string,
  logger?: Logger,
  options: MigrateOptions = {},
): Promise<MigrateResult> {
  const fp = fingerprint(streams, schema)
  const s = quoteIdent(schema)

  // Only when missing: creating needs a privilege the day-to-day user may not have.
  const databases = await names(db, `SELECT name FROM system.databases WHERE name = {schema:String}`, { schema })
  if (!databases.has(schema)) await db.command(createSchemaSql(schema))
  // The bookkeeping tables must exist before we can check whether they say
  // this fingerprint is already applied.
  const tables = await names(db, `SELECT name FROM system.tables WHERE database = {schema:String}`, { schema })
  for (const [name, sql] of bookkeepingSql(schema)) if (!tables.has(name)) await db.command(sql)

  const applied = await db.query(`SELECT 1 FROM ${s}.\`_migrations\` WHERE fingerprint = {fp:String}`, { fp })
  if (applied.length > 0) {
    logger?.debug('schema up to date', { fingerprint: fp })
    return { applied: false, fingerprint: fp, statements: 0, drift: [], repaired: [] }
  }

  let statements = 0
  for (const stream of streams) {
    const table = safeIdentifier(stream.name)
    const live = await names(
      db,
      `SELECT name FROM system.columns WHERE database = {schema:String} AND table = {table:String}`,
      { schema, table },
    )
    if (live.size === 0) {
      await db.command(createTableSql(stream, schema))
      statements++
      for (const col of stream.columns) {
        await recordColumn(db, schema, table, safeIdentifier(col.name), generatedColumnSql(col, stream))
      }
    } else {
      for (const col of stream.columns) {
        if (live.has(safeIdentifier(col.name))) continue
        // Computed for the rows already present, and written into their
        // parts: left unmaterialised, every read of an old part would
        // evaluate the expression again.
        await db.command(addColumnSql(stream, col, schema))
        await db.command(materializeColumnSql(stream, col, schema))
        await recordColumn(db, schema, table, safeIdentifier(col.name), generatedColumnSql(col, stream))
        statements++
      }
      const indexes = await names(
        db,
        `SELECT name FROM system.data_skipping_indices WHERE database = {schema:String} AND table = {table:String}`,
        { schema, table },
      )
      for (const idx of indexSpecs(stream)) {
        if (indexes.has(idx.name)) continue
        await db.command(addIndexSql(stream, idx, schema))
        statements++
      }
    }
    // Last, so the view carries every column, including any added above. A
    // view that is the generated shape and already does is left alone:
    // `init` runs no DDL it does not need. Anything else under the name —
    // a column short, or a definition that is not ours, however many
    // columns it has — goes through `applyLiveView`, which refuses a
    // foreign definition unless `repairViews` says to replace it.
    if (stream.mode !== 'append' && live.size > 0) {
      const [view] = await db.query<{ engine: string; as_select: string }>(
        `SELECT engine, as_select FROM system.tables WHERE database = {schema:String} AND name = {name:String}`,
        { schema, name: liveViewName(stream) },
      )
      if (view?.engine === 'View' && isGeneratedDefinition(view.as_select, schema, table)) {
        // Name and type both: a view fixes its columns' types when it is
        // created, so a column redefined since declares the old one there.
        const viewColumns = await typedColumns(db, schema, liveViewName(stream))
        const tableColumns = await typedColumns(db, schema, table)
        if ([...tableColumns].every(([c, t]) => viewColumns.get(c) === t)) continue
      }
    }
    if (await applyLiveView(db, stream, schema, logger, options)) statements++
  }

  // Detection runs only on this path — once per catalog change, not on every
  // restart — because a recorded fingerprint means it already came back clean.
  let drift = await detectColumnDrift(db, streams, schema)
  let repaired: ColumnDrift[] = []
  const failures = new Map<ColumnDrift, string>()
  if (options.repair && drift.some((d) => unrepairableReason(d) === null)) {
    // A recorded fingerprint means "the live columns were checked against
    // this catalog". Redefining a column breaks that promise for every
    // catalog recorded before now, so every other fingerprint is forgotten,
    // before the first statement rather than after the last, and a rollback
    // re-runs detection.
    await db.command(
      `ALTER TABLE ${s}.\`_migrations\` DELETE WHERE fingerprint != {fp:String}`,
      { fp },
      { ...KEEPER_SETTINGS, keeper_map_strict_mode: 0 },
    )
    const r = await repairColumnDrift(db, drift, streams, schema, logger)
    repaired = r.repaired
    for (const f of r.failed) failures.set(f.drift, f.error)
    drift = drift.filter((d) => !repaired.includes(d))
    // A redefined column changes the type its view declares for it, and the
    // view fixed that when it was created: the views of the streams touched
    // are re-created now, after the repair rather than before it.
    for (const name of new Set(repaired.map((d) => d.stream))) {
      const stream = streams.find((st) => st.name === name)
      if (stream && stream.mode !== 'append' && (await applyLiveView(db, stream, schema, logger, options))) statements++
    }
  }

  for (const d of drift) {
    const fields = { stream: d.stream, column: d.column, expected: d.expected, actual: d.actual, key: d.key }
    if (d.kind === 'orphan') {
      logger?.warn('orphan projected column', fields)
      continue
    }
    const reason = failures.get(d) ?? unrepairableReason(d) ?? 'run `init --repair`, or apply the DDL by hand'
    logger?.error('column drift: projection does not match the catalog', { ...fields, kind: d.kind, reason })
  }

  const unresolved = drift.filter((d) => d.kind !== 'orphan')
  if (unresolved.length > 0) {
    logger?.error('schema fingerprint not recorded: projected columns have drifted', {
      fingerprint: fp,
      columns: unresolved.length,
    })
    return { applied: true, fingerprint: fp, statements, drift, repaired }
  }

  await db.command(
    `INSERT INTO ${s}.\`_migrations\` (fingerprint, applied_at, detail) VALUES ({fp:String}, now64(3, 'UTC'), {detail:String})`,
    { fp, detail: JSON.stringify({ streams: streams.map((st) => st.name), statements }) },
    { ...KEEPER_SETTINGS, keeper_map_strict_mode: 0 },
  )

  logger?.info('schema applied', { fingerprint: fp, statements, repaired: repaired.length })
  return { applied: true, fingerprint: fp, statements, drift, repaired }
}
