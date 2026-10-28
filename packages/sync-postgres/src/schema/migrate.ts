/**
 * Schema setup.
 *
 * The DDL is derived from the stream catalog rather than hand-written per
 * field. A comparable sync engine accumulated 42 hand-written migration
 * files, one per upstream schema change, each needing a release; deriving the
 * DDL instead means adding a projected column is a catalog edit.
 *
 * A fingerprint gates per-stream DDL, so unchanged catalogs skip that work.
 * A per-schema transaction lock serializes setup, including the bootstrap DDL.
 *
 * The DDL is additive, which cannot change a column that already exists. So
 * after it runs, and only when the fingerprint is new, the live tables are
 * compared against the catalog (see `drift.ts`). The fingerprint is withheld
 * while any projection is wrong: recording it would turn the next `init` into
 * a silent no-op on a database that needs attention.
 */
import { createHash } from 'node:crypto'
import type { SqlClient } from '../db/client.js'
import { buildSchemaSql, createSchemaSql, migrationsTableSql, parseIso8601Function, quoteIdent } from './ddl.js'
import { applyViewStatement, isViewStatement, type ApplyViewOptions } from './views.js'
import { detectColumnDrift, repairColumnDrift, unrepairableReason, type ColumnDrift } from './drift.js'
import type { Logger, TableDef } from '@workos/sync-engine'

/**
 * Bumped whenever the DDL generated for an unchanged catalog changes: a new
 * kind of object, a reworked index, a view. The fingerprint otherwise hashes
 * only the catalog, so without this a database whose streams did not change
 * would never run the new statements on upgrade.
 *
 *   2: `<stream>_live` views (views.ts)
 */
export const DDL_VERSION = 2

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

/**
 * Both repairs ride on the CLI's single `init --repair`: a column that drifted
 * from the catalog and a generated view that can no longer be replaced in
 * place are the two things additive DDL cannot fix on its own.
 */
export interface MigrateOptions extends ApplyViewOptions {
  /**
   * Rebuild drifted generated columns from `_raw` instead of only reporting
   * them. Key columns, non-generated columns, columns with your own indexes
   * or constraints on them, and orphans are still only reported; see
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
   * Generated columns that still disagree with the catalog after this run.
   * Any `expression` or `type` item here means the fingerprint was *not*
   * recorded, so the next run detects and reports again. Orphans are reported
   * but do not hold the fingerprint back: they are harmless to leave and the
   * engine will never drop one.
   */
  drift: ColumnDrift[]
  /**
   * Columns that match the catalog because of this run's repair pass: rebuilt
   * from `_raw` here, or found already rebuilt by a concurrent process. Only
   * with `repair: true`.
   */
  repaired: ColumnDrift[]
}

export async function migrate(
  db: SqlClient,
  streams: TableDef[],
  schema: string,
  logger?: Logger,
  options: MigrateOptions = {},
): Promise<MigrateResult> {
  return db.transaction(async (tx) => {
    // Lock before even CREATE SCHEMA: IF NOT EXISTS does not serialize catalog writes.
    await tx.query(
      `select pg_advisory_xact_lock(hashtext('workos-sync-postgres:migrate'), hashtext($1::name::text))`,
      [schema],
    )
    // Repairs must recover from failed DDL without aborting the migration or releasing its lock.
    const nested: SqlClient = {
      query: tx.query.bind(tx),
      exec: tx.exec.bind(tx),
      close: tx.close.bind(tx),
      async transaction<T>(fn: (client: SqlClient) => Promise<T>): Promise<T> {
        await tx.exec('savepoint workos_sync_migrate')
        try {
          const result = await fn(nested)
          await tx.exec('release savepoint workos_sync_migrate')
          return result
        } catch (err) {
          try {
            await tx.exec('rollback to savepoint workos_sync_migrate; release savepoint workos_sync_migrate')
          } catch {
            // The outer transaction still rolls back; preserve the original error.
          }
          throw err
        }
      },
    }
    return applySchema(nested, streams, schema, logger, options)
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

  // The fingerprint table must exist before we can check whether it says
  // this fingerprint is already applied. The bookkeeping store's tables are
  // its own to create.
  await db.exec(createSchemaSql(schema))
  await db.exec(parseIso8601Function(schema))
  await db.exec(migrationsTableSql(schema))

  const existing = await db.query(
    `select 1 from ${quoteIdent(schema)}."_migrations" where fingerprint = $1`,
    [fp],
  )
  if (existing.rows.length > 0) {
    logger?.debug('schema up to date', { fingerprint: fp })
    return { applied: false, fingerprint: fp, statements: 0, drift: [], repaired: [] }
  }

  const statements = buildSchemaSql(streams, schema)
  for (const sql of statements) {
    // A view is the one statement that can legitimately fail on an existing
    // database; everything else is `if not exists`. See applyViewStatement.
    if (isViewStatement(sql)) await applyViewStatement(db, sql, logger, options)
    else await db.exec(sql)
  }

  // Detection runs only on this path — once per catalog change, not on every
  // restart — because a recorded fingerprint means it already came back clean.
  let drift = await detectColumnDrift(db, streams, schema)
  let repaired: ColumnDrift[] = []
  const failures = new Map<ColumnDrift, string>()
  if (options.repair && drift.length > 0) {
    const r = await repairColumnDrift(db, drift, streams, schema, logger)
    repaired = r.repaired
    for (const f of r.failed) failures.set(f.drift, f.error)
    drift = drift.filter((d) => !repaired.includes(d))
  }

  // A recorded fingerprint means "the live columns were checked against this
  // catalog". Rebuilding a column breaks that promise for every catalog
  // recorded before now: roll back to one of them and the early return above
  // would trust a fingerprint the live table no longer satisfies, with no
  // detection to say so. So the moment columns change, every other
  // fingerprint is forgotten, and a rollback re-runs detection like any other
  // catalog change. Additive-only runs keep theirs: no column changed.
  if (repaired.length > 0) {
    await db.query(`delete from ${quoteIdent(schema)}."_migrations" where fingerprint <> $1`, [fp])
  }

  for (const d of drift) {
    const fields = { stream: d.stream, column: d.column, expected: d.expected, actual: d.actual, key: d.key, ddl: d.ddl }
    if (d.kind === 'orphan') {
      // Still computes from `_raw` with its old expression, so it is harmless
      // to leave; reported so a renamed or removed column does not linger unnoticed.
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
    return { applied: true, fingerprint: fp, statements: statements.length, drift, repaired }
  }

  await db.query(
    `insert into ${quoteIdent(schema)}."_migrations" (fingerprint, detail)
     values ($1, $2::jsonb) on conflict (fingerprint) do nothing`,
    [fp, JSON.stringify({ streams: streams.map((s) => s.name), statements: statements.length })],
  )

  logger?.info('schema applied', { fingerprint: fp, statements: statements.length, repaired: repaired.length })
  return { applied: true, fingerprint: fp, statements: statements.length, drift, repaired }
}
