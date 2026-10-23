/**
 * Schema setup.
 *
 * The DDL is derived from the stream catalog rather than hand-written per
 * field. A comparable sync engine accumulated 42 hand-written migration
 * files, one per upstream schema change, each needing a release; deriving the
 * DDL instead means adding a projected column is a catalog edit.
 *
 * Setup is gated on a fingerprint of the catalog, so a restart with no changes
 * is a single SELECT rather than dozens of DDL round trips.
 *
 * The DDL is additive, which cannot change a column that already exists. So
 * after it runs, and only when the fingerprint is new, the live tables are
 * compared against the catalog (see `drift.ts`). The fingerprint is withheld
 * while any projection is wrong: recording it would turn the next `init` into
 * a silent no-op on a database that needs attention.
 */
import { createHash } from 'node:crypto'
import type { SqlClient } from '../db/client.js'
import { buildSchemaSql, createSchemaSql, bookkeepingSql, parseIso8601Function, quoteIdent } from './ddl.js'
import { detectColumnDrift, repairColumnDrift, unrepairableReason, type ColumnDrift } from './drift.js'
import type { Logger, StreamDef } from '@workos/sync-engine'

export function fingerprint(streams: StreamDef[], schema: string): string {
  const canonical = JSON.stringify({
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

export interface MigrateOptions {
  /**
   * Rebuild drifted generated columns from `_raw` instead of only reporting
   * them. Key columns, non-generated columns and orphans are still only
   * reported; see `repairColumnDrift`.
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
  /** Columns rebuilt from `_raw` by this run. Only with `repair: true`. */
  repaired: ColumnDrift[]
}

export async function migrate(
  db: SqlClient,
  streams: StreamDef[],
  schema: string,
  logger?: Logger,
  options: MigrateOptions = {},
): Promise<MigrateResult> {
  const fp = fingerprint(streams, schema)

  // The bookkeeping tables must exist before we can check whether they say
  // this fingerprint is already applied.
  await db.exec(createSchemaSql(schema))
  await db.exec(parseIso8601Function(schema))
  await db.exec(bookkeepingSql(schema))

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
    await db.exec(sql)
  }

  // Detection runs only on this path — once per catalog change, not on every
  // restart — because a recorded fingerprint means it already came back clean.
  let drift = await detectColumnDrift(db, streams, schema)
  let repaired: ColumnDrift[] = []
  const failures = new Map<ColumnDrift, string>()
  if (options.repair && drift.length > 0) {
    const r = await repairColumnDrift(db, drift, logger)
    repaired = r.repaired
    for (const f of r.failed) failures.set(f.drift, f.error)
    drift = drift.filter((d) => !repaired.includes(d))
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
