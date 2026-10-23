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
 */
import { createHash } from 'node:crypto'
import type { SqlClient } from '../db/client.js'
import { buildSchemaSql, createSchemaSql, bookkeepingSql, parseIso8601Function, quoteIdent } from './ddl.js'
import { applyViewStatement, isViewStatement, type ApplyViewOptions } from './views.js'
import type { Logger, StreamDef } from '@workos/sync-engine'

/**
 * Bumped whenever the DDL generated for an unchanged catalog changes: a new
 * kind of object, a reworked index, a view. The fingerprint otherwise hashes
 * only the catalog, so without this a database whose streams did not change
 * would never run the new statements on upgrade.
 *
 *   2: `<stream>_live` views (views.ts)
 */
export const DDL_VERSION = 2

export function fingerprint(streams: StreamDef[], schema: string): string {
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

export interface MigrateResult {
  applied: boolean
  fingerprint: string
  statements: number
}

/** See `PostgresDestinationOptions.repairViews`. */
export type MigrateOptions = ApplyViewOptions

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
    return { applied: false, fingerprint: fp, statements: 0 }
  }

  const statements = buildSchemaSql(streams, schema)
  for (const sql of statements) {
    // A view is the one statement that can legitimately fail on an existing
    // database; everything else is `if not exists`. See applyViewStatement.
    if (isViewStatement(sql)) await applyViewStatement(db, sql, logger, options)
    else await db.exec(sql)
  }

  await db.query(
    `insert into ${quoteIdent(schema)}."_migrations" (fingerprint, detail)
     values ($1, $2::jsonb) on conflict (fingerprint) do nothing`,
    [fp, JSON.stringify({ streams: streams.map((s) => s.name), statements: statements.length })],
  )

  logger?.info('schema applied', { fingerprint: fp, statements: statements.length })
  return { applied: true, fingerprint: fp, statements: statements.length }
}
