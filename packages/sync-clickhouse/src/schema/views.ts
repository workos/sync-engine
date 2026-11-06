/**
 * Live views.
 *
 * Every query against a mirror table has to remember `FINAL` and `WHERE
 * _deleted_at IS NULL`, and forgetting either is the most common query
 * mistake: `FINAL` left out counts every version a row ever had, the filter
 * left out counts users who left. Ours are generated from the catalog, in
 * the same pass as the tables, so they can never drift from them.
 *
 * ClickHouse fixes a view's column list when it is created, so a column
 * promoted later only reaches the view once it is re-created; `migrate`
 * does that whenever the catalog changes. The engine recognises its own
 * views by the query `system.tables` stores for them: `select * from
 * <table> final where _deleted_at is null` is ours and is replaced freely,
 * anything else under that name belongs to somebody and is left alone
 * unless `repairViews` says to replace it.
 */
import type { Logger, TableDef } from '@workos/sync-engine'
import type { ChClient } from '../db/client.js'
import { quoteIdent, safeIdentifier } from './identifiers.js'

export const LIVE_VIEW_SUFFIX = '_live'

/** `<stream>_live`, shortened the same way table names are. */
export function liveViewName(stream: TableDef): string {
  return safeIdentifier(`${stream.name}${LIVE_VIEW_SUFFIX}`)
}

/**
 * DDL for one stream's live view. Empty for append-only streams: they have
 * no versions to collapse and no `_deleted_at` to filter, so a view would
 * only suggest there was.
 */
export function liveViewSql(stream: TableDef, schema: string): string[] {
  if (stream.mode === 'append') return []
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  const view = quoteIdent(liveViewName(stream))
  return [`CREATE OR REPLACE VIEW ${s}.${view} AS SELECT * FROM ${s}.${table} FINAL WHERE \`_deleted_at\` IS NULL`]
}

export interface ApplyViewOptions {
  /**
   * Replace a view under a generated name that is not the generated shape.
   * Off, such a view fails `init` and is reported, because replacing it
   * would silently change what its users see.
   */
  repairViews?: boolean
}

/** Whether a stored view query is the generated shape over `table`. */
export function isGeneratedDefinition(query: string, schema: string, table: string): boolean {
  const normalized = query.replace(/`/g, '').replace(/\s+/g, ' ').trim().toLowerCase()
  return normalized.endsWith(`from ${schema}.${table} final where _deleted_at is null`.toLowerCase())
}

/**
 * Create or refresh a stream's live view, refusing to overwrite one that is
 * not the engine's. Returns whether a statement ran.
 */
export async function applyLiveView(
  db: ChClient,
  stream: TableDef,
  schema: string,
  logger: Logger | undefined,
  options: ApplyViewOptions = {},
): Promise<boolean> {
  const [sql] = liveViewSql(stream, schema)
  if (!sql) return false
  const name = liveViewName(stream)
  const existing = await db.query<{ engine: string; as_select: string }>(
    `SELECT engine, as_select FROM system.tables WHERE database = {schema:String} AND name = {name:String}`,
    { schema, name },
  )
  const found = existing[0]
  if (found && found.engine !== 'View') {
    throw new Error(
      `${schema}.${name} is a table, where the engine generates a view over ${stream.name}: rename or drop it, or exclude the stream`,
    )
  }
  if (found && !isGeneratedDefinition(found.as_select, schema, safeIdentifier(stream.name))) {
    if (!options.repairViews) {
      throw new Error(
        `${schema}.${name} is a view the engine did not generate; run init --repair to replace it with the generated definition, or rename it`,
      )
    }
    logger?.warn('replacing a view that is not the generated shape', { view: name, previous: found.as_select })
  }
  await db.command(sql)
  return true
}
