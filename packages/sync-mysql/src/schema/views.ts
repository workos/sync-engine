/**
 * Live views.
 *
 * Every query against a mirror table has to remember `where _deleted_at is
 * null`, and forgetting it is the most common query mistake: a seat count
 * that includes users who left, a join that resurrects a revoked membership.
 * Ours are generated from the catalog, in the same pass as the tables, so
 * they can never drift from them.
 *
 * MySQL views carry no comment, so the engine recognises its own by shape:
 * a view whose definition is `select ... from <table> where <table>._deleted_at
 * is null` is ours and is re-created freely, which is also how it picks up a
 * column added since (`select *` is expanded when the view is created, not
 * when it is queried). Anything else under that name belongs to somebody
 * and is left alone unless `repairViews` says to replace it.
 */
import type { Logger, TableDef } from '@workos/sync-engine'
import type { SqlClient } from '../db/client.js'
import { quoteIdent, safeIdentifier } from './identifiers.js'

export const LIVE_VIEW_SUFFIX = '_live'

/** `<stream>_live`, shortened the same way table names are. */
export function liveViewName(stream: TableDef): string {
  return safeIdentifier(`${stream.name}${LIVE_VIEW_SUFFIX}`)
}

/**
 * DDL for one stream's live view. Empty for append-only streams: they have
 * no `_deleted_at` column, so there is nothing to filter and a view would
 * only suggest there was.
 */
export function liveViewSql(stream: TableDef, schema: string): string[] {
  if (stream.mode === 'append') return []
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  const view = quoteIdent(liveViewName(stream))
  return [`create or replace view ${s}.${view} as select * from ${s}.${table} where \`_deleted_at\` is null`]
}

export interface ApplyViewOptions {
  /**
   * Replace a view under a generated name that is not the generated shape.
   * Off, such a view fails `init` and is reported, because replacing it
   * would silently change what its users see.
   */
  repairViews?: boolean
}

/** What `information_schema` says exists under the view's name. */
async function describe(
  db: SqlClient,
  schema: string,
  name: string,
): Promise<{ type: 'none' } | { type: 'table' } | { type: 'view'; definition: string }> {
  const t = await db.query<{ table_type: string }>(
    `select table_type as table_type from information_schema.tables where table_schema = ? and table_name = ?`,
    [schema, name],
  )
  const type = t.rows[0]?.table_type
  if (!type) return { type: 'none' }
  if (type !== 'VIEW') return { type: 'table' }
  const v = await db.query<{ view_definition: string }>(
    `select view_definition as view_definition from information_schema.views where table_schema = ? and table_name = ?`,
    [schema, name],
  )
  return { type: 'view', definition: v.rows[0]?.view_definition ?? '' }
}

/** Whether a stored view definition is the generated shape over `table`. */
export function isGeneratedDefinition(definition: string, schema: string, table: string): boolean {
  const ref = `${quoteIdent(schema)}.${quoteIdent(table)}`
  const tail = `from ${ref} where (${ref}.\`_deleted_at\` is null)`
  return definition.trimEnd().endsWith(tail)
}

/** The columns a view or table exposes, in order. */
async function columnsOf(db: SqlClient, schema: string, name: string): Promise<string[]> {
  const r = await db.query<{ column_name: string }>(
    `select column_name as column_name from information_schema.columns
     where table_schema = ? and table_name = ? order by ordinal_position`,
    [schema, name],
  )
  return r.rows.map((row) => row.column_name)
}

/**
 * Create or refresh a stream's live view, refusing to overwrite one that is
 * not the engine's. A generated view that already exposes every column of
 * its table is left alone: `init` runs no DDL it does not need, which is
 * what lets it finish on a host that gates DDL once the printed schema has
 * been applied by hand. Returns whether a statement ran.
 */
export async function applyLiveView(
  db: SqlClient,
  stream: TableDef,
  schema: string,
  logger: Logger | undefined,
  options: ApplyViewOptions = {},
): Promise<boolean> {
  const [sql] = liveViewSql(stream, schema)
  if (!sql) return false
  const name = liveViewName(stream)
  const table = safeIdentifier(stream.name)
  const existing = await describe(db, schema, name)
  if (existing.type === 'table') {
    throw new Error(
      `${schema}.${name} is a table, where the engine generates a view over ${stream.name}: rename or drop it, or exclude the stream`,
    )
  }
  if (existing.type === 'view') {
    if (!isGeneratedDefinition(existing.definition, schema, table)) {
      if (!options.repairViews) {
        throw new Error(
          `${schema}.${name} is a view the engine did not generate; run init --repair to replace it with the generated definition, or rename it`,
        )
      }
      logger?.warn('replacing a view that is not the generated shape', { view: name, previous: existing.definition })
    } else {
      const [viewColumns, tableColumns] = await Promise.all([columnsOf(db, schema, name), columnsOf(db, schema, table)])
      if (viewColumns.join('\0') === tableColumns.join('\0')) return false
    }
  }
  await db.exec(sql)
  return true
}
