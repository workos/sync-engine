/**
 * Column drift: a generated column whose definition no longer matches the
 * catalog.
 *
 * Additive DDL cannot change a column that already exists, so once a
 * projection is wrong — an `extraColumns` entry whose path or type was
 * edited, a catalog change to a column that is already there — it stays
 * wrong until something rebuilds it. This finds those columns and, when
 * asked, rebuilds them from `_raw`.
 *
 * The comparison is against `_columns`, the engine's own record of the DDL
 * it emitted for every generated column, not against what
 * `information_schema` shows: MySQL rewrites a generated expression into its
 * own normal form, which differs between versions, so text written by the
 * catalog and text read back from the server never compare equal. A
 * generated column that has no record (a table made by hand to the right
 * shape) is recorded as current if its type is right, and reported if not.
 *
 * Never touched, only reported: key columns (a rebuild would drop the
 * primary key under live rows), columns that are not generated (they are
 * yours), columns carrying an index the engine did not create (the rebuild
 * would drop it), and orphans — recorded columns the catalog no longer
 * names, which still compute from `_raw` with their old expression and are
 * harmless to leave.
 */
import type { Logger, TableDef } from '@workos/sync-engine'
import type { SqlClient } from '../db/client.js'
import {
  addColumnSql,
  addIndexSql,
  columnType,
  generatedColumnSql,
  indexSpecs,
  quoteIdent,
  safeIdentifier,
} from './ddl.js'

export type DriftKind = 'expression' | 'type' | 'orphan' | 'not_generated'

export interface ColumnDrift {
  stream: string
  column: string
  kind: DriftKind
  /** The clause the catalog would emit now; null for an orphan. */
  expected: string | null
  /** What is recorded or, failing that, what `information_schema` shows. */
  actual: string | null
  /** Part of the primary key, so never rebuilt. */
  key: boolean
}

export interface RepairResult {
  repaired: ColumnDrift[]
  failed: { drift: ColumnDrift; error: string }[]
}

interface LiveColumn {
  column_type: string
  extra: string
  generation_expression: string | null
}

/** `COLUMN_TYPE` as `information_schema` spells the catalog's types. */
function informationSchemaType(type: string): string {
  return type === 'boolean' ? 'tinyint(1)' : type
}

async function liveColumns(db: SqlClient, schema: string, table: string): Promise<Map<string, LiveColumn>> {
  const r = await db.query<{ column_name: string; column_type: string; extra: string; generation_expression: string }>(
    `select column_name as column_name, column_type as column_type, extra as extra,
            generation_expression as generation_expression
     from information_schema.columns where table_schema = ? and table_name = ?`,
    [schema, table],
  )
  return new Map(
    r.rows.map((row) => [
      row.column_name,
      { column_type: row.column_type, extra: row.extra, generation_expression: row.generation_expression },
    ]),
  )
}

async function recordedColumns(db: SqlClient, schema: string, table: string): Promise<Map<string, string>> {
  const r = await db.query<{ column_name: string; ddl: string }>(
    `select column_name, ddl from ${quoteIdent(schema)}.\`_columns\` where table_name = ?`,
    [table],
  )
  return new Map(r.rows.map((row) => [row.column_name, row.ddl]))
}

export async function recordColumn(db: SqlClient, schema: string, table: string, column: string, ddl: string) {
  await db.query(
    `insert into ${quoteIdent(schema)}.\`_columns\` (table_name, column_name, ddl) values (?, ?, ?) as new
     on duplicate key update ddl = new.ddl, recorded_at = now(6)`,
    [table, column, ddl],
  )
}

export async function detectColumnDrift(db: SqlClient, streams: TableDef[], schema: string): Promise<ColumnDrift[]> {
  const out: ColumnDrift[] = []
  for (const stream of streams) {
    const table = safeIdentifier(stream.name)
    const live = await liveColumns(db, schema, table)
    const recorded = await recordedColumns(db, schema, table)
    const catalog = new Set<string>()
    for (const col of stream.columns) {
      const name = safeIdentifier(col.name)
      catalog.add(name)
      const actual = live.get(name)
      // Missing entirely: `migrate` adds it, or already failed loudly trying.
      if (!actual) continue
      const key = stream.key.includes(col.name)
      const expected = generatedColumnSql(col, stream)
      if (!actual.extra.toUpperCase().includes('GENERATED')) {
        out.push({
          stream: stream.name,
          column: name,
          kind: 'not_generated',
          expected,
          actual: actual.column_type,
          key,
        })
        continue
      }
      const typeMatches = actual.column_type.toLowerCase() === informationSchemaType(columnType(col, key))
      const record = recorded.get(name)
      if (record === undefined) {
        if (typeMatches) {
          await recordColumn(db, schema, table, name, expected)
        } else {
          out.push({ stream: stream.name, column: name, kind: 'type', expected, actual: actual.column_type, key })
        }
        continue
      }
      if (record !== expected) {
        out.push({
          stream: stream.name,
          column: name,
          kind: typeMatches ? 'expression' : 'type',
          expected,
          actual: record,
          key,
        })
      }
    }
    for (const [name, ddl] of recorded) {
      if (catalog.has(name)) continue
      if (!live.has(name)) {
        // Dropped by hand: nothing to report, and the record would only mislead.
        await db.query(`delete from ${quoteIdent(schema)}.\`_columns\` where table_name = ? and column_name = ?`, [
          table,
          name,
        ])
        continue
      }
      out.push({ stream: stream.name, column: name, kind: 'orphan', expected: null, actual: ddl, key: false })
    }
  }
  return out
}

/** Why a drifted column is not rebuilt, or null when `repairColumnDrift` may try. */
export function unrepairableReason(d: ColumnDrift): string | null {
  if (d.kind === 'orphan') return 'the catalog no longer names this column; drop it by hand if it is no longer wanted'
  if (d.kind === 'not_generated')
    return 'the column is not generated, so it is yours: rename it or project the field under another name'
  if (d.key) return 'key columns are never rebuilt: rebuilding one would drop the primary key under live rows'
  return null
}

/**
 * Rebuild each repairable drifted column from `_raw`: drop it, add it back
 * with the catalog's clause, and re-create the engine's own index on it if
 * it has one. MySQL DDL commits as it goes, so a failure between the two
 * leaves the column missing; the next `init` then adds it like any other
 * missing column.
 */
export async function repairColumnDrift(
  db: SqlClient,
  drift: ColumnDrift[],
  streams: TableDef[],
  schema: string,
  logger?: Logger,
): Promise<RepairResult> {
  const repaired: ColumnDrift[] = []
  const failed: RepairResult['failed'] = []
  for (const d of drift) {
    if (unrepairableReason(d)) continue
    const stream = streams.find((s) => s.name === d.stream)
    const col = stream?.columns.find((c) => safeIdentifier(c.name) === d.column)
    if (!stream || !col) continue
    const table = safeIdentifier(stream.name)
    const ours = new Set(indexSpecs(stream).map((i) => i.name))
    try {
      const idx = await db.query<{ index_name: string }>(
        `select distinct index_name as index_name from information_schema.statistics
         where table_schema = ? and table_name = ? and column_name = ? and index_name <> 'PRIMARY'`,
        [schema, table, d.column],
      )
      const foreign = idx.rows.map((r) => r.index_name).filter((n) => !ours.has(n))
      if (foreign.length > 0) {
        throw new Error(`index ${foreign.join(', ')} on it is not the engine's and would be dropped by the rebuild`)
      }
      const ref = `${quoteIdent(schema)}.${quoteIdent(table)}`
      await db.exec(`alter table ${ref} drop column ${quoteIdent(d.column)}`)
      await db.exec(addColumnSql(stream, col, schema))
      const spec = indexSpecs(stream).find((i) => i.column === d.column)
      if (spec) await db.exec(addIndexSql(stream, spec, schema))
      await recordColumn(db, schema, table, d.column, generatedColumnSql(col, stream))
      repaired.push(d)
      logger?.info('rebuilt drifted column from _raw', { stream: d.stream, column: d.column, kind: d.kind })
    } catch (err) {
      failed.push({ drift: d, error: err instanceof Error ? err.message : String(err) })
    }
  }
  return { repaired, failed }
}
