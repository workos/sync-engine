/**
 * Column drift: a projected column whose definition no longer matches the
 * catalog.
 *
 * Additive DDL cannot change a column that already exists, so once a
 * projection is wrong — an `extraColumns` entry whose path or type was
 * edited, a catalog change to a column that is already there — it stays
 * wrong until something rebuilds it. This finds those columns and, when
 * asked, redefines them with `ALTER TABLE ... MODIFY COLUMN`, which
 * ClickHouse applies to the rows already there.
 *
 * The expression is compared against `_columns`, the engine's own record of
 * the DDL it emitted for every projected column, not against what
 * `system.columns` shows: the server stores a re-formatted expression, so
 * text written by the catalog and text read back never compare equal. The
 * live type is compared with the catalog's directly; `system.columns`
 * spells types canonically, and so does `columnType`.
 *
 * Never touched, only reported: key columns (they sort the table), columns
 * that are not `DEFAULT` expressions (they are yours), and orphans —
 * recorded columns the catalog no longer names, which still compute from
 * `_raw` with their old expression and are harmless to leave.
 */
import type { Logger, TableDef } from '@workos/sync-engine'
import { KEEPER_SETTINGS, type ChClient } from '../db/client.js'
import { isKeeperConflict } from '../db/errors.js'
import { columnExpression, columnType, generatedColumnSql, quoteIdent, safeIdentifier } from './ddl.js'

export type DriftKind = 'expression' | 'type' | 'orphan' | 'not_generated'

export interface ColumnDrift {
  stream: string
  column: string
  kind: DriftKind
  /** The clause the catalog would emit now; null for an orphan. */
  expected: string | null
  /** What is recorded or, failing that, what `system.columns` shows. */
  actual: string | null
  /** Part of the sort key, so never rebuilt. */
  key: boolean
}

export interface RepairResult {
  repaired: ColumnDrift[]
  failed: { drift: ColumnDrift; error: string }[]
}

interface LiveColumn {
  type: string
  default_kind: string
  default_expression: string
}

async function liveColumns(db: ChClient, schema: string, table: string): Promise<Map<string, LiveColumn>> {
  const rows = await db.query<{ name: string; type: string; default_kind: string; default_expression: string }>(
    `SELECT name, type, default_kind, default_expression FROM system.columns
     WHERE database = {schema:String} AND table = {table:String}`,
    { schema, table },
  )
  return new Map(rows.map((r) => [r.name, r]))
}

async function recordedColumns(db: ChClient, schema: string, table: string): Promise<Map<string, string>> {
  const rows = await db.query<{ column_name: string; ddl: string }>(
    `SELECT column_name, ddl FROM ${quoteIdent(schema)}.\`_columns\` WHERE table_name = {table:String}`,
    { table },
  )
  return new Map(rows.map((r) => [r.column_name, r.ddl]))
}

/**
 * Record (or re-record) the clause a column was created with. Strict mode
 * is off, so this overwrites; two processes recording the same column at
 * once can still race on the first write, and the loser simply writes again.
 */
export async function recordColumn(db: ChClient, schema: string, table: string, column: string, ddl: string) {
  for (let attempt = 0; ; attempt++) {
    try {
      await db.command(
        `INSERT INTO ${quoteIdent(schema)}.\`_columns\` (k, table_name, column_name, ddl)
         VALUES ({k:String}, {table:String}, {column:String}, {ddl:String})`,
        { k: `${table}\u0001${column}`, table, column, ddl },
        { ...KEEPER_SETTINGS, keeper_map_strict_mode: 0 },
      )
      return
    } catch (err) {
      if (attempt >= 3 || !isKeeperConflict(err)) throw err
    }
  }
}

async function forgetColumn(db: ChClient, schema: string, table: string, column: string) {
  await db.command(
    `ALTER TABLE ${quoteIdent(schema)}.\`_columns\` DELETE WHERE k = {k:String}`,
    { k: `${table}\u0001${column}` },
    { ...KEEPER_SETTINGS, keeper_map_strict_mode: 0 },
  )
}

export async function detectColumnDrift(db: ChClient, streams: TableDef[], schema: string): Promise<ColumnDrift[]> {
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
      if (actual.default_kind !== 'DEFAULT') {
        out.push({ stream: stream.name, column: name, kind: 'not_generated', expected, actual: actual.type, key })
        continue
      }
      // The live type is checked on its own, before the record is consulted:
      // a column retyped by hand leaves `_columns` saying what the engine
      // created, and that must not hide what is actually there.
      if (actual.type !== columnType(col, key)) {
        out.push({ stream: stream.name, column: name, kind: 'type', expected, actual: actual.type, key })
        continue
      }
      const record = recorded.get(name)
      if (record === undefined) {
        await recordColumn(db, schema, table, name, expected)
        continue
      }
      if (record !== expected) {
        out.push({ stream: stream.name, column: name, kind: 'expression', expected, actual: record, key })
      }
    }
    for (const [name, ddl] of recorded) {
      if (catalog.has(name)) continue
      if (!live.has(name)) {
        // Dropped by hand: nothing to report, and the record would only mislead.
        await forgetColumn(db, schema, table, name)
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
    return 'the column is not a DEFAULT expression, so it is yours: rename it or project the field under another name'
  if (d.key) return 'key columns are never rebuilt: they sort the table'
  return null
}

/**
 * Redefine each repairable drifted column with the catalog's type and
 * expression, then recompute it for the rows already there: `MODIFY COLUMN`
 * changes what future inserts compute and converts the stored values to the
 * new type, but only a mutation re-evaluates the expression over `_raw`.
 * Both run under `mutations_sync`, so the call returns once they are done.
 */
export async function repairColumnDrift(
  db: ChClient,
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
    try {
      const ref = `${quoteIdent(schema)}.${quoteIdent(table)}`
      const expr = columnExpression(col, false)
      await db.command(
        `ALTER TABLE ${ref} MODIFY COLUMN ${quoteIdent(d.column)} ${columnType(col, false)} DEFAULT ${expr}`,
      )
      await db.command(`ALTER TABLE ${ref} UPDATE ${quoteIdent(d.column)} = ${expr} WHERE 1`)
      await recordColumn(db, schema, table, d.column, generatedColumnSql(col, stream))
      repaired.push(d)
      logger?.info('redefined drifted column from _raw', { stream: d.stream, column: d.column, kind: d.kind })
    } catch (err) {
      failed.push({ drift: d, error: err instanceof Error ? err.message : String(err) })
    }
  }
  return { repaired, failed }
}
