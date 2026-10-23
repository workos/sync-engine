/**
 * Generated-column drift: where a live table disagrees with the catalog.
 *
 * Schema setup is additive. `ADD COLUMN IF NOT EXISTS` is a no-op for a column
 * that already exists, so a release that changes an existing column's JSON
 * path or type — or an `extraColumns` entry edited after the fact — changes
 * nothing on a database that already has the column. The projection is then
 * wrong while ingest looks perfectly healthy, because the engine only ever
 * writes `_raw`. A sync engine with hand-written migrations ships a numbered
 * file for every such change; here the DDL is derived from the catalog, so the
 * equivalent is to detect where the table disagrees with the catalog and
 * rebuild the column from `_raw`, which is the source of truth for every
 * generated column. Nothing is lost by dropping one.
 *
 * "Expected" cannot be the DDL text. Postgres normalizes an expression on the
 * way in — `_raw->>'email'` is stored as `(_raw ->> 'email'::text)` — and
 * `pg_get_expr` prints that normalized form. So the catalog's columns are
 * created on a temp table in the same session and read back through the same
 * catalog query as the live table. Both sides are then rendered by the same
 * server with the same search_path, and compare as plain strings.
 */
import type { ColumnDef, Logger, StreamDef } from '@workos/sync-engine'
import type { SqlClient } from '../db/client.js'
import { ENV, RAW, createIndexesSql, generatedColumnSql, quoteIdent, safeIdentifier } from './ddl.js'

export type DriftKind = 'expression' | 'type' | 'orphan'

export interface ColumnDrift {
  stream: string
  /** Column name as it exists in Postgres. */
  column: string
  /**
   * - `expression`: the stored expression differs from the catalog's, or the
   *   column exists but is not generated at all (`actual: null`).
   * - `type`: the column's type differs from the catalog's.
   * - `orphan`: a generated column the catalog no longer declares. Reported,
   *   never dropped.
   */
  kind: DriftKind
  /** What the catalog calls for: the normalized expression, or the type. `null` for an orphan. */
  expected: string | null
  /** What the table has. `null` when the column exists but is not a generated column. */
  actual: string | null
  /** Part of the primary key. Dropping it drops the key, so repair is refused and left to an operator. */
  key: boolean
  /**
   * The DDL that brings the column in line. Printed for every item so the
   * report is actionable on its own; run only by `repairColumnDrift`, and only
   * for the items it accepts.
   */
  ddl: string[]
}

interface LiveColumn {
  name: string
  type: string
  expr: string | null
  generated: boolean
}

/**
 * Every user column of a relation with its type, its default or generation
 * expression, and whether it is `GENERATED ... STORED`. `$1` is a regclass
 * literal, so it resolves temp tables (`pg_temp."x"`) and real ones alike.
 * All columns are read, not only generated ones, so that a plain column
 * squatting on a catalog name is noticed rather than silently skipped.
 */
const COLUMNS_SQL = `
select a.attname as name,
       format_type(a.atttypid, a.atttypmod) as type,
       pg_get_expr(d.adbin, d.adrelid) as expr,
       a.attgenerated = 's' as generated
  from pg_attribute a
  left join pg_attrdef d on d.adrelid = a.attrelid and d.adnum = a.attnum
 where a.attrelid = $1::regclass and a.attnum > 0 and not a.attisdropped
 order by a.attnum`

async function liveColumns(db: SqlClient, relation: string): Promise<Map<string, LiveColumn>> {
  const r = await db.query<LiveColumn>(COLUMNS_SQL, [relation])
  return new Map(r.rows.map((c) => [c.name, c]))
}

/** Only whitespace is normalized: everything else already went through the server's deparser on both sides. */
const normalize = (expr: string | null): string | null => (expr === null ? null : expr.replace(/\s+/g, ' ').trim())

const tableRef = (stream: StreamDef, schema: string): string =>
  `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`

/**
 * The catalog's columns as Postgres would store them: a temp table built from
 * the same generated-column DDL, read back through the same catalog query.
 * The table lives for one transaction (`ON COMMIT DROP`), so nothing persists
 * and concurrent runs cannot collide. Needs `TEMP` on the database, which
 * `PUBLIC` has by default.
 */
async function expectedColumns(db: SqlClient, stream: StreamDef, schema: string): Promise<Map<string, LiveColumn>> {
  return db.transaction(async (tx) => {
    const tmp = quoteIdent(safeIdentifier(`_drift_${stream.name}`))
    const cols = [`${quoteIdent(RAW)} jsonb`, ...stream.columns.map((c) => generatedColumnSql(c, schema))]
    await tx.exec(`create temp table ${tmp} (${cols.join(', ')}) on commit drop;`)
    return liveColumns(tx, `pg_temp.${tmp}`)
  })
}

/**
 * The DDL that brings one column in line. Drop, then re-add: a generated
 * column's expression cannot be altered in place, and `STORED` recomputes the
 * new one from `_raw` for every row already present, so this is also the
 * backfill. The column's index goes with it on drop, so it is recreated. For a
 * key column the primary key goes too, which is why keys are never repaired
 * automatically: between the drop and the re-add the table accepts duplicates,
 * and the re-add fails if the new expression yields any.
 */
function repairSql(stream: StreamDef, schema: string, col: ColumnDef, key: boolean): string[] {
  const table = tableRef(stream, schema)
  const out = [
    `alter table ${table} drop column ${quoteIdent(safeIdentifier(col.name))};`,
    `alter table ${table} add column ${generatedColumnSql(col, schema)};`,
  ]
  // `createIndexesSql` emits the column's own index first, then the fixed ones.
  if (col.index) out.push(createIndexesSql({ ...stream, columns: [col] }, schema)[0]!)
  if (key) {
    const pk = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
    out.push(`alter table ${table} add primary key (${pk});`)
  }
  return out
}

/**
 * Compare every stream's table against the catalog. Columns the table lacks
 * entirely are not drift — `addColumnsSql` owns those — so on a fresh schema
 * this returns nothing, and a test holds it to that for the whole catalog.
 */
export async function detectColumnDrift(db: SqlClient, streams: StreamDef[], schema: string): Promise<ColumnDrift[]> {
  const out: ColumnDrift[] = []
  for (const stream of streams) {
    const actual = await liveColumns(db, tableRef(stream, schema))
    const expected = await expectedColumns(db, stream, schema)
    const declared = new Set<string>()

    for (const col of stream.columns) {
      const name = safeIdentifier(col.name)
      declared.add(name)
      const have = actual.get(name)
      if (!have) continue
      const want = expected.get(name)!
      const key = stream.key.includes(col.name)
      const item = (kind: 'expression' | 'type', exp: string | null, act: string | null): ColumnDrift => ({
        stream: stream.name,
        column: name,
        kind,
        expected: exp,
        actual: act,
        key,
        ddl: repairSql(stream, schema, col, key),
      })
      // A type change usually changes the expression too; report the type,
      // which is the more useful of the two to read.
      if (!have.generated) out.push(item('expression', normalize(want.expr), null))
      else if (have.type !== want.type) out.push(item('type', want.type, have.type))
      else if (normalize(have.expr) !== normalize(want.expr)) {
        out.push(item('expression', normalize(want.expr), normalize(have.expr)))
      }
    }

    for (const have of actual.values()) {
      if (!have.generated || declared.has(have.name)) continue
      out.push({
        stream: stream.name,
        column: have.name,
        kind: 'orphan',
        expected: null,
        actual: normalize(have.expr),
        key: stream.key.includes(have.name),
        ddl: [`alter table ${tableRef(stream, schema)} drop column ${quoteIdent(have.name)};`],
      })
    }
  }
  return out
}

/**
 * Why `repairColumnDrift` leaves an item alone, for the report. `null` means
 * it would repair it.
 */
export function unrepairableReason(d: ColumnDrift): string | null {
  if (d.kind === 'orphan') return 'not in the catalog; drop it by hand if it is no longer wanted'
  if (d.key) return 'key column: dropping it drops the primary key; run the DDL in a maintenance window'
  if (d.actual === null) return 'not a generated column: dropping it would discard data that is not in _raw'
  return null
}

export interface RepairResult {
  /** Rebuilt from `_raw`; the live column now matches the catalog. */
  repaired: ColumnDrift[]
  /**
   * Accepted for repair, but the DDL failed — typically a view depends on the
   * column (SQLSTATE 2BP01). Nothing changed for these: each column's DDL runs
   * in its own transaction, so a failed re-add never leaves a column dropped.
   */
  failed: { drift: ColumnDrift; error: string }[]
}

/**
 * Rebuild the drifted columns that can be rebuilt safely: non-key
 * `expression`/`type` drift on a generated column. Everything else is left for
 * an operator with the DDL in hand; see `unrepairableReason`.
 *
 * A dependent view makes `DROP COLUMN` fail rather than cascade. Cascading
 * would silently remove the view, and the error names what to drop first.
 */
export async function repairColumnDrift(db: SqlClient, drift: ColumnDrift[], logger?: Logger): Promise<RepairResult> {
  const result: RepairResult = { repaired: [], failed: [] }
  for (const d of drift) {
    if (unrepairableReason(d) !== null) continue
    try {
      await db.transaction(async (tx) => {
        for (const sql of d.ddl) await tx.exec(sql)
      })
      result.repaired.push(d)
      logger?.info('column rebuilt from _raw', { stream: d.stream, column: d.column, kind: d.kind })
    } catch (err) {
      result.failed.push({ drift: d, error: err instanceof Error ? err.message : String(err) })
    }
  }
  return result
}
