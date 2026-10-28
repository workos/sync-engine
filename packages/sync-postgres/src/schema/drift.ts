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
import type { ColumnDef, Logger, TableDef } from '@workos/sync-engine'
import type { SqlClient } from '../db/client.js'
import { withoutLiveView } from './views.js'
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
  /** What the catalog calls for: the one of `catalog`'s two values that `kind` names. `null` for an orphan. */
  expected: string | null
  /** What the table has. `null` when the column exists but is not a generated column. */
  actual: string | null
  /**
   * The catalog's column in full, as Postgres renders it. `kind` reports the
   * one of these that differs, but a repair has to check both: a column that
   * another process just rebuilt to the right type from a different catalog
   * still has the wrong expression. `null` for an orphan.
   */
  catalog: { type: string; expr: string } | null
  /** Part of the primary key. Dropping it drops the key, so repair is refused and left to an operator. */
  key: boolean
  /**
   * Indexes, constraints and other objects of yours that `DROP COLUMN` takes
   * with it silently, as Postgres describes them. The catalog's own index is
   * not listed because repair puts it back. Anything here means repair is
   * refused: the engine cannot know how to recreate what it did not create.
   */
  dependents: string[]
  /**
   * The DDL that brings the column in line. Printed for every item so the
   * report is actionable on its own; run only by `repairColumnDrift`, and only
   * for the items it accepts.
   */
  ddl: string[]
}

interface LiveColumn {
  name: string
  /** `pg_attribute.attnum`, which is how `pg_depend` refers to a column. */
  num: number
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
       a.attnum as num,
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

/**
 * What `DROP COLUMN` would silently take with the column: every auto
 * dependency (`deptype = 'a'`) on it. Normal dependencies — a view, a foreign
 * key from another table — make the drop fail instead, so they need no
 * lookup. The column's own generation expression is an attrdef dependency
 * (auto or internal depending on the server version) and is excluded, as is
 * the index the catalog declares for the column, which repair recreates.
 */
const DEPENDENTS_SQL = `
select distinct pg_describe_object(d.classid, d.objid, d.objsubid) as what
  from pg_depend d
 where d.refclassid = 'pg_class'::regclass
   and d.refobjid = $1::regclass
   and d.refobjsubid = $2
   and d.deptype = 'a'
   and d.classid <> 'pg_attrdef'::regclass
   and not (d.classid = 'pg_class'::regclass and d.objid = coalesce(to_regclass($3)::oid, 0))
 order by 1`

async function dependents(db: SqlClient, table: string, column: LiveColumn, ownIndex: string): Promise<string[]> {
  const r = await db.query<{ what: string }>(DEPENDENTS_SQL, [table, column.num, ownIndex])
  return r.rows.map((row) => row.what)
}

const tableRef = (stream: string, schema: string): string =>
  `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream))}`

/** The name `createIndexesSql` gives a column's index; qualified, for `to_regclass`. */
const ownIndexRef = (stream: string, column: string, schema: string): string =>
  `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(`${safeIdentifier(stream)}_${column}_idx`))}`

/**
 * The catalog's columns as Postgres would store them: a temp table built from
 * the same generated-column DDL, read back through the same catalog query.
 * The table lives for one transaction (`ON COMMIT DROP`), so nothing persists
 * and concurrent runs cannot collide. Needs `TEMP` on the database, which
 * `PUBLIC` has by default.
 */
async function expectedColumns(db: SqlClient, stream: TableDef, schema: string): Promise<Map<string, LiveColumn>> {
  return db.transaction(async (tx) => {
    const tmp = quoteIdent(safeIdentifier(`_drift_${stream.name}`))
    const cols = [`${quoteIdent(RAW)} jsonb`, ...stream.columns.map((c) => generatedColumnSql(c, schema))]
    await tx.exec(`create temp table ${tmp} (${cols.join(', ')}) on commit drop;`)
    const columns = await liveColumns(tx, `pg_temp.${tmp}`)
    // Migration savepoints do not trigger ON COMMIT DROP before the next drift check.
    await tx.exec(`drop table pg_temp.${tmp};`)
    return columns
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
function repairSql(stream: TableDef, schema: string, col: ColumnDef, key: boolean): string[] {
  const table = tableRef(stream.name, schema)
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
 *
 * Expressions compare byte for byte. Both sides came out of the same
 * deparser in the same session, so any difference is real; collapsing
 * whitespace would also collapse it inside string literals, and a JSON key
 * that differs only by a space is a different key.
 */
export async function detectColumnDrift(db: SqlClient, streams: TableDef[], schema: string): Promise<ColumnDrift[]> {
  const out: ColumnDrift[] = []
  for (const stream of streams) {
    const table = tableRef(stream.name, schema)
    const actual = await liveColumns(db, table)
    const expected = await expectedColumns(db, stream, schema)
    const declared = new Set<string>()

    for (const col of stream.columns) {
      const name = safeIdentifier(col.name)
      declared.add(name)
      const have = actual.get(name)
      if (!have) continue
      const want = expected.get(name)!
      const key = stream.key.includes(col.name)
      const item = async (
        kind: 'expression' | 'type',
        exp: string | null,
        act: string | null,
      ): Promise<ColumnDrift> => ({
        stream: stream.name,
        column: name,
        kind,
        expected: exp,
        actual: act,
        // Every column on the temp table is generated, so `want.expr` is never null.
        catalog: { type: want.type, expr: want.expr! },
        key,
        dependents: await dependents(db, table, have, ownIndexRef(stream.name, col.name, schema)),
        ddl: repairSql(stream, schema, col, key),
      })
      // A type change usually changes the expression too; report the type,
      // which is the more useful of the two to read.
      if (!have.generated) out.push(await item('expression', want.expr, null))
      else if (have.type !== want.type) out.push(await item('type', want.type, have.type))
      else if (have.expr !== want.expr) out.push(await item('expression', want.expr, have.expr))
    }

    for (const have of actual.values()) {
      if (!have.generated || declared.has(have.name)) continue
      out.push({
        stream: stream.name,
        column: have.name,
        kind: 'orphan',
        expected: null,
        actual: have.expr,
        catalog: null,
        key: stream.key.includes(have.name),
        dependents: await dependents(db, table, have, ownIndexRef(stream.name, have.name, schema)),
        ddl: [`alter table ${table} drop column ${quoteIdent(have.name)};`],
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
  if (d.dependents.length > 0) {
    return `dropping it would also drop ${d.dependents.join(', ')}; drop those first, repair, then recreate them`
  }
  return null
}

export interface RepairResult {
  /**
   * Now match the catalog: rebuilt from `_raw` here, or found already rebuilt
   * by another process that got to the table first.
   */
  repaired: ColumnDrift[]
  /**
   * Accepted for repair, but the DDL failed — typically a view depends on the
   * column (SQLSTATE 2BP01). Nothing changed for these: each column's DDL runs
   * in its own transaction or migration savepoint, so a failed re-add never
   * leaves a column dropped.
   */
  failed: { drift: ColumnDrift; error: string }[]
}

/**
 * Rebuild the drifted columns that can be rebuilt safely: non-key
 * `expression`/`type` drift on a generated column nothing else depends on.
 * Everything else is left for an operator with the DDL in hand; see
 * `unrepairableReason`.
 *
 * Several processes can share one database and start together, each
 * detecting the same drift before any of them repairs it. Each column's
 * transaction or migration savepoint takes an advisory lock on the table and
 * reads the column again before dropping it: the second process finds it
 * already rebuilt and leaves it alone, rather than rewriting the whole table
 * a second time behind an exclusive lock. A transaction-scoped
 * advisory lock is used rather than a table lock so that nothing is blocked
 * until the drop itself.
 *
 * A dependent view makes `DROP COLUMN` fail rather than cascade. Cascading
 * would silently remove the view, and the error names what to drop first.
 */
export async function repairColumnDrift(
  db: SqlClient,
  drift: ColumnDrift[],
  /** The tables being applied, so a drifted stream's `_live` view can be set aside by its own definition. */
  streams: TableDef[],
  schema: string,
  logger?: Logger,
): Promise<RepairResult> {
  const result: RepairResult = { repaired: [], failed: [] }
  for (const d of drift) {
    if (unrepairableReason(d) !== null) continue
    const table = tableRef(d.stream, schema)
    try {
      const rebuilt = await db.transaction(async (tx) => {
        await tx.query(`select pg_advisory_xact_lock(hashtext($1))`, [`workos-sync-postgres:drift:${table}`])
        const live = (await liveColumns(tx, table)).get(d.column)
        // Both halves, not only the one `kind` reports; see `ColumnDrift.catalog`.
        if (live?.generated && d.catalog && live.type === d.catalog.type && live.expr === d.catalog.expr) {
          return false
        }
        // The stream's own `_live` view depends on every column, so it is set
        // aside for the drop and restored, grants included, afterwards. Any
        // other dependent view still makes the drop fail, by design.
        const run = async () => {
          for (const sql of d.ddl) await tx.exec(sql)
        }
        // Looked up in the tables this apply was given, not the WorkOS catalog:
        // the destination only knows the tables it is handed.
        const stream = streams.find((s) => s.name === d.stream)
        if (stream) await withoutLiveView(tx, stream, schema, run)
        else await run()
        return true
      })
      result.repaired.push(d)
      logger?.info(rebuilt ? 'column rebuilt from _raw' : 'column already rebuilt by another process', {
        stream: d.stream,
        column: d.column,
        kind: d.kind,
      })
    } catch (err) {
      result.failed.push({ drift: d, error: err instanceof Error ? err.message : String(err) })
    }
  }
  return result
}
