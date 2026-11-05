/**
 * DDL generation.
 *
 * `_raw` is the only column the engine ever writes. Every typed column is a
 * `DEFAULT` expression over it, which ClickHouse computes when the engine's
 * insert leaves the column out, and which `SELECT *` includes (a
 * `MATERIALIZED` column would not be). A new field appearing upstream can
 * never break ingest, is immediately queryable with `JSONExtract(_raw, ...)`,
 * and can be promoted to a typed column later with pure DDL: a column added
 * with `ALTER TABLE ... ADD COLUMN` is computed for the rows already there.
 *
 * Three things are ClickHouse's rather than Postgres's:
 *
 * - **Versions, not updates.** A mirror table is a `ReplacingMergeTree`
 *   versioned by `_updated_at`: every write inserts the row's new state, and
 *   the newest source timestamp wins at merge time and under `FINAL`. That
 *   is the staleness gate, enforced by the table itself whatever order the
 *   writes land in. The `_live` views read with `FINAL`. Append-only
 *   streams are plain `MergeTree`, the shape analytics wants, with
 *   re-delivery filtered by the write path.
 * - **A projection that cannot be cast is NULL, not an error.** `JSONExtract`
 *   with a `Nullable` return type gives NULL for a missing key, a JSON null
 *   and a value of the wrong shape alike, and the value stays in `_raw`.
 * - **Indexes are data-skipping indexes.** A column marked `index` gets a
 *   bloom filter (text) or a min-max index (timestamps, integers); the sort
 *   key is `(_env, <key columns>)`, which is also what the write path looks
 *   rows up by.
 *
 * There are no foreign keys between mirror tables. Events arrive with
 * children before parents routinely (a `group.user_added` before the
 * `user.created`), and an FK would turn that ordering artifact into a hard
 * write failure.
 */
import type { ColumnDef, TableDef } from '@workos/sync-engine'
import { quoteIdent, quoteLiteral, safeIdentifier } from './identifiers.js'
import { liveViewSql } from './views.js'

// Re-exported so callers keep importing the identifier helpers from here.
export { quoteIdent, quoteLiteral, safeIdentifier } from './identifiers.js'

export const RAW = '_raw'
export const ENV = '_env'

/** Keeper path under `keeper_map_path_prefix` for a schema's bookkeeping. */
export const keeperPath = (schema: string, table: string) => quoteLiteral(`/workos_sync/${schema}/${table}`)

const TIMESTAMP = `DateTime64(3, 'UTC')`

/** The JSON path as `JSONExtract` takes it: one quoted step per argument. */
export function jsonPathArgs(path: string | string[]): string {
  const parts = Array.isArray(path) ? path : [path]
  return parts.map((p) => quoteLiteral(p)).join(', ')
}

/** The ClickHouse column type for a projected column. Keys are not nullable: they sort the table. */
export function columnType(col: ColumnDef, isKey: boolean): string {
  switch (col.type) {
    case 'text':
      return isKey ? 'String' : 'Nullable(String)'
    case 'boolean':
      return 'Nullable(Bool)'
    case 'bigint':
      return 'Nullable(Int64)'
    case 'numeric':
      return 'Nullable(Decimal(38, 10))'
    case 'jsonb':
      return 'Nullable(String)'
    case 'timestamptz':
      return `Nullable(${TIMESTAMP})`
  }
}

/** The default expression for a projected field. */
export function columnExpression(col: ColumnDef, isKey: boolean): string {
  const raw = quoteIdent(RAW)
  const path = jsonPathArgs(col.path)
  switch (col.type) {
    case 'text':
      // A key reads as '' when missing; the engine never writes a keyless row.
      return isKey ? `JSONExtractString(${raw}, ${path})` : `JSONExtract(${raw}, ${path}, 'Nullable(String)')`
    case 'boolean':
    case 'bigint':
    case 'numeric':
      return `JSONExtract(${raw}, ${path}, '${columnType(col, isKey)}')`
    case 'jsonb':
      // The subtree as JSON text; `JSONExtractRaw` alone reads '' for a missing key.
      return `if(JSONHas(${raw}, ${path}), JSONExtractRaw(${raw}, ${path}), NULL)`
    case 'timestamptz':
      // Reads every ISO-8601 shape WorkOS emits, offsets included; NULL for anything else.
      return `parseDateTime64BestEffortOrNull(JSONExtractString(${raw}, ${path}), 3, 'UTC')`
  }
}

/**
 * One `` `name` type DEFAULT expr `` clause. Every path that creates a
 * projected column — table creation, the additive upgrade, drift detection
 * and repair — goes through here, so they cannot disagree about what a
 * column should look like. The text is also what `_columns` records, and
 * what drift is measured against.
 */
export function generatedColumnSql(col: ColumnDef, stream: TableDef): string {
  const isKey = stream.key.includes(col.name)
  return `${quoteIdent(safeIdentifier(col.name))} ${columnType(col, isKey)} DEFAULT ${columnExpression(col, isKey)}`
}

export function createSchemaSql(schema: string): string {
  return `CREATE DATABASE IF NOT EXISTS ${quoteIdent(schema)}`
}

/**
 * Engine bookkeeping. Cursors and the single-writer lease share a KeeperMap
 * row per environment: KeeperMap is a key-value table held in ClickHouse
 * Keeper with linearizable writes, and under `keeper_map_strict_mode` an
 * `ALTER ... UPDATE` that names the row's current values is a compare-and-
 * swap, which is what the lease and the fenced checkpoint need. See
 * `db/lease.ts`. The applied-schema fingerprints and the record of generated
 * columns live there too: an `INSERT` of an existing key is refused, which
 * makes "record once" a single statement.
 *
 * `_rejected` holds events the tail set aside after they failed
 * deterministically on two consecutive polls (see the engine's
 * `sync/quarantine.ts`). It keeps the whole event, not a reference into
 * `events`, because the archive write may itself have been the failure. A
 * rejection written again replaces the earlier one: the table is a
 * `ReplacingMergeTree` versioned by `rejected_at`. `created_at` is
 * nullable and parsed in Node on the way in, so a quarantine write can
 * never fail on the same bad data that caused it.
 */
export function bookkeepingSql(schema: string): [name: string, sql: string][] {
  const s = quoteIdent(schema)
  return [
    [
      '_sync',
      `CREATE TABLE IF NOT EXISTS ${s}.\`_sync\` (
  k           String,
  holder      String,
  acquired_at ${TIMESTAMP},
  expires_at  ${TIMESTAMP},
  states      String
) ENGINE = KeeperMap(${keeperPath(schema, '_sync')}) PRIMARY KEY k`,
    ],
    [
      '_migrations',
      `CREATE TABLE IF NOT EXISTS ${s}.\`_migrations\` (
  fingerprint String,
  applied_at  ${TIMESTAMP},
  detail      String
) ENGINE = KeeperMap(${keeperPath(schema, '_migrations')}) PRIMARY KEY fingerprint`,
    ],
    [
      '_columns',
      `CREATE TABLE IF NOT EXISTS ${s}.\`_columns\` (
  k           String,
  table_name  String,
  column_name String,
  ddl         String
) ENGINE = KeeperMap(${keeperPath(schema, '_columns')}) PRIMARY KEY k`,
    ],
    [
      '_rejected',
      `CREATE TABLE IF NOT EXISTS ${s}.\`_rejected\` (
  sync_id     String,
  env         String,
  event_id    String,
  event_type  String,
  object_id   Nullable(String),
  created_at  Nullable(${TIMESTAMP}),
  rejected_at ${TIMESTAMP} DEFAULT now64(3),
  error       String,
  \`_raw\`      String
) ENGINE = ReplacingMergeTree(rejected_at) ORDER BY (sync_id, env, event_id)`,
    ],
  ]
}

export interface IndexSpec {
  name: string
  column: string
  type: 'bloom_filter' | 'minmax'
  granularity: number
}

/** The data-skipping indexes a stream's table carries, by name, so `migrate` can tell which are missing. */
export function indexSpecs(stream: TableDef): IndexSpec[] {
  const tableName = safeIdentifier(stream.name)
  const out: IndexSpec[] = []
  for (const c of stream.columns) {
    if (!c.index) continue
    const minmax = c.type === 'timestamptz' || c.type === 'bigint' || c.type === 'numeric'
    out.push({
      name: safeIdentifier(`${tableName}_${c.name}_idx`),
      column: safeIdentifier(c.name),
      type: minmax ? 'minmax' : 'bloom_filter',
      granularity: minmax ? 1 : 4,
    })
  }
  // Freshness and reconciliation queries scan these.
  out.push({ name: safeIdentifier(`${tableName}_synced_at_idx`), column: '_synced_at', type: 'minmax', granularity: 1 })
  if (stream.mode !== 'append') {
    out.push({
      name: safeIdentifier(`${tableName}_deleted_at_idx`),
      column: '_deleted_at',
      type: 'minmax',
      granularity: 1,
    })
  }
  return out
}

const indexClause = (idx: IndexSpec) =>
  `INDEX ${quoteIdent(idx.name)} ${quoteIdent(idx.column)} TYPE ${idx.type} GRANULARITY ${idx.granularity}`

export function createTableSql(stream: TableDef, schema: string): string {
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  const cols: string[] = [
    `${quoteIdent(ENV)}       String`,
    `${quoteIdent(RAW)}       String`,
    // Source-derived. The staleness gate compares this, and so does the merge.
    `\`_updated_at\` ${TIMESTAMP}`,
    // Destination write time. Lag is `_synced_at - _updated_at`.
    `\`_synced_at\`  DateTime64(6, 'UTC') DEFAULT now64(6)`,
  ]
  // Append-only streams are immutable logs: no tombstones.
  if (stream.mode !== 'append') cols.push(`\`_deleted_at\` Nullable(${TIMESTAMP})`)
  for (const c of stream.columns) cols.push(generatedColumnSql(c, stream))
  for (const idx of indexSpecs(stream)) cols.push(indexClause(idx))
  const order = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  const engine = stream.mode === 'append' ? 'MergeTree' : 'ReplacingMergeTree(`_updated_at`)'
  return `CREATE TABLE IF NOT EXISTS ${s}.${table} (\n  ${cols.join(',\n  ')}\n) ENGINE = ${engine} ORDER BY (${order})`
}

/** Additive DDL for a table that already exists; both are idempotent. */
export function addColumnSql(stream: TableDef, col: ColumnDef, schema: string): string {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  return `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${generatedColumnSql(col, stream)}`
}

/** Write a column added later into the parts that predate it, so reads stop recomputing it. */
export function materializeColumnSql(stream: TableDef, col: ColumnDef, schema: string): string {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  return `ALTER TABLE ${table} MATERIALIZE COLUMN ${quoteIdent(safeIdentifier(col.name))}`
}

export function addIndexSql(stream: TableDef, idx: IndexSpec, schema: string): string {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  return `ALTER TABLE ${table} ADD ${indexClause(idx).replace('INDEX ', 'INDEX IF NOT EXISTS ')}`
}

/**
 * Full DDL for a set of streams, in dependency-free order: what
 * `init --print-sql` prints. Every statement is idempotent, so the script
 * can be applied to a database in any state.
 */
export function buildSchemaSql(streams: TableDef[], schema: string): string[] {
  const out: string[] = [createSchemaSql(schema), ...bookkeepingSql(schema).map(([, sql]) => sql)]
  for (const s of streams) {
    out.push(createTableSql(s, schema))
    for (const c of s.columns) out.push(addColumnSql(s, c, schema))
    for (const idx of indexSpecs(s)) out.push(addIndexSql(s, idx, schema))
    out.push(...liveViewSql(s, schema))
  }
  return out
}
