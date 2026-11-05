/**
 * DDL generation.
 *
 * `_raw` is the only column the engine ever writes. Every typed column is
 * `GENERATED ALWAYS AS (...) STORED`, projected out of `_raw` with
 * `JSON_VALUE`. A new field appearing upstream can never break ingest, is
 * immediately queryable as `_raw->>'$.field'`, and can be promoted to a
 * typed column later with pure DDL: `ALTER TABLE ... ADD COLUMN ... STORED`
 * computes it from the rows already in the table, so there is no backfill.
 *
 * Three things are MySQL's rather than Postgres's:
 *
 * - **A projection that cannot be cast is NULL, not an error.** Every
 *   `JSON_VALUE` carries `NULL ON ERROR`, so `"email_verified": "maybe"`
 *   lands as a row with a NULL column and the value still in `_raw`, where
 *   in Postgres the cast would reject the whole row. MySQL's strict mode
 *   would otherwise turn every such cast into a quarantined event.
 * - **Key columns are `VARCHAR(191)`, other text columns `TEXT`.** A
 *   primary key or index needs a bounded type; 191 characters of utf8mb4 is
 *   764 bytes, so a key of `_env` plus two columns stays under InnoDB's
 *   3072-byte limit. Indexed `TEXT` columns get a 255-character prefix.
 *   A key longer than 191 characters projects as NULL, which the primary
 *   key refuses, so the row is quarantined rather than silently rekeyed.
 * - **Timestamps are parsed by an inline expression.** MySQL's own
 *   conversions do not understand the `T` and `Z` of ISO-8601, so
 *   `iso8601Expression` takes the string apart with `REGEXP_SUBSTR`,
 *   range-checks every field (`STR_TO_DATE` raises on `2026-02-30` under
 *   strict mode, where it must return NULL), applies the UTC offset, and
 *   builds a `DATETIME(6)`. A stored function would read better, but MySQL
 *   does not allow one in a generated column.
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

/** Length of `VARCHAR` key columns and of the bookkeeping identifiers. */
export const KEY_LENGTH = 191
/** Index prefix length for `TEXT` columns. */
export const TEXT_INDEX_PREFIX = 255
/** `JSON_VALUE ... RETURNING CHAR(n)` for `TEXT` columns: the type's own capacity. */
export const TEXT_RETURNING = 65535

const TABLE_OPTIONS = 'character set utf8mb4 collate utf8mb4_bin'

/** JSON path literal: `'$."a"."b"'`, every step quoted so a key with a dot or space is one step. */
export function jsonPath(path: string | string[]): string {
  const parts = Array.isArray(path) ? path : [path]
  return quoteLiteral('$' + parts.map((p) => `."${p.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`).join(''))
}

const jsonValue = (path: string | string[], returning: string) =>
  `json_value(${quoteIdent(RAW)}, ${jsonPath(path)} returning ${returning} null on error)`

/** Regex for the ISO-8601 shapes WorkOS emits, plus a space separator and a bare offset. */
const ISO_PATTERN =
  '^[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt ][0-9]{2}:[0-9]{2}:[0-9]{2}(\\\\.[0-9]+)?([Zz]|[+-][0-9]{2}:?[0-9]{2})?$'

/**
 * A `DATETIME(6)` in UTC from the ISO-8601 string `v`, or NULL for anything
 * that does not parse or is out of range. `v` is a SQL expression and is
 * repeated, so give it something cheap: a `JSON_VALUE`, not a subquery.
 *
 * The field positions are fixed by the pattern, which is what lets plain
 * `SUBSTRING` pull them out. The day-of-month check needs the month to be
 * valid already; `CASE` evaluates its branches in order, so it is.
 */
export function iso8601Expression(v: string): string {
  const field = (from: number) => `cast(substring(${v}, ${from}, 2) as unsigned)`
  const year = `cast(substring(${v}, 1, 4) as unsigned)`
  const month = field(6)
  const day = field(9)
  const leap = `((${year} % 4 = 0 and ${year} % 100 <> 0) or ${year} % 400 = 0)`
  const daysInMonth = `(case ${month} when 2 then (case when ${leap} then 29 else 28 end) when 4 then 30 when 6 then 30 when 9 then 30 when 11 then 30 else 31 end)`
  const offset = `regexp_substr(${v}, '[+-][0-9]{2}:?[0-9]{2}$')`
  // `+05:30` means the wall time is 5h30 ahead of UTC, so UTC is that much earlier.
  // `signed`, not `unsigned`: MySQL refuses to multiply an unsigned value by -1.
  const offsetMinutes = `(case when ${offset} is null then 0 else (case when left(${offset}, 1) = '-' then 1 else -1 end) * (cast(substring(${offset}, 2, 2) as signed) * 60 + cast(right(${offset}, 2) as signed)) end)`
  // An offset is hours and minutes too; `+00:99` names no zone.
  const offsetOutOfRange = `(${offset} is not null and (cast(substring(${offset}, 2, 2) as unsigned) > 23 or cast(right(${offset}, 2) as unsigned) > 59))`
  const fraction = `coalesce(regexp_substr(${v}, '\\\\.[0-9]{1,6}'), '.0')`
  const wall = `concat(substring(${v}, 1, 10), ' ', substring(${v}, 12, 8), ${fraction})`
  return `(case
    when ${v} is null or not (${v} regexp '${ISO_PATTERN}') then null
    when ${year} < 1 or ${month} < 1 or ${month} > 12 then null
    when ${day} < 1 or ${day} > ${daysInMonth} then null
    when ${field(12)} > 23 or ${field(15)} > 59 or ${field(18)} > 59 then null
    when ${offsetOutOfRange} then null
    else timestampadd(minute, ${offsetMinutes}, str_to_date(${wall}, '%Y-%m-%d %H:%i:%s.%f'))
  end)`
    .replace(/\s+/g, ' ')
    .trim()
}

/** The MySQL column type for a projected column. Keys are bounded so they can be indexed. */
export function columnType(col: ColumnDef, isKey: boolean): string {
  switch (col.type) {
    case 'text':
      return isKey ? `varchar(${KEY_LENGTH})` : 'text'
    case 'boolean':
      return 'boolean'
    case 'bigint':
      return 'bigint'
    case 'numeric':
      return 'decimal(65,30)'
    case 'jsonb':
      return 'json'
    case 'timestamptz':
      return 'datetime(6)'
  }
}

/** The generated-column expression for a projected field. */
export function columnExpression(col: ColumnDef, isKey: boolean): string {
  switch (col.type) {
    case 'jsonb':
      return `(json_extract(${quoteIdent(RAW)}, ${jsonPath(col.path)}))`
    case 'text':
      return `(${jsonValue(col.path, `char(${isKey ? KEY_LENGTH : TEXT_RETURNING})`)})`
    case 'boolean':
      // JSON `true` unquotes to the text `true`; anything else is not a boolean.
      return `(case ${jsonValue(col.path, 'char(5)')} when 'true' then true when 'false' then false else null end)`
    case 'bigint':
      return `(${jsonValue(col.path, 'signed')})`
    case 'numeric':
      return `(${jsonValue(col.path, 'decimal(65,30)')})`
    case 'timestamptz':
      return iso8601Expression(jsonValue(col.path, 'char(64)'))
  }
}

/**
 * One `` `name` type generated always as (...) stored `` clause. Every path
 * that creates a projected column — table creation, the additive upgrade,
 * drift detection and repair — goes through here, so they cannot disagree
 * about what a column should look like. The text is also what `_columns`
 * records, and what drift is measured against.
 */
export function generatedColumnSql(col: ColumnDef, stream: TableDef): string {
  const isKey = stream.key.includes(col.name)
  return `${quoteIdent(safeIdentifier(col.name))} ${columnType(col, isKey)} generated always as ${columnExpression(
    col,
    isKey,
  )} stored`
}

export function createSchemaSql(schema: string): string {
  return `create database if not exists ${quoteIdent(schema)} ${TABLE_OPTIONS}`
}

/**
 * Engine bookkeeping: cursors, applied-schema fingerprints, the
 * single-writer lease for the events tail, the quarantine, and the record
 * of generated columns the engine created. These run on every `migrate`
 * before the fingerprint check, so a table added here reaches existing
 * deployments without a new fingerprint.
 *
 * `_rejected` holds events the tail set aside after they failed
 * deterministically on two consecutive polls (see the engine's
 * `sync/quarantine.ts`). It keeps the whole event, not a reference into
 * `events`, because the archive write may itself have been the failure.
 * `created_at` is nullable and parsed in Node on the way in: a quarantine
 * write must never fail on the same bad data that caused it.
 *
 * `_columns` is how drift is detected. MySQL rewrites a generated
 * expression into its own normal form (`_utf8mb4'$.id'`, and so on), which
 * differs between versions, so the DDL the engine emitted is recorded as
 * written and compared with what the catalog would emit now. See `drift.ts`.
 */
export function bookkeepingSql(schema: string): [name: string, sql: string][] {
  const s = quoteIdent(schema)
  const id = `varchar(${KEY_LENGTH})`
  return [
    [
      '_sync_state',
      `create table if not exists ${s}.\`_sync_state\` (
  sync_id    ${id} not null,
  env        ${id} not null,
  stream     ${id} not null,
  state      json not null,
  updated_at datetime(6) not null default current_timestamp(6),
  primary key (sync_id, env, stream)
) ${TABLE_OPTIONS}`,
    ],
    [
      '_migrations',
      `create table if not exists ${s}.\`_migrations\` (
  fingerprint varchar(64) not null,
  applied_at  datetime(6) not null default current_timestamp(6),
  detail      json null,
  primary key (fingerprint)
) ${TABLE_OPTIONS}`,
    ],
    [
      '_sync_lease',
      `create table if not exists ${s}.\`_sync_lease\` (
  sync_id     ${id} not null,
  env         ${id} not null,
  holder      varchar(255) not null,
  acquired_at datetime(6) not null,
  expires_at  datetime(6) not null,
  primary key (sync_id, env)
) ${TABLE_OPTIONS}`,
    ],
    [
      '_rejected',
      `create table if not exists ${s}.\`_rejected\` (
  sync_id     ${id} not null,
  env         ${id} not null,
  event_id    ${id} not null,
  event_type  varchar(255) not null,
  object_id   varchar(255) null,
  created_at  datetime(6) null,
  rejected_at datetime(6) not null default current_timestamp(6),
  error       text not null,
  \`_raw\`      json not null,
  primary key (sync_id, env, event_id)
) ${TABLE_OPTIONS}`,
    ],
    [
      '_columns',
      `create table if not exists ${s}.\`_columns\` (
  table_name  varchar(64) not null,
  column_name varchar(64) not null,
  ddl         text not null,
  recorded_at datetime(6) not null default current_timestamp(6),
  primary key (table_name, column_name)
) ${TABLE_OPTIONS}`,
    ],
  ]
}

export interface IndexSpec {
  name: string
  column: string
  /** Prefix length, for `TEXT` columns. */
  prefix?: number
}

/** The indexes a stream's table carries, by name, so `migrate` can tell which are missing. */
export function indexSpecs(stream: TableDef): IndexSpec[] {
  const tableName = safeIdentifier(stream.name)
  const out: IndexSpec[] = []
  for (const c of stream.columns) {
    if (!c.index) continue
    const isKey = stream.key.includes(c.name)
    out.push({
      name: safeIdentifier(`${tableName}_${c.name}_idx`),
      column: safeIdentifier(c.name),
      ...(c.type === 'text' && !isKey ? { prefix: TEXT_INDEX_PREFIX } : {}),
    })
  }
  // Freshness and reconciliation queries scan these.
  out.push({ name: safeIdentifier(`${tableName}_synced_at_idx`), column: '_synced_at' })
  if (stream.mode !== 'append') out.push({ name: safeIdentifier(`${tableName}_deleted_at_idx`), column: '_deleted_at' })
  return out
}

const indexClause = (idx: IndexSpec) =>
  `index ${quoteIdent(idx.name)} (${quoteIdent(idx.column)}${idx.prefix ? `(${idx.prefix})` : ''})`

export function createTableSql(stream: TableDef, schema: string): string {
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  const cols: string[] = [
    `${quoteIdent(ENV)}       varchar(${KEY_LENGTH}) not null`,
    `${quoteIdent(RAW)}       json not null`,
    // Source-derived. The staleness gate compares this.
    `\`_updated_at\` datetime(6) not null`,
    // Destination write time. Lag is `_synced_at - _updated_at`.
    `\`_synced_at\`  datetime(6) not null default current_timestamp(6)`,
  ]
  // Append-only streams are immutable logs: no tombstones.
  if (stream.mode !== 'append') cols.push(`\`_deleted_at\` datetime(6) null`)

  for (const c of stream.columns) cols.push(generatedColumnSql(c, stream))
  const pk = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  cols.push(`primary key (${pk})`)
  for (const idx of indexSpecs(stream)) cols.push(indexClause(idx))

  return `create table if not exists ${s}.${table} (\n  ${cols.join(',\n  ')}\n) ${TABLE_OPTIONS}`
}

/**
 * Additive DDL for a table that already exists. MySQL has no `ADD COLUMN IF
 * NOT EXISTS` or `CREATE INDEX IF NOT EXISTS`, so `migrate` asks
 * `information_schema` what is there and emits only what is missing;
 * these build the statements it then runs.
 */
export function addColumnSql(stream: TableDef, col: ColumnDef, schema: string): string {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  return `alter table ${table} add column ${generatedColumnSql(col, stream)}`
}

export function addIndexSql(stream: TableDef, idx: IndexSpec, schema: string): string {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  return `alter table ${table} add ${indexClause(idx)}`
}

/**
 * Full DDL for a set of streams on a fresh database, in dependency-free
 * order: what `init --print-sql` prints. Every table is complete, columns
 * and indexes included, so applied by hand to an empty database it leaves
 * the same schema `init` would. A column added to an existing table is the
 * one thing it cannot express; `init` does that.
 */
export function buildSchemaSql(streams: TableDef[], schema: string): string[] {
  const out: string[] = [createSchemaSql(schema), ...bookkeepingSql(schema).map(([, sql]) => sql)]
  for (const s of streams) {
    out.push(createTableSql(s, schema))
    out.push(...liveViewSql(s, schema))
  }
  return out
}
