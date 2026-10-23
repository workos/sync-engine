/**
 * DDL generation.
 *
 * `_raw` is the only column the engine ever writes. Every typed column is
 * `GENERATED ALWAYS AS ... STORED`, projected out of `_raw`. That means a new
 * field appearing upstream can never break ingest, is immediately queryable as
 * `_raw->>'field'`, and can be promoted to a typed column later with pure DDL —
 * `STORED` computes it from data already in the table, so there is no backfill.
 *
 * There are no foreign keys between mirror tables. Events arrive with children
 * before parents routinely (a `group.user_added` before the `user.created`),
 * and an FK would turn that ordering artifact into a hard write failure.
 */
import type { ColumnDef, StreamDef } from '@workos/sync-engine'

export const RAW = '_raw'
export const ENV = '_env'

/** Postgres identifiers are limited to 63 bytes. */
export function safeIdentifier(name: string): string {
  if (Buffer.byteLength(name) <= 63) return name
  let hash = 5381
  for (let i = 0; i < name.length; i++) hash = ((hash << 5) + hash + name.charCodeAt(i)) >>> 0
  const suffix = `_${hash.toString(16).padStart(8, '0')}`
  return name.slice(0, 63 - suffix.length) + suffix
}

export const quoteIdent = (name: string): string => `"${name.replace(/"/g, '""')}"`

/** JSON path expression: `_raw->'a'->>'b'` for text, `->` for jsonb. */
function jsonPath(path: string | string[], asText: boolean): string {
  const parts = Array.isArray(path) ? path : [path]
  const head = parts.slice(0, -1)
  const last = parts[parts.length - 1]!
  let expr = RAW
  for (const p of head) expr += `->'${p.replace(/'/g, "''")}'`
  expr += `${asText ? '->>' : '->'}'${last.replace(/'/g, "''")}'`
  return expr
}

/**
 * The generated-column expression for a projected field.
 *
 * Timestamps go through `parse_iso8601`, not a plain `::timestamptz` cast:
 * casting text to timestamptz depends on the DateStyle/TimeZone settings, so
 * Postgres rejects it as non-immutable in a generated column. (Engines for
 * APIs that serialize times as Unix integers never hit this; WorkOS uses
 * ISO-8601 strings.) `parse_iso8601` is built from immutable primitives only.
 */
export function columnExpression(col: ColumnDef, schema: string): string {
  switch (col.type) {
    case 'jsonb':
      return `(${jsonPath(col.path, false)})`
    case 'text':
      return `(${jsonPath(col.path, true)})`
    case 'boolean':
      return `((NULLIF(${jsonPath(col.path, true)}, ''))::boolean)`
    case 'bigint':
      return `((NULLIF(${jsonPath(col.path, true)}, ''))::bigint)`
    case 'numeric':
      return `((NULLIF(${jsonPath(col.path, true)}, ''))::numeric)`
    case 'timestamptz':
      return `(${quoteIdent(schema)}.parse_iso8601(${jsonPath(col.path, true)}))`
  }
}

const PG_TYPE: Record<ColumnDef['type'], string> = {
  text: 'text',
  boolean: 'boolean',
  bigint: 'bigint',
  numeric: 'numeric',
  jsonb: 'jsonb',
  timestamptz: 'timestamptz',
}

/**
 * One `"name" type generated always as (...) stored` clause. Every path that
 * creates a projected column — table creation, the additive upgrade, drift
 * detection and repair — goes through here, so they cannot disagree about
 * what a column should look like.
 */
export function generatedColumnSql(col: ColumnDef, schema: string): string {
  return `${quoteIdent(safeIdentifier(col.name))} ${PG_TYPE[col.type]} generated always as ${columnExpression(
    col,
    schema,
  )} stored`
}

/**
 * A strict ISO-8601 parser built only from immutable primitives, so it is
 * legal in a generated column. Returns NULL rather than raising on input it
 * cannot parse — a generated column that throws would reject the whole row and
 * poison the batch.
 */
export function parseIso8601Function(schema: string): string {
  return `
create or replace function ${quoteIdent(schema)}.parse_iso8601(iso text)
returns timestamptz
language sql immutable strict parallel safe
as $fn$
  select case when p is null then null else
    timezone('UTC', make_timestamp(
      p[1]::int, p[2]::int, p[3]::int, p[4]::int, p[5]::int, p[6]::double precision))
    - case when p[8] is null then interval '0'
        else (case when p[8] = '-' then -1 else 1 end)
             * make_interval(hours => p[9]::int, mins => p[10]::int)
      end
  end
  from (
    select regexp_match(
      iso,
      '^(\\d{4})-(\\d{2})-(\\d{2})[Tt ](\\d{2}):(\\d{2}):(\\d{2}(?:\\.\\d+)?)\\s*(?:([Zz])|([+-])(\\d{2}):?(\\d{2}))?$'
    ) as p
  ) s
$fn$;`.trim()
}

export function createSchemaSql(schema: string): string {
  return `create schema if not exists ${quoteIdent(schema)};`
}

/** Engine bookkeeping: cursors and applied-schema fingerprints. */
export function bookkeepingSql(schema: string): string {
  const s = quoteIdent(schema)
  return `
create table if not exists ${s}."_sync_state" (
  sync_id    text not null,
  env        text not null,
  stream     text not null,
  state      jsonb not null,
  updated_at timestamptz not null default now(),
  primary key (sync_id, env, stream)
);

create table if not exists ${s}."_migrations" (
  fingerprint text primary key,
  applied_at  timestamptz not null default now(),
  detail      jsonb
);`.trim()
}

export function createTableSql(stream: StreamDef, schema: string): string {
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  const cols: string[] = [
    `${quoteIdent(ENV)}       text not null`,
    `${quoteIdent(RAW)}       jsonb not null`,
    // Source-derived. The staleness gate compares this.
    `"_updated_at" timestamptz not null`,
    // Destination write time. Lag is `_synced_at - _updated_at`.
    `"_synced_at"  timestamptz not null default now()`,
  ]
  // Append-only streams are immutable logs: no tombstones.
  if (stream.mode !== 'append') cols.push(`"_deleted_at" timestamptz`)

  for (const c of stream.columns) cols.push(generatedColumnSql(c, schema))
  const pk = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  cols.push(`primary key (${pk})`)

  return `create table if not exists ${s}.${table} (\n  ${cols.join(',\n  ')}\n);`
}

export function createIndexesSql(stream: StreamDef, schema: string): string[] {
  const s = quoteIdent(schema)
  const tableName = safeIdentifier(stream.name)
  const table = quoteIdent(tableName)
  const out: string[] = []
  for (const c of stream.columns) {
    if (!c.index) continue
    const idx = quoteIdent(safeIdentifier(`${tableName}_${c.name}_idx`))
    out.push(`create index if not exists ${idx} on ${s}.${table} (${quoteIdent(safeIdentifier(c.name))});`)
  }
  // Freshness and reconciliation queries scan these.
  out.push(
    `create index if not exists ${quoteIdent(
      safeIdentifier(`${tableName}_synced_at_idx`),
    )} on ${s}.${table} ("_synced_at");`,
  )
  if (stream.mode !== 'append') {
    out.push(
      `create index if not exists ${quoteIdent(
        safeIdentifier(`${tableName}_deleted_at_idx`),
      )} on ${s}.${table} ("_deleted_at") where "_deleted_at" is not null;`,
    )
  }
  return out
}

/**
 * Additive column DDL for a table that already exists. Adding a projected
 * column is `ADD COLUMN IF NOT EXISTS ... GENERATED ALWAYS AS ... STORED`,
 * which Postgres computes from rows already present. No backfill, no downtime.
 */
export function addColumnsSql(stream: StreamDef, schema: string): string[] {
  const s = quoteIdent(schema)
  const table = quoteIdent(safeIdentifier(stream.name))
  return stream.columns.map(
    (c) => `alter table ${s}.${table} add column if not exists ${generatedColumnSql(c, schema)};`,
  )
}

/** Full DDL for a set of streams, in dependency-free order. */
export function buildSchemaSql(streams: StreamDef[], schema: string): string[] {
  const out: string[] = [createSchemaSql(schema), parseIso8601Function(schema), bookkeepingSql(schema)]
  for (const s of streams) {
    out.push(createTableSql(s, schema))
    out.push(...addColumnsSql(s, schema))
    out.push(...createIndexesSql(s, schema))
  }
  return out
}
