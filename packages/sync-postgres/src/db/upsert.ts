/**
 * Write path.
 *
 * Three rules, each of which exists because of a documented failure in a
 * comparable engine:
 *
 * 1. **Staleness gate, `>=` not `>`.** Writes are rejected when the incoming
 *    row is older than the row already stored. It is `>=` because event
 *    timestamps are not infinitely precise: with a strict `<` comparison two
 *    events sharing a timestamp cause the second to be dropped forever, which
 *    is exactly how a comparable billing sync engine kept subscriptions
 *    "trialing" after they converted. Ties resolve last-writer-wins, and the
 *    backstop is a re-list rather than the comparison — `refresh` for
 *    top-level streams, `backfill` for those listed per parent — because
 *    `reconcile` compares ids only and cannot see a wrong payload.
 *
 * 2. **Merge, don't replace, when writing from an event.** WorkOS event
 *    payloads are subsets of the REST objects, so a whole-row replace would
 *    null out `external_id`, `metadata`, `last_sign_in_at` and friends —
 *    precisely the columns people join on. Event writes shallow-merge into
 *    `_raw`; REST-sourced writes (backfill, reconcile, hydrate) replace it.
 *
 * 3. **Deletion is terminal.** Tombstones are not staleness-gated, and
 *    `_deleted_at` is never cleared. A late `updated` event arriving after a
 *    delete refreshes the payload but must not resurrect the row. WorkOS IDs
 *    are ULIDs and are never reused, so this is safe. Memberships are the
 *    exception, in both directions: a re-add (`relinkRows`) clears the
 *    tombstone and a removal (`unlinkRows`) sets it, each only when it is at
 *    least as new as the row. Events can be replayed out of order from
 *    quarantine, and an older link or unlink must not undo a newer one.
 */
import { emptyStats, type Row, type TableDef, type WriteMode, type WriteStats } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { quoteIdent, safeIdentifier, ENV, RAW } from '../schema/ddl.js'

export function readPath(data: Record<string, unknown>, path: string | string[]): unknown {
  const parts = Array.isArray(path) ? path : [path]
  let cur: unknown = data
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return undefined
    cur = (cur as Record<string, unknown>)[p]
  }
  return cur
}

/** Key values for a row, in `stream.key` order. Undefined if any part is missing. */
export function keyValues(stream: TableDef, data: Record<string, unknown>): string[] | undefined {
  const out: string[] = []
  for (const k of stream.key) {
    const col = stream.columns.find((c) => c.name === k)
    const v = readPath(data, col ? col.path : k)
    if (v == null || v === '') return undefined
    out.push(String(v))
  }
  return out
}

/**
 * Collapse rows that share a primary key into one. Postgres refuses to let
 * one INSERT ... ON CONFLICT DO UPDATE touch the same row twice, so this is
 * required, not an optimization.
 *
 * The one row has to be what writing them one at a time would have left
 * behind. In batch order, each row passes the same `>=` staleness gate the
 * statement applies, and a row that passes either replaces the previous one
 * (`replace`: REST-sourced rows are whole objects) or is overlaid on it
 * (`merge`: the shallow `_raw || excluded._raw` an event write performs).
 * The overlay is what keeps a hydrated create whole when a later update
 * event for the same object shares its batch. The snapshot is the only row
 * that carries the fields event payloads omit, and keeping the newest row
 * alone would drop them until the next refresh.
 */
export function dedupeByKey(
  stream: TableDef,
  rows: Row[],
  mode: WriteMode = 'replace',
): { rows: Row[]; dropped: number } {
  const byKey = new Map<string, Row>()
  let dropped = 0
  for (const row of rows) {
    const key = keyValues(stream, row.data)
    if (!key) {
      dropped++
      continue
    }
    const k = key.join('\0')
    const existing = byKey.get(k)
    if (existing && row.updatedAt < existing.updatedAt) continue
    byKey.set(
      k,
      existing && mode === 'merge'
        ? { data: { ...existing.data, ...row.data }, updatedAt: row.updatedAt }
        : row,
    )
  }
  return { rows: [...byKey.values()], dropped }
}

interface BuiltStatement {
  sql: string
  params: unknown[]
}

export function buildUpsert(
  schema: string,
  stream: TableDef,
  env: string,
  rows: Row[],
  mode: WriteMode,
): BuiltStatement {
  const table = `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  const params: unknown[] = []
  const tuples = rows.map((r) => {
    const i = params.length
    params.push(env, JSON.stringify(r.data), r.updatedAt)
    return `($${i + 1}, $${i + 2}::jsonb, $${i + 3}::timestamptz)`
  })

  const conflict = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  const raw = quoteIdent(RAW)
  const newRaw = mode === 'merge' ? `t.${raw} || excluded.${raw}` : `excluded.${raw}`

  if (stream.mode === 'append') {
    // Immutable log. Re-delivery is a no-op, which is the whole idempotency story.
    return {
      sql: `insert into ${table} as t (${quoteIdent(ENV)}, ${raw}, "_updated_at")
values ${tuples.join(', ')}
on conflict (${conflict}) do nothing
returning true as _created`,
      params,
    }
  }

  return {
    sql: `insert into ${table} as t (${quoteIdent(ENV)}, ${raw}, "_updated_at")
values ${tuples.join(', ')}
on conflict (${conflict}) do update set
  ${raw} = ${newRaw},
  "_updated_at" = excluded."_updated_at",
  "_synced_at" = now()
where excluded."_updated_at" >= t."_updated_at"
  and (excluded."_updated_at" > t."_updated_at" or t.${raw} is distinct from (${newRaw}))
returning (xmax = 0) as _created`,
    params,
  }
}

export interface WriteOptions {
  schema: string
  batchSize?: number
}

async function runBatch(
  db: SqlClient,
  built: BuiltStatement,
  attempted: number,
): Promise<{ created: number; updated: number; skipped: number }> {
  const res = await db.query<{ _created: boolean }>(built.sql, built.params)
  const created = res.rows.filter((r) => r._created).length
  const updated = res.rows.length - created
  return { created, updated, skipped: attempted - res.rows.length }
}

export const chunk = <T>(items: T[], size: number): T[][] => {
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

export async function upsertRows(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
  mode: WriteMode,
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows, mode)
  // Rows we could not key (a payload with no id and no slug) are counted as
  // skipped rather than silently vanishing.
  stats.skipped += dropped
  for (const batch of chunk(deduped, opts.batchSize ?? 500)) {
    const built = buildUpsert(opts.schema, stream, env, batch, mode)
    const r = await runBatch(db, built, batch.length)
    stats.created += r.created
    stats.updated += r.updated
    stats.skipped += r.skipped
  }
  return stats
}

export async function deleteRows(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  const table = `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  const conflict = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  const raw = quoteIdent(RAW)

  for (const batch of chunk(deduped, opts.batchSize ?? 500)) {
    const params: unknown[] = []
    const tuples = batch.map((r) => {
      const i = params.length
      params.push(env, JSON.stringify(r.data), r.updatedAt)
      return `($${i + 1}, $${i + 2}::jsonb, $${i + 3}::timestamptz, $${i + 3}::timestamptz)`
    })
    // `greatest`, not `coalesce`, for `_deleted_at`: a membership removed,
    // re-added and removed again must carry the *latest* removal, because that
    // is what `relinkRows` compares a re-add against. `greatest` ignores NULL,
    // so a live row is tombstoned at the incoming time, as before.
    const sql = `insert into ${table} as t (${quoteIdent(ENV)}, ${raw}, "_updated_at", "_deleted_at")
values ${tuples.join(', ')}
on conflict (${conflict}) do update set
  ${raw} = t.${raw} || excluded.${raw},
  "_deleted_at" = greatest(t."_deleted_at", excluded."_deleted_at"),
  "_synced_at" = now()
returning (xmax = 0) as _created`
    const res = await db.query(sql, params)
    stats.deleted += res.rows.length
  }
  return stats
}

/** Restore a tombstoned membership row (a re-add after a removal). */
export async function relinkRows(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  const table = `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  const conflict = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  const raw = quoteIdent(RAW)

  for (const batch of chunk(deduped, opts.batchSize ?? 500)) {
    const params: unknown[] = []
    const tuples = batch.map((r) => {
      const i = params.length
      params.push(env, JSON.stringify(r.data), r.updatedAt)
      return `($${i + 1}, $${i + 2}::jsonb, $${i + 3}::timestamptz)`
    })
    // A membership is a fact that can legitimately come back: added, removed,
    // added again. Unlike object deletes, a re-link clears the tombstone —
    // but only a re-link at least as new as the removal. `_updated_at` alone
    // is not enough: a tombstone written by `deleteRows` (a reconcile repair)
    // leaves it untouched, so a link replayed from quarantine after that
    // removal would pass the staleness gate and resurrect the membership.
    const sql = `insert into ${table} as t (${quoteIdent(ENV)}, ${raw}, "_updated_at")
values ${tuples.join(', ')}
on conflict (${conflict}) do update set
  ${raw} = t.${raw} || excluded.${raw},
  "_updated_at" = excluded."_updated_at",
  "_deleted_at" = null,
  "_synced_at" = now()
where excluded."_updated_at" >= t."_updated_at"
  and (t."_deleted_at" is null or excluded."_updated_at" >= t."_deleted_at")
returning (xmax = 0) as _created`
    const res = await db.query<{ _created: boolean }>(sql, params)
    const created = res.rows.filter((r) => r._created).length
    stats.created += created
    stats.updated += res.rows.length - created
    stats.skipped += batch.length - res.rows.length
  }
  return stats
}

/**
 * Tombstone membership rows (a removal). The counterpart of `relinkRows`,
 * and staleness-gated like it, which is what sets it apart from `deleteRows`:
 * a membership can come back, so a removal is a fact with a time, not a
 * terminal state, and one replayed from quarantine after a newer re-add has
 * landed must not hide a membership that is active now.
 */
export async function unlinkRows(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  const table = `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`
  const conflict = [ENV, ...stream.key].map((k) => quoteIdent(safeIdentifier(k))).join(', ')
  const raw = quoteIdent(RAW)

  for (const batch of chunk(deduped, opts.batchSize ?? 500)) {
    const params: unknown[] = []
    const tuples = batch.map((r) => {
      const i = params.length
      params.push(env, JSON.stringify(r.data), r.updatedAt)
      return `($${i + 1}, $${i + 2}::jsonb, $${i + 3}::timestamptz, $${i + 3}::timestamptz)`
    })
    // `_updated_at` moves to the removal time, so the next re-add is gated
    // against the removal by the same comparison every membership write uses.
    // `_deleted_at` keeps the latest removal for the same reason `deleteRows`
    // does.
    const sql = `insert into ${table} as t (${quoteIdent(ENV)}, ${raw}, "_updated_at", "_deleted_at")
values ${tuples.join(', ')}
on conflict (${conflict}) do update set
  ${raw} = t.${raw} || excluded.${raw},
  "_updated_at" = excluded."_updated_at",
  "_deleted_at" = greatest(t."_deleted_at", excluded."_deleted_at"),
  "_synced_at" = now()
where excluded."_updated_at" >= t."_updated_at"
returning 1`
    const res = await db.query(sql, params)
    stats.deleted += res.rows.length
    stats.skipped += batch.length - res.rows.length
  }
  return stats
}
