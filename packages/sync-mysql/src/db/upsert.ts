/**
 * Write path.
 *
 * The rules are the Postgres destination's, and for the same reasons (see
 * `db/upsert.ts` there): the staleness gate is `>=` and never `>`, event
 * writes merge where REST writes replace, deletion is terminal except for
 * memberships, which relink and unlink only when the write is at least as
 * new as the row.
 *
 * What differs is where the gate runs. Postgres applies it inside one
 * `INSERT ... ON CONFLICT DO UPDATE ... WHERE`, which also returns the rows
 * it wrote. MySQL's `ON DUPLICATE KEY UPDATE` has no `WHERE` and no
 * `RETURNING`, and its only JSON merge, `JSON_MERGE_PATCH`, is deep where
 * the contract wants shallow: an event whose `metadata` dropped a key would
 * keep the stale key. So each batch is a transaction that first locks the
 * stored rows (`SELECT ... FOR UPDATE`), decides every row in Node against
 * what it read — the gate, the merge, whether anything changed — and then
 * writes only the rows that passed, in one multi-row statement. The row
 * locks are what make the read-then-write safe: a competing writer waits on
 * an existing row, and under REPEATABLE READ a missing row's gap is locked
 * too, so nothing newer can land between the read and the write. Two
 * writers inserting into one gap deadlock, which the engine sees as a
 * transient error and retries. The statistics are exact, since the decision
 * is made here.
 */
import {
  emptyStats,
  type Row,
  type TableDef,
  type WriteMode,
  type WriteStats,
  keyValues,
  dedupeByKey,
  wavesByKey,
  chunk,
  parseUpdatedAt,
  jsonEqual,
} from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { quoteIdent, safeIdentifier, ENV, RAW } from '../schema/ddl.js'

/** Shallow merge: the jsonb `||` of the Postgres destination. */
const merge = (stored: Record<string, unknown>, incoming: Record<string, unknown>): Record<string, unknown> => ({
  ...stored,
  ...incoming,
})

const later = (a: Date | null, b: Date): Date => (a !== null && a.getTime() > b.getTime() ? a : b)

export interface WriteOptions {
  schema: string
  batchSize?: number
}

interface StoredRow {
  raw: Record<string, unknown>
  updatedAt: Date
  deletedAt: Date | null
}

interface Pending {
  key: string[]
  raw: Record<string, unknown>
  updatedAt: Date
  deletedAt: Date | null
}

const tableRef = (opts: WriteOptions, stream: TableDef) =>
  `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`

/**
 * The stored rows for `keys`, locked for the rest of the transaction, by
 * key. Keys that have no row are simply absent from the map.
 */
async function lockRows(
  tx: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  keys: string[][],
): Promise<Map<string, StoredRow>> {
  const keyCols = stream.key.map((k) => quoteIdent(safeIdentifier(k)))
  const params: unknown[] = [env]
  const tuples = keys.map((k) => {
    params.push(...k)
    return keyCols.length === 1 ? '?' : `(${keyCols.map(() => '?').join(', ')})`
  })
  const target = keyCols.length === 1 ? keyCols[0]! : `(${keyCols.join(', ')})`
  const r = await tx.query<Record<string, unknown>>(
    `select ${keyCols.join(', ')}, ${quoteIdent(RAW)} as raw, \`_updated_at\` as updated_at, \`_deleted_at\` as deleted_at
     from ${tableRef(opts, stream)}
     where ${quoteIdent(ENV)} = ? and ${target} in (${tuples.join(', ')})
     for update`,
    params,
  )
  const out = new Map<string, StoredRow>()
  for (const row of r.rows) {
    const k = stream.key.map((name) => String(row[safeIdentifier(name)])).join('\0')
    out.set(k, {
      raw: row['raw'] as Record<string, unknown>,
      updatedAt: row['updated_at'] as Date,
      deletedAt: (row['deleted_at'] as Date | null) ?? null,
    })
  }
  return out
}

/**
 * Write the decided rows. Every value is final, so the update clause has
 * nothing left to decide; `_synced_at` moves because something changed, or
 * the row would not be here.
 */
async function writeRows(tx: SqlClient, opts: WriteOptions, stream: TableDef, env: string, rows: Pending[]) {
  if (rows.length === 0) return
  const raw = quoteIdent(RAW)
  const params: unknown[] = []
  const tuples = rows.map((r) => {
    params.push(env, JSON.stringify(r.raw), r.updatedAt, r.deletedAt)
    return '(?, ?, ?, ?)'
  })
  await tx.query(
    `insert into ${tableRef(opts, stream)} (${quoteIdent(ENV)}, ${raw}, \`_updated_at\`, \`_deleted_at\`)
     values ${tuples.join(', ')} as new
     on duplicate key update
       ${raw} = new.${raw},
       \`_updated_at\` = new.\`_updated_at\`,
       \`_deleted_at\` = new.\`_deleted_at\`,
       \`_synced_at\` = now(6)`,
    params,
  )
}

/** One batch of keyed rows with their parsed timestamps, ready to be decided. */
interface Keyed {
  key: string[]
  data: Record<string, unknown>
  updatedAt: Date
}

const keyed = (stream: TableDef, rows: Row[]): Keyed[] =>
  rows.map((r) => ({ key: keyValues(stream, r.data)!, data: r.data, updatedAt: parseUpdatedAt(r.updatedAt) }))

/**
 * Lock, decide, write: the shape of every mirror write. `decide` is handed
 * each row with what is stored for it and returns what to write, or null to
 * leave the row alone.
 */
async function applyBatch(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  batch: Keyed[],
  decide: (row: Keyed, stored: StoredRow | undefined) => Pending | null,
): Promise<void> {
  await db.transaction(async (tx) => {
    const stored = await lockRows(
      tx,
      opts,
      stream,
      env,
      batch.map((r) => r.key),
    )
    const pending: Pending[] = []
    for (const row of batch) {
      const out = decide(row, stored.get(row.key.join('\0')))
      if (out) pending.push(out)
    }
    await writeRows(tx, opts, stream, env, pending)
  })
}

/** Append-only streams: immutable log, re-delivery is a no-op. */
async function appendRows(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  batch: Keyed[],
): Promise<number> {
  const params: unknown[] = []
  const tuples = batch.map((r) => {
    params.push(env, JSON.stringify(r.data), r.updatedAt)
    return '(?, ?, ?)'
  })
  const r = await db.query(
    `insert ignore into ${tableRef(opts, stream)} (${quoteIdent(ENV)}, ${quoteIdent(RAW)}, \`_updated_at\`)
     values ${tuples.join(', ')}`,
    params,
  )
  return r.affectedRows
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
  const { waves, dropped } = wavesByKey(stream, rows)
  // Rows we could not key (a payload with no id and no slug) are counted as
  // skipped rather than silently vanishing.
  stats.skipped += dropped
  for (const wave of waves) {
    for (const batch of chunk(keyed(stream, wave), opts.batchSize ?? 500)) {
      if (stream.mode === 'append') {
        const created = await appendRows(db, opts, stream, env, batch)
        stats.created += created
        stats.skipped += batch.length - created
        continue
      }
      await applyBatch(db, opts, stream, env, batch, (row, stored) => {
        if (!stored) {
          stats.created++
          return { key: row.key, raw: row.data, updatedAt: row.updatedAt, deletedAt: null }
        }
        if (row.updatedAt.getTime() < stored.updatedAt.getTime()) {
          stats.skipped++
          return null
        }
        const raw = mode === 'merge' ? merge(stored.raw, row.data) : row.data
        // An equal timestamp with nothing new is a re-delivery: leave
        // `_synced_at` alone so lag is not measured against a no-op.
        if (row.updatedAt.getTime() === stored.updatedAt.getTime() && jsonEqual(stored.raw, raw)) {
          stats.skipped++
          return null
        }
        stats.updated++
        // The tombstone is untouched: a late update never resurrects a row.
        return { key: row.key, raw, updatedAt: row.updatedAt, deletedAt: stored.deletedAt }
      })
    }
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
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? 500)) {
    await applyBatch(db, opts, stream, env, batch, (row, stored) => {
      stats.deleted++
      if (!stored) return { key: row.key, raw: row.data, updatedAt: row.updatedAt, deletedAt: row.updatedAt }
      // The *latest* removal is kept, not the first: a membership removed,
      // re-added and removed again must carry the last removal, because that
      // is what `relinkRows` compares a re-add against. `_updated_at` stays:
      // a delete is not a staleness-gated write.
      return {
        key: row.key,
        raw: merge(stored.raw, row.data),
        updatedAt: stored.updatedAt,
        deletedAt: later(stored.deletedAt, row.updatedAt),
      }
    })
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
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? 500)) {
    await applyBatch(db, opts, stream, env, batch, (row, stored) => {
      if (!stored) {
        stats.created++
        return { key: row.key, raw: row.data, updatedAt: row.updatedAt, deletedAt: null }
      }
      // A membership is a fact that can legitimately come back: added,
      // removed, added again. Unlike object deletes, a re-link clears the
      // tombstone — but only a re-link at least as new as the removal.
      // `_updated_at` alone is not enough: a tombstone written by `deleteRows`
      // (a reconcile repair) leaves it untouched, so a link replayed from
      // quarantine after that removal would pass the staleness gate and
      // resurrect the membership.
      const t = row.updatedAt.getTime()
      if (t < stored.updatedAt.getTime() || (stored.deletedAt !== null && t < stored.deletedAt.getTime())) {
        stats.skipped++
        return null
      }
      stats.updated++
      return { key: row.key, raw: merge(stored.raw, row.data), updatedAt: row.updatedAt, deletedAt: null }
    })
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
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? 500)) {
    await applyBatch(db, opts, stream, env, batch, (row, stored) => {
      if (!stored) {
        stats.deleted++
        return { key: row.key, raw: row.data, updatedAt: row.updatedAt, deletedAt: row.updatedAt }
      }
      if (row.updatedAt.getTime() < stored.updatedAt.getTime()) {
        stats.skipped++
        return null
      }
      // `_updated_at` moves to the removal time, so the next re-add is gated
      // against the removal by the same comparison every membership write
      // uses. `_deleted_at` keeps the latest removal for the same reason
      // `deleteRows` does.
      stats.deleted++
      return {
        key: row.key,
        raw: merge(stored.raw, row.data),
        updatedAt: row.updatedAt,
        deletedAt: later(stored.deletedAt, row.updatedAt),
      }
    })
  }
  return stats
}
