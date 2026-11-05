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
 * Collapse rows that share a primary key, keeping the newest. One
 * multi-row statement must not touch a row twice, so this is required, not
 * an optimization. It serves the delete, relink and unlink paths, whose rows
 * are whole; upserts use `wavesByKey`.
 */
export function dedupeByKey(stream: TableDef, rows: Row[]): { rows: Row[]; dropped: number } {
  const byKey = new Map<string, { row: Row; at: number }>()
  let dropped = 0
  for (const row of rows) {
    const key = keyValues(stream, row.data)
    if (!key) {
      dropped++
      continue
    }
    const k = key.join('\0')
    const at = micros(row.updatedAt)
    const existing = byKey.get(k)
    if (!existing || !isBefore(row.updatedAt, at, existing.row.updatedAt, existing.at)) byKey.set(k, { row, at })
  }
  return { rows: [...byKey.values()].map((e) => e.row), dropped }
}

/**
 * The instant `iso` names, in microseconds since the epoch, or NaN.
 * `Date.parse` reads three fractional digits; a `DATETIME(6)` keeps six, so
 * the rest of the fraction is read here and two rows a microsecond apart
 * stay ordered the way the staleness gate will order them.
 */
function micros(iso: string): number {
  const frac = /\.(\d+)/.exec(iso)?.[1] ?? ''
  const ms = Date.parse(frac.length > 3 ? iso.replace(`.${frac}`, `.${frac.slice(0, 3)}`) : iso)
  if (Number.isNaN(ms)) return ms
  return ms * 1000 + Number(frac.slice(3, 6).padEnd(3, '0'))
}

/**
 * Whether one timestamp is strictly earlier than another, by the instant
 * each names: `01:00:00+01:00` sorts after `00:30:00Z` as text and is half an
 * hour earlier as a time. A value that is not a date is ordered as text; the
 * database refuses it on the write either way.
 */
function isBefore(a: string, atA: number, b: string, atB: number): boolean {
  if (Number.isNaN(atA) || Number.isNaN(atB)) return a < b
  return atA < atB
}

/**
 * Split a batch into waves in which no key appears twice, keeping order: the
 * n-th row for a key goes in wave n. Written one after another, the waves
 * are exactly the rows written one at a time — each row meets the staleness
 * gate against what the row before it left stored, and a merge overlays it
 * on that — while a batch with no repeated key, which is nearly every batch,
 * is still a single statement. See the Postgres destination for why keeping
 * only the newest row would be wrong for a merge.
 */
export function wavesByKey(stream: TableDef, rows: Row[]): { waves: Row[][]; dropped: number } {
  const waves: Row[][] = []
  const seen = new Map<string, number>()
  let dropped = 0
  for (const row of rows) {
    const key = keyValues(stream, row.data)
    if (!key) {
      dropped++
      continue
    }
    const k = key.join('\0')
    const n = seen.get(k) ?? 0
    seen.set(k, n + 1)
    const wave = waves[n] ?? (waves[n] = [])
    wave.push(row)
  }
  return { waves, dropped }
}

export const chunk = <T>(items: T[], size: number): T[][] => {
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

/**
 * `Row.updatedAt` as a `Date`. Postgres casts the string in SQL and a bad one
 * fails the statement; here the same verdict is reached before the
 * statement, with a code the error classifier reports as deterministic, so
 * the event is quarantined rather than retried forever.
 */
export function parseUpdatedAt(value: string): Date {
  const d = new Date(value)
  if (typeof value !== 'string' || Number.isNaN(d.getTime())) {
    throw Object.assign(new Error(`invalid updatedAt timestamp: ${JSON.stringify(value)}`), {
      code: 'ER_INVALID_TIMESTAMP',
    })
  }
  return d
}

/** Structural equality with key order ignored, which is what `is distinct from` means for jsonb. */
export function jsonEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a)) {
    const bb = b as unknown[]
    return a.length === bb.length && a.every((v, i) => jsonEqual(v, bb[i]))
  }
  const ao = a as Record<string, unknown>
  const bo = b as Record<string, unknown>
  const keys = Object.keys(ao)
  if (keys.length !== Object.keys(bo).length) return false
  return keys.every((k) => Object.hasOwn(bo, k) && jsonEqual(ao[k], bo[k]))
}

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
