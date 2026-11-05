/**
 * Write path.
 *
 * The rules are the Postgres destination's, and for the same reasons (see
 * `db/upsert.ts` there): the staleness gate is `>=` and never `>`, event
 * writes merge where REST writes replace, deletion is terminal except for
 * memberships, which relink and unlink only when the write is at least as
 * new as the row.
 *
 * ClickHouse has no row update, so a write is an insert of the row's next
 * state into a `ReplacingMergeTree` versioned by `_updated_at`. Each batch
 * reads the current state of its rows (`FINAL`), decides every row in Node
 * — the gate, the shallow merge, whether anything changed, which tombstone
 * to carry forward — and inserts only the rows that passed. The table then
 * keeps, for each key, the version with the newest `_updated_at`, and the
 * last inserted among equals; a refused row, never inserted, changes
 * nothing. Reading and writing are two statements, so each batch holds the
 * stream's write lock (`lock.ts`) across both: two writers on one key — the
 * tail and a backfill, or a poll outliving its lease — then see each
 * other's rows instead of each judging against the state before the other
 * wrote. Append-only streams are plain `MergeTree`: under the same lock the
 * batch reads which ids are already there and inserts the rest, which is
 * what makes re-delivery a no-op.
 */
import { emptyStats, type Row, type TableDef, type WriteMode, type WriteStats } from '@workos/sync-engine'
import { hostname } from 'node:os'
import { fromCh, toCh, type ChClient } from './client.js'
import { lockKey, withStreamLock, type HeldCheck, type LockOptions } from './lock.js'
import { quoteIdent, safeIdentifier, ENV, RAW } from '../schema/ddl.js'

/** Joins the components of a composite key, on both sides of every lookup. */
export const KEY_SEPARATOR = '\u0001'

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
 * Collapse rows that share a primary key, keeping the newest. It serves the
 * delete, relink and unlink paths, whose rows are whole; upserts use
 * `wavesByKey`.
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
    const k = key.join(KEY_SEPARATOR)
    const at = micros(row.updatedAt)
    const existing = byKey.get(k)
    if (!existing || !isBefore(row.updatedAt, at, existing.row.updatedAt, existing.at)) byKey.set(k, { row, at })
  }
  return { rows: [...byKey.values()].map((e) => e.row), dropped }
}

/**
 * The instant `iso` names, in microseconds since the epoch, or NaN.
 * `Date.parse` reads three fractional digits, and so does the column
 * (`DateTime64(3)`), so two rows within a millisecond tie at the staleness
 * gate and the last one written wins; reading the rest of the fraction
 * here is what makes that last one the newer of the two.
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
 * hour earlier as a time. A value that is not a date is ordered as text;
 * `parseUpdatedAt` refuses it on the write either way.
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
 * on that. See the Postgres destination for why keeping only the newest row
 * would be wrong for a merge.
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
    const k = key.join(KEY_SEPARATOR)
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
      code: 'INVALID_TIMESTAMP',
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
  /** Names this writer in `_locks`. One per process by default. */
  lockHolder?: string
  lockTtlMs?: number
  lockWaitMs?: number
}

const DEFAULT_BATCH_SIZE = 500
const PROCESS_ID = `${hostname()}:${process.pid}`

const lockOptions = (opts: WriteOptions): LockOptions => ({
  schema: opts.schema,
  holder: opts.lockHolder ?? PROCESS_ID,
  ttlMs: opts.lockTtlMs,
  waitMs: opts.lockWaitMs,
})

/** Run `fn` holding the write lock for `stream` in `env`. */
const locked = <T>(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  fn: (held: HeldCheck) => Promise<T>,
) => withStreamLock(db, lockOptions(opts), lockKey(safeIdentifier(stream.name), env), fn)
export const tableRef = (schema: string, stream: TableDef) =>
  `${quoteIdent(schema)}.${quoteIdent(safeIdentifier(stream.name))}`

/** The whole key as one SQL expression: the column, or the components joined by the separator. */
export function keyExpression(stream: TableDef): string {
  const cols = stream.key.map((k) => quoteIdent(safeIdentifier(k)))
  if (cols.length === 1) return cols[0]!
  const parts: string[] = []
  for (const [i, col] of cols.entries()) {
    if (i > 0) parts.push('char(1)')
    parts.push(col)
  }
  return `concat(${parts.join(', ')})`
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

/** The current state of the rows for `keys`, by joined key; keys with no row are absent. */
async function readRows(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  keys: string[][],
): Promise<Map<string, StoredRow>> {
  const rows = await db.query<{ k: string; raw: string; updated_at: string; deleted_at: string | null }>(
    `SELECT ${keyExpression(stream)} AS k, ${quoteIdent(RAW)} AS raw, \`_updated_at\` AS updated_at, \`_deleted_at\` AS deleted_at
     FROM ${tableRef(opts.schema, stream)} FINAL
     WHERE ${quoteIdent(ENV)} = {env:String} AND ${keyExpression(stream)} IN ({keys:Array(String)})`,
    { env, keys: keys.map((k) => k.join(KEY_SEPARATOR)) },
  )
  const out = new Map<string, StoredRow>()
  for (const row of rows) {
    out.set(row.k, {
      raw: JSON.parse(row.raw) as Record<string, unknown>,
      updatedAt: fromCh(row.updated_at),
      deletedAt: row.deleted_at ? fromCh(row.deleted_at) : null,
    })
  }
  return out
}

/** Which of `keys` already have a row, for append-only tables. */
async function existingKeys(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  keys: string[][],
): Promise<Set<string>> {
  const rows = await db.query<{ k: string }>(
    `SELECT DISTINCT ${keyExpression(stream)} AS k FROM ${tableRef(opts.schema, stream)}
     WHERE ${quoteIdent(ENV)} = {env:String} AND ${keyExpression(stream)} IN ({keys:Array(String)})`,
    { env, keys: keys.map((k) => k.join(KEY_SEPARATOR)) },
  )
  return new Set(rows.map((r) => r.k))
}

/** Insert the decided rows as their next version. `_synced_at` is the server's clock. */
async function writeRows(db: ChClient, opts: WriteOptions, stream: TableDef, env: string, rows: Pending[]) {
  if (rows.length === 0) return
  const hasDeleted = stream.mode !== 'append'
  await db.insert(
    tableRef(opts.schema, stream),
    rows.map((r) => ({
      [ENV]: env,
      [RAW]: JSON.stringify(r.raw),
      _updated_at: toCh(r.updatedAt),
      ...(hasDeleted ? { _deleted_at: r.deletedAt ? toCh(r.deletedAt) : null } : {}),
    })),
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
 * Read, decide, write: the shape of every mirror write, under the stream's
 * lock. `decide` is handed each row with what is stored for it and returns
 * what to write, or null to leave the row alone.
 */
async function applyBatch(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  batch: Keyed[],
  decide: (row: Keyed, stored: StoredRow | undefined) => Pending | null,
): Promise<void> {
  await locked(db, opts, stream, env, async (held) => {
    const stored = await readRows(
      db,
      opts,
      stream,
      env,
      batch.map((r) => r.key),
    )
    const pending: Pending[] = []
    for (const row of batch) {
      const out = decide(row, stored.get(row.key.join(KEY_SEPARATOR)))
      if (out) pending.push(out)
    }
    // Decided against rows read under the lock; written only if it is still ours.
    if (pending.length > 0) await held()
    await writeRows(db, opts, stream, env, pending)
  })
}

export async function upsertRows(
  db: ChClient,
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
    for (const batch of chunk(keyed(stream, wave), opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
      if (stream.mode === 'append') {
        // Immutable log. Re-delivery is a no-op, which is the whole
        // idempotency story; the lock is what makes it hold for two polls
        // delivering the same page at once.
        await locked(db, opts, stream, env, async (held) => {
          const present = await existingKeys(
            db,
            opts,
            stream,
            env,
            batch.map((r) => r.key),
          )
          const fresh = batch.filter((r) => !present.has(r.key.join(KEY_SEPARATOR)))
          if (fresh.length > 0) await held()
          await writeRows(
            db,
            opts,
            stream,
            env,
            fresh.map((r) => ({ key: r.key, raw: r.data, updatedAt: r.updatedAt, deletedAt: null })),
          )
          stats.created += fresh.length
          stats.skipped += batch.length - fresh.length
        })
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
        // An equal timestamp with nothing new is a re-delivery: no new version,
        // so `_synced_at` stays and lag is not measured against a no-op.
        if (row.updatedAt.getTime() === stored.updatedAt.getTime() && jsonEqual(stored.raw, raw)) {
          stats.skipped++
          return null
        }
        stats.updated++
        // The tombstone is carried forward: a late update never resurrects a row.
        return { key: row.key, raw, updatedAt: row.updatedAt, deletedAt: stored.deletedAt }
      })
    }
  }
  return stats
}

export async function deleteRows(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
    await applyBatch(db, opts, stream, env, batch, (row, stored) => {
      stats.deleted++
      if (!stored) return { key: row.key, raw: row.data, updatedAt: row.updatedAt, deletedAt: row.updatedAt }
      // The *latest* removal is kept, not the first: a membership removed,
      // re-added and removed again must carry the last removal, because that
      // is what `relinkRows` compares a re-add against. The version moves to
      // the deletion time, where Postgres leaves `_updated_at` alone: here
      // the tombstone has to be the newest version of the row, and the
      // deletion came after anything a listing could still show of it, so
      // it outranks any older snapshot on the version itself — not on which
      // insert happened to land last.
      return {
        key: row.key,
        raw: merge(stored.raw, row.data),
        updatedAt: later(stored.updatedAt, row.updatedAt),
        deletedAt: later(stored.deletedAt, row.updatedAt),
      }
    })
  }
  return stats
}

/** Restore a tombstoned membership row (a re-add after a removal). */
export async function relinkRows(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
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
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped
  for (const batch of chunk(keyed(stream, deduped), opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
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
