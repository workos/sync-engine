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
 * 2. **Merge, don't replace, when writing from an event.** Most WorkOS event
 *    payloads are the full REST object, but deprecated fields are left out
 *    of them, and any stream whose events are narrower than its REST object
 *    would have those fields dropped by a whole-document replace. Event
 *    writes shallow-merge into `_raw` (`$mergeObjects`); REST-sourced writes
 *    (backfill, reconcile, hydrate) replace it.
 *
 * 3. **Deletion is terminal.** Tombstones are not staleness-gated, and
 *    `_deleted_at` is never cleared. A late `updated` event arriving after a
 *    delete refreshes the payload but must not resurrect the row. WorkOS IDs
 *    are ULIDs and are never reused, so this is safe. Memberships are the
 *    exception, in both directions: a re-add (`relinkRows`) clears the
 *    tombstone and a removal (`unlinkRows`) sets it, each only when it is at
 *    least as new as the row. Events can be replayed out of order from
 *    quarantine, and an older link or unlink must not undo a newer one.
 *
 * Every write is an update pipeline run by the server with `upsert: true`,
 * so the gate is applied against the stored timestamp in the same operation
 * that writes — there is no window between a read and a write for another
 * writer to slip through. Payloads go into the pipeline as `$literal`: an
 * expression context reads a string like `"$100 plan"` as a field path, and
 * `$literal` is what stops it.
 */
import { emptyStats, type Row, type TableDef, type WriteMode, type WriteStats } from '@workos/sync-engine'
import type { AnyBulkWriteOperation, Collection, Document, Filter } from 'mongodb'
import { fieldPath, keyPath } from '../schema/names.js'
import { parseInstant } from './timestamps.js'

/** One mirrored row. */
export interface MirrorDoc extends Document {
  _env: string
  /** The payload as it arrived, the only thing the engine ever writes about an object. */
  _raw: Record<string, unknown>
  /** Source-derived. The staleness gate compares this. */
  _updated_at: Date
  /** Destination write time, by the server's clock. Lag is `_synced_at - _updated_at`. */
  _synced_at: Date
  /** Tombstone. Absent on append-only streams. */
  _deleted_at?: Date | null
}

export function readPath(data: Record<string, unknown>, path: string | string[]): unknown {
  const parts = Array.isArray(path) ? path : [path]
  let cur: unknown = data
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return undefined
    cur = (cur as Record<string, unknown>)[p]
  }
  return cur
}

/**
 * Key values for a row, in `stream.key` order. Undefined if any part is
 * missing, or is not a scalar: an object where an id should be is a malformed
 * row, not a key, and is never let near a filter.
 */
export function keyValues(stream: TableDef, data: Record<string, unknown>): string[] | undefined {
  const out: string[] = []
  for (const k of stream.key) {
    const v = readPath(data, keyPath(stream, k))
    if (v == null || v === '' || typeof v === 'object') return undefined
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
    if (!existing || row.updatedAt >= existing.updatedAt) byKey.set(k, row)
  }
  return { rows: [...byKey.values()], dropped }
}

/**
 * Split a batch into waves in which no key appears twice, keeping order: the
 * n-th row for a key goes in wave n. Written one after another, the waves
 * are exactly the rows written one at a time — each row meets the staleness
 * gate against what the row before it left stored, and a merge overlays it
 * on that — while a batch with no repeated key, which is nearly every batch,
 * is still a single bulk write.
 *
 * An ordered bulk write already applies its operations one after another,
 * so the waves are not what makes this correct; they are what keeps the
 * write's shape the same as the Postgres destination's, where they are.
 *
 * Keeping the newest row, as `dedupeByKey` does, is wrong for a merge write:
 * a hydrated create (the whole REST object) followed in the same poll by an
 * update event whose payload is narrower would lose every field the event
 * omits until the next refresh, which is what hydration exists to prevent.
 * Folding the payloads here instead would be wrong the other way: a row
 * older than the stored one is refused on its own, but folded under a newer
 * row's timestamp its fields would overwrite the stored values. Only the
 * database knows the stored timestamp, so the database applies the gate.
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
  // A size that never advances the loop would grow `out` until the process
  // ran out of memory; a bad setting gets an error instead.
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`batchSize must be a positive integer, got ${String(size)}`)
  }
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

export interface WriteOptions {
  /** Rows per bulk write. Defaults to 500. */
  batchSize?: number
}

const DEFAULT_BATCH_SIZE = 500

/**
 * The filter that names one row: its environment and every key column at its
 * catalog path. Each value is an explicit `$eq`, so a payload whose key is an
 * object — `{ $ne: null }`, say — is compared as a value rather than read as
 * an operator that could match somebody else's row.
 */
export function keyFilter(stream: TableDef, env: string, data: Record<string, unknown>): Filter<MirrorDoc> {
  const filter: Record<string, unknown> = { _env: env }
  for (const k of stream.key) {
    const path = keyPath(stream, k)
    filter[fieldPath(path)] = { $eq: readPath(data, path) }
  }
  return filter as Filter<MirrorDoc>
}

type Op = AnyBulkWriteOperation<MirrorDoc>

/**
 * The gate, as an expression. True when the document is being inserted:
 * a missing `_updated_at` sorts below any date in BSON order.
 */
const gate = (at: Date) => ({ $gte: [at, '$_updated_at'] })
/** Shallow merge, matching Postgres's `jsonb || jsonb`: a field in `data` replaces the stored one whole. */
const merged = (data: Record<string, unknown>) => ({ $mergeObjects: ['$_raw', { $literal: data }] })
/** `_synced_at` moves to the server's clock when `when` holds, and is set on insert either way. */
const synced = (when: Document) => ({ $cond: [when, '$$NOW', { $ifNull: ['$_synced_at', '$$NOW'] }] })
/** Whether this operation is inserting rather than updating. */
const inserting = { $eq: [{ $type: '$_updated_at' }, 'missing'] }

function upsertOp(stream: TableDef, env: string, row: Row, mode: WriteMode): Op {
  const at = parseInstant(row.updatedAt)
  const filter = keyFilter(stream, env, row.data)

  if (stream.mode === 'append') {
    // Immutable log. Re-delivery is a no-op, which is the whole idempotency story.
    return {
      updateOne: {
        filter,
        update: [
          {
            $set: {
              _env: { $literal: env },
              _raw: { $cond: [inserting, { $literal: row.data }, '$_raw'] },
              _updated_at: { $ifNull: ['$_updated_at', at] },
              _synced_at: { $ifNull: ['$_synced_at', '$$NOW'] },
            },
          },
        ],
        upsert: true,
      },
    }
  }

  const newRaw = mode === 'merge' ? merged(row.data) : { $literal: row.data }
  const passes = gate(at)
  // An equal timestamp with an identical payload is the same observation
  // again; rewriting it would move `_synced_at` for nothing and report an
  // update that changed nothing. (`$ne` compares documents field by field in
  // order, so a payload that differs only in field order counts as changed.)
  const changed = { $and: [passes, { $or: [{ $gt: [at, '$_updated_at'] }, { $ne: [newRaw, '$_raw'] }] }] }
  return {
    updateOne: {
      filter,
      update: [
        {
          $set: {
            _env: { $literal: env },
            _raw: { $cond: [passes, newRaw, '$_raw'] },
            _updated_at: { $cond: [passes, at, '$_updated_at'] },
            _synced_at: synced(changed),
          },
        },
      ],
      upsert: true,
    },
  }
}

export async function upsertRows(
  coll: Collection<MirrorDoc>,
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
    for (const batch of chunk(wave, opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
      const ops = batch.map((row) => upsertOp(stream, env, row, mode))
      const r = await coll.bulkWrite(ops, { ordered: true })
      // A gate that refuses modifies nothing, so the counts fall out of the
      // result: inserted, changed, and the rest.
      stats.created += r.upsertedCount
      stats.updated += r.modifiedCount
      stats.skipped += batch.length - r.upsertedCount - r.modifiedCount
    }
  }
  return stats
}

export async function deleteRows(
  coll: Collection<MirrorDoc>,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped

  for (const batch of chunk(deduped, opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
    const ops: Op[] = batch.map((row) => {
      const at = parseInstant(row.updatedAt)
      // `$max`, not `$ifNull`, for `_deleted_at`: a membership removed,
      // re-added and removed again must carry the *latest* removal, because
      // that is what `relinkRows` compares a re-add against. `$max` ignores a
      // missing value, so a live row is tombstoned at the incoming time.
      return {
        updateOne: {
          filter: keyFilter(stream, env, row.data),
          update: [
            {
              $set: {
                _env: { $literal: env },
                _raw: merged(row.data),
                _updated_at: { $ifNull: ['$_updated_at', at] },
                _deleted_at: { $max: ['$_deleted_at', at] },
                _synced_at: '$$NOW',
              },
            },
          ],
          upsert: true,
        },
      }
    })
    const r = await coll.bulkWrite(ops, { ordered: true })
    stats.deleted += r.upsertedCount + r.matchedCount
  }
  return stats
}

/** Restore a tombstoned membership row (a re-add after a removal). */
export async function relinkRows(
  coll: Collection<MirrorDoc>,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped

  for (const batch of chunk(deduped, opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
    const ops: Op[] = batch.map((row) => {
      const at = parseInstant(row.updatedAt)
      // A membership is a fact that can legitimately come back: added, removed,
      // added again. Unlike object deletes, a re-link clears the tombstone —
      // but only a re-link at least as new as the removal. `_updated_at` alone
      // is not enough: a tombstone written by `deleteRows` (a reconcile repair)
      // leaves it untouched, so a link replayed from quarantine after that
      // removal would pass the staleness gate and resurrect the membership.
      const passes = {
        $and: [
          gate(at),
          { $or: [{ $eq: [{ $ifNull: ['$_deleted_at', null] }, null] }, { $gte: [at, '$_deleted_at'] }] },
        ],
      }
      return {
        updateOne: {
          filter: keyFilter(stream, env, row.data),
          update: [
            {
              $set: {
                _env: { $literal: env },
                _raw: { $cond: [passes, merged(row.data), '$_raw'] },
                _updated_at: { $cond: [passes, at, '$_updated_at'] },
                _deleted_at: { $cond: [passes, null, '$_deleted_at'] },
                _synced_at: synced(passes),
              },
            },
          ],
          upsert: true,
        },
      }
    })
    const r = await coll.bulkWrite(ops, { ordered: true })
    stats.created += r.upsertedCount
    stats.updated += r.modifiedCount
    stats.skipped += batch.length - r.upsertedCount - r.modifiedCount
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
  coll: Collection<MirrorDoc>,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  rows: Row[],
): Promise<WriteStats> {
  const stats = emptyStats()
  if (rows.length === 0) return stats
  const { rows: deduped, dropped } = dedupeByKey(stream, rows)
  stats.skipped += dropped

  for (const batch of chunk(deduped, opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
    const ops: Op[] = batch.map((row) => {
      const at = parseInstant(row.updatedAt)
      // `_updated_at` moves to the removal time, so the next re-add is gated
      // against the removal by the same comparison every membership write
      // uses. `_deleted_at` keeps the latest removal for the same reason
      // `deleteRows` does.
      const passes = gate(at)
      return {
        updateOne: {
          filter: keyFilter(stream, env, row.data),
          update: [
            {
              $set: {
                _env: { $literal: env },
                _raw: { $cond: [passes, merged(row.data), '$_raw'] },
                _updated_at: { $cond: [passes, at, '$_updated_at'] },
                _deleted_at: { $cond: [passes, { $max: ['$_deleted_at', at] }, '$_deleted_at'] },
                _synced_at: synced(passes),
              },
            },
          ],
          upsert: true,
        },
      }
    })
    const r = await coll.bulkWrite(ops, { ordered: true })
    const deleted = r.upsertedCount + r.modifiedCount
    stats.deleted += deleted
    stats.skipped += batch.length - deleted
  }
  return stats
}
