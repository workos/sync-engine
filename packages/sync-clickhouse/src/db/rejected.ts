/**
 * The quarantine store: `_rejected`, one row per event the tail set aside.
 *
 * Keyed by `(sync_id, env, event_id)` like the rest of the bookkeeping, so
 * several syncs sharing a database never see each other's rejects. The whole
 * event is stored verbatim in `_raw`, which is what makes a replay possible
 * and is also why `redact` has to reach in here as well as into `events`.
 *
 * The table is a `ReplacingMergeTree` versioned by `rejected_at`, so a
 * rejection written again is a new version that wins, and reads use
 * `FINAL`. Forgetting an event is a lightweight `DELETE`, which the
 * client's `mutations_sync` makes visible before the call returns.
 */
import type { RejectedEvent, WorkOSEvent } from '@workos/sync-engine'
import { isoOf, toCh, type ChClient } from './client.js'
import { quoteIdent } from '../schema/ddl.js'

export interface RejectedOptions {
  schema: string
  syncId: string
}

const table = (schema: string) => `${quoteIdent(schema)}.\`_rejected\``

/** The event's `created_at` as the server stores it, or null: a quarantine write must never fail on the data it is quarantining. */
function createdAt(value: unknown): string | null {
  if (typeof value !== 'string') return null
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? null : toCh(d)
}

export async function rejectEvents(
  db: ChClient,
  opts: RejectedOptions,
  env: string,
  rows: { event: WorkOSEvent; error: string }[],
): Promise<void> {
  if (rows.length === 0) return
  // A replay that fails again lands here a second time, as a newer version:
  // `rejected` then reports what is wrong now, not what was.
  await db.insert(
    table(opts.schema),
    rows.map(({ event, error }) => {
      const objectId = (event.data as Record<string, unknown> | undefined)?.['id']
      return {
        sync_id: opts.syncId,
        env,
        event_id: event.id,
        event_type: event.event,
        object_id: typeof objectId === 'string' ? objectId : null,
        created_at: createdAt(event.created_at),
        error,
        _raw: JSON.stringify(event),
      }
    }),
  )
}

export async function listRejected(
  db: ChClient,
  opts: RejectedOptions,
  env: string,
  ids?: string[],
): Promise<RejectedEvent[]> {
  if (ids && ids.length === 0) return []
  const rows = await db.query<{
    event_id: string
    event_type: string
    object_id: string | null
    rejected_at: string
    error: string
    raw: string
  }>(
    `SELECT event_id, event_type, object_id, rejected_at, error, \`_raw\` AS raw
     FROM ${table(opts.schema)} FINAL
     WHERE sync_id = {syncId:String} AND env = {env:String}${ids ? ' AND event_id IN ({ids:Array(String)})' : ''}
     ORDER BY event_id`,
    { syncId: opts.syncId, env, ids: ids ?? [] },
  )
  return rows.map((row) => {
    const raw = JSON.parse(row.raw) as WorkOSEvent
    return {
      eventId: row.event_id,
      eventType: row.event_type,
      objectId: row.object_id,
      // The event's own timestamp, as it arrived; the column is only for SQL.
      createdAt: raw.created_at,
      rejectedAt: isoOf(row.rejected_at),
      error: row.error,
      raw,
    }
  })
}

/** How many events are quarantined, without reading their payloads. */
export async function countRejected(db: ChClient, opts: RejectedOptions, env: string): Promise<number> {
  const rows = await db.query<{ n: string }>(
    `SELECT toString(count()) AS n FROM ${table(opts.schema)} FINAL WHERE sync_id = {syncId:String} AND env = {env:String}`,
    { syncId: opts.syncId, env },
  )
  return Number(rows[0]?.n ?? 0)
}

export async function clearRejected(db: ChClient, opts: RejectedOptions, env: string, ids: string[]): Promise<number> {
  if (ids.length === 0) return 0
  const params = { syncId: opts.syncId, env, ids }
  const where = `sync_id = {syncId:String} AND env = {env:String} AND event_id IN ({ids:Array(String)})`
  // A delete reports no count, so count first; the quarantine has one writer.
  const rows = await db.query<{ n: string }>(
    `SELECT toString(count(DISTINCT event_id)) AS n FROM ${table(opts.schema)} FINAL WHERE ${where}`,
    params,
  )
  const n = Number(rows[0]?.n ?? 0)
  if (n > 0) await db.command(`DELETE FROM ${table(opts.schema)} WHERE ${where}`, params)
  return n
}
