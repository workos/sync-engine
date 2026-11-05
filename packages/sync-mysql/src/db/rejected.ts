/**
 * The quarantine store: `_rejected`, one row per event the tail set aside.
 *
 * Keyed by `(sync_id, env, event_id)` like the rest of the bookkeeping, so
 * several syncs sharing a database never see each other's rejects. The whole
 * event is stored verbatim in `_raw`, which is what makes a replay possible
 * and is also why `redact` has to reach in here as well as into `events`.
 */
import type { RejectedEvent, WorkOSEvent } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { quoteIdent } from '../schema/ddl.js'
import { iso } from './lease.js'

export interface RejectedOptions {
  schema: string
  syncId: string
}

const table = (schema: string) => `${quoteIdent(schema)}.\`_rejected\``

/** The event's `created_at` as a `Date`, or null: a quarantine write must never fail on the data it is quarantining. */
function createdAt(value: unknown): Date | null {
  if (typeof value !== 'string') return null
  const d = new Date(value)
  return Number.isNaN(d.getTime()) ? null : d
}

export async function rejectEvents(
  db: SqlClient,
  opts: RejectedOptions,
  env: string,
  rows: { event: WorkOSEvent; error: string }[],
): Promise<void> {
  if (rows.length === 0) return
  const params: unknown[] = []
  const tuples = rows.map(({ event, error }) => {
    const objectId = (event.data as Record<string, unknown> | undefined)?.['id']
    params.push(
      opts.syncId,
      env,
      event.id,
      event.event,
      typeof objectId === 'string' ? objectId : null,
      createdAt(event.created_at),
      error,
      JSON.stringify(event),
    )
    return '(?, ?, ?, ?, ?, ?, ?, ?)'
  })
  // A replay that fails again lands here a second time: keep the latest error
  // and timestamp so `rejected` reports what is wrong now, not what was.
  await db.query(
    `insert into ${table(opts.schema)} (sync_id, env, event_id, event_type, object_id, created_at, error, \`_raw\`)
     values ${tuples.join(', ')} as new
     on duplicate key update
       error = new.error,
       rejected_at = now(6),
       \`_raw\` = new.\`_raw\``,
    params,
  )
}

export async function listRejected(
  db: SqlClient,
  opts: RejectedOptions,
  env: string,
  ids?: string[],
): Promise<RejectedEvent[]> {
  if (ids && ids.length === 0) return []
  const params: unknown[] = [opts.syncId, env]
  let filter = ''
  if (ids) {
    params.push(...ids)
    filter = ` and event_id in (${ids.map(() => '?').join(', ')})`
  }
  const r = await db.query<{
    event_id: string
    event_type: string
    object_id: string | null
    rejected_at: string
    error: string
    raw: WorkOSEvent
  }>(
    `select event_id, event_type, object_id, ${iso('rejected_at')} as rejected_at, error, \`_raw\` as raw
     from ${table(opts.schema)}
     where sync_id = ? and env = ?${filter}
     order by event_id`,
    params,
  )
  return r.rows.map((row) => ({
    eventId: row.event_id,
    eventType: row.event_type,
    objectId: row.object_id,
    // The event's own timestamp, as it arrived; the column is only for SQL.
    createdAt: row.raw.created_at,
    rejectedAt: row.rejected_at,
    error: row.error,
    raw: row.raw,
  }))
}

/** How many events are quarantined, without reading their payloads. */
export async function countRejected(db: SqlClient, opts: RejectedOptions, env: string): Promise<number> {
  const r = await db.query<{ n: number }>(
    `select count(*) as n from ${table(opts.schema)} where sync_id = ? and env = ?`,
    [opts.syncId, env],
  )
  return Number(r.rows[0]?.n ?? 0)
}

export async function clearRejected(db: SqlClient, opts: RejectedOptions, env: string, ids: string[]): Promise<number> {
  if (ids.length === 0) return 0
  const r = await db.query(
    `delete from ${table(opts.schema)} where sync_id = ? and env = ? and event_id in (${ids.map(() => '?').join(', ')})`,
    [opts.syncId, env, ...ids],
  )
  return r.affectedRows
}
