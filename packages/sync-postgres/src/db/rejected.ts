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

export interface RejectedOptions {
  schema: string
  syncId: string
}

const table = (schema: string) => `${quoteIdent(schema)}."_rejected"`

export async function rejectEvents(
  db: SqlClient,
  opts: RejectedOptions,
  env: string,
  rows: { event: WorkOSEvent; error: string }[],
): Promise<void> {
  if (rows.length === 0) return
  const params: unknown[] = []
  const tuples = rows.map(({ event, error }) => {
    const i = params.length
    const objectId = (event.data as Record<string, unknown> | undefined)?.['id']
    params.push(
      opts.syncId,
      env,
      event.id,
      event.event,
      typeof objectId === 'string' ? objectId : null,
      event.created_at ?? null,
      error,
      JSON.stringify(event),
    )
    // `created_at` goes through parse_iso8601 rather than a `::timestamptz`
    // cast: that cast is one of the ways an event ends up here in the first
    // place, and the quarantine write must not fail on the data it is
    // quarantining. Unparseable input stores NULL.
    return `($${i + 1}, $${i + 2}, $${i + 3}, $${i + 4}, $${i + 5}, ${quoteIdent(opts.schema)}.parse_iso8601($${i + 6}), $${i + 7}, $${i + 8}::jsonb)`
  })
  // A replay that fails again lands here a second time: keep the latest error
  // and timestamp so `rejected` reports what is wrong now, not what was.
  await db.query(
    `insert into ${table(opts.schema)} (sync_id, env, event_id, event_type, object_id, created_at, error, "_raw")
     values ${tuples.join(', ')}
     on conflict (sync_id, env, event_id) do update set
       error = excluded.error,
       rejected_at = now(),
       "_raw" = excluded."_raw"`,
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
    // An IN list rather than `= any($3::text[])`: both drivers this package
    // runs on accept it without array-parameter serialization rules.
    filter = ` and event_id in (${ids.map((id) => `$${params.push(id)}`).join(', ')})`
  }
  const r = await db.query<{
    event_id: string
    event_type: string
    object_id: string | null
    rejected_at: string
    error: string
    raw: WorkOSEvent
  }>(
    // to_json gives ISO-8601 with an offset whatever the session's DateStyle;
    // `::text` would not.
    `select event_id, event_type, object_id,
            to_json(rejected_at) #>> '{}' as rejected_at,
            error, "_raw" as raw
     from ${table(opts.schema)}
     where sync_id = $1 and env = $2${filter}
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

export async function clearRejected(
  db: SqlClient,
  opts: RejectedOptions,
  env: string,
  ids: string[],
): Promise<number> {
  if (ids.length === 0) return 0
  const params: unknown[] = [opts.syncId, env]
  const list = ids.map((id) => `$${params.push(id)}`).join(', ')
  const r = await db.query(
    `delete from ${table(opts.schema)}
     where sync_id = $1 and env = $2 and event_id in (${list}) returning 1`,
    params,
  )
  return r.rows.length
}
