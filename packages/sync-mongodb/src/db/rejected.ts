/**
 * The quarantine store: `_rejected`, one document per event the tail set aside.
 *
 * Keyed by `(sync_id, env, event_id)` like the rest of the bookkeeping, so
 * several syncs sharing a database never see each other's rejects. The whole
 * event is stored verbatim in `_raw`, which is what makes a replay possible
 * and is also why `redact` has to reach in here as well as into `events`.
 */
import type { RejectedEvent, WorkOSEvent } from '@workos/sync-engine'
import type { Collection, Document } from 'mongodb'
import { parseInstantOrNull } from './timestamps.js'

export interface RejectedDoc extends Document {
  sync_id: string
  env: string
  event_id: string
  event_type: string
  object_id: string | null
  /** The event's own `created_at`, as a date when it is one; null otherwise. */
  created_at: Date | null
  rejected_at: Date
  /** The error from the most recent attempt to apply it. */
  error: string
  _raw: WorkOSEvent
}

export interface RejectedOptions {
  syncId: string
}

export async function rejectEvents(
  coll: Collection<RejectedDoc>,
  opts: RejectedOptions,
  env: string,
  rows: { event: WorkOSEvent; error: string }[],
): Promise<void> {
  if (rows.length === 0) return
  await coll.bulkWrite(
    rows.map(({ event, error }) => {
      const objectId = (event.data as Record<string, unknown> | undefined)?.['id']
      return {
        updateOne: {
          filter: { sync_id: opts.syncId, env, event_id: event.id },
          // A replay that fails again lands here a second time: keep the
          // latest error and timestamp so `rejected` reports what is wrong
          // now, not what was. `created_at` is parsed leniently rather than
          // strictly: an unparseable timestamp is one of the ways an event
          // ends up here, and the quarantine write must not fail on the data
          // it is quarantining. Unparseable input stores null.
          update: {
            $set: {
              event_type: event.event,
              object_id: typeof objectId === 'string' ? objectId : null,
              created_at: parseInstantOrNull(event.created_at),
              error,
              _raw: event,
            },
            $currentDate: { rejected_at: true },
            $setOnInsert: { sync_id: opts.syncId, env, event_id: event.id },
          },
          upsert: true,
        },
      }
    }),
    { ordered: true },
  )
}

export async function listRejected(
  coll: Collection<RejectedDoc>,
  opts: RejectedOptions,
  env: string,
  ids?: string[],
): Promise<RejectedEvent[]> {
  if (ids && ids.length === 0) return []
  const docs = await coll
    .find({ sync_id: opts.syncId, env, ...(ids ? { event_id: { $in: ids } } : {}) })
    .sort({ event_id: 1 })
    .toArray()
  return docs.map((doc) => ({
    eventId: doc.event_id,
    eventType: doc.event_type,
    objectId: doc.object_id,
    // The event's own timestamp, as it arrived; the date field is only for queries.
    createdAt: doc._raw.created_at,
    rejectedAt: doc.rejected_at.toISOString(),
    error: doc.error,
    raw: doc._raw,
  }))
}

/** How many events are quarantined, without reading their payloads. */
export function countRejected(coll: Collection<RejectedDoc>, opts: RejectedOptions, env: string): Promise<number> {
  return coll.countDocuments({ sync_id: opts.syncId, env })
}

export async function clearRejected(
  coll: Collection<RejectedDoc>,
  opts: RejectedOptions,
  env: string,
  ids: string[],
): Promise<number> {
  if (ids.length === 0) return 0
  const r = await coll.deleteMany({ sync_id: opts.syncId, env, event_id: { $in: ids } })
  return r.deletedCount
}
