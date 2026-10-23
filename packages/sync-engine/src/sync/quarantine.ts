/**
 * Poison-event quarantine.
 *
 * There is one cursor per environment. When one event fails deterministically
 * — an `updated_at` string the `::timestamptz` cast rejects, a generated
 * column that cannot cast one payload's value, a router bug on a payload shape
 * nobody has seen before — the tick fails, the cursor never moves, and every
 * stream in that environment stops. Sign-in capture stops with it, and that
 * history exists nowhere else, while the 90-day retention clock keeps running.
 * Before this file the only remedy was hand-editing `_sync_state`.
 *
 * The engine's asymmetry rule — reprocessing is free, skipping is permanent —
 * makes the design conservative in three ways:
 *
 *   1. Only a *deterministic* failure is ever set aside. Anything that could
 *      be the network, the API or the database having a bad moment is
 *      rethrown so the tick fails and retries exactly as it always has.
 *   2. An event is quarantined only after it fails on two consecutive polls
 *      (the two-sightings rule), which costs one poll interval and buys
 *      protection against transient errors the classifier does not know.
 *   3. Quarantine is reversible: the whole raw event is kept in the
 *      destination's rejected store, and `SyncEngine.replayRejected` puts it
 *      back through the normal write path once the cause is fixed.
 */
import type { Destination } from '../destination.js'
import { applyEvents, HydrationError, type ApplyOptions, type ApplyResult } from './apply.js'
import { WorkOSApiError } from '../workos/client.js'
import type { WorkOSEvent } from '../types.js'
import { addStats, emptyStats } from '../types.js'

/** An event that failed on the previous poll and will be quarantined if it fails again. */
export interface Suspect {
  error: string
  seenAt: string
}

/** Per-environment memory of first sightings. Lives on the engine, not in the database. */
export type SuspectMap = Map<string, Suspect>

export interface RejectedRow {
  event: WorkOSEvent
  error: string
}

export interface QuarantineResult extends ApplyResult {
  /** Events set aside this tick. The cursor is checkpointed past them. */
  rejected: RejectedRow[]
}

/**
 * Whether an error could plausibly succeed on retry. Everything the engine
 * cannot classify is treated as deterministic, which is why the two-sightings
 * rule exists as a second line of defence.
 */
export function isTransientError(err: unknown, destination: Destination): boolean {
  // A hydration GET that fails is the API, not the event: the client already
  // retried 429s and 5xx, so what reaches here is an outage or a bug upstream,
  // and neither is a reason to skip the event.
  if (err instanceof HydrationError) return true
  if (err instanceof WorkOSApiError) return true
  return destination.isTransientError?.(err) === true
}

/**
 * `applyEvents`, with the failure path above. The happy path is unchanged:
 * one batch, one write per stream, one checkpoint.
 */
export async function applyWithQuarantine(
  destination: Destination,
  events: WorkOSEvent[],
  opts: ApplyOptions,
  suspects: SuspectMap,
): Promise<QuarantineResult> {
  let batchError: unknown
  try {
    const result = await applyEvents(destination, events, opts)
    // Whatever was suspected in this batch has just been written: forget it.
    for (const e of events) suspects.delete(e.id)
    return { ...result, rejected: [] }
  } catch (err) {
    if (isTransientError(err, destination)) throw err
    batchError = err
  }

  // Isolate: apply the events one at a time, in order. Re-applying the ones
  // that already landed in the batch attempt is free — writes are idempotent
  // and the `>=` staleness gate makes a second identical write a no-op — so
  // nothing is lost by replaying the whole batch at single-event granularity.
  let survivors: ApplyResult | undefined
  const failed: RejectedRow[] = []
  for (const event of events) {
    try {
      const one = await applyEvents(destination, [event], opts)
      survivors = survivors ? merge(survivors, one) : one
    } catch (err) {
      // The isolation pass can hit a transient error too, and then nothing
      // it has learned about this batch can be trusted. Retry next poll.
      if (isTransientError(err, destination)) throw err
      failed.push({ event, error: String(err) })
    }
  }

  if (failed.length === 0) {
    // Every event succeeds alone, so the batch failure was not any one event's
    // fault: an interaction between rows, or a transient the classifier
    // missed. Either way, nothing to quarantine; retry the batch next poll.
    throw batchError
  }
  if (failed.length === events.length && events.length > 1) {
    // One poison event does not take a whole batch down with it. If they all
    // fail, the database or the schema is the problem, and quarantining the
    // lot would be data loss dressed up as resilience.
    throw batchError
  }

  // Two sightings. An event is only quarantined once it has failed on two
  // consecutive polls: the first failure is recorded in memory and the tick
  // fails, leaving the cursor where it was, so the next poll refetches the
  // same batch and either confirms the failure or watches it succeed. That
  // costs one poll interval per poison event and buys protection against
  // every transient error `isTransientError` does not recognise, which is
  // worth it when skipping an event is permanent and reprocessing is free.
  const now = new Date().toISOString()
  const firstSightings: RejectedRow[] = []
  const confirmed: RejectedRow[] = []
  for (const row of failed) {
    if (suspects.has(row.event.id)) confirmed.push(row)
    else firstSightings.push(row)
    suspects.set(row.event.id, { error: row.error, seenAt: now })
  }
  // Suspects that did not fail this time were transient after all.
  const failedIds = new Set(failed.map((r) => r.event.id))
  for (const e of events) if (!failedIds.has(e.id)) suspects.delete(e.id)

  if (firstSightings.length > 0) {
    for (const row of firstSightings) {
      opts.logger?.warn('event failed; will be quarantined if it fails again on the next poll', {
        env: opts.env,
        eventId: row.event.id,
        eventType: row.event.event,
        objectId: eventObjectId(row.event),
        error: row.error,
      })
    }
    throw new PoisonEventSuspectedError(firstSightings)
  }

  await destination.reject(opts.env, confirmed)
  for (const row of confirmed) suspects.delete(row.event.id)
  return { ...(survivors ?? emptyResult()), rejected: confirmed }
}

/** Thrown on a first sighting so the tick fails and the cursor stays put. */
export class PoisonEventSuspectedError extends Error {
  constructor(readonly suspects: RejectedRow[]) {
    super(
      `${suspects.length} event(s) failed deterministically and will be quarantined if they fail ` +
        `again on the next poll: ${suspects.map((s) => `${s.event.id} (${s.error})`).join('; ')}`,
    )
    this.name = 'PoisonEventSuspectedError'
  }
}

/** Best-effort object id for logging; many payloads carry one, membership events do not. */
export function eventObjectId(event: WorkOSEvent): string | null {
  const id = (event.data as Record<string, unknown> | undefined)?.['id']
  return typeof id === 'string' ? id : null
}

function emptyResult(): ApplyResult {
  return { stats: emptyStats(), byStream: {}, archived: 0, unknownEvents: [], hydrated: 0 }
}

function merge(a: ApplyResult, b: ApplyResult): ApplyResult {
  const byStream = { ...a.byStream }
  for (const [stream, stats] of Object.entries(b.byStream)) {
    byStream[stream] = byStream[stream] ? addStats(byStream[stream]!, stats) : stats
  }
  return {
    stats: addStats(a.stats, b.stats),
    byStream,
    archived: a.archived + b.archived,
    unknownEvents: [...new Set([...a.unknownEvents, ...b.unknownEvents])],
    hydrated: a.hydrated + b.hydrated,
  }
}
