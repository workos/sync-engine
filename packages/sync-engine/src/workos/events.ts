/**
 * The Events API tail.
 *
 * WorkOS recommends the Events API over webhooks for keeping a datastore in
 * sync, and the reasons are structural rather than stylistic: events come back
 * in a consistent order, the stream is replayable from a cursor, and your
 * process controls its own pace instead of absorbing whatever spike the
 * upstream sends. Webhooks stay useful as a low-latency nudge — see
 * `webhook.ts` — but state flows through here.
 *
 * Two constraints shape everything below:
 *   - Events are retained for **90 days**, and at most **30 days** can be
 *     queried per request. A cursor that falls behind retention cannot be
 *     recovered by replay; it needs a fresh backfill.
 *   - Event ids are ULIDs, so lexicographic id order is creation order. That
 *     makes the id a safe monotonic watermark.
 */
import type { WorkOSEvent, Logger } from '../types.js'
import type { WorkOSClient } from './client.js'

export const EVENT_RETENTION_DAYS = 90
/** Warn while there is still time to act, not once the cursor has expired. */
export const CURSOR_AGE_WARN_DAYS = 75

export interface FetchOptions {
  /** Cursor: the last event id processed. */
  after?: string | null
  /** Bootstrap position when there is no cursor yet. */
  rangeStart?: string
  limit?: number
  events?: string[]
  /**
   * Hold back events younger than this many ms.
   *
   * A change feed can be briefly non-monotonic: an event created moments ago
   * may not be visible when a page covering its timestamp is served, but will
   * be on a later replay. Sequin found exactly this on a comparable vendor's
   * events endpoint and mitigated it with a lag window. Reprocessing an event is free
   * here — writes are idempotent — while skipping one is permanent, so the
   * asymmetry justifies the delay.
   */
  lagWindowMs?: number
  now?: () => number
}

export class CursorDirectionError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'CursorDirectionError'
  }
}

export interface FetchResult {
  events: WorkOSEvent[]
  /** Cursor to persist once the events above are durably written. */
  cursor: string | null
  /** Events withheld by the lag window; they arrive on the next poll. */
  withheld: number
  hasMore: boolean
}

/**
 * Fetch one batch of events after the cursor.
 *
 * The direction of `after` under each `order` mode is genuinely ambiguous in
 * the WorkOS documentation, and getting it wrong would walk the sync backwards
 * through history while looking perfectly healthy. Rather than assume, we
 * assert: ids must sort strictly after the cursor. If they do not, we stop with
 * an explicit error instead of quietly rewriting the mirror with stale state.
 */
export async function fetchEvents(
  client: WorkOSClient,
  opts: FetchOptions = {},
  logger?: Logger,
): Promise<FetchResult> {
  const params: Record<string, string> = {
    limit: String(opts.limit ?? 100),
    order: 'asc',
  }
  if (opts.after) params['after'] = opts.after
  else if (opts.rangeStart) params['range_start'] = opts.rangeStart
  if (opts.events?.length) params['events'] = opts.events.join(',')

  const page = await client.withPhase('tail').list<WorkOSEvent>('/events', params, 'global')
  const rows = page.data

  if (opts.after) {
    const backwards = rows.filter((e) => e.id <= opts.after!)
    if (backwards.length > 0 && backwards.length === rows.length) {
      throw new CursorDirectionError(
        `Every event returned after cursor ${opts.after} sorts before it. ` +
          `The 'after' parameter appears to page backwards under order=asc, ` +
          `which would replay history instead of tailing. Refusing to advance the cursor.`,
      )
    }
  }

  const now = opts.now ? opts.now() : Date.now()
  const lag = opts.lagWindowMs ?? 5_000
  const cutoff = now - lag

  const fresh: WorkOSEvent[] = []
  let withheld = 0
  for (const e of rows) {
    const created = Date.parse(e.created_at)
    if (Number.isFinite(created) && created > cutoff) {
      withheld++
      continue
    }
    fresh.push(e)
  }

  // Never advance past a withheld event: the cursor is the last event we are
  // actually prepared to have processed.
  const cursor = fresh.length > 0 ? fresh[fresh.length - 1]!.id : (opts.after ?? null)

  if (withheld > 0) {
    logger?.debug('holding back events inside lag window', { withheld, lagWindowMs: lag })
  }

  return {
    events: fresh,
    cursor,
    withheld,
    hasMore: Boolean(page.listMetadata.after) && withheld === 0 && rows.length > 0,
  }
}

/** Age of a cursor's last event, for the retention-cliff alarm. */
export function cursorAgeDays(lastEventAt: string | null | undefined, now = Date.now()): number | null {
  if (!lastEventAt) return null
  const t = Date.parse(lastEventAt)
  if (!Number.isFinite(t)) return null
  return (now - t) / 86_400_000
}

export function checkCursorAge(lastEventAt: string | null | undefined, logger?: Logger, now = Date.now()): void {
  const age = cursorAgeDays(lastEventAt, now)
  if (age === null) return
  if (age >= EVENT_RETENTION_DAYS) {
    logger?.error('events cursor is older than the retention window; a full backfill is required', {
      ageDays: Math.round(age),
      retentionDays: EVENT_RETENTION_DAYS,
    })
  } else if (age >= CURSOR_AGE_WARN_DAYS) {
    logger?.warn('events cursor is approaching the retention window', {
      ageDays: Math.round(age),
      retentionDays: EVENT_RETENTION_DAYS,
    })
  }
}
