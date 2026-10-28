/**
 * The bookkeeping store: cursors, the single-writer lease, and the quarantine.
 *
 * These are the engine's own records, not mirror data, and they do not have
 * to live where the mirror does. By default they do: `PostgresDestination`
 * keeps a store on its own connection and offers it as
 * `destination.bookkeeping`, so a cursor checkpoint and the rows it covers
 * share one database and the single-database deployment needs no extra
 * configuration. A destination that cannot make an atomic conditional write —
 * a spreadsheet, an object store, a queue — offers no store, is paired with
 * one through `SyncEngineConfig.bookkeeping`, and is then only ever asked to
 * write rows.
 *
 * Three properties the engine depends on:
 *
 * - **Durable on return.** The engine treats a resolved `setState` as a
 *   checkpoint, so a write must not be buffered past its own call.
 * - **`acquireLease` is atomic.** Of two processes asking at once, exactly
 *   one is told it holds the lease. The events tail runs under it, so a
 *   rolling deploy cannot have both the old and the new process advancing
 *   the same cursor. Expiry is judged by the store's clock, never the
 *   caller's, so hosts with skewed clocks still agree on who holds it.
 * - **The checkpoint is fenced.** `setStateIfHeld` writes only while `holder`
 *   holds the unexpired lease and the stored state is still the one the poll
 *   started from, both checked atomically with the write. A process that
 *   stalled past its lease is refused by the store itself, not by a check
 *   made a moment earlier.
 */
import type { Logger, WorkOSEvent } from './types.js'

/** Who holds the events-tail lease for one environment, and until when (ISO-8601). */
export interface Lease {
  holder: string
  expiresAt: string
}

export interface LeaseResult {
  /** True when `holder` now holds the lease, whether freshly taken or renewed. */
  acquired: boolean
  /**
   * The current holder. When `acquired` is false this names the process to
   * wait for; it is null if the lease was released between the attempt and
   * the read, in which case the next attempt will succeed.
   */
  holder: string | null
  expiresAt: string | null
}

/** One quarantined event, as `rejected` reports it. */
export interface RejectedEvent {
  eventId: string
  eventType: string
  /** `data.id` when the payload has one; membership events do not. */
  objectId: string | null
  /** The event's own `created_at`. */
  createdAt: string
  rejectedAt: string
  /** The error from the most recent attempt to apply it. */
  error: string
  /** The whole event, verbatim, so it can be replayed. */
  raw: WorkOSEvent
}

export interface Bookkeeping {
  /**
   * Connect if needed and create whatever the store keeps its records in.
   * Called before every engine operation, so it must be idempotent and cheap
   * once the store is ready.
   */
  init(logger?: Logger): Promise<void>
  close(): Promise<void>

  /** Cursor and run state, keyed by `(env, stream)`. */
  getState<T>(env: string, stream: string): Promise<T | null>
  setState(env: string, stream: string, state: unknown): Promise<void>
  /**
   * Write `state` only if `holder` holds the unexpired lease for `env` and
   * the stored state for `(env, stream)` still equals `expected` (`null` for
   * no row), with both conditions checked atomically with the write.
   * Returns whether it was written. The engine checkpoints the tail through
   * this, so a former holder saving an older cursor, or a poll racing a
   * backfill that re-captured the position, is refused by the store itself:
   * there is no window between the check and the write.
   */
  setStateIfHeld(env: string, stream: string, state: unknown, holder: string, expected: unknown): Promise<boolean>

  /**
   * Take or renew the single-writer lease for `env`, atomically. Succeeds when
   * the lease is free, expired, or already held by `holder`; otherwise it is
   * left alone and the result names who holds it. A lease is scoped to the
   * store's sync identifier, so two syncs sharing a database never contend.
   * It is held in the store, not a connection, so it survives pool churn and
   * needs no pinned session.
   */
  acquireLease(env: string, holder: string, ttlMs: number): Promise<LeaseResult>
  /** Give the lease back, only if `holder` holds it. A no-op for anyone else. */
  releaseLease(env: string, holder: string): Promise<void>
  /** The current, unexpired lease for `env`, or null when it is free. Read-only. */
  getLease(env: string): Promise<Lease | null>

  /**
   * Set aside events the tail could not apply, so the cursor can move past
   * them. The whole raw event is stored because the archive write to `events`
   * may itself have been what failed. Rejecting an id that is already stored
   * (a replay that failed again) replaces its error and timestamp.
   */
  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void>
  /** Quarantined events, oldest first by event id; only `ids` when given. */
  rejected(env: string, opts?: { ids?: string[] }): Promise<RejectedEvent[]>
  /**
   * How many events are quarantined. `status` calls this on every monitoring
   * poll, so it must not fetch the payloads the way `rejected` does.
   */
  rejectedCount(env: string): Promise<number>
  /** Forget quarantined events once they have been replayed. Returns how many were removed. */
  clearRejected(env: string, ids: string[]): Promise<number>

  /**
   * Erase one subject from the quarantine. It embeds whole events and may
   * hold the only copy of one, so `Destination.redact` alone is not enough.
   * Returns per-table counts in the same shape, for one combined report.
   */
  redact(env: string, userId: string): Promise<{ table: string; rows: number }[]>
}
