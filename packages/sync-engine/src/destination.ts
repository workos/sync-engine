/**
 * The destination contract.
 *
 * Everything the engine knows about WorkOS — the catalog, event routing, the
 * staleness rules, backfill ordering, reconciliation — lives in this package.
 * Everything it knows about a particular database lives behind this interface.
 * A new engine (Snowflake, SQLite, ...) is a new implementation of it; see
 * `@workos/sync-postgres` for the Postgres one.
 *
 * Implementations own the write semantics the engine depends on:
 *
 * - **Staleness gate.** An `upsert` must reject a row whose `updatedAt` is
 *   older than the stored one, and accept an equal one (`>=`, never `>`).
 * - **Merge vs replace.** `merge` shallow-merges the payload into the stored
 *   one; `replace` overwrites it. Event payloads are subsets of the REST
 *   objects, so event writes merge.
 * - **Deletion is terminal.** `delete` tombstones without a staleness check
 *   and never clears the tombstone; only `relink` (membership re-add) does.
 * - **Durable on return.** The engine checkpoints its cursor as soon as a
 *   write resolves, so a write must not be buffered past its own call.
 *
 * The `created`/`updated`/`skipped` split in `WriteStats` is a health metric
 * the engine logs but never branches on; an engine that cannot tell an insert
 * from an update may approximate it.
 *
 * Besides the mirror, a destination keeps a small **quarantine store** for
 * events the tail could not apply (`reject` / `rejected` / `clearRejected`).
 * It holds whole raw events, so it must be scrubbed by `redact` like the
 * `events` archive is. See `sync/quarantine.ts` for when it is written.
 */
import type { Logger, StreamDef, WorkOSEvent, WriteStats } from './types.js'

export interface Row {
  data: Record<string, unknown>
  /** Source-derived staleness timestamp. Never `now()` from the database. */
  updatedAt: string
}

export type WriteMode = 'merge' | 'replace'

export interface FreshnessRow {
  stream: string
  rows: number
  deleted: number
  lastSyncedAt: string | null
  /** Seconds since the most recent upstream change landed. */
  stalenessSeconds: number | null
  /**
   * Seconds between an object changing upstream and the mirror writing it,
   * averaged over objects that changed within the lag window.
   *
   * Deliberately restricted to recently-changed rows. Measured across every
   * row it would report the age of the objects rather than the delay of the
   * sync: a freshly backfilled mirror of year-old objects would show a lag of
   * months while being perfectly current. `null` means nothing changed
   * upstream in the window, so there is no delay to measure.
   */
  lagSeconds: number | null
  /** Rows the lag average is computed from. Zero means lagSeconds is null. */
  lagSampleSize: number
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

export interface Destination {
  /**
   * Connect if needed and create or upgrade the schema for `streams`. Called
   * before every engine operation, so it must be idempotent and cheap once
   * the schema is current.
   */
  init(streams: StreamDef[], logger?: Logger): Promise<void>
  close(): Promise<void>

  upsert(stream: StreamDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats>
  delete(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats>
  /** Restore tombstoned membership rows (a re-add after a removal). */
  relink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats>

  /** Primary key values of every non-tombstoned row, in a stable order. */
  liveIds(stream: StreamDef, env: string): Promise<string[]>
  /** Stored payloads of every row, tombstoned or not. */
  rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]>
  freshness(stream: StreamDef, env: string, lagWindowHours: number): Promise<FreshnessRow>
  /**
   * Erase every trace of one subject from the given streams and from the
   * event archive, which embeds payloads. Returns per-table counts.
   */
  redact(streams: StreamDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]>

  /** Cursor and run state, keyed by `(env, stream)`. */
  getState<T>(env: string, stream: string): Promise<T | null>
  setState(env: string, stream: string, state: unknown): Promise<void>

  /**
   * Set aside events the tail could not apply, so the cursor can move past
   * them. The whole raw event is stored because the archive write to `events`
   * may itself have been what failed. Rejecting an id that is already stored
   * (a replay that failed again) replaces its error and timestamp.
   */
  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void>
  /** Quarantined events, oldest first by event id; only `ids` when given. */
  rejected(env: string, opts?: { ids?: string[] }): Promise<RejectedEvent[]>
  /** Forget quarantined events once they have been replayed. Returns how many were removed. */
  clearRejected(env: string, ids: string[]): Promise<number>
  /**
   * Whether a write error is the database having a bad moment — a dropped
   * connection, a deadlock, a full disk — rather than a verdict on the row.
   * The engine never quarantines on an error this returns true for. Optional:
   * without it every error is presumed deterministic, and only the
   * two-sightings rule stands between a flaky connection and a quarantine.
   */
  isTransientError?(err: unknown): boolean
}
