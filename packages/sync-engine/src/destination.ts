/**
 * The destination contract.
 *
 * Everything the engine knows about WorkOS — the catalog, event routing, the
 * staleness rules, backfill ordering, reconciliation — lives in this package.
 * Everything it knows about a particular database lives behind this interface.
 * A new engine (Snowflake, SQLite, ...) is a new implementation of it; see
 * `@workos/sync-postgres` for the Postgres one.
 *
 * Streams cross this boundary as `TableDef`s: name, key, mode, columns,
 * group. The WorkOS half of a stream — endpoints, events, hydration,
 * credential paths — stays on `StreamDef` and never reaches a destination,
 * so a destination cannot come to depend on it.
 *
 * Implementations own the write semantics the engine depends on:
 *
 * - **Staleness gate.** An `upsert` must reject a row whose `updatedAt` is
 *   older than the stored one, and accept an equal one (`>=`, never `>`).
 * - **Merge vs replace.** `merge` shallow-merges the payload into the stored
 *   one; `replace` overwrites it. Event payloads are subsets of the REST
 *   objects, so event writes merge.
 * - **Deletion is terminal.** `delete` tombstones without a staleness check
 *   and never clears the tombstone. Memberships are the exception, in both
 *   directions and only when the write is at least as new as the row:
 *   `relink` (a re-add) clears the tombstone and `unlink` (a removal) sets
 *   it. Events can be replayed from quarantine after newer ones have landed,
 *   and an older link or unlink must not undo what a newer one established.
 * - **Durable on return.** The engine checkpoints its cursor as soon as a
 *   write resolves, so a write must not be buffered past its own call.
 * - **One writer per cursor.** `acquireLease` is atomic: of two processes
 *   asking at once, exactly one is told it holds the lease. The tail runs
 *   under it so a rolling deploy cannot have both the old and the new process
 *   advancing the same cursor. `setStateIfHeld` makes the cursor write itself
 *   atomically conditional on still holding it, so a process that
 *   stalled past its lease is refused by the database rather than by a check
 *   made a moment earlier.
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
import type { Logger, TableDef, WorkOSEvent, WriteStats } from './types.js'

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

/** The reconcile set difference for one stream. See `Destination.diffIds`. */
export interface IdDiff {
  /** Live rows whose id is also upstream. */
  matched: number
  /** Keys of live rows absent upstream, encoded like `liveIds`; only ever tombstoned. */
  localOnly: string[]
  /** Upstream payloads with no live row. Whole payloads: they are re-inserted. */
  remoteOnly: Record<string, unknown>[]
}

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

export interface Destination {
  /**
   * Connect if needed and create or upgrade the schema for `streams`. Called
   * before every engine operation, so it must be idempotent and cheap once
   * the schema is current.
   */
  init(streams: TableDef[], logger?: Logger): Promise<void>
  close(): Promise<void>

  upsert(stream: TableDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats>
  delete(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>
  /** Restore tombstoned membership rows (a re-add after a removal). */
  relink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>
  /**
   * Tombstone membership rows (a removal). Unlike `delete`, this is
   * staleness-gated: a removal older than the row is a no-op, because a
   * quarantined removal can be replayed after a newer re-add has landed.
   */
  unlink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>

  /**
   * Complete keys of non-tombstoned rows, in a stable order. Single-column
   * keys are plain strings; composite keys are JSON arrays of strings in
   * `stream.key` order, serialized with `encodeKey`.
   */
  liveIds(stream: TableDef, env: string): Promise<string[]>
  /** Stored payloads of every row, tombstoned or not. */
  rawRows(stream: TableDef, env: string): Promise<Record<string, unknown>[]>
  /**
   * The set difference between the live rows and the stream's list endpoint,
   * for reconciliation. `remote` yields the endpoint one page at a time. The
   * implementation must stage those pages in the database and compute the
   * difference there, so memory is bounded by the size of the drift and not
   * by the size of the stream; a million-row stream with ten missed deletes
   * must cost ten ids. "Live" means non-tombstoned. `localOnly` contains
   * complete keys encoded like `liveIds`, because they are only tombstoned;
   * `remoteOnly` contains whole payloads, because they are re-inserted.
   * A destination with nothing to stage into
   * may delegate to `diffIdsInMemory` and accept the memory cost.
   *
   * The live set must be captured before the first page is consumed. Listing
   * takes time and the tail keeps writing throughout, so a row inserted
   * mid-listing that the pages never show must not become `localOnly`: repair
   * would tombstone an object that exists upstream, and deletion is terminal.
   * The engine reads nothing from the destination while this runs, so an
   * implementation may hold one pinned connection for the duration.
   */
  diffIds(stream: TableDef, env: string, remote: AsyncIterable<Record<string, unknown>[]>): Promise<IdDiff>
  freshness(stream: TableDef, env: string, lagWindowHours: number): Promise<FreshnessRow>
  /**
   * Erase every trace of one subject from the given streams and from the
   * event archive, which embeds payloads. Returns per-table counts.
   */
  redact(streams: TableDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]>

  /** Cursor and run state, keyed by `(env, stream)`. */
  getState<T>(env: string, stream: string): Promise<T | null>
  setState(env: string, stream: string, state: unknown): Promise<void>
  /**
   * Write `state` only if `holder` holds the unexpired lease for `env` and
   * the stored state for `(env, stream)` still equals `expected` (`null` for
   * no row), with both conditions checked atomically with the write.
   * Returns whether it was written. The engine checkpoints the tail through
   * this, so a former holder saving an older cursor, or a poll
   * racing a backfill that re-captured the position, is refused by the
   * database itself: there is no window between the check and the write.
   */
  setStateIfHeld(env: string, stream: string, state: unknown, holder: string, expected: unknown): Promise<boolean>

  /**
   * Take or renew the single-writer lease for `env`, atomically. Succeeds when
   * the lease is free, expired, or already held by `holder`; otherwise it is
   * left alone and the result names who holds it. A lease is scoped to this
   * destination's sync identifier, so two syncs sharing a database never
   * contend. It is held in the database, not the connection, so it survives
   * pool churn and needs no pinned session.
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
   * Whether a write error is the database having a bad moment — a dropped
   * connection, a deadlock, a full disk — rather than a verdict on the row.
   * The engine never quarantines on an error this returns true for. Optional:
   * without it every error is presumed deterministic, and only the
   * two-sightings rule stands between a flaky connection and a quarantine.
   */
  isTransientError?(err: unknown): boolean
}
