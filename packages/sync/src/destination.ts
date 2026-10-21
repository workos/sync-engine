/**
 * The destination contract.
 *
 * Everything the engine knows about WorkOS — the catalog, event routing, the
 * staleness rules, backfill ordering, reconciliation — lives in this package.
 * Everything it knows about a particular database lives behind this interface.
 * A new engine (Snowflake, SQLite, ...) is a new implementation of it; see
 * `@workos/pg-sync` for the Postgres one.
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
 */
import type { Logger, StreamDef, WriteStats } from './types.js'

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
}
