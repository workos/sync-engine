/**
 * Core types for the WorkOS sync engine.
 */
import type { Destination } from './destination.js'

/** Column types we project out of the raw JSONB payload. */
export type ColumnType = 'text' | 'boolean' | 'bigint' | 'numeric' | 'jsonb' | 'timestamptz'

export interface ColumnDef {
  /** Column name in Postgres. */
  name: string
  type: ColumnType
  /**
   * Path into the raw payload. A string is a top-level key; an array walks
   * nested objects (e.g. `['role', 'slug']` for `role.slug`).
   */
  path: string | string[]
  /** Create a btree index on this column. */
  index?: boolean
}

/**
 * How a stream is kept up to date.
 *
 * - `mirror`  — upserted current state, driven by events and repaired by backfill/reconcile.
 * - `append`  — immutable log rows; never updated, never deleted (auth attempts, raw events).
 * - `poll`    — no events exist upstream, so the only path is a periodic full re-list.
 */
export type StreamMode = 'mirror' | 'append' | 'poll'

export interface ListEndpoint {
  /** Path template. `{organization_id}` etc. are filled from the parent stream. */
  path: string
  /** Extra query params sent on every page. */
  params?: Record<string, string>
  /**
   * Stream this endpoint must be iterated over (N+1). The engine lists the
   * parent first and then calls this endpoint once per parent row.
   */
  parent?: { stream: string; param: string; from?: string }
  /**
   * Rate-limit bucket. Defaults to `global`. `directory` uses the per-directory
   * 4 req/s limit documented for /directory_users.
   */
  rateKey?: 'global' | 'user_management' | 'directory'
  /**
   * Response shape. WorkOS list endpoints normally return
   * `{data, list_metadata}`; a few return a bare array.
   */
  shape?: 'list' | 'array'
}

export interface StreamEvents {
  /** Events that create or update the object (upsert). */
  upsert?: string[]
  /** Events that delete the object (tombstone). */
  delete?: string[]
}

export interface StreamDef {
  /** Stream name, also the Postgres table name. */
  name: string
  /** Human description, used in docs and `status` output. */
  description: string
  mode: StreamMode
  /**
   * Natural key columns (in addition to `_env`). Most objects use `['id']`;
   * slug-keyed objects (roles, permissions, flags) have no `id` in their event
   * payloads and must key on slug.
   */
  key: string[]
  /** ID prefixes that identify this object type, for prefix dispatch. */
  prefixes?: string[]
  columns: ColumnDef[]
  events?: StreamEvents
  list?: ListEndpoint
  /** Get-by-id endpoint used for hydration and delete detection. */
  get?: (id: string) => string
  /**
   * Payload transform applied before write. Used for events that wrap the
   * object (e.g. `organization_domain.verification_failed`).
   */
  unwrap?: (data: Record<string, unknown>) => Record<string, unknown>
  /**
   * Tier for docs/config grouping.
   */
  tier: 1 | 2 | 3 | 4
}

export interface WorkOSEvent {
  id: string
  event: string
  data: Record<string, unknown>
  created_at: string
  context?: Record<string, unknown>
}

export interface EnvironmentConfig {
  /** Stable label written to every row. Never derived from a display name. */
  name: string
  apiKey: string
  /** Defaults to https://api.workos.com */
  apiUrl?: string
}

export type HydrateMode = 'off' | 'on_create' | 'always'

export interface SyncEngineConfig {
  /** Where the mirror is written, e.g. `new PostgresDestination(...)` from `@workos/sync-postgres`. */
  destination: Destination
  environments: EnvironmentConfig[]
  /** Streams to sync. Defaults to every tier 1-3 stream. */
  streams?: string[]
  /**
   * Event types to subscribe to. `GET /events` requires this filter, so it
   * defaults to the union of the event types the selected streams declare.
   * Override to widen the subscription — for example to archive event families
   * that have no mirror table.
   */
  eventTypes?: string[]
  /** Re-fetch objects from the API on event receipt. Per-stream override. */
  hydrate?: HydrateMode | Record<string, HydrateMode>
  /** Poll interval for the events tail, in ms. */
  pollIntervalMs?: number
  /**
   * Never advance the cursor past events younger than this, in ms. Guards
   * against a change feed that is briefly non-monotonic under concurrent
   * ID generation. Reprocessing is free; a skipped event is not.
   */
  lagWindowMs?: number
  /** Page size for list and event calls. The API default is 10; always send 100. */
  pageSize?: number
  /**
   * Events to apply per tick, fetched across as many pages as needed and
   * rounded up to a whole page. Defaults to one page. Raising it means fewer,
   * larger writes and one checkpoint per tick instead of per page — which
   * matters for destinations where every statement is expensive — at the
   * cost of holding that many events in memory and replaying up to that many
   * after a mid-tick crash.
   */
  maxEventsPerTick?: number
  logger?: Logger
}

export interface Logger {
  debug(msg: string, fields?: Record<string, unknown>): void
  info(msg: string, fields?: Record<string, unknown>): void
  warn(msg: string, fields?: Record<string, unknown>): void
  error(msg: string, fields?: Record<string, unknown>): void
}

export interface WriteStats {
  created: number
  updated: number
  /** Rows rejected by the staleness gate or unchanged. This is a health metric. */
  skipped: number
  deleted: number
}

export const emptyStats = (): WriteStats => ({ created: 0, updated: 0, skipped: 0, deleted: 0 })

export const addStats = (a: WriteStats, b: WriteStats): WriteStats => ({
  created: a.created + b.created,
  updated: a.updated + b.updated,
  skipped: a.skipped + b.skipped,
  deleted: a.deleted + b.deleted,
})
