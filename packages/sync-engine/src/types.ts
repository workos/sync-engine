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

/** Product area a stream belongs to. Purely descriptive. */
export type StreamGroup = 'identity' | 'directory_sync' | 'sso' | 'authorization' | 'logs' | 'platform'

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

/**
 * What a destination needs to know about a stream: the table it becomes.
 *
 * Name, key, mode, the columns projected out of the payload, and the product
 * area. Nothing here says where rows come from. The WorkOS half of a stream —
 * its list endpoint, its events, hydration, credential paths — lives on
 * `StreamDef`, which extends this and is read only by the engine. The
 * `Destination` interface is typed on `TableDef`, so a destination is
 * source-neutral by construction: it cannot reach for an endpoint it was
 * never given.
 */
export interface TableDef {
  /** Stream name, also the table name. */
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
  columns: ColumnDef[]
  /** Product area, for docs and the `streams` command. */
  group: StreamGroup
}

/** A stream: a table, plus everything the engine needs to fill it from WorkOS. */
export interface StreamDef extends TableDef {
  /** ID prefixes that identify this object type, for prefix dispatch. */
  prefixes?: string[]
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
   * Live credentials to strip from the payload before any row reaches the
   * destination — e.g. an invitation's acceptance `token`. Each entry is a
   * top-level key, or a nested path as a string array. No column may project
   * from a redacted path.
   */
  redact?: (string | string[])[]
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

/**
 * Default hydration: re-fetch users and organizations when their create event
 * arrives. These are the only streams with a get-by-id endpoint, creates are
 * low-volume, and `user.created` / `organization.created` omit the fields
 * people join on (`external_id`, `metadata`), so the row would otherwise be
 * incomplete until the next backfill or refresh. Pass `hydrate: 'off'` to
 * disable.
 */
export const DEFAULT_HYDRATE: Readonly<Record<string, HydrateMode>> = Object.freeze({
  users: 'on_create',
  organizations: 'on_create',
})

export interface SyncEngineConfig {
  /** Where the mirror is written, e.g. `new PostgresDestination(...)` from `@workos/sync-postgres`. */
  destination: Destination
  environments: EnvironmentConfig[]
  /**
   * Streams to sync. Defaults to `defaultStreams()`: every stream that has
   * events. Poll-only streams (`fga_resources`, `webhook_endpoints`) are
   * opt-in. The `events` archive is always included, because every event is
   * written there before it is routed.
   */
  streams?: string[]
  /**
   * Additional typed columns to project out of `_raw`, keyed by stream name.
   * Each becomes a `GENERATED ALWAYS AS ... STORED` column on the next
   * `init()`, computed from rows already present. Adding a column never
   * rewrites data. Changing one you added before (its path or type) is
   * detected on the next `init()` as column drift and can be rebuilt from
   * `_raw` with `init --repair` (`repairColumns` on the Postgres destination);
   * removing one leaves the column in place and reports it as an orphan.
   */
  extraColumns?: Record<string, ColumnDef[]>
  /**
   * Event types to subscribe to. `GET /events` requires this filter, so it
   * defaults to the union of the event types the selected streams declare.
   * Override to widen the subscription — for example to archive event families
   * that have no mirror table.
   */
  eventTypes?: string[]
  /**
   * Re-fetch objects from the API on event receipt, globally or per stream.
   * Defaults to `DEFAULT_HYDRATE` (users and organizations on create). Only
   * streams with a get-by-id endpoint can be hydrated; others ignore it.
   */
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
  /**
   * How long the events-tail lease lasts without renewal, in ms. Defaults to
   * 60 000 and must be at least 1. The engine renews it every third of this
   * while it polls, in `listen` and `tick` alike, so it only expires when the
   * holder has crashed or stalled; that makes it the worst-case delay before
   * a replacement process takes over after a crash. A clean stop releases
   * the lease immediately. A process that stalls past it loses the lease to
   * its replacement, and the batch it was applying is not checkpointed.
   */
  leaseTtlMs?: number
  logger?: Logger
}

/**
 * A `name/version` pair, sent as one product token of the `User-Agent` on
 * every request to WorkOS. See `Destination.product`.
 */
export interface ProductToken {
  name: string
  version: string
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
