/**
 * @workos/pg-sync
 *
 * Mirror WorkOS data into your own Postgres so you can join it with your
 * application's tables in SQL.
 *
 * The mirror is a **read model**. It is eventually consistent, typically within
 * seconds. Read from it for joins, lists, dashboards, analytics and agents;
 * keep writes and authoritative post-signup checks on the WorkOS API.
 */
export { SyncEngine } from './engine.js'
export type { EngineDeps } from './engine.js'

export {
  STREAMS,
  streamByName,
  streamForId,
  defaultStreams,
  subscribedEventTypes,
  ARCHIVE_ONLY_EVENT_TYPES,
} from './catalog.js'
export { routeEvent, sourceUpdatedAt } from './router.js'
export type { RouteAction } from './router.js'

export { WorkOSClient, WorkOSApiError, TokenBucket } from './workos/client.js'
export {
  fetchEvents,
  cursorAgeDays,
  checkCursorAge,
  CursorDirectionError,
  EVENT_RETENTION_DAYS,
} from './workos/events.js'

export { verifyWebhook, parseSignatureHeader, WebhookVerificationError } from './webhook.js'

export { migrate, fingerprint } from './schema/migrate.js'
export { buildSchemaSql, createTableSql, columnExpression, parseIso8601Function } from './schema/ddl.js'

export { upsertRows, deleteRows, relinkRows, dedupeByKey, keyValues, buildUpsert } from './db/upsert.js'
export { StateStore } from './db/state.js'
export { connect, fromPgPool } from './db/client.js'
export type { SqlClient } from './db/client.js'

export { applyEvents } from './sync/apply.js'
export { backfillStream } from './sync/backfill.js'
export { diffStream, freshness, refreshStream } from './sync/reconcile.js'

export { createLogger, silentLogger } from './logger.js'

export type {
  ColumnDef,
  ColumnType,
  EnvironmentConfig,
  HydrateMode,
  ListEndpoint,
  Logger,
  StreamDef,
  StreamMode,
  SyncEngineConfig,
  WorkOSEvent,
  WriteStats,
} from './types.js'
