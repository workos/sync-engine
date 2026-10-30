/**
 * @workos/sync-engine
 *
 * The engine-agnostic core for mirroring WorkOS data into a database of your
 * choosing: the stream catalog, event routing, backfill, the events tail and
 * reconciliation. Writes go through the `Destination` interface; pair this
 * with a destination package such as `@workos/sync-postgres`.
 *
 * The mirror is a **read model**. It is eventually consistent, typically within
 * seconds. Read from it for joins, lists, dashboards, analytics and agents;
 * keep writes and authoritative post-signup checks on the WorkOS API.
 */
export { SyncEngine, LeaseHeldError } from './engine.js'
export type { EngineDeps, EnvironmentStatus } from './engine.js'

export { startHealthServer, healthReport, renderMetrics, DEFAULT_MAX_POLL_AGE_MS } from './health.js'
export type {
  EngineHealth,
  EnvironmentHealth,
  HealthCounters,
  HealthCheckOptions,
  HealthReport,
  HealthServer,
  HealthServerOptions,
} from './health.js'

export type { Destination, FreshnessRow, IdDiff, Lease, LeaseResult, RejectedEvent, Row, WriteMode } from './destination.js'
export { StateStore } from './state.js'
export { redactPayload, redactRows, redactEvent, RedactingDestination, redacting } from './redact.js'

export {
  STREAMS,
  streamByName,
  streamForId,
  defaultStreams,
  selectStreams,
  subscribedEventTypes,
} from './catalog.js'
export { routeEvent, sourceUpdatedAt } from './router.js'
export type { RouteAction } from './router.js'

export { WorkOSClient, WorkOSApiError, TokenBucket } from './workos/client.js'
export type { RequestPhase, WorkOSClientOptions } from './workos/client.js'
export { VERSION as ENGINE_VERSION } from './version.js'
export {
  fetchEvents,
  cursorAgeDays,
  checkCursorAge,
  CursorDirectionError,
  EVENT_RETENTION_DAYS,
} from './workos/events.js'

export { verifyWebhook, parseSignatureHeader, WebhookVerificationError } from './webhook.js'

export { applyEvents, HydrationError } from './sync/apply.js'
export {
  applyWithQuarantine,
  isTransientError,
  eventObjectId,
  PoisonEventSuspectedError,
} from './sync/quarantine.js'
export type { QuarantineResult, RejectedRow, Suspect, SuspectMap } from './sync/quarantine.js'
export { backfillStream, pageBudget } from './sync/backfill.js'
export type { BackfillOptions, BackfillStreamResult, PageBudget } from './sync/backfill.js'
export { diffStream, freshness, refreshStream } from './sync/reconcile.js'
export { diffIdsInMemory, encodeKey } from './sync/diff.js'

export { createLogger, silentLogger } from './logger.js'
export type { LogLevel } from './logger.js'

export { addStats, emptyStats, DEFAULT_HYDRATE } from './types.js'
export type {
  ColumnDef,
  ColumnType,
  EnvironmentConfig,
  HydrateMode,
  ListEndpoint,
  Logger,
  StreamDef,
  StreamGroup,
  StreamMode,
  SyncEngineConfig,
  TableDef,
  WorkOSEvent,
  WriteStats,
} from './types.js'
