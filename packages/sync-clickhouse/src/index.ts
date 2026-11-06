/**
 * @workos/sync-clickhouse
 *
 * Mirror WorkOS data into your own ClickHouse: the sign-in and event logs
 * that WorkOS keeps for 90 days, as tables you keep for as long as you like,
 * and the identity objects beside them for the joins.
 *
 * The engine itself lives in `@workos/sync-engine` and is re-exported here, so this
 * one package is all an application needs to import. This package adds the
 * ClickHouse destination, the DDL and write path behind it, and the CLI.
 *
 * The mirror is a **read model**. It is eventually consistent, typically within
 * seconds. Read from it for analytics, dashboards, lists and agents; keep
 * writes and authoritative post-signup checks on the WorkOS API.
 */
export * from '@workos/sync-engine'

export { ClickHouseDestination } from './destination.js'
export type { ClickHouseDestinationOptions } from './destination.js'

export { migrate, fingerprint, DDL_VERSION } from './schema/migrate.js'
export type { MigrateOptions, MigrateResult } from './schema/migrate.js'
export { detectColumnDrift, repairColumnDrift, unrepairableReason } from './schema/drift.js'
export type { ColumnDrift, DriftKind, RepairResult } from './schema/drift.js'
export {
  buildSchemaSql,
  createTableSql,
  columnExpression,
  columnType,
  generatedColumnSql,
  indexSpecs,
  jsonPathArgs,
} from './schema/ddl.js'
export { liveViewSql, liveViewName } from './schema/views.js'

export {
  upsertRows,
  deleteRows,
  relinkRows,
  unlinkRows,
  dedupeByKey,
  wavesByKey,
  keyValues,
  parseUpdatedAt,
  jsonEqual,
} from './db/upsert.js'
export { diffIdsInClickHouse } from './db/reconcile.js'
export type { ChDiff } from './db/reconcile.js'
export { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
export { canonicalJson } from './db/lease.js'
export { isTransientClickHouseError } from './db/errors.js'
export { connect, fromClient, clientConfig, APPLICATION_NAME, DEFAULT_SETTINGS, KEEPER_SETTINGS } from './db/client.js'
export type { ConnectOptions, ChClient } from './db/client.js'
