/**
 * @workos/sync-mysql
 *
 * Mirror WorkOS data into your own MySQL so you can join it with your
 * application's tables in SQL.
 *
 * The engine itself lives in `@workos/sync-engine` and is re-exported here, so this
 * one package is all an application needs to import. This package adds the
 * MySQL destination, the DDL and write path behind it, and the CLI.
 *
 * The mirror is a **read model**. It is eventually consistent, typically within
 * seconds. Read from it for joins, lists, dashboards, analytics and agents;
 * keep writes and authoritative post-signup checks on the WorkOS API.
 */
export * from '@workos/sync-engine'

export { MysqlDestination } from './destination.js'
export type { MysqlDestinationOptions } from './destination.js'

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
  iso8601Expression,
  jsonPath,
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
export { diffIdsInSql } from './db/reconcile.js'
export type { SqlDiff } from './db/reconcile.js'
export { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
export { isTransientMysqlError } from './db/errors.js'
export { connect, fromPool, poolConfig, withoutDatabase, APPLICATION_NAME, SESSION_SQL } from './db/client.js'
export type { ConnectOptions, SqlClient, PoolLike } from './db/client.js'
