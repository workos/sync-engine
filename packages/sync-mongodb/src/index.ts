/**
 * @workos/sync-mongodb
 *
 * Mirror WorkOS data into your own MongoDB so you can query it next to your
 * application's collections, and `$lookup` across them.
 *
 * The engine itself lives in `@workos/sync-engine` and is re-exported here, so
 * this one package is all an application needs to import. This package adds
 * the MongoDB destination, the collections, indexes and write path behind
 * it, and the CLI.
 *
 * The mirror is a **read model**. It is eventually consistent, typically within
 * seconds. Read from it for lookups, lists, dashboards, analytics and agents;
 * keep writes and authoritative post-signup checks on the WorkOS API.
 */
export * from '@workos/sync-engine'

export { MongoDestination } from './destination.js'
export type { MongoDestinationOptions } from './destination.js'

export { migrate, fingerprint, indexSpecs, indexName, ensureBookkeeping, DDL_VERSION } from './schema/migrate.js'
export type { MigrateOptions, MigrateResult, OrphanIndex } from './schema/migrate.js'
export { ensureLiveView, LIVE_PIPELINE } from './schema/views.js'
export type { ApplyViewOptions, ViewOutcome } from './schema/views.js'
export { Names, DEFAULT_PREFIX, LIVE_VIEW_SUFFIX, validatePrefix, fieldPath, keyPath } from './schema/names.js'

export {
  upsertRows,
  deleteRows,
  relinkRows,
  unlinkRows,
  dedupeByKey,
  wavesByKey,
  keyValues,
  keyFilter,
  readPath,
} from './db/upsert.js'
export type { MirrorDoc, WriteOptions } from './db/upsert.js'
export { diffIdsInMongo } from './db/reconcile.js'
export type { MongoDiff } from './db/reconcile.js'
export { rejectEvents, listRejected, countRejected, clearRejected } from './db/rejected.js'
export type { RejectedDoc } from './db/rejected.js'
export { acquireLease, releaseLease, getLease, getState, setState, setStateIfHeld, canonicalJson } from './db/lease.js'
export type { SyncDoc } from './db/lease.js'
export { isTransientMongoError } from './db/errors.js'
export { parseIso8601, parseInstant, parseInstantOrNull, InvalidTimestampError } from './db/timestamps.js'
export { connect, fromClient, clientOptions, databaseFromUrl, APPLICATION_NAME } from './db/client.js'
export type { ConnectOptions } from './db/client.js'
