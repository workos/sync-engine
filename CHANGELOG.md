# Changelog

## [0.2.0](https://github.com/workos/workos-pg-sync/compare/v0.1.0...v0.2.0) (2026-10-21)


### ⚠ BREAKING CHANGES

* @workos/sync is now @workos/sync-engine, @workos/pg-sync is now @workos/sync-postgres, and the CLI is workos-sync-postgres.
* SyncEngineConfig no longer takes databaseUrl, schema, syncId or batchSize. Pass `destination: new PostgresDestination({...})` with those options instead. EngineDeps.db is gone for the same reason.

### Features

* **sync:** Add maxEventsPerTick to drain pages in one batch ([85b70ce](https://github.com/workos/workos-pg-sync/commit/85b70ce811ae6a216414c0f3b4e81978fdccda61))


### Code Refactoring

* Rename to @workos/sync-engine and @workos/sync-postgres ([55362af](https://github.com/workos/workos-pg-sync/commit/55362af25045242176ee28eced065454e1185de0))
* Split the engine into @workos/sync ([685a078](https://github.com/workos/workos-pg-sync/commit/685a078fd23cd17738c79367f78f2b235ee0373e))
