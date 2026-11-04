# Changelog

## [0.1.0](https://github.com/workos/sync-engine/compare/sync-engine-v0.0.1...sync-engine-v0.1.0) (2026-11-04)


### ⚠ BREAKING CHANGES

* @workos/sync is now @workos/sync-engine, @workos/pg-sync is now @workos/sync-postgres, and the CLI is workos-sync-postgres.

### Features

* Bound a backfill call with maxPages ([#19](https://github.com/workos/sync-engine/issues/19)) ([a859418](https://github.com/workos/sync-engine/commit/a8594188fb138c0f8755a45629dd64ded19bdcee))
* Bound a reconcile call with maxPages ([#31](https://github.com/workos/sync-engine/issues/31)) ([b937a17](https://github.com/workos/sync-engine/commit/b937a1789927ac7deac84cdd042a4ed3cdbd6550))
* Compute the reconcile set difference in Postgres ([#7](https://github.com/workos/sync-engine/issues/7)) ([a2f8c4a](https://github.com/workos/sync-engine/commit/a2f8c4ae240f87400c5eee3962c4a9d78ec5afc8))
* Detect and repair generated-column drift on init ([#11](https://github.com/workos/sync-engine/issues/11)) ([a846f73](https://github.com/workos/sync-engine/commit/a846f73ffd60afd58437b52c6fbc16376de14d2e))
* Emit metrics through OpenTelemetry ([#36](https://github.com/workos/sync-engine/issues/36)) ([513bb4d](https://github.com/workos/sync-engine/commit/513bb4db04a708a8d94adbdede26f7e4fac152ac))
* Hold a single-writer lease while tailing the Events API ([#6](https://github.com/workos/sync-engine/issues/6)) ([342476c](https://github.com/workos/sync-engine/commit/342476c9e4d54e8de6ef1e584941ea3c02b7981e))
* Identify the engine and its phase in the User-Agent ([#27](https://github.com/workos/sync-engine/issues/27)) ([5ca5528](https://github.com/workos/sync-engine/commit/5ca5528ef297bed84c595b27ad6ac5a971462b60))
* **postgres:** Add `init --print-sql` ([#25](https://github.com/workos/sync-engine/issues/25)) ([d613623](https://github.com/workos/sync-engine/commit/d6136230d81d52fc28d54c4ee4ebaf1cbc1b199a))
* Quarantine poison events instead of stalling the tail ([#12](https://github.com/workos/sync-engine/issues/12)) ([3959847](https://github.com/workos/sync-engine/commit/3959847238bb8a2cd968fbc6c7195e226540c0a5))
* Serve /healthz and Prometheus metrics from listen ([#8](https://github.com/workos/sync-engine/issues/8)) ([55176cb](https://github.com/workos/sync-engine/commit/55176cbcb33c4cf415fd98108c98df44b3a6195a))
* Slice refresh like backfill, reset every environment first ([#33](https://github.com/workos/sync-engine/issues/33)) ([ca3b76b](https://github.com/workos/sync-engine/commit/ca3b76b07900ea4c75448c6ce1a0dfdd191b7445))


### Bug Fixes

* **engine:** Strip domain verification tokens from rows and archived events ([#29](https://github.com/workos/sync-engine/issues/29)) ([07d6dc0](https://github.com/workos/sync-engine/commit/07d6dc0596d14b0a5eda68db794d4fc7afcbc6c9))
* **postgres:** Fence blocked cursor checkpoints ([#16](https://github.com/workos/sync-engine/issues/16)) ([3f6962d](https://github.com/workos/sync-engine/commit/3f6962d401a83c79c7b651a76236602a7d434b07))
* Stop claiming user and organization events omit fields ([#37](https://github.com/workos/sync-engine/issues/37)) ([54af5c2](https://github.com/workos/sync-engine/commit/54af5c2a165370a27119e48faa5d66dc89455c10))
* Stop mirroring invitation acceptance tokens into _raw ([#2](https://github.com/workos/sync-engine/issues/2)) ([62d2842](https://github.com/workos/sync-engine/commit/62d284221ca0a7d52666275bdaecfe59bcd51c84))
* **sync:** Reconcile complete composite keys ([#15](https://github.com/workos/sync-engine/issues/15)) ([ffc3712](https://github.com/workos/sync-engine/commit/ffc37124218a61f593bda3ba3535c9d9318398b3))


### Code Refactoring

* Rename to @workos/sync-engine and @workos/sync-postgres ([012b35a](https://github.com/workos/sync-engine/commit/012b35a4399baf432c46d776df5df2eeee47b9d1))
