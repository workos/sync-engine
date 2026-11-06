# Changelog

## [0.1.0](https://github.com/workos/sync-engine/compare/sync-mysql-v0.0.1...sync-mysql-v0.1.0) (2026-11-06)


### Features

* **mysql:** add the MySQL destination package ([#40](https://github.com/workos/sync-engine/issues/40)) ([01fa153](https://github.com/workos/sync-engine/commit/01fa153ad69b8dec00f8461ebf58137e2625f760))


### Bug Fixes

* **mysql:** publish releases, port --exclude-streams and the oversized verdict ([#47](https://github.com/workos/sync-engine/issues/47)) ([bac1bfc](https://github.com/workos/sync-engine/commit/bac1bfc5332b78f309cd324242d864261bf21e1b))
* **mysql:** run the installed command, dedupe by instant, refuse out-of-range offsets ([#50](https://github.com/workos/sync-engine/issues/50)) ([05e8fb4](https://github.com/workos/sync-engine/commit/05e8fb4e91630191fbad850529d03f9935f0fc83))
* redact every column that names a user, verify unrecorded MySQL columns ([#52](https://github.com/workos/sync-engine/issues/52)) ([613ae96](https://github.com/workos/sync-engine/commit/613ae9642ac5dae0ea87368d1c700ef92874507d))
* slight correct for locking in migration ([#56](https://github.com/workos/sync-engine/issues/56)) ([4457aa3](https://github.com/workos/sync-engine/commit/4457aa356244b1701cd209ab43092decf673e335))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @workos/sync-engine bumped to 0.2.0
