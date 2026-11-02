# Database tests

`pnpm test` runs the PGlite-backed suite without a database service.
Multi-connection regressions also run when `TEST_DATABASE_URL` is set:

```sh
TEST_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres pnpm test
```

Use a disposable PostgreSQL database with permission to create schemas. Each
integration test creates a uniquely named schema and drops it on completion.
CI supplies PostgreSQL 17 and runs these tests on both supported Node versions.

For a hosted endpoint, run the targeted compatibility suite:

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@HOST/DATABASE?sslmode=verify-full' \
  pnpm --filter @workos/sync-postgres test:database
```

Set `TEST_DATABASE_SSL_CA_FILE` to the provider's PEM certificate file when a
custom CA is needed. This suite exercises schema creation and the sync lifecycle
using a fake WorkOS API. It requires neither real WorkOS credentials nor server
administration permissions. Unlike a bare `pnpm test`, `test:database` fails
when its URL is missing instead of skipping the database checks.

The other `*-postgres.test.ts` regressions also inspect backend sessions or
terminate a connection they create; use local Postgres for that full suite.
See [Hosted Postgres setup](../../../docs/hosted-postgres.md).
