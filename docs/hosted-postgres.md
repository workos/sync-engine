# Hosted Postgres

A hosted Postgres exposes an ordinary PostgreSQL connection that the existing
`pg` destination can use. No provider SDK, logical replication, extension, or
superuser is needed. The database role needs `CREATE` and `TEMPORARY` on the
database and ownership of the sync schema's objects so the engine can create
and migrate them.

Use a dedicated database or branch for evaluation. The engine creates its own
`workos` schema and touches nothing else, including any schema the provider
manages for its own features, such as authentication.

Neon has a guide of its own, [Neon](neon.md), which also covers running the
sync on Neon as a Function.

## Choose a connection

Providers typically offer two or three URIs for the same database:

| Connection             | For a persistent sync process                                                                     |
| ---------------------- | ------------------------------------------------------------------------------------------------- |
| **Direct**             | Works. Some providers resolve the direct host to IPv6 only, so check that your host can reach it. |
| **Session pooler**     | Works. Session semantics are the same as a direct connection.                                     |
| **Transaction pooler** | Works, and has passed the compatibility test below against a live hosted endpoint.                |

Copy the complete URI from the provider rather than assembling one. A pooler
may use a tenant-qualified username and a hostname of its own, and a provider's
connection dialog usually only selects which URI is displayed; it does not turn
the pooler on or off. Use the database password, not a provider API key, and
percent-encode password characters that have meaning in a URL.

Transaction pooling is enough because of how the engine uses the database: it
takes transaction-scoped migration locks, holds row-based writer leases, and
creates temporary tables with `ON COMMIT DROP` inside reconciliation
transactions. It needs neither a session-level advisory lock nor named prepared
statements. The engine's Node `pg` pool reuses client connections with any of
the three URIs. Validate a specific endpoint with the compatibility test below
before relying on it.

## TLS

Use `sslmode=verify-full` in the connection URL. Keep any other parameters the
provider supplies, such as `channel_binding=require`.

Some providers' certificates chain to the system CA trust store and need
nothing more. Others issue their own CA certificate, downloadable from the
project's database settings. The CLI accepts that certificate as PEM or
base64-encoded PEM in `WORKOS_SYNC_PG_SSL_CA`:

```sh
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full'
export WORKOS_SYNC_PG_SSL_CA="$(cat /path/to/database-ca.crt)"
export WORKOS_API_KEY=sk_test_...
export WORKOS_ENVIRONMENT_NAME=demo

node packages/sync-postgres/dist/cli.js init
node packages/sync-postgres/dist/cli.js backfill
node packages/sync-postgres/dist/cli.js listen
```

Run `pnpm install` and `pnpm build` first when using this checkout. Omit the CA
variable when the endpoint is already trusted by the system. In embedded code,
pass the PEM text as `sslCa` to `PostgresDestination` or `connect`; those APIs do
not read the CLI's environment variables. A pool passed through `fromPgPool`
must carry its own TLS configuration.

## Verify the actual endpoint

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full' \
  pnpm --filter @workos/sync-postgres test:database

# For an endpoint that needs its own CA:
TEST_DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE?sslmode=verify-full' \
  TEST_DATABASE_SSL_CA_FILE=/path/to/database-ca.crt \
  pnpm --filter @workos/sync-postgres test:database
```

This targeted suite creates a unique schema for each test and removes it
afterward. It checks all stream definitions, repeat initialization, backfill,
joins, live updates, authentication events, cursor resume, deletion tombstones,
reconciliation, and writer leases. WorkOS responses are local fixtures, so no
WorkOS key or upstream changes are involved. The command fails if the database
URL is missing. The same tests run against PostgreSQL in CI.

Passing locally does not establish that a hosted endpoint's credentials,
network access, TLS, or permissions are correct. Run the test separately for
each hosted URL once you have created the projects.

If the provider exposes schemas automatically over an HTTP data API, keep
`workos` out of it for this SQL-only integration. Access from browser clients
needs a separate grants and row-level security design; the generated mirror
includes raw event payloads and deleted rows.
