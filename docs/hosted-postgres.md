# Neon and Supabase

Both providers expose PostgreSQL connections that the existing `pg` destination
can use. No provider SDK, logical replication, extension, or superuser is needed.
The database role needs `CREATE` and `TEMPORARY` on the database and ownership of
the sync schema's objects so the engine can create and migrate them.

Use a dedicated demo database or branch for evaluation. The engine creates its
own `workos` schema; it does not replace Supabase Auth or write to `auth.users`.

## Choose a connection

| Provider | Connection for a persistent sync process |
| --- | --- |
| Neon | Either URI. The **pooled** URI, whose host contains `-pooler`, has passed the compatibility test below against a live Neon endpoint; the **direct** URI works as well. |
| Supabase | Copy the **Session pooler** URI on port 5432 for IPv4, or the **Direct connection** URI if your host supports IPv6 (or the project has the IPv4 add-on). |

Copy the complete URI from the provider. In particular, Supabase's shared pooler
uses `postgres.<project-ref>` and a project-specific pooler hostname. Use the
database password, not a Supabase API key. Percent-encode password characters
that have meaning in a URL.

Direct connections and session poolers support the setup DDL and the persistent
demo process. Neon's transaction pooler does too: the compatibility test below
has passed against a live pooled Neon endpoint. The engine uses
transaction-scoped migration locks, row-based writer leases, and temporary
tables with `ON COMMIT DROP` inside reconciliation transactions, and needs
neither a session-level advisory lock nor named prepared statements, which is
why transaction pooling is enough. Validate any other transaction pooler with
the same test before using it.

Neon's Connect toggle only selects which URI is displayed; it does not turn the
project's pooler on or off. The demo's Node `pg` pool reuses client connections
with either URI.

Provider references: [Neon connections](https://neon.com/docs/connect/connect-from-any-app),
[Neon pooling](https://neon.com/docs/connect/connection-pooling), and
[Supabase connections](https://supabase.com/docs/guides/database/connecting-to-postgres).

## TLS

Use `sslmode=verify-full` in the connection URL. Keep any other parameters the
provider supplies, such as Neon's `channel_binding=require`.

Neon normally uses the system CA trust store. For Supabase, download the database
CA certificate from its database SSL settings. The CLI accepts that certificate
as PEM or base64-encoded PEM in `WORKOS_SYNC_PG_SSL_CA`:

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

See [Supabase SSL configuration](https://supabase.com/docs/guides/platform/ssl-enforcement).

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

Keep `workos` outside Supabase's exposed Data API schemas for this SQL-only
integration. Access from browser clients needs a separate grants/RLS design;
the generated mirror includes raw event payloads and deleted rows.
