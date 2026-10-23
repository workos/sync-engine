# Get started with the sync engine

The sync engine mirrors your WorkOS data into a Postgres database you control,
so you can query users, organizations, memberships and directory data with SQL
and join them to your own tables.

This guide takes you from an empty database to a mirror that stays current.

## Before you start

You need:

- A Postgres database (12 or newer, for generated columns) that the engine can create a schema in.
- Node 22 or newer.
- A WorkOS API key for the environment you want to mirror. Each key is scoped to
  a single environment, so mirroring both staging and production means two keys.

## Install

```bash
npm install @workos/sync-postgres
```

The package ships both a library and a `workos-sync-postgres` command. It depends on
and re-exports `@workos/sync-engine`, the engine-agnostic core, so this is the only
package you install.

## Create the schema

```bash
export DATABASE_URL="postgres://localhost:5432/app"
export WORKOS_API_KEY="sk_live_..."

npx workos-sync-postgres init
```

This creates a `workos` schema, one table per synced object, and two
bookkeeping tables: `_sync_state` (cursors and backfill checkpoints) and
`_migrations` (which version of the schema has been applied). It is safe to run
repeatedly. The schema is fingerprinted, so a second run with no changes does
nothing.

To use a different schema name, set `WORKOS_SYNC_SCHEMA` or pass `--schema`.

With a single key, every row is labelled `_env = 'production'`. If that is not
what this key is, set `WORKOS_ENVIRONMENT_NAME` before the first write. The
label is part of every primary key and must not change once you have data.

## Load what already exists

```bash
npx workos-sync-postgres backfill
```

Backfill pages the REST list endpoints and writes everything it finds. Before
the first page it records your current position in the event stream, so
anything that changes while it runs is picked up afterwards rather than lost.

Backfill is not optional. The Events API retains 90 days of history, so
everything older than that exists only through the list endpoints. This is the
step webhook-only integrations skip, and the reason they start life already
incomplete.

Large directories take a while: `/directory_users` is limited to 4 requests per
second **per directory**, and that limit does not aggregate. The engine paces
itself accordingly. If backfill is interrupted, `--resume` continues from the
last checkpoint instead of starting over.

## Start syncing

```bash
npx workos-sync-postgres listen
```

The engine now polls the Events API from its stored cursor and applies each
change. Leave it running as a service. If a few minutes of lag is acceptable,
`tick` does one poll and exits, which suits a cron job instead of a process.

> [!NOTE]
> The engine polls the Events API rather than receiving webhooks. Events come back
> in a consistent order, the stream is replayable from any cursor, and your
> process controls its own pace instead of absorbing whatever spike arrives. You
> do not need to expose a public endpoint.

### New objects are complete immediately

WorkOS event payloads are subsets of the REST objects: `user.created` does not
carry `external_id` or `metadata`. By default the engine re-fetches a user or
organization when its create event arrives, so the row is complete from the
start rather than at the next backfill. Creates are low-volume, so this costs
one extra request each. Set `WORKOS_SYNC_HYDRATE=off` (or `hydrate: 'off'` in
the library) to turn it off.

## Query it

```sql
SELECT o.name AS organization, u.email, m.role_slug
   FROM workos.organization_memberships_live m
      JOIN workos.users_live u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations_live o ON o.id = m.organization_id AND o._env = m._env
   ORDER BY o.name, u.email;
```

Two conventions to know:

- **Read the `_live` views for live rows.** Deleted objects are tombstoned in
  the base table (`_deleted_at`) rather than removed, so you can still see what
  was deleted and when by querying `workos.users` directly.
- **Always join on `_env` as well as the id.** Object ids are unique within an
  environment, and the mirror can hold several environments at once.

## Embed it

The same engine runs as a library, so you can reuse the database connection you already have.

```ts
import { SyncEngine, PostgresDestination } from "@workos/sync-postgres";

const engine = new SyncEngine({
  destination: new PostgresDestination({
    databaseUrl: process.env.DATABASE_URL,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});

await engine.backfill();
await engine.listen(); // resolves when you call engine.stop()
```

Every method applies the schema before it runs, so there is no separate
`init()` call to remember. It exists if you want to create the schema ahead of
time, for example during a deploy.

## Mirror several environments

A workspace can hold many environments, and their display names repeat across
projects. Give each one a stable label of your own. It is written to every row
and must not change once you have data.

On the command line, run one process per environment into the same database.
Each process keeps its own cursor under its label, so they never collide:

```bash
WORKOS_API_KEY=sk_live_... WORKOS_ENVIRONMENT_NAME=production npx workos-sync-postgres listen
WORKOS_API_KEY=sk_test_... WORKOS_ENVIRONMENT_NAME=staging    npx workos-sync-postgres listen
```

In the library, one engine can hold several:

```ts
environments: [
  { name: "production", apiKey: process.env.WORKOS_PROD_KEY! },
  { name: "staging", apiKey: process.env.WORKOS_STAGING_KEY! },
];
```

Each environment gets its own cursor. Cursors are not comparable across
environments, and neither are object ids, which is why `_env` is part of every
primary key.

## Keep it honest

Schedule a reconciliation:

```bash
npx workos-sync-postgres reconcile            # report drift, exit 2 if any is found
npx workos-sync-postgres reconcile --repair   # fix it
```

Reconciliation compares the set of ids in the mirror against the API and
reports two numbers per stream: rows you have that no longer exist upstream
(missed deletes), and rows upstream that you do not have (missed creates). It
does not re-read rows that match. Daily is a reasonable default for the report,
weekly for `--repair`, and run it after any incident that touched the sync.

## Next steps

- [How it works](how-it-works.md): the lifecycle and the design decisions
- [Querying the mirror](queries.md): worked examples
- [Consistency](consistency.md): what the mirror guarantees, and what it does not
- [Operations](operations.md): monitoring, alerting and recovery
