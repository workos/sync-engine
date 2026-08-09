# Get started with the sync engine

The sync engine mirrors your WorkOS data into a Postgres database you control,
so you can query users, organizations, memberships and directory data with SQL
and join them to your own tables.

This guide takes you from an empty database to a mirror that stays current.

## Before you start

You need:

- A Postgres database (14 or newer) that the engine can create a schema in.
- A WorkOS API key for the environment you want to mirror. Each key is scoped to
  a single environment, so mirroring both staging and production means two keys.

## Install

```bash
npm install @workos/pg-sync
```

The package ships both a library and a `workos-pg-sync` command.

## Create the schema

```bash
export DATABASE_URL="postgres://localhost:5432/app"
export WORKOS_API_KEY="sk_live_..."

npx workos-pg-sync init
```

This creates a `workos` schema, one table per synced object, and two
bookkeeping tables. It is safe to run repeatedly — the schema is fingerprinted,
so a second run with no changes does nothing.

To use a different schema name, set `WORKOS_SYNC_SCHEMA` or pass `--schema`.

## Load what already exists

```bash
npx workos-pg-sync backfill
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
npx workos-pg-sync listen
```

The engine now polls the Events API from its stored cursor and applies each
change. Leave it running as a service.

<Note>
The engine polls the Events API rather than receiving webhooks. Events come back
in a consistent order, the stream is replayable from any cursor, and your
process controls its own pace instead of absorbing whatever spike arrives. You
do not need to expose a public endpoint.
</Note>

## Query it

```sql
select o.name as organization, u.email, m.role_slug
from workos.organization_memberships m
join workos.users u on u.id = m.user_id and u._env = m._env
join workos.organizations o on o.id = m.organization_id and o._env = m._env
where u._deleted_at is null and m._deleted_at is null
order by o.name, u.email;
```

Two conventions to know:

- **Always filter `_deleted_at is null`** for live rows. Deleted objects are
  tombstoned rather than removed, so you can still see what was deleted and
  when.
- **Always join on `_env` as well as the id.** Object ids are unique within an
  environment, and the mirror can hold several environments at once.

## Embed it instead

The same engine runs as a library, which is how most people deploy it — no
extra service, and it uses the database connection you already have.

```ts
import { SyncEngine } from '@workos/pg-sync'

const engine = new SyncEngine({
  databaseUrl: process.env.DATABASE_URL,
  environments: [
    { name: 'production', apiKey: process.env.WORKOS_API_KEY! },
  ],
})

await engine.init()
await engine.backfill()
await engine.listen()   // resolves when you call engine.stop()
```

## Mirror several environments

A workspace can hold many environments, and their display names repeat across
projects. Give each one a stable label of your own — it is written to every row
and must not change once you have data.

```bash
export WORKOS_ENVIRONMENTS="production=sk_live_...,staging=sk_test_..."
```

```ts
environments: [
  { name: 'production', apiKey: process.env.WORKOS_PROD_KEY! },
  { name: 'staging', apiKey: process.env.WORKOS_STAGING_KEY! },
]
```

Each environment gets its own cursor. Cursors are not comparable across
environments, and neither are object ids, which is why `_env` is part of every
primary key.

## Keep it honest

Schedule a reconciliation:

```bash
npx workos-pg-sync reconcile            # report drift, exit 2 if any is found
npx workos-pg-sync reconcile --repair   # fix it
```

Reconciliation compares the mirror against the API and reports two numbers per
stream: rows you have that no longer exist upstream (missed deletes), and rows
upstream that you do not have (missed creates). Weekly is a reasonable default;
run it after any incident that touched the sync.

## Next steps

- [How it works](how-it-works.md) — the lifecycle and the design decisions
- [Querying the mirror](queries.md) — worked examples
- [Consistency](consistency.md) — what the mirror guarantees, and what it does not
- [Operations](operations.md) — monitoring, alerting and recovery
