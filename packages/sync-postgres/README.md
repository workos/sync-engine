# @workos/sync-postgres

Mirror your WorkOS data into your own Postgres, and query it with SQL.

Backfills what already exists from the REST API, tails the Events API from a
durable cursor, and reconciles the two so drift is found rather than assumed.
The result is a `workos` schema you can join against your own tables:

```sql
SELECT o.name AS organization, u.email, m.role_slug
   FROM workos.organization_memberships m
      JOIN workos.users u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations o ON o.id = m.organization_id AND o._env = m._env
   WHERE u._deleted_at IS NULL AND m._deleted_at IS NULL;
```

## Install

```bash
npm install @workos/sync-postgres
```

This package ships the Postgres destination, the `workos-sync-postgres`
command, and re-exports the engine from
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine),
so it is the only package you need for Postgres.

## Command line

```bash
export DATABASE_URL=postgres://localhost/app
export WORKOS_API_KEY=sk_live_...

npx workos-sync-postgres init       # create the schema and bookkeeping tables
npx workos-sync-postgres backfill   # load everything that already exists
npx workos-sync-postgres listen     # tail the Events API, forever
```

| Command                                 | What it does                                                                                                                                                                                                                                                                                                             |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `init [--repair] [--print-sql]`         | Create the schema and bookkeeping tables. Idempotent; safe on every deploy. Exits 2 if a generated column has drifted from the catalog, and fails if a generated `_live` view must be rebuilt; `--repair` rebuilds columns from `_raw` and views with their grants carried over. `--print-sql` prints the DDL instead of running it, with no database needed; apply it, then run `init` once so it records the fingerprint. |
| `backfill [--resume] [--max-pages <n>]` | Load current state from the REST list endpoints. `--resume` continues each stream from its checkpoint and skips streams already complete. `--max-pages` stops after `n` pages across all streams, for a host with a time limit; run again with `--resume` until every stream reports `complete`.                          |
| `listen [--health-port <n>]`            | Poll the Events API continuously as the sole writer; waits if another listener holds the lease. Finishes the current batch on `SIGINT` or `SIGTERM`. With a port, serves `GET /healthz` and `/metrics` on it.                                                                                                              |
| `tick`                                  | One poll, then exit. For a cron job instead of a long-running process. Exits 3 if a listener holds the lease.                                                                                                                                                                                                              |
| `reconcile [--repair]`                  | Diff ids against the API. Exits 2 on drift; `--repair` tombstones missed deletes and inserts missed creates.                                                                                                                                                                                                               |
| `refresh`                               | Re-list top-level streams to refresh fields that event payloads omit. Re-run `backfill` for per-parent streams.                                                                                                                                                                                                            |
| `status`                                | Cursor, time since the last successful poll, and per-stream row counts, tombstones and lag.                                                                                                                                                                                                                                |
| `redact --user <id>`                    | Erase one subject from the mirror and the event archive.                                                                                                                                                                                                                                                                   |
| `rejected [--replay] [--id <event_id>]` | Show events the tail quarantined; exits 2 if there are any. `--replay` re-applies them (one, with `--id`) once the cause is fixed.                                                                                                                                                                                        |
| `streams`                               | List what this build can mirror, with group, mode, and whether it is on by default.                                                                                                                                                                                                                                        |

Flags every command takes:

| Flag                   | Overrides            | What it does                                               |
| ---------------------- | -------------------- | ---------------------------------------------------------- |
| `--database-url <url>` | `DATABASE_URL`       | Postgres connection string.                                |
| `--schema <name>`      | `WORKOS_SYNC_SCHEMA` | Target schema.                                             |
| `--streams <a,b,c>`    |                      | Restrict to specific streams. `events` is always included. |
| `--help`               |                      | Print usage.                                               |

Rows are labelled `_env = 'production'` by default; set
`WORKOS_ENVIRONMENT_NAME` if the key is for something else. The label is
written to every row and must never change once you have data. To mirror
several environments into one database, run one process per key with a
different label each; cursors are kept per label, so they never collide.

The command reads its settings from the environment; where a flag exists for
the same setting, the flag wins.

| Variable                             | Default                             | What it does                                                                                                                         |
| ------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `DATABASE_URL`                       | required                            | Postgres connection string.                                                                                                          |
| `WORKOS_API_KEY`                     | required                            | API key for the environment to mirror. One key, one environment, one process; dedicate it to the sync (see Operations).              |
| `WORKOS_ENVIRONMENT_NAME`            | `production`                        | Label written to `_env` on every row. Must not change once you have data. Run one process per environment to mirror several.         |
| `WORKOS_API_URL`                     | `https://api.workos.com`            | API base URL.                                                                                                                        |
| `WORKOS_SYNC_SCHEMA`                 | `workos`                            | Schema the mirror lives in, for DDL and queries alike.                                                                               |
| `WORKOS_SYNC_ID`                     | `default`                           | Identifier when several syncs share one database. Each keeps its own cursors.                                                        |
| `WORKOS_SYNC_HYDRATE`                | users and organizations `on_create` | Re-fetch objects when their events arrive: `off`, `on_create`, `always`, or per stream as `users=always,organizations=off`.          |
| `WORKOS_SYNC_POLL_MS`                | `5000`                              | Idle wait between polls, in ms, when the last poll returned nothing.                                                                 |
| `WORKOS_SYNC_LAG_MS`                 | `5000`                              | Hold back events younger than this, in ms, before advancing the cursor.                                                              |
| `WORKOS_SYNC_MAX_EVENTS_PER_TICK`    | one page (100)                      | Events to apply per poll, fetched across as many pages as needed.                                                                    |
| `WORKOS_SYNC_HEALTH_PORT`            | off (`8080` in the container image) | `listen` only: serve `GET /healthz` and `GET /metrics` on this port, from in-memory state, with no database query.                  |
| `WORKOS_SYNC_HEALTH_HOST`            | `0.0.0.0`                           | Interface the health server binds.                                                                                                   |
| `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` | `60000`                             | `/healthz` returns 503 once any environment's last successful poll is older than this.                                               |
| `WORKOS_SYNC_LEASE_TTL_MS`           | `60000`                             | How long the tail lease outlives a crashed listener, in ms; at least 1. The most lag a crash can add before a replacement takes over. |
| `WORKOS_SYNC_PG_SSL_CA`              | system trust store                  | CA certificate the database's certificate must chain to, as PEM or base64 of it. TLS is then required and verified against it.       |
| `LOG_LEVEL`                          | `info`                              | `debug`, `info`, `warn`, `error` or `silent`. Logs are JSON lines on stdout.                                                         |

`listen` with a health port answers `GET /healthz` with `200` while the tail
is polling (and while it is still starting) and `503` once a poll is overdue
or the engine has stopped, and `GET /metrics` in Prometheus text format. Both
are served from memory, with no database query, so a probe keeps working when
the database does not. The container image turns this on and declares a
`HEALTHCHECK` against it; see
[Operations](https://github.com/workos/workos-pg-sync/blob/main/docs/operations.md)
for the metric names and a Kubernetes probe snippet.

## Embed it

Most deployments embed the engine in a service that already has a Postgres
connection:

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

Every method applies the schema first, so there is no separate `init()` to
call. New users and organizations are re-fetched when their create event
arrives, because those events omit `external_id` and `metadata`; pass
`hydrate: 'off'` to skip that.

To reuse an existing `pg` pool instead of a connection string:

```ts
import { fromPgPool, PostgresDestination } from "@workos/sync-postgres";

new PostgresDestination({ db: fromPgPool(pool) });
```

`PostgresDestination` options:

| Option          | Default   |                                                                                                                                                                    |
| --------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `databaseUrl`   | —         | Connection string. Either this or `db`.                                                                                                                            |
| `sslCa`         | —         | PEM CA certificate the server's certificate must chain to. TLS is then required and verified against it. With `databaseUrl` only.                                  |
| `db`            | —         | A pre-built client, e.g. `fromPgPool(pool)`. Not closed by the engine.                                                                                             |
| `schema`        | `workos`  | Schema the mirror lives in, for DDL and DML alike.                                                                                                                 |
| `syncId`        | `default` | Identifier when several syncs share one database.                                                                                                                  |
| `batchSize`     | `500`     | Rows per upsert statement.                                                                                                                                         |
| `repairViews`   | `false`   | On `init()`, rebuild a generated `_live` view that can no longer be replaced in place and carry its grants over. Off, `init()` fails and lists the grants instead. |
| `repairColumns` | `false`   | On `init()`, rebuild generated columns that no longer match the catalog from `_raw`. Off, drift is reported and `init` withholds the schema fingerprint.           |

Engine options (`environments`, `streams`, `extraColumns`, `hydrate`,
`pollIntervalMs`, `lagWindowMs`, `maxEventsPerTick`, …) are documented in
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine).

## What it mirrors

Users, organizations, memberships, domains, invitations and sessions;
directories, directory users and groups; SSO connections; AuthKit groups, roles,
permissions and feature flags. Every event is archived verbatim to
`workos.events`, including event types this version has never heard of.

One table has no equivalent anywhere in the API: `auth_attempts`. The
`authentication.*` events have no REST endpoint, so if you do not capture them
as they happen that history is gone after 90 days. Mirrored, "which accounts had
five failed sign-ins in the last hour" is a `GROUP BY`.

## The schema

The engine writes one column, `_raw`, and generates the rest from it:

```sql
CREATE TABLE workos.users (
  _env        text NOT NULL,
  _raw        jsonb NOT NULL,        -- the only column the engine writes
  _updated_at timestamptz NOT NULL,  -- source time; the staleness gate compares this
  _synced_at  timestamptz NOT NULL,  -- write time; lag is the difference
  _deleted_at timestamptz,           -- tombstone
  id          text  GENERATED ALWAYS AS ((_raw->>'id')) STORED,
  email       text  GENERATED ALWAYS AS ((_raw->>'email')) STORED,
  external_id text  GENERATED ALWAYS AS ((_raw->>'external_id')) STORED,
  metadata    jsonb GENERATED ALWAYS AS ((_raw->'metadata')) STORED,
  PRIMARY KEY (_env, id)
);
```

A field added upstream can never break ingest — it is queryable as
`_raw->>'field'` immediately, and promoting it to a typed column is pure DDL.
There are no foreign keys: events routinely arrive with children before
parents, and a constraint would turn that ordering artifact into a stuck
stream. Deletes are tombstones (`_deleted_at`), never row removals.

Every mirror table also gets a generated `<table>_live` view —
`workos.users_live`, `workos.organizations_live`, and so on — that is the table
without its tombstoned rows:

```sql
CREATE VIEW workos.users_live AS SELECT * FROM workos.users WHERE _deleted_at IS NULL;
```

Query the view for live data and the base table when you want deleted rows too.
The views are re-created whenever the schema is applied, so they pick up any
column you promote later. The append-only logs (`events`, `auth_attempts`) have
no tombstones and no view.

## What it is not

The mirror is a **read model**: eventually consistent, usually within seconds.
Read from it for joins, lists, dashboards, analytics and agents. Keep writes,
authorization decisions and post-signup checks on the WorkOS API — a
just-created user should be read from the API response you already have. The
sync is one-directional; there is no write-back.

## Documentation

- [Getting started](https://github.com/workos/workos-pg-sync/blob/main/docs/getting-started.md)
- [How it works](https://github.com/workos/workos-pg-sync/blob/main/docs/how-it-works.md) — backfill, tail, reconcile, and why all three
- [Schema](https://github.com/workos/workos-pg-sync/blob/main/docs/schema.md) and [example queries](https://github.com/workos/workos-pg-sync/blob/main/docs/queries.md)
- [Operations](https://github.com/workos/workos-pg-sync/blob/main/docs/operations.md) — deployment, alerting, the 90-day retention cliff
- [Consistency](https://github.com/workos/workos-pg-sync/blob/main/docs/consistency.md) — what the mirror does and does not guarantee

## License

MIT
