# @workos/sync-clickhouse

Keep your WorkOS sign-in and event history in ClickHouse for as long as you
like, with the users and organizations beside it for the joins.

WorkOS keeps events for 90 days, and the `authentication.*` events — every
sign-in, success or failure — have no REST endpoint at all. This package tails
the Events API from a durable cursor into append-only ClickHouse tables, so
"failed sign-ins per organization per hour, last 18 months" is a `GROUP BY`
over data you own:

```sql
SELECT o.name AS organization, toStartOfHour(a.created_at) AS hour, count() AS failures
FROM workos.auth_attempts a
JOIN workos.users_live u ON u.id = a.user_id AND u._env = a._env
JOIN workos.organization_memberships_live m ON m.user_id = u.id AND m._env = u._env
JOIN workos.organizations_live o ON o.id = m.organization_id AND o._env = m._env
WHERE a.status = 'failed' AND a.created_at > now() - INTERVAL 7 DAY
GROUP BY organization, hour
ORDER BY failures DESC;
```

It also mirrors the identity objects (users, organizations, memberships,
directories, connections, roles, …) the way `@workos/sync-postgres` does, as
versioned tables with `_live` views, so the join above works in one database.
Start with `--streams auth_attempts` if the logs are what you came for; add
the mirror streams when you want to join against them.

## Requirements

- ClickHouse 23.11 or later, self-hosted or ClickHouse Cloud. Tested against
  26.x in CI and against ClickHouse Cloud.
- A ClickHouse Keeper (or ZooKeeper) reachable by the server, with
  `keeper_map_path_prefix` set. The single-writer lease, the cursors, and
  the per-stream write lock that keeps two processes from writing one row
  at once live in
  [KeeperMap](https://clickhouse.com/docs/engines/table-engines/special/keeper-map)
  tables: it is the one place ClickHouse offers an atomic compare-and-swap.
  Cloud and any replicated cluster have this already. A single self-hosted
  node needs the embedded Keeper enabled; the few lines are in
  [`test/clickhouse/config.xml`](test/clickhouse/config.xml).
- The HTTP interface (port 8123, or 8443 with TLS). The package uses the
  official `@clickhouse/client`.

## Install

```bash
npm install @workos/sync-clickhouse
```

This package ships the ClickHouse destination, the `workos-sync-clickhouse`
command, and re-exports the engine from
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine),
so it is the only package you need for ClickHouse.

## Command line

```bash
export DATABASE_URL=https://default:password@abc123.us-east-1.aws.clickhouse.cloud:8443
export WORKOS_API_KEY=sk_live_...

npx workos-sync-clickhouse init                              # create the database, tables and views
npx workos-sync-clickhouse backfill --streams auth_attempts  # nothing to list for logs; sets the cursor
npx workos-sync-clickhouse listen --streams auth_attempts    # tail the Events API, forever
```

Auth history begins the day you turn the tail on: there is no endpoint to
list past sign-ins, so `backfill` cannot recover them. That is the argument
for starting the tail early.

| Command                                                                       | What it does                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init [--repair] [--print-sql]`                                               | Create the database, tables, `_live` views and bookkeeping. Idempotent; safe on every deploy. Exits 2 if a projected column has drifted from the catalog, and fails if a view under a generated `_live` name is not the generated shape; `--repair` redefines columns and replaces such views. `--print-sql` prints the DDL instead of running it.                                                                                                                                                                                                                                                                        |
| `backfill [--resume] [--max-pages <n>]`                                       | Load current state from the REST list endpoints. `--resume` continues each stream from its checkpoint and skips streams already complete. `--max-pages` stops after `n` pages across all streams, for a host with a time limit; run again with `--resume` until every stream reports `complete`.                                                                                                                                                                                                                                                                                                                          |
| `listen [--health-port <n>]`                                                  | Poll the Events API continuously as the sole writer; waits if another listener holds the lease. Finishes the current batch on `SIGINT` or `SIGTERM`. With a port, serves `GET /healthz` and `/metrics` on it.                                                                                                                                                                                                                                                                                                                                                                                                             |
| `tick`                                                                        | One poll, then exit. For a cron job instead of a long-running process. Exits 3 if a listener holds the lease.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `reconcile [--repair] [--resume] [--max-pages <n>] [--exclude-streams <a,b>]` | Diff ids against the API. Exits 2 on drift; `--repair` tombstones missed deletes and inserts missed creates. `--max-pages` stops starting streams once `n` pages have been fetched, for a host with a time limit; a stream that starts runs to its last page, since a diff needs the whole listing, unless it alone needs more than `n`: it is then abandoned, reported `oversized`, and skipped by `--resume` until a run with a larger budget or none. Run again with `--resume` until every stream reports `complete`. `--exclude-streams` leaves streams out, for one that has to run somewhere without a time limit. |
| `refresh [--resume] [--max-pages <n>]`                                        | Re-list top-level streams from the REST API, replacing each row; `--max-pages` and `--resume` slice it like `backfill`, with which it shares checkpoints. Re-run `backfill` for per-parent streams.                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `status`                                                                      | Cursor, time since the last successful poll, streams `reconcile` has abandoned as oversized, and per-stream row counts, tombstones and lag.                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `redact --user <id>`                                                          | Erase one subject from the mirror, the sign-in log and the event archive, with a lightweight `DELETE`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `rejected [--replay] [--id <event_id>]`                                       | Show events the tail quarantined; exits 2 if there are any. `--replay` re-applies them (one, with `--id`) once the cause is fixed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `streams`                                                                     | List what this build can mirror, with group, mode, and whether it is on by default.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

Flags every command takes:

| Flag                   | Overrides            | What it does                                               |
| ---------------------- | -------------------- | ---------------------------------------------------------- |
| `--database-url <url>` | `DATABASE_URL`       | The HTTP interface, `https://user:password@host:8443`.     |
| `--schema <name>`      | `WORKOS_SYNC_SCHEMA` | The database the mirror lives in.                          |
| `--streams <a,b,c>`    |                      | Restrict to specific streams. `events` is always included. |
| `--help`               |                      | Print usage.                                               |

Rows are labelled `_env = 'production'` by default; set
`WORKOS_ENVIRONMENT_NAME` if the key is for something else. The label is
written to every row and must never change once you have data. To mirror
several environments into one database, run one process per key with a
different label each; cursors are kept per label, so they never collide.

The command reads its settings from the environment; where a flag exists for
the same setting, the flag wins.

| Variable                             | Default                             | What it does                                                                                                                                                  |
| ------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                       | required                            | The HTTP interface with credentials. A path in it is ignored; see `WORKOS_SYNC_SCHEMA`.                                                                       |
| `WORKOS_API_KEY`                     | required                            | API key for the environment to mirror. One key, one environment, one process; dedicate it to the sync (see Operations).                                       |
| `WORKOS_ENVIRONMENT_NAME`            | `production`                        | Label written to `_env` on every row. Must not change once you have data. Run one process per environment to mirror several.                                  |
| `WORKOS_API_URL`                     | `https://api.workos.com`            | API base URL.                                                                                                                                                 |
| `WORKOS_SYNC_SCHEMA`                 | `workos`                            | The database the mirror lives in, created if missing. Every statement qualifies its tables with it.                                                           |
| `WORKOS_SYNC_ID`                     | `default`                           | Identifier when several syncs share one database. Each keeps its own cursors.                                                                                 |
| `WORKOS_SYNC_HYDRATE`                | `off`                               | Re-fetch objects when their events arrive: `off`, `on_create`, `always`, or per stream as `users=always,organizations=off`.                                   |
| `WORKOS_SYNC_POLL_MS`                | `5000`                              | Idle wait between polls, in ms, when the last poll returned nothing.                                                                                          |
| `WORKOS_SYNC_LAG_MS`                 | `5000`                              | Hold back events younger than this, in ms, before advancing the cursor.                                                                                       |
| `WORKOS_SYNC_MAX_EVENTS_PER_TICK`    | one page (100)                      | Events to apply per poll, fetched across as many pages as needed. Raise it: each poll is a few inserts, and ClickHouse prefers fewer, larger ones.            |
| `WORKOS_SYNC_HEALTH_PORT`            | off (`8080` in the container image) | `listen` only: serve `GET /healthz` and `GET /metrics` on this port, from in-memory state, with no database query.                                            |
| `WORKOS_SYNC_HEALTH_HOST`            | `0.0.0.0`                           | Interface the health server binds.                                                                                                                            |
| `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` | `60000`                             | `/healthz` returns 503 once any environment's last successful poll is older than this.                                                                        |
| `WORKOS_SYNC_OTEL`                   | off                                 | `1` starts the OpenTelemetry Node SDK for the command, configured by the standard `OTEL_*` variables, and flushes it on exit. Every command, `tick` included. |
| `WORKOS_SYNC_LEASE_TTL_MS`           | `60000`                             | How long the tail lease outlives a crashed listener, in ms; at least 1. The most lag a crash can add before a replacement takes over.                         |
| `WORKOS_SYNC_CLICKHOUSE_TLS_CA`      | system trust store                  | CA certificate the server's certificate must chain to, as PEM or base64 of it. Only with an `https://` URL.                                                   |
| `LOG_LEVEL`                          | `info`                              | `debug`, `info`, `warn`, `error` or `silent`. Logs are JSON lines on stdout.                                                                                  |

The user the sync connects as needs `CREATE DATABASE` the first time, or an
existing database it may create tables in, plus `CREATE TABLE`, `CREATE VIEW`,
`DROP TABLE` (for the staging tables `reconcile` builds and drops), `ALTER`,
`INSERT`, `SELECT` and `ALTER DELETE` on that database.

## Embed it

```ts
import { SyncEngine, ClickHouseDestination } from "@workos/sync-clickhouse";

const engine = new SyncEngine({
  destination: new ClickHouseDestination({
    databaseUrl: process.env.DATABASE_URL,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
  streams: ["auth_attempts", "users", "organizations"],
});

await engine.backfill();
await engine.listen(); // resolves when you call engine.stop()
```

To reuse an existing `@clickhouse/client` instance instead of a connection
string, wrap it with `fromClient` and pass it as `db`. Give it the settings
in `DEFAULT_SETTINGS`: the engine relies on waited async inserts, synchronous
mutations and sequentially consistent reads.

`ClickHouseDestination` options:

| Option          | Default   |                                                                                                                                                                  |
| --------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `databaseUrl`   | —         | The HTTP interface with credentials. Either this or `db`.                                                                                                        |
| `sslCa`         | —         | PEM CA certificate the server's certificate must chain to. With an `https://` `databaseUrl` only.                                                                |
| `db`            | —         | A pre-built client, e.g. `fromClient(client)`. Not closed by the engine.                                                                                         |
| `schema`        | `workos`  | The database the mirror lives in. Created if missing.                                                                                                            |
| `syncId`        | `default` | Identifier when several syncs share one database.                                                                                                                |
| `batchSize`     | `500`     | Rows per statement.                                                                                                                                              |
| `repairViews`   | `false`   | On `init()`, replace a view under a generated `_live` name that is not the generated shape. Off, `init()` fails and names it.                                    |
| `repairColumns` | `false`   | On `init()`, redefine projected columns that no longer match the catalog and recompute them. Off, drift is reported and `init` withholds the schema fingerprint. |

Engine options (`environments`, `streams`, `extraColumns`, `hydrate`,
`pollIntervalMs`, `lagWindowMs`, `maxEventsPerTick`, …) are documented in
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine).

## The schema

The engine writes one column, `_raw`, and computes the rest from it with
`DEFAULT` expressions:

```sql
CREATE TABLE workos.auth_attempts (
  _env        String,
  _raw        String,                         -- the event payload, the only column the engine writes
  _updated_at DateTime64(3, 'UTC'),           -- the event's own time; a tombstone's is its deletion time
  _synced_at  DateTime64(6, 'UTC') DEFAULT now64(6),
  event_id    String DEFAULT JSONExtractString(_raw, 'event_id'),
  status      Nullable(String) DEFAULT JSONExtract(_raw, 'status', 'Nullable(String)'),
  user_id     Nullable(String) DEFAULT JSONExtract(_raw, 'user_id', 'Nullable(String)'),
  created_at  Nullable(DateTime64(3, 'UTC')) DEFAULT if(match(JSONExtractString(_raw, 'created_at'), '<strict ISO-8601>'),
                parseDateTime64BestEffortOrNull(upper(JSONExtractString(_raw, 'created_at')), 3, 'UTC'), NULL),
  INDEX auth_attempts_user_id_idx user_id TYPE bloom_filter GRANULARITY 4,
  INDEX auth_attempts_created_at_idx created_at TYPE minmax GRANULARITY 1
) ENGINE = ReplacingMergeTree ORDER BY (_env, event_id);
```

Append-only streams (`auth_attempts`, `events`) are a `ReplacingMergeTree`
keyed by the event id, with no version column: one row per event, never
updated. A second copy of an event, which only a poll that outlived its lease
could write, collapses at the next merge; add `FINAL` where an exact count
matters before then.

Mirror streams are `ReplacingMergeTree` versioned by `_updated_at`. Every
write inserts the row's next state, and the newest source timestamp wins at
merge time and under `FINAL`; that is the staleness gate, enforced by the
table whatever order writes land in. A deletion's tombstone carries the deletion time as its version, so it outranks every earlier version of the row. Each mirror table has a generated
`<table>_live` view — `workos.users_live`, `workos.organizations_live`, and so
on — that reads with `FINAL` and hides tombstoned rows. **Query the views.**
The base table keeps the newest version of each row, tombstones
(`_deleted_at`) included, for when you want to ask who left; older versions
survive only until the next merge. `workos.events` is where history is kept.

A field added upstream can never break ingest — it is queryable as
`JSONExtractString(_raw, 'field')` immediately, and promoting it to a typed
column is pure DDL, computed for the rows already there. A value a typed
column cannot cast projects as NULL, with the value still in `_raw`. Columns
marked for an index get a data-skipping index: a bloom filter for text, a
min-max index for timestamps and numbers.

## What it mirrors

Users, organizations, memberships, domains, invitations and sessions;
directories, directory users and groups; SSO connections; AuthKit groups,
roles, permissions and feature flags. Every event is archived verbatim to
`workos.events`, including event types this version has never heard of; the
`authentication.*` events are projected into `auth_attempts`.

## What it is not

The mirror is a **read model**: eventually consistent, usually within seconds.
Read from it for analytics, dashboards, lists and agents. Keep writes,
authorization decisions and post-signup checks on the WorkOS API. The sync is
one-directional; there is no write-back. ClickHouse is not where the mirror
tables are at their best: for an application that reads single rows by key,
use `@workos/sync-postgres` or `@workos/sync-mysql`, and if you want both, run
both; the cursors are kept per package.

## Testing

`pnpm test` runs the pure tests without a server. The database-backed suite
needs a ClickHouse with a Keeper, started from the config in
`test/clickhouse/`:

```sh
docker run -d --name clickhouse -p 127.0.0.1:8123:8123 \
  -v "$PWD/packages/sync-clickhouse/test/clickhouse/config.xml:/etc/clickhouse-server/config.d/sync.xml:ro" \
  -v "$PWD/packages/sync-clickhouse/test/clickhouse/users.xml:/etc/clickhouse-server/users.d/sync.xml:ro" \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:latest
TEST_CLICKHOUSE_URL=http://default@127.0.0.1:8123 pnpm test
```

Each test creates a database of its own and drops it on completion.
`pnpm test:database` fails when the URL is missing instead of skipping.

## Documentation

- [Getting started](https://github.com/workos/sync-engine/blob/main/docs/getting-started.md)
- [How it works](https://github.com/workos/sync-engine/blob/main/docs/how-it-works.md) — backfill, tail, reconcile, and why all three
- [Operations](https://github.com/workos/sync-engine/blob/main/docs/operations.md) — deployment, alerting, the 90-day retention cliff
- [Consistency](https://github.com/workos/sync-engine/blob/main/docs/consistency.md) — what the mirror does and does not guarantee

## License

MIT
