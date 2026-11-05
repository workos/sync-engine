# @workos/sync-mysql

Mirror your WorkOS data into your own MySQL, and query it with SQL.

Backfills what already exists from the REST API, tails the Events API from a
durable cursor, and reconciles the two so drift is found rather than assumed.
The result is a `workos` database you can join against your own tables:

```sql
SELECT o.name AS organization, u.email, m.role_slug
   FROM workos.organization_memberships_live m
      JOIN workos.users_live u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations_live o ON o.id = m.organization_id AND o._env = m._env;
```

Requires MySQL 8.0.21 or later (`JSON_VALUE`, `INSERT ... AS new`); tested
against 8.4. MariaDB is not supported: its JSON functions differ.

## Install

```bash
npm install @workos/sync-mysql
```

This package ships the MySQL destination, the `workos-sync-mysql` command, and
re-exports the engine from
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine), so
it is the only package you need for MySQL.

## Command line

```bash
export DATABASE_URL=mysql://root@localhost:3306/app
export WORKOS_API_KEY=sk_live_...

npx workos-sync-mysql init       # create the database and bookkeeping tables
npx workos-sync-mysql backfill   # load everything that already exists
npx workos-sync-mysql listen     # tail the Events API, forever
```

| Command                                             | What it does                                                                                                                                                                                                                                                                                                                                                                                       |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init [--repair] [--print-sql]`                     | Create the database and bookkeeping tables. Idempotent; safe on every deploy. Exits 2 if a generated column has drifted from the catalog, and fails if a view under a generated `_live` name is not the generated shape; `--repair` rebuilds columns from `_raw` and replaces such views. `--print-sql` prints the DDL for a fresh database instead of running it; apply it, then run `init` once. |
| `backfill [--resume] [--max-pages <n>]`             | Load current state from the REST list endpoints. `--resume` continues each stream from its checkpoint and skips streams already complete. `--max-pages` stops after `n` pages across all streams, for a host with a time limit; run again with `--resume` until every stream reports `complete`.                                                                                                   |
| `listen [--health-port <n>]`                        | Poll the Events API continuously as the sole writer; waits if another listener holds the lease. Finishes the current batch on `SIGINT` or `SIGTERM`. With a port, serves `GET /healthz` and `/metrics` on it.                                                                                                                                                                                      |
| `tick`                                              | One poll, then exit. For a cron job instead of a long-running process. Exits 3 if a listener holds the lease.                                                                                                                                                                                                                                                                                      |
| `reconcile [--repair] [--resume] [--max-pages <n>]` | Diff ids against the API. Exits 2 on drift; `--repair` tombstones missed deletes and inserts missed creates. `--max-pages` stops starting streams once `n` pages have been fetched; run again with `--resume` until every stream reports `complete`.                                                                                                                                               |
| `refresh [--resume] [--max-pages <n>]`              | Re-list top-level streams from the REST API, replacing each row; `--max-pages` and `--resume` slice it like `backfill`, with which it shares checkpoints. Re-run `backfill` for per-parent streams.                                                                                                                                                                                                |
| `status`                                            | Cursor, time since the last successful poll, and per-stream row counts, tombstones and lag.                                                                                                                                                                                                                                                                                                        |
| `redact --user <id>`                                | Erase one subject from the mirror and the event archive.                                                                                                                                                                                                                                                                                                                                           |
| `rejected [--replay] [--id <event_id>]`             | Show events the tail quarantined; exits 2 if there are any. `--replay` re-applies them (one, with `--id`) once the cause is fixed.                                                                                                                                                                                                                                                                 |
| `streams`                                           | List what this build can mirror, with group, mode, and whether it is on by default.                                                                                                                                                                                                                                                                                                                |

Flags every command takes:

| Flag                   | Overrides            | What it does                                                   |
| ---------------------- | -------------------- | -------------------------------------------------------------- |
| `--database-url <url>` | `DATABASE_URL`       | MySQL connection string, `mysql://user:password@host:3306/db`. |
| `--schema <name>`      | `WORKOS_SYNC_SCHEMA` | The database the mirror lives in.                              |
| `--streams <a,b,c>`    |                      | Restrict to specific streams. `events` is always included.     |
| `--help`               |                      | Print usage.                                                   |

Rows are labelled `_env = 'production'` by default; set
`WORKOS_ENVIRONMENT_NAME` if the key is for something else. The label is
written to every row and must never change once you have data. To mirror
several environments into one database, run one process per key with a
different label each; cursors are kept per label, so they never collide.

The command reads its settings from the environment; where a flag exists for
the same setting, the flag wins.

| Variable                             | Default                             | What it does                                                                                                                                                  |
| ------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                       | required                            | MySQL connection string. The database in its path is not used; see `WORKOS_SYNC_SCHEMA`.                                                                      |
| `WORKOS_API_KEY`                     | required                            | API key for the environment to mirror. One key, one environment, one process; dedicate it to the sync (see Operations).                                       |
| `WORKOS_ENVIRONMENT_NAME`            | `production`                        | Label written to `_env` on every row. Must not change once you have data. Run one process per environment to mirror several.                                  |
| `WORKOS_API_URL`                     | `https://api.workos.com`            | API base URL.                                                                                                                                                 |
| `WORKOS_SYNC_SCHEMA`                 | `workos`                            | The database (MySQL also calls it a schema) the mirror lives in, created if missing. Every statement qualifies its tables with it.                            |
| `WORKOS_SYNC_ID`                     | `default`                           | Identifier when several syncs share one database. Each keeps its own cursors.                                                                                 |
| `WORKOS_SYNC_HYDRATE`                | `off`                               | Re-fetch objects when their events arrive: `off`, `on_create`, `always`, or per stream as `users=always,organizations=off`.                                   |
| `WORKOS_SYNC_POLL_MS`                | `5000`                              | Idle wait between polls, in ms, when the last poll returned nothing.                                                                                          |
| `WORKOS_SYNC_LAG_MS`                 | `5000`                              | Hold back events younger than this, in ms, before advancing the cursor.                                                                                       |
| `WORKOS_SYNC_MAX_EVENTS_PER_TICK`    | one page (100)                      | Events to apply per poll, fetched across as many pages as needed.                                                                                             |
| `WORKOS_SYNC_HEALTH_PORT`            | off (`8080` in the container image) | `listen` only: serve `GET /healthz` and `GET /metrics` on this port, from in-memory state, with no database query.                                            |
| `WORKOS_SYNC_HEALTH_HOST`            | `0.0.0.0`                           | Interface the health server binds.                                                                                                                            |
| `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` | `60000`                             | `/healthz` returns 503 once any environment's last successful poll is older than this.                                                                        |
| `WORKOS_SYNC_OTEL`                   | off                                 | `1` starts the OpenTelemetry Node SDK for the command, configured by the standard `OTEL_*` variables, and flushes it on exit. Every command, `tick` included. |
| `WORKOS_SYNC_LEASE_TTL_MS`           | `60000`                             | How long the tail lease outlives a crashed listener, in ms; at least 1. The most lag a crash can add before a replacement takes over.                         |
| `WORKOS_SYNC_MYSQL_SSL_CA`           | system trust store                  | CA certificate the database's certificate must chain to, as PEM or base64 of it. TLS is then required and verified against it.                                |
| `LOG_LEVEL`                          | `info`                              | `debug`, `info`, `warn`, `error` or `silent`. Logs are JSON lines on stdout.                                                                                  |

The user the sync connects as needs `CREATE` on the server the first time (to
create the database), and `CREATE`, `ALTER`, `INDEX`, `CREATE VIEW`,
`CREATE TEMPORARY TABLES`, `SELECT`, `INSERT`, `UPDATE` and `DELETE` on the
mirror database. If the database is created for you, the first privilege is
not needed.

On a host that gates DDL, such as a PlanetScale Vitess branch with safe
migrations on, apply the schema through the host's own change process and let
`init` find it:

```bash
npx workos-sync-mysql init --print-sql --schema <branch database> > schema.sql
# drop the first statement (the database is the branch), open a deploy request with the rest, then:
npx workos-sync-mysql init --schema <branch database>
```

Once the tables and views are in place, `init` runs no DDL at all: it checks
the live columns, records what it finds, and records the fingerprint. A later
catalog change that adds a column is the one thing that needs another deploy
request; `init` reports the missing column and the DDL for it.

`listen` with a health port answers `GET /healthz` with `200` while the tail
is polling and `503` once a poll is overdue, and `GET /metrics` in Prometheus
text format, both from memory. See
[Operations](https://github.com/workos/sync-engine/blob/main/docs/operations.md)
for the metric names and a Kubernetes probe snippet.

## Embed it

```ts
import { SyncEngine, MysqlDestination } from "@workos/sync-mysql";

const engine = new SyncEngine({
  destination: new MysqlDestination({
    databaseUrl: process.env.DATABASE_URL,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});

await engine.backfill();
await engine.listen(); // resolves when you call engine.stop()
```

Every method applies the schema first, so there is no separate `init()` to
call. To reuse an existing `mysql2` promise pool instead of a connection
string:

```ts
import mysql from "mysql2/promise";
import { fromPool, MysqlDestination, SESSION_SQL } from "@workos/sync-mysql";

const pool = mysql.createPool({ uri: process.env.DATABASE_URL, timezone: "Z" });
new MysqlDestination({ db: fromPool(pool) });
```

A pool you pass in must read and write `DATETIME` as UTC (`timezone: "Z"`)
and should run `SESSION_SQL` on every new connection, which puts the session
in UTC. The engine's timestamps are `DATETIME(6)`, which MySQL stores without
a zone; `_updated_at` comes from WorkOS in UTC and `_synced_at` from `NOW(6)`
in the session zone, and lag is the difference between them.

`MysqlDestination` options:

| Option          | Default   |                                                                                                                                                          |
| --------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `databaseUrl`   | —         | Connection string. Either this or `db`.                                                                                                                  |
| `sslCa`         | —         | PEM CA certificate the server's certificate must chain to. TLS is then required and verified against it. With `databaseUrl` only.                        |
| `db`            | —         | A pre-built client, e.g. `fromPool(pool)`. Not closed by the engine.                                                                                     |
| `schema`        | `workos`  | The database the mirror lives in, for DDL and DML alike. Created if missing.                                                                             |
| `syncId`        | `default` | Identifier when several syncs share one database.                                                                                                        |
| `batchSize`     | `500`     | Rows per statement.                                                                                                                                      |
| `repairViews`   | `false`   | On `init()`, replace a view under a generated `_live` name that is not the generated shape. Off, `init()` fails and names it.                            |
| `repairColumns` | `false`   | On `init()`, rebuild generated columns that no longer match the catalog from `_raw`. Off, drift is reported and `init` withholds the schema fingerprint. |

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
  _env        varchar(191) NOT NULL,
  _raw        json NOT NULL,          -- the only column the engine writes
  _updated_at datetime(6) NOT NULL,   -- source time, UTC; the staleness gate compares this
  _synced_at  datetime(6) NOT NULL,   -- write time, UTC; lag is the difference
  _deleted_at datetime(6),            -- tombstone
  id          varchar(191) GENERATED ALWAYS AS (json_value(_raw, '$."id"' returning char(191) null on error)) STORED,
  email       text GENERATED ALWAYS AS (json_value(_raw, '$."email"' returning char(65535) null on error)) STORED,
  metadata    json GENERATED ALWAYS AS (json_extract(_raw, '$."metadata"')) STORED,
  created_at  datetime(6) GENERATED ALWAYS AS (/* ISO-8601 parsed, range-checked, in UTC */) STORED,
  PRIMARY KEY (_env, id),
  INDEX users_email_idx (email(255))
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
```

A field added upstream can never break ingest — it is queryable as
`_raw->>'$.field'` immediately, and promoting it to a typed column is pure
DDL computed from the rows already there. There are no foreign keys: events
routinely arrive with children before parents, and a constraint would turn
that ordering artifact into a stuck stream. Deletes are tombstones
(`_deleted_at`), never row removals.

Three things are MySQL's rather than Postgres's:

- **A value a typed column cannot cast projects as NULL**, with the value
  still in `_raw`. Where Postgres would reject the row and quarantine the
  event, `NULL ON ERROR` keeps it flowing. A JSON `null` is a SQL `NULL`,
  not the text `null`.
- **Key columns are `VARCHAR(191)`; other text columns are `TEXT`** with a
  255-character index prefix, so every key and index fits InnoDB's limits.
  A key longer than 191 characters projects as NULL, which the primary key
  refuses, so the event is quarantined rather than silently rekeyed.
- **Timestamps are parsed by an inline expression.** MySQL's own conversions
  do not understand the `T` and `Z` of ISO-8601, so the generated column
  takes the string apart, range-checks every field, applies the UTC offset,
  and builds a `DATETIME(6)`. Anything that is not an instant, such as
  `2026-02-30`, is NULL.

Every mirror table also gets a generated `<table>_live` view —
`workos.users_live`, `workos.organizations_live`, and so on — that is the table
without its tombstoned rows. Query the view for live data and the base table
when you want deleted rows too. The views are re-created whenever the schema
is applied, so they pick up any column you promote later. The append-only logs
(`events`, `auth_attempts`) have no tombstones and no view.

Booleans are `TINYINT(1)`, so they read back as `1` and `0`. Timestamps are
`DATETIME(6)` in UTC; compare them with `UTC_TIMESTAMP(6)`, or put your
session in UTC, rather than with `NOW()` in a local zone.

## How writes work

MySQL's `ON DUPLICATE KEY UPDATE` has no `WHERE` and no `RETURNING`, and its
only JSON merge is deep where event writes must be shallow. So each batch is
a transaction that locks the stored rows (`SELECT ... FOR UPDATE`), decides
every row in Node — the staleness gate, the merge, whether anything changed —
and writes only what passed. Under REPEATABLE READ a missing row's gap is
locked too, so a competing writer cannot slip a newer row in between; two
writers inserting into one gap deadlock, which the engine retries. One
writer per environment, which is what the lease provides, never sees any of
this.

## What it is not

The mirror is a **read model**: eventually consistent, usually within seconds.
Read from it for joins, lists, dashboards, analytics and agents. Keep writes,
authorization decisions and post-signup checks on the WorkOS API — a
just-created user should be read from the API response you already have. The
sync is one-directional; there is no write-back.

## Testing

`pnpm test` runs the pure tests without a server. The database-backed suite
needs a MySQL 8 with a user that may create and drop databases:

```sh
docker run -d --name mysql -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes mysql:8.4
TEST_MYSQL_URL=mysql://root@127.0.0.1:3306 pnpm test
```

Each test creates a database of its own and drops it on completion.
`pnpm test:database` fails when the URL is missing instead of skipping.

## Documentation

- [Getting started](https://github.com/workos/sync-engine/blob/main/docs/getting-started.md)
- [How it works](https://github.com/workos/sync-engine/blob/main/docs/how-it-works.md) — backfill, tail, reconcile, and why all three
- [Schema](https://github.com/workos/sync-engine/blob/main/docs/schema.md) and [example queries](https://github.com/workos/sync-engine/blob/main/docs/queries.md) (written for Postgres; the tables and views are the same here)
- [Operations](https://github.com/workos/sync-engine/blob/main/docs/operations.md) — deployment, alerting, the 90-day retention cliff
- [Consistency](https://github.com/workos/sync-engine/blob/main/docs/consistency.md) — what the mirror does and does not guarantee

## License

MIT
