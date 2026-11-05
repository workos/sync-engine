# @workos/sync-mongodb

Mirror your WorkOS data into your own MongoDB, and query it next to your
application's collections.

Backfills what already exists from the REST API, tails the Events API from a
durable cursor, and reconciles the two so drift is found rather than assumed.
The result is a set of `workos_*` collections in your database that you can
`$lookup` against your own:

```js
db.workos_organization_memberships_live.aggregate([
  { $match: { _env: "production", "_raw.organization_id": "org_01H..." } },
  {
    $lookup: {
      from: "workos_users_live",
      localField: "_raw.user_id",
      foreignField: "_raw.id",
      as: "user",
    },
  },
  { $unwind: "$user" },
  { $project: { email: "$user._raw.email", role: "$_raw.role.slug" } },
]);
```

The `_live` views hide deleted documents; the base collections keep them as
tombstones for when you want to ask who left.

## Install

```bash
npm install @workos/sync-mongodb
```

This package ships the MongoDB destination, the `workos-sync-mongodb`
command, and re-exports the engine from
[`@workos/sync-engine`](https://www.npmjs.com/package/@workos/sync-engine),
so it is the only package you need for MongoDB. It needs MongoDB 5.0 or
later: update pipelines with `$$NOW`, `$expr` in update filters, and `.` and
`$` inside the field names of stored payloads (directory users' SCIM
attributes carry both) all arrived in 5.0. Atlas, a replica set and a
standalone `mongod` are all fine; nothing here needs a transaction.

## Command line

```bash
export DATABASE_URL=mongodb://localhost:27017/app
export WORKOS_API_KEY=sk_live_...

npx workos-sync-mongodb init       # create the collections, indexes and views
npx workos-sync-mongodb backfill   # load everything that already exists
npx workos-sync-mongodb listen     # tail the Events API, forever
```

| Command                                             | What it does                                                                                                                                                                                                                                                                                                                                                         |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `init [--repair]`                                   | Create the collections, indexes, `_live` views and bookkeeping. Idempotent; safe on every deploy. Fails if a generated `_live` view has a pipeline that is not the generated one; `--repair` redefines it. Reports indexes the catalog no longer calls for, and never drops them.                                                                                    |
| `backfill [--resume] [--max-pages <n>]`             | Load current state from the REST list endpoints. `--resume` continues each stream from its checkpoint and skips streams already complete. `--max-pages` stops after `n` pages across all streams, for a host with a time limit; run again with `--resume` until every stream reports `complete`.                                                                     |
| `listen [--health-port <n>]`                        | Poll the Events API continuously as the sole writer; waits if another listener holds the lease. Finishes the current batch on `SIGINT` or `SIGTERM`. With a port, serves `GET /healthz` and `/metrics` on it.                                                                                                                                                        |
| `tick`                                              | One poll, then exit. For a cron job instead of a long-running process. Exits 3 if a listener holds the lease.                                                                                                                                                                                                                                                        |
| `reconcile [--repair] [--resume] [--max-pages <n>]` | Diff ids against the API. Exits 2 on drift; `--repair` tombstones missed deletes and inserts missed creates. `--max-pages` stops starting streams once `n` pages have been fetched, for a host with a time limit; a stream that starts runs to its last page, since a diff needs the whole listing. Run again with `--resume` until every stream reports `complete`. |
| `refresh [--resume] [--max-pages <n>]`              | Re-list top-level streams from the REST API, replacing each document; `--max-pages` and `--resume` slice it like `backfill`, with which it shares checkpoints. Re-run `backfill` for per-parent streams.                                                                                                                                                             |
| `status`                                            | Cursor, time since the last successful poll, and per-stream document counts, tombstones and lag.                                                                                                                                                                                                                                                                     |
| `redact --user <id>`                                | Erase one subject from the mirror, the event archive and the quarantine.                                                                                                                                                                                                                                                                                             |
| `rejected [--replay] [--id <event_id>]`             | Show events the tail quarantined; exits 2 if there are any. `--replay` re-applies them (one, with `--id`) once the cause is fixed.                                                                                                                                                                                                                                   |
| `streams`                                           | List what this build can mirror, with group, mode, and whether it is on by default.                                                                                                                                                                                                                                                                                  |

Flags every command takes:

| Flag                   | Overrides              | What it does                                                                            |
| ---------------------- | ---------------------- | --------------------------------------------------------------------------------------- |
| `--database-url <url>` | `DATABASE_URL`         | MongoDB connection string.                                                              |
| `--database <name>`    | `WORKOS_SYNC_DATABASE` | Database the mirror lives in. Defaults to the one the connection string names.          |
| `--prefix <prefix>`    | `WORKOS_SYNC_PREFIX`   | Collection prefix for the mirror, the views and the bookkeeping. Defaults to `workos_`. |
| `--streams <a,b,c>`    |                        | Restrict to specific streams. `events` is always included.                              |
| `--help`               |                        | Print usage.                                                                            |

Documents are labelled `_env: 'production'` by default; set
`WORKOS_ENVIRONMENT_NAME` if the key is for something else. The label is
written to every document and must never change once you have data. To mirror
several environments into one database, run one process per key with a
different label each; cursors are kept per label, so they never collide.

The command reads its settings from the environment; where a flag exists for
the same setting, the flag wins.

| Variable                             | Default                             | What it does                                                                                                                                                  |
| ------------------------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                       | required                            | MongoDB connection string (`mongodb://` or `mongodb+srv://`).                                                                                                 |
| `WORKOS_API_KEY`                     | required                            | API key for the environment to mirror. One key, one environment, one process; dedicate it to the sync.                                                        |
| `WORKOS_ENVIRONMENT_NAME`            | `production`                        | Label written to `_env` on every document. Must not change once you have data. Run one process per environment to mirror several.                             |
| `WORKOS_API_URL`                     | `https://api.workos.com`            | API base URL.                                                                                                                                                 |
| `WORKOS_SYNC_DATABASE`               | from the connection string          | Database the mirror lives in; `workos` when the connection string names none.                                                                                 |
| `WORKOS_SYNC_PREFIX`                 | `workos_`                           | Collection prefix. Letters, digits and underscores.                                                                                                           |
| `WORKOS_SYNC_ID`                     | `default`                           | Identifier when several syncs share one database. Each keeps its own cursors, lease and quarantine.                                                           |
| `WORKOS_SYNC_HYDRATE`                | `off`                               | Re-fetch objects when their events arrive: `off`, `on_create`, `always`, or per stream as `users=always,organizations=off`.                                   |
| `WORKOS_SYNC_POLL_MS`                | `5000`                              | Idle wait between polls, in ms, when the last poll returned nothing.                                                                                          |
| `WORKOS_SYNC_LAG_MS`                 | `5000`                              | Hold back events younger than this, in ms, before advancing the cursor.                                                                                       |
| `WORKOS_SYNC_MAX_EVENTS_PER_TICK`    | one page (100)                      | Events to apply per poll, fetched across as many pages as needed.                                                                                             |
| `WORKOS_SYNC_HEALTH_PORT`            | off (`8080` in the container image) | `listen` only: serve `GET /healthz` and `GET /metrics` on this port, from in-memory state, with no database query.                                            |
| `WORKOS_SYNC_HEALTH_HOST`            | `0.0.0.0`                           | Interface the health server binds.                                                                                                                            |
| `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` | `60000`                             | `/healthz` returns 503 once any environment's last successful poll is older than this.                                                                        |
| `WORKOS_SYNC_OTEL`                   | off                                 | `1` starts the OpenTelemetry Node SDK for the command, configured by the standard `OTEL_*` variables, and flushes it on exit. Every command, `tick` included. |
| `WORKOS_SYNC_LEASE_TTL_MS`           | `60000`                             | How long the tail lease outlives a crashed listener, in ms; at least 1. The most lag a crash can add before a replacement takes over.                         |
| `WORKOS_SYNC_MONGODB_TLS_CA`         | system trust store                  | CA certificate the database's certificate must chain to, as PEM or base64 of it. TLS is then required and verified against it.                                |
| `LOG_LEVEL`                          | `info`                              | `debug`, `info`, `warn`, `error` or `silent`. Logs are JSON lines on stdout.                                                                                  |

`listen` with a health port answers `GET /healthz` with `200` while the tail
is polling (and while it is still starting) and `503` once a poll is overdue
or the engine has stopped, and `GET /metrics` in Prometheus text format. Both
are served from memory, with no database query, so a probe keeps working when
the database does not. See
[Operations](https://github.com/workos/sync-engine/blob/main/docs/operations.md)
for the metric names and a Kubernetes probe snippet.

## Embed it

Most deployments embed the engine in a service that already has a
`MongoClient`:

```ts
import { MongoClient } from "mongodb";
import { SyncEngine, MongoDestination } from "@workos/sync-mongodb";

const client = new MongoClient(process.env.DATABASE_URL!);
const engine = new SyncEngine({
  destination: new MongoDestination({ client, database: "app" }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});

await engine.backfill();
await engine.listen();
```

A pre-built `client` is never closed by the destination. Pass `url` instead
and the destination opens and closes a client of its own.

| Option        | Default                   | What it does                                                                                                |
| ------------- | ------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `url`         |                           | Connection string. Either this or `client`.                                                                 |
| `client`      |                           | A pre-built `MongoClient`, e.g. the application's own.                                                      |
| `sslCa`       |                           | PEM CA certificate(s) the server must chain to; TLS is then required. Only with `url`.                      |
| `database`    | from `url`, else `workos` | Database the mirror lives in.                                                                               |
| `prefix`      | `workos_`                 | Collection prefix.                                                                                          |
| `syncId`      | `default`                 | Identifier when several syncs share one database.                                                           |
| `batchSize`   | `500`                     | Documents per bulk write.                                                                                   |
| `repairViews` | `false`                   | On `init()`, redefine a generated `_live` view whose pipeline is not the generated one, instead of failing. |

The engine options (`streams`, `extraColumns`, `hydrate`, `pollIntervalMs`,
`lagWindowMs`, `maxEventsPerTick`, `pageSize`, `logger`) are in the
[`@workos/sync-engine` README](https://github.com/workos/sync-engine/blob/main/packages/sync-engine/README.md).

## Querying the mirror

Every stream becomes a collection named after it with the prefix in front,
and every document in it has the same shape:

```js
{
  _id: ObjectId("..."),
  _env: "production",          // the environment label
  _raw: { id: "user_01H...", email: "ada@example.com", ... },  // the object, verbatim
  _updated_at: ISODate("..."), // source time; the staleness gate compares this
  _synced_at: ISODate("..."),  // write time, by the server's clock; lag is the difference
  _deleted_at: null            // tombstone; absent on append-only streams
}
```

`_raw` is the WorkOS object as the API returned it, timestamps included as
ISO-8601 strings. There are no projected fields to keep in step with it:
query `_raw.<field>` directly, and sort ISO-8601 strings lexically, which is
chronological order.

```js
// Users of one organization, with their role.
db.workos_organization_memberships_live.find(
  { _env: "production", "_raw.organization_id": "org_01H..." },
  { "_raw.user_id": 1, "_raw.role.slug": 1 },
);

// Failed sign-ins in the last hour. authentication.* events have no REST
// endpoint; this collection is the only record of them.
db.workos_auth_attempts.countDocuments({
  _env: "production",
  "_raw.status": "failed",
  "_raw.created_at": { $gt: new Date(Date.now() - 3600_000).toISOString() },
});

// Users whose domain verification lapsed, joined to your own accounts.
db.workos_organization_domains_live.aggregate([
  { $match: { _env: "production", "_raw.state": "failed" } },
  {
    $lookup: {
      from: "accounts",
      localField: "_raw.organization_id",
      foreignField: "workos_org_id",
      as: "account",
    },
  },
]);
```

What the Postgres destination calls a column is an index here. Every stream's
natural key is a unique index on `_env` plus the key's `_raw.<path>`, every
column the catalog marks `index: true` is a compound index on `_env` and
`_raw.<path>`, and `extraColumns` add indexes the same way:

```ts
new SyncEngine({
  destination,
  environments,
  extraColumns: {
    users: [
      {
        name: "region",
        type: "text",
        path: ["metadata", "region"],
        index: true,
      },
    ],
  },
});
```

The `type` is accepted for parity with the other destinations and ignored. An
index the catalog no longer calls for is reported by `init` as an orphan and
left in place.

Each mirror collection has a `<collection>_live` view next to it —
`workos_users_live` — that is the collection without its tombstones, so the
common query needs no `_deleted_at` filter. Append-only streams (`events`,
`auth_attempts`) have no tombstones and no view.

There are three bookkeeping collections. `workos__sync` holds the
events-tail lease and every cursor for an environment in one document per
`(sync_id, env)`, which is what lets the tail's checkpoint be a single
conditional update — the holder, the lease's expiry by the server's clock and
the cursor it expects are all in the filter — without a multi-document
transaction, so a standalone `mongod` is as safe a target as a replica set.
`workos__rejected` is the quarantine for events the tail could not apply, and
`workos__migrations` records which catalogs have been applied so `init` is one
query when nothing changed.

## Testing

`pnpm test` runs against an embedded `mongod` from mongodb-memory-server,
downloaded on first run and cached. Set `TEST_MONGODB_URL` to run the same
suite against a server of your own instead:

```sh
TEST_MONGODB_URL=mongodb://127.0.0.1:27017 pnpm --filter @workos/sync-mongodb test
```

Each test creates a database of its own and drops it on completion.

## License

MIT
