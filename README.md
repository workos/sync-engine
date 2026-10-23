# WorkOS Postgres Sync

Mirror WorkOS data into your own Postgres, and query it with SQL.

## The problem

Every app built on WorkOS eventually needs to ask something that spans two
systems. _Which enterprise seats have we provisioned that nobody uses? Which
organizations verified a domain and then never finished SCIM? Which accounts saw
a spike in failed sign-ins last night?_

None of those have an endpoint. Each is a join between data in WorkOS and data
in the customer's database. Through an API that means paginating one list,
holding it in memory, paginating another, and reconciling the two in application
code.

Against a mirror, it becomes one statement:

```sql
SELECT o.name AS organization, u.email, m.role_slug
   FROM workos.organization_memberships m
      JOIN workos.users u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations o ON o.id = m.organization_id AND o._env = m._env
   WHERE u._deleted_at IS NULL AND m._deleted_at IS NULL;
```

## Try it in 30 seconds

No credentials, no Docker, no network. Runs the whole lifecycle against a
stand-in WorkOS API and an embedded Postgres:

```bash
pnpm install
pnpm build
pnpm --filter workos-sync-postgres-example start
```

You will see the schema get created, existing data backfilled, live events
tailed, joins answered, and drift detected and repaired.

## Against a real environment

```bash
export DATABASE_URL=postgres://localhost/app
export WORKOS_API_KEY=sk_live_...

npx workos-sync-postgres init       # create the schema
npx workos-sync-postgres backfill   # load what already exists
npx workos-sync-postgres listen     # tail the Events API
```

The same command ships as a container image, one per destination, for
`linux/amd64` and `linux/arm64`:

```bash
docker run --rm -e DATABASE_URL -e WORKOS_API_KEY ghcr.io/workos/sync-postgres init
docker run --rm -e DATABASE_URL -e WORKOS_API_KEY ghcr.io/workos/sync-postgres backfill
docker run --rm -e DATABASE_URL -e WORKOS_API_KEY ghcr.io/workos/sync-postgres listen
```

Tags are `latest`, the version (e.g. `0.2.0`), `beta` for prereleases, and
`sha-<commit>`. To build the image yourself:

```bash
docker build --build-arg DESTINATION=postgres -t sync-postgres .
```

Or embed it in your app:

```ts
import { SyncEngine, PostgresDestination } from "@workos/sync-postgres";

const engine = new SyncEngine({
  destination: new PostgresDestination({
    databaseUrl: process.env.DATABASE_URL,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});

await engine.backfill();
await engine.listen();
```

## Configuration

The command reads its settings from the environment. Where a flag exists for
the same setting, the flag wins.

### Environment variables

| Variable                          | Default                             | What it does                                                                                                                 |
| --------------------------------- | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`                    | required                            | Postgres connection string.                                                                                                  |
| `WORKOS_API_KEY`                  | required                            | API key for the environment to mirror. One key, one environment, one process.                                                |
| `WORKOS_ENVIRONMENT_NAME`         | `production`                        | Label written to `_env` on every row. Must not change once you have data. Run one process per environment to mirror several. |
| `WORKOS_API_URL`                  | `https://api.workos.com`            | API base URL.                                                                                                                |
| `WORKOS_SYNC_SCHEMA`              | `workos`                            | Schema the mirror lives in, for DDL and queries alike.                                                                       |
| `WORKOS_SYNC_ID`                  | `default`                           | Identifier when several syncs share one database. Each keeps its own cursors.                                                |
| `WORKOS_SYNC_HYDRATE`             | users and organizations `on_create` | Re-fetch objects when their events arrive: `off`, `on_create`, `always`, or per stream as `users=always,organizations=off`.  |
| `WORKOS_SYNC_POLL_MS`             | `5000`                              | Idle wait between polls, in ms, when the last poll returned nothing.                                                         |
| `WORKOS_SYNC_LAG_MS`              | `5000`                              | Hold back events younger than this, in ms, before advancing the cursor.                                                      |
| `WORKOS_SYNC_MAX_EVENTS_PER_TICK` | one page (100)                      | Events to apply per poll, fetched across as many pages as needed.                                                            |
| `LOG_LEVEL`                       | `info`                              | `debug`, `info`, `warn`, `error` or `silent`. Logs are JSON lines on stdout.                                                 |

### Commands

| Command     | Flags         | What it does                                                                                                 |
| ----------- | ------------- | ------------------------------------------------------------------------------------------------------------ |
| `init`      | `--repair`    | Create the schema and bookkeeping tables. Idempotent; safe on every deploy. Exits 2 if a generated column has drifted from the catalog; `--repair` rebuilds it from `_raw`. |
| `backfill`  | `--resume`    | Load current state from the REST list endpoints. `--resume` continues from the last checkpoint.              |
| `listen`    |               | Poll the Events API continuously. Finishes the current batch on `SIGINT` or `SIGTERM`.                       |
| `tick`      |               | One poll, then exit. For a cron job instead of a long-running process.                                       |
| `reconcile` | `--repair`    | Diff ids against the API. Exits 2 on drift; `--repair` tombstones missed deletes and inserts missed creates. |
| `refresh`   |               | Re-list top-level streams to refresh fields that event payloads omit.                                        |
| `status`    |               | Cursor, time since the last successful poll, and per-stream row counts, tombstones and lag.                  |
| `redact`    | `--user <id>` | Erase one subject from the mirror and the event archive.                                                     |
| `streams`   |               | List what this build can mirror, with group, mode, and whether it is on by default.                          |

### Flags available for every command

| Flag                   | Overrides            | What it does                                               |
| ---------------------- | -------------------- | ---------------------------------------------------------- |
| `--database-url <url>` | `DATABASE_URL`       | Postgres connection string.                                |
| `--schema <name>`      | `WORKOS_SYNC_SCHEMA` | Target schema.                                             |
| `--streams <a,b,c>`    |                      | Restrict to specific streams. `events` is always included. |
| `--help`               |                      | Print usage.                                               |

Library options (`streams`, `extraColumns`, `hydrate`, `pollIntervalMs`,
`lagWindowMs`, `maxEventsPerTick`, `pageSize`, `logger`) are documented in the
[`@workos/sync-engine` README](packages/sync-engine/README.md), and the
`PostgresDestination` options (`databaseUrl` or `db`, `schema`, `syncId`,
`batchSize`) in the [`@workos/sync-postgres` README](packages/sync-postgres/README.md).

## How it works

|               | What it does                               | Why it exists                                                                                                                      |
| ------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- |
| **Backfill**  | Pages the REST list endpoints              | Events are retained 90 days. Everything older exists only here — a webhook-only integration starts life incomplete.                |
| **Tail**      | Polls the Events API from a durable cursor | Ordered, replayable, paced by the consumer. No public endpoint to expose.                                                          |
| **Reconcile** | Diffs the mirror against the API           | The only thing that finds a missed delete. A deleted object is absent from every list endpoint, so re-listing can never reveal it. |

The schema writes one column and generates the rest:

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
`_raw->>'field'` immediately, and promoting it to a typed column is pure DDL
computed from rows already present. There are **no foreign keys**: events
routinely arrive with children before parents, and an FK turns that ordering
artifact into a stuck stream.

## What it mirrors

Users, organizations, memberships, domains, invitations and sessions;
directories, directory users and groups; SSO connections; AuthKit groups,
roles, organization roles, permissions and feature flags. Every event is
archived to `workos.events`.

One table is worth calling out: the fifteen `authentication.*` event types have
**no REST endpoint anywhere in the API**. There is no way to list sign-ins, so
if you do not capture them as they happen that history is gone after 90 days.
Mirrored, "which accounts had five failed sign-ins in the last hour" is a
`GROUP BY`.

### Known Gaps

- Sessions are only listable per user and only while active
- there is no `dsync.updated` event, so a directory going `linked → invalid_credentials` is invisible until reconcile
- SSO profiles cannot be mirrored at all.

The mirror is a **read model** — eventually consistent, usually within seconds. Authorization decisions belong on the session token, and a just-created user should be read from the API response you already have. The sync is one-directional and there is no write-back.

## License

MIT
