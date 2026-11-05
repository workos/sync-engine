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
   FROM workos.organization_memberships_live m
      JOIN workos.users_live u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations_live o ON o.id = m.organization_id AND o._env = m._env;
```

The `_live` views are generated next to the tables and hide deleted rows; the
base tables keep them as tombstones for when you want to ask who left.

## Destinations

One engine, one package per database. Each ships a CLI with the same
commands and a container image under the same name:

| Destination | Package                                           | Mirror                                                                 |
| ----------- | ------------------------------------------------- | ---------------------------------------------------------------------- |
| Postgres    | [`@workos/sync-postgres`](packages/sync-postgres) | A `workos` schema of tables with generated columns and `_live` views   |
| MySQL       | [`@workos/sync-mysql`](packages/sync-mysql)       | A `workos` database of tables with generated columns and `_live` views |

The rest of this page shows Postgres; every command and option below has a
MySQL counterpart, and each package's README is the reference for it.

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

For a hosted Postgres, including connection selection, TLS, and endpoint
verification, see [Hosted Postgres setup](docs/hosted-postgres.md). On a host
with a time limit, such as a scheduled function, see
[Serverless hosts](docs/serverless.md): every command but `listen` is bounded
and resumable.

The command reads its settings from the environment, and a flag overrides the
variable where both exist. Every variable, command and flag is documented in
the [`@workos/sync-postgres` README](packages/sync-postgres/README.md), which
ships with the package and so always describes the version you installed. The
`PostgresDestination` options are there too; the engine options (`streams`,
`extraColumns`, `hydrate`, `pollIntervalMs`, `lagWindowMs`, `maxEventsPerTick`,
`pageSize`, `logger`) are in the
[`@workos/sync-engine` README](packages/sync-engine/README.md).

## Integration

Although any accessible database is supported, we also have guides for integrating with the following platforms:

- [Neon](integrations/neon.md)

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
artifact into a stuck stream. Every mirror table also gets a `<table>_live`
view (`workos.users_live`) that is the table minus its tombstoned rows, so the
common query needs no `_deleted_at` filter.

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
