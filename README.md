# WorkOS Postgres Sync

Mirror WorkOS data into your own Postgres, and query it with SQL.

> **Prototype.** This is a working proof of concept built to test whether a
> first-party sync engine is worth shipping — not a supported product. It runs,
> it is tested, and it has been verified against the live WorkOS API.
>
> **📄 [Read the full proposal →](https://drop.workos.site/postgres-sync-proposal-3evjzdgzgv)**
> The pitch, the design, the API, what building it found in our own API, and the
> open questions. Start there if you want the argument rather than the code.

---

## The problem

Every app built on WorkOS eventually needs to ask something that spans two
systems. *Which enterprise seats have we provisioned that nobody uses? Which
organizations verified a domain and then never finished SCIM? Which accounts saw
a spike in failed sign-ins last night?*

None of those have an endpoint. Each is a join between data in WorkOS and data
in the customer's database. Through an API that means paginating one list,
holding it in memory, paginating another, and reconciling the two in application
code. Against a mirror it is one statement:

```sql
select o.name as organization, u.email, m.role_slug
from workos.organization_memberships m
join workos.users u on u.id = m.user_id and u._env = m._env
join workos.organizations o on o.id = m.organization_id and o._env = m._env
where u._deleted_at is null and m._deleted_at is null;
```

## Try it in 30 seconds

No credentials, no Docker, no network. Runs the whole lifecycle against a
stand-in WorkOS API and an embedded Postgres:

```bash
pnpm install
pnpm --filter workos-pg-sync-example start
```

You will see the schema get created, existing data backfilled, live events
tailed, joins answered, and drift detected and repaired.

## Against a real environment

```bash
export DATABASE_URL=postgres://localhost/app
export WORKOS_API_KEY=sk_live_...

npx workos-pg-sync init       # create the schema
npx workos-pg-sync backfill   # load what already exists
npx workos-pg-sync listen     # tail the Events API
```

Or embed it — most deployments would, since it then uses the connection pool
that already exists:

```ts
import { SyncEngine } from '@workos/pg-sync'

const engine = new SyncEngine({
  databaseUrl: process.env.DATABASE_URL,
  environments: [{ name: 'production', apiKey: process.env.WORKOS_API_KEY! }],
  hydrate: { users: 'on_create' },
})

await engine.backfill()
await engine.listen()
```

## How it works

Three subsystems, and all three are required. This is what makes the
hand-rolled version harder than it looks.

| | What it does | Why it exists |
|---|---|---|
| **Backfill** | Pages the REST list endpoints | Events are retained 90 days. Everything older exists only here — a webhook-only integration starts life incomplete. |
| **Tail** | Polls the Events API from a durable cursor | Ordered, replayable, paced by the consumer. No public endpoint to expose. |
| **Reconcile** | Diffs the mirror against the API | The only thing that finds a missed delete. A deleted object is absent from every list endpoint, so re-listing can never reveal it. |

The schema writes one column and generates the rest:

```sql
create table workos.users (
  _env        text not null,
  _raw        jsonb not null,        -- the only column the engine writes
  _updated_at timestamptz not null,  -- source time; the staleness gate compares this
  _synced_at  timestamptz not null,  -- write time; lag is the difference
  _deleted_at timestamptz,           -- tombstone
  id          text  generated always as ((_raw->>'id')) stored,
  email       text  generated always as ((_raw->>'email')) stored,
  external_id text  generated always as ((_raw->>'external_id')) stored,
  metadata    jsonb generated always as ((_raw->'metadata')) stored,
  primary key (_env, id)
);
```

A field added upstream can never break ingest — it is queryable as
`_raw->>'field'` immediately, and promoting it to a typed column is pure DDL
computed from rows already present. There are **no foreign keys**: events
routinely arrive with children before parents, and an FK turns that ordering
artifact into a stuck stream.

## What it mirrors

Users, organizations, memberships, domains, invitations and sessions;
directories, directory users and groups; SSO connections; roles, permissions and
feature flags. Every event is archived to `workos.events`.

One table is worth calling out: the fifteen `authentication.*` event types have
**no REST endpoint anywhere in the API**. There is no way to list sign-ins, so
if you do not capture them as they happen that history is gone after 90 days.
Mirrored, "which accounts had five failed sign-ins in the last hour" is a
`GROUP BY`.

## What building it found in our own API

Three issues, all confirmed against production, all actionable independently of
whether this ships. Detail is in the
[proposal](https://drop.workos.site/postgres-sync-proposal-3evjzdgzgv#findings).

1. **`GET /events` requires the `events` filter.** The published OpenAPI spec
   marks it optional; the live endpoint returns `400 invalid_events_request`
   without it. A consumer cannot subscribe to "everything", so a newly shipped
   event type does not reach any customer's sync until they redeploy.
2. **`order` has no effect, and `order=desc&limit=1` returns the *oldest*
   event.** `asc`, `desc` and omitting it return byte-identical pages. The
   natural way to record "start from now" therefore sets the cursor to the start
   of the retention window and silently replays 90 days.
3. **Event payloads are strict subsets of the REST objects.** A live
   `user.updated` carries neither `external_id` nor `metadata`, so a whole-row
   upsert nulls the exact column customers join on.

## Status

Verified end to end against the live API on a disposable environment, and
read-only against one with real data. 92 tests run against real Postgres via
PGlite — actual generated columns, `xmax`, jsonb operators and immutability
rules, with no Docker and no setup.

Known gaps, stated plainly: sessions are only listable per user and only while
active; there is no `dsync.updated` event, so a directory going
`linked → invalid_credentials` is invisible until reconcile; SSO profiles cannot
be mirrored at all. The mirror is a **read model** — eventually consistent,
usually within seconds. Authorization decisions belong on the session token, and
a just-created user should be read from the API response you already have. The
sync is one-directional and there is no write-back.

## Repo layout

```
packages/pg-sync/   the library and CLI
examples/basic/         runnable demo + the live-API test scripts
docs/                   getting started, how it works, schema, queries,
                        operations, consistency
docs/workos-site/       DRAFT docs page, blog post and changelog entry —
                        unreviewed copy, not approved for publication
PLAN.md                 the research and the argument behind the design
```

## License

MIT
