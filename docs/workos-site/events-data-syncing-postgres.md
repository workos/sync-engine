<!--
Proposed page for the WorkOS docs site.

Path:           /docs/events/data-syncing/postgres
Sidebar label:  Syncing to Postgres
Description:    A step-by-step guide on how to mirror your WorkOS data into your own Postgres database.
Placement:      Under Events and webhooks → Data syncing, as a fourth page after
                "Data reconciliation". It is a sync strategy, not an integration,
                so it belongs beside the events API and webhooks guides rather
                than under /docs/integrations (which is IdP setup) or /docs/sdks.

Two companion edits are proposed at the bottom of this file:
  - an added row on the Data syncing overview comparison table
  - an "Up next" card on the data-reconciliation page
-->

# Sync data to Postgres

The sync engine mirrors your WorkOS data into a Postgres database you control.
Once mirrored, users, organizations, memberships, and directory data are
ordinary tables that you can query with SQL and join to your own application
data.

This guide takes you from an empty database to a mirror that stays current.

## What you'll build

In this guide, we will walk you through everything needed to keep a Postgres
mirror of your WorkOS data:

- Create the schema in your database
- Backfill the objects that already exist
- Tail the events API to stay current
- Reconcile the mirror to detect and repair drift

## Before getting started

To get the most out of this guide, you'll need:

- A [WorkOS account](https://dashboard.workos.com/)
- A Postgres database, version 14 or newer
- Node.js 20 or newer

***

## (1) Install the sync engine

:::code-group{title="Install the sync engine"}

```bash language="js" title="npm" tab="js-1"
npm install @workos/pg-sync
```

```bash language="js" title="Yarn" tab="js-2"
yarn add @workos/pg-sync
```

```bash language="js" title="pnpm" tab="js-3"
pnpm add @workos/pg-sync
```

:::

The package includes both a library and a `workos-pg-sync` command.

### Set secrets

To make calls to WorkOS, provide the API key for the environment you want to
mirror, along with the connection string for your database. Store these as
managed secrets and pass them as environment variables.

```plain title="Environment variables"
DATABASE_URL='postgres://localhost:5432/app'
WORKOS_API_KEY='sk_example_123456789'
```

> Each API key is scoped to a single environment. To mirror both staging and
> production, provide a key for each — see [Mirror multiple environments](#mirror-multiple-environments).

***

## (2) Create the schema

```bash title="Terminal"
npx workos-pg-sync init
```

This creates a `workos` schema containing one table per synced object. The
command is safe to run repeatedly: the schema is fingerprinted, so a second run
with no changes does nothing.

Each table stores the object exactly as WorkOS returned it in a `_raw` column,
and projects typed columns out of it:

```sql title="workos.users"
create table workos.users (
  _env        text not null,
  _raw        jsonb not null,
  _updated_at timestamptz not null,
  _synced_at  timestamptz not null,
  _deleted_at timestamptz,
  id          text generated always as ((_raw->>'id')) stored,
  email       text generated always as ((_raw->>'email')) stored,
  external_id text generated always as ((_raw->>'external_id')) stored,
  metadata    jsonb generated always as ((_raw->'metadata')) stored,
  primary key (_env, id)
);
```

Because `_raw` is the only column ever written, a new field added to the WorkOS
API cannot break your sync. It is queryable as `_raw->>'field'` immediately.

***

## (3) Backfill existing data

```bash title="Terminal"
npx workos-pg-sync backfill
```

Backfill pages the list endpoints and writes every object that currently
exists. Before fetching the first page it records your position in the event
stream, so anything that changes while backfill runs is applied afterwards
rather than missed.

> The events API returns events up to 90 days old. Objects older than that
> exist only through the list endpoints, which is why backfill is a required
> step rather than an optional one.

Backfill checkpoints after each page. If it is interrupted, resume it:

```bash title="Terminal"
npx workos-pg-sync backfill --resume
```

Large directories take time. The `/directory_users` endpoint is limited to 4
requests per second **per directory**, and that limit does not aggregate across
directories. The engine paces itself to stay within it.

***

## (4) Start syncing

```bash title="Terminal"
npx workos-pg-sync listen
```

The engine now polls the [events API](https://workos.com/docs/reference/events)
from its stored cursor and applies each change as it arrives. Run it as a
long-lived service.

To embed it in an existing service instead of running a separate process:

```js title="Embedding the sync engine"
import { SyncEngine, PostgresDestination } from '@workos/pg-sync'

const engine = new SyncEngine({
  destination: new PostgresDestination({ databaseUrl: process.env.DATABASE_URL }),
  environments: [{ name: 'production', apiKey: process.env.WORKOS_API_KEY }],
})

await engine.init()
await engine.backfill()
await engine.listen()
```

### Keep the cursor moving

The engine stores its cursor in `workos._sync_state` and advances it only after
the writes it covers are committed. A process that dies mid-batch replays that
batch on restart rather than skipping it.

Because the cursor is a position in a 90-day window, a sync that stops for long
enough cannot be replayed. Monitor the cursor age:

```bash title="Terminal"
npx workos-pg-sync status
```

***

## (5) Query the mirror

Two conventions apply to every query.

Filter `_deleted_at is null` for live rows: deleted objects are tombstoned
rather than removed, so you can still see what was deleted and when. Join on
`_env` as well as the object id, because ids are unique within an environment
and one database can mirror several.

```sql title="Members and roles by organization"
select o.name as organization, u.email, m.role_slug
from workos.organization_memberships m
join workos.users u on u.id = m.user_id and u._env = m._env
join workos.organizations o on o.id = m.organization_id and o._env = m._env
where u._deleted_at is null and m._deleted_at is null
order by o.name, u.email;
```

The mirror is most useful joined to data WorkOS does not have. Set an
`external_id` on your WorkOS users and it becomes the join key:

```sql title="Identity data joined to product usage"
select u.email,
       u.metadata->>'plan' as plan,
       count(s.id) as sessions_30d
from workos.users u
join app.accounts a on a.workos_user_id = u.external_id
left join app.sessions s on s.account_id = a.id
  and s.created_at > now() - interval '30 days'
where u._deleted_at is null
group by u.email, u.metadata->>'plan';
```

Authentication events are worth calling out. They have no REST endpoint
anywhere in the API, so once mirrored the sign-in history exists only in your
database:

```sql title="Accounts with repeated sign-in failures"
select email, count(*) as failures, count(distinct ip_address) as distinct_ips
from workos.auth_attempts
where status = 'failed' and created_at > now() - interval '1 hour'
group by email
having count(*) >= 5;
```

***

## (6) Reconcile

Even with an ordered, replayable event stream, an app can drift out of sync — a
bug in event processing, or a deploy that wrote bad data. Reconciliation
compares the mirror against the WorkOS API and reports the differences.

```bash title="Terminal"
npx workos-pg-sync reconcile
```

The command reports two numbers per object type, and they mean different
things. Rows present locally but absent upstream are missed deletes; rows
present upstream but absent locally are missed creates.

> A missed delete cannot be found any other way. A deleted object is not
> returned by any list endpoint, so re-listing will never reveal that it is
> gone. Only a set difference finds it.

To repair what it finds:

```bash title="Terminal"
npx workos-pg-sync reconcile --repair
```

Reconciliation also refreshes fields that event payloads do not carry. Weekly
is a reasonable cadence, plus a run after any incident that affected the sync.

***

## Mirror multiple environments

A workspace can contain many environments, and environment names repeat across
projects. Give each one a stable label of your own — it is written to every row
as `_env` and should not change once you have data.

```plain title="Environment variables"
WORKOS_ENVIRONMENTS='production=sk_live_123,staging=sk_test_456'
```

Each environment has its own cursor. Cursors are not comparable across
environments, and neither are object ids, which is why `_env` is part of every
primary key.

***

## Best practices

### Treat the mirror as a read model

The mirror is eventually consistent, typically within seconds. Read from it for
lists, joins, aggregates, dashboards, and analytics. Continue to read from the
session token for authorization decisions, and from the API response you
already have for objects you just created — a new user may not have reached the
mirror yet.

The sync is one-directional. Send writes to the WorkOS API and let them flow
back through the event stream.

### Let events merge, not replace

Event payloads are subsets of the corresponding API objects. A `user.updated`
event does not include `external_id`, `metadata`, `last_sign_in_at`, or
`locale`. The engine merges event data into the stored object rather than
replacing it, so those fields survive.

To populate them the moment an object is created rather than at the next
reconcile, enable hydration:

```js title="Hydrate new objects on creation"
const engine = new SyncEngine({
  destination: new PostgresDestination({ databaseUrl: process.env.DATABASE_URL }),
  environments: [{ name: 'production', apiKey: process.env.WORKOS_API_KEY }],
  hydrate: { users: 'on_create', organizations: 'on_create' },
})
```

### Alert on cursor age, not error rate

A stalled sync produces no errors. The tables simply stop changing while every
query continues to return plausible results. Alert when `cursorAgeDays` exceeds
a few hours, well before the 90-day retention limit makes replay impossible.

### Use webhooks as a trigger, not a source

If you want lower latency than the poll interval, configure a webhook and use a
verified delivery to prompt an immediate poll. State still flows through the
ordered events API, so you keep the ordering and replay guarantees.

```js title="Webhook as a trigger"
import { verifyWebhook } from '@workos/pg-sync'

app.post('/workos/webhook', async (req, res) => {
  verifyWebhook({
    payload: req.rawBody,
    signature: req.header('WorkOS-Signature'),
    secret: process.env.WORKOS_WEBHOOK_SECRET,
  })
  res.sendStatus(200)
  await engine.processWebhook()
})
```

***

## Handle deletion requests

A mirror carries the same deletion obligations as any other copy of the data,
including the archived event payloads. To remove a subject completely:

```bash title="Terminal"
npx workos-pg-sync redact --user user_01EHZNVPK3SFK441A1RGBFSHRT
```

Unlike a tombstone, this hard-deletes the subject's rows across every mirrored
table and removes matching rows from the event archive.

<!--
============================================================================
COMPANION EDITS
============================================================================

1. On /docs/events/data-syncing, add a third column to the "API vs. webhooks"
   comparison table. Suggested column and cells:

   | Aspect | Events API | Webhooks | Postgres sync |
   |---|---|---|---|
   | Timing | Controlled by your app... | Real-time... | Controlled by your app. Data is queried from your own database. |
   | Order | A consistent order is guaranteed. | No guarantee of order... | Guaranteed, with stale updates rejected on write. |
   | Reconciliation | Replayable... | Failed requests are retried... | Replayable, plus a set-difference check that detects deletions. |
   | Security | Authentication, confidentiality... | You must expose a public endpoint... | Authenticated pull. No public endpoint required. |

   And a section after "Sync using webhooks":

   ## Sync to your own database

   With the sync engine, WorkOS objects are mirrored into a Postgres database
   you control, so you can query them with SQL and join them to your own
   application data. It combines the events API for live changes, the list
   endpoints for historical data, and a reconciliation step that detects drift.
   To mirror data into Postgres, continue to the Postgres guide.

2. On /docs/events/data-syncing/data-reconciliation, add an "Up next" card:

   Sync data to Postgres — A step-by-step guide on how to mirror your WorkOS
   data into your own database.
-->
