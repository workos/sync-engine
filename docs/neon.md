# Neon

[Neon](https://neon.com) is a hosted Postgres with branching, autoscaling and
scale to zero. The engine's `pg` destination reaches it over an ordinary
PostgreSQL connection, so this guide is about the choices Neon puts in front
of you, and about the one way of running the sync that only Neon offers: as a
Function on the branch itself, next to the data.

| Run the sync                                      | Suits                                                                                                     |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| From a host you operate, pointed at Neon          | A server, container or cron you already run; `listen` as a long-lived process                             |
| On Neon, as a Function from the registry template | No host to run: schedule triggers call `tick`, `backfill` and `reconcile` on the branch, next to the data |

Both start with a database. The first also needs a connection string you
choose; a Function has one injected.

## Create the database

Create a Neon project for the mirror, or a branch of an existing one. The
engine creates its own `workos` schema and touches nothing else, so a database
your app already uses is fine; a branch is the cheap way to evaluate without
involving it. See [Branches](#branches) for what a branch carries with it.

The project's default role owns the default database, which covers the
`CREATE` and `TEMPORARY` privileges the engine needs. A role you create
yourself needs those two grants on the database and must own the `workos`
schema's objects, so run `init` as that role.

To run the sync as a [Neon Function](#run-the-sync-as-a-neon-function), put
the project in a region where Functions are available. At the time of writing
those are `aws-us-east-1`, `aws-us-east-2`, `aws-eu-central-1` and
`aws-ap-southeast-1`; the
[Functions overview](https://neon.com/docs/compute/functions/overview) has the
current list.

## Choose a connection

Neon gives two connection strings for the same database. Both work with the
engine.

| Connection | Host                                    | Notes                                                                                                                                                |
| ---------- | --------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Direct     | `ep-<id>.<region>.aws.neon.tech`        | A simple starting point.                                                                                                                             |
| Pooled     | `ep-<id>-pooler.<region>.aws.neon.tech` | PgBouncer in transaction mode. Has passed the engine's compatibility suite against a live Neon endpoint. What a Function receives as `DATABASE_URL`. |

Copy the URI from the **Connect** dialog in the console, or print it from a
linked checkout:

```sh
neon connection-string --ssl verify-full            # direct
neon connection-string --ssl verify-full --pooled   # pooled
```

The **Connection pooling** switch in that dialog only chooses which URI is
shown; it is not a project setting. Keep every parameter the URI comes with,
such as `channel_binding=require`, use the database password rather than a
Neon API key, and percent-encode any password character that has meaning in a
URL:

```
postgresql://USER:PASSWORD@ep-<id>-pooler.<region>.aws.neon.tech/neondb?sslmode=verify-full&channel_binding=require
```

Transaction pooling is enough because of how the engine uses the database:
short transactions, transaction-scoped migration locks, row-based writer
leases and temporary tables created `ON COMMIT DROP` inside a transaction.
[Hosted Postgres](hosted-postgres.md#choose-a-connection) has the reasoning.

## TLS

Neon's server certificates chain to ISRG Root X1, the Let's Encrypt root,
which Node's bundled root store already trusts. `sslmode=verify-full`
therefore works with no certificate download, and `WORKOS_SYNC_PG_SSL_CA`
stays unset. In embedded code there is likewise no `sslCa` to pass.

## Verify the endpoint

From a checkout of this repository, after `pnpm install` and `pnpm build`:

```sh
TEST_DATABASE_URL='postgresql://USER:PASSWORD@ep-<id>-pooler.<region>.aws.neon.tech/neondb?sslmode=verify-full' \
  pnpm --filter @workos/sync-postgres test:database
```

The suite creates a uniquely named schema per test and drops it afterward,
exercising every stream definition, generated columns, live views,
transactions, reconciliation, writer leases, checkpoints and tombstones
against the real endpoint. WorkOS responses are local fixtures, so it makes
no WorkOS calls and needs no key. Run it once for each URI you intend to use:
it proves the credentials, network path, TLS and permissions that a local run
cannot. [Hosted Postgres](hosted-postgres.md#verify-the-actual-endpoint) has
more on what it covers.

## Run the sync from your own host

Point the command at Neon as you would at any Postgres:

```sh
export DATABASE_URL='postgresql://USER:PASSWORD@ep-<id>-pooler.<region>.aws.neon.tech/neondb?sslmode=verify-full&channel_binding=require'
export WORKOS_API_KEY=sk_...

npx workos-sync-postgres init
npx workos-sync-postgres backfill
npx workos-sync-postgres listen
```

The [container image](../README.md#against-a-real-environment) and the
[library](getting-started.md#embed-it) take the same URL. Two things about
Neon's compute to plan for:

- **The compute stays awake while the sync runs.** Neon suspends an idle
  compute after five minutes by default and resumes it on the next
  connection. A running sync never leaves that gap: `listen` renews its
  writer lease every 20 seconds, and a `tick` every minute takes and releases
  the lease each time. Budget for the compute to run continuously at its
  minimum size for as long as the sync does. Where a few minutes of lag is
  acceptable, a `tick` on a longer interval than the suspend timeout lets the
  compute sleep between polls, at the cost of a cold start of a few hundred
  milliseconds on each one, which the sync simply waits out.
- **An IP allow list must include the sync's host.** If the project restricts
  connections by address, add the address the sync connects from before the
  first `init`.

## Run the sync as a Neon Function

Neon can host the sync itself. The
[Mirror WorkOS data into Postgres](https://neon.com/functions/templates/workos-sync)
template in the Neon Functions registry packages the engine as one function
on your branch. Schedule triggers call its `tick`, `backfill` and `reconcile`
routes, a `/webhook` route lets a WorkOS webhook make it poll right away, and
`GET /status` reports the mirror's health. The branch's pooled `DATABASE_URL`
is injected; you supply three variables. The
[template source](https://github.com/neondatabase/examples/tree/main/registry/workos-sync)
is the full walkthrough; this is the shape of it.

| Route                   | Called by                         | Purpose                                                              |
| ----------------------- | --------------------------------- | -------------------------------------------------------------------- |
| `POST /tick`            | schedule trigger, every minute    | Poll the Events API once                                             |
| `POST /backfill`        | schedule trigger, every 5 minutes | One slice of the initial load; a no-op once every stream is complete |
| `POST /reconcile`       | schedule trigger, hourly          | Continue the reconcile pass in progress and repair drift             |
| `POST /reconcile/fresh` | schedule trigger, weekly          | Start a new reconcile pass                                           |
| `POST /webhook`         | WorkOS                            | Verify the signature, then poll now                                  |
| `GET /status`           | you, with a bearer token          | Per-stream counts, poll age and lag                                  |

### Scaffold and configure

```sh
npm install -g neon@latest   # 8.11.0 or later
neon login
neon link
neon functions new workos-sync --slug workossync
```

The CLI writes `functions/workossync/index.ts`, declares the function in
`neon.ts`, and adds the three WorkOS variables to `.env.example`. Put their
values in `.env.local`, next to the Neon values `neon link` pulled:

| Variable                   | Value                                                                                                               |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `WORKOS_API_KEY`           | A secret key (`sk_...`) for the environment to mirror, from **API Keys** in the WorkOS Dashboard                    |
| `WORKOS_WEBHOOK_SECRET`    | The signing secret of the webhook endpoint below; any placeholder until it exists                                   |
| `WORKOS_SYNC_STATUS_TOKEN` | A long random string, for example `openssl rand -hex 32`. It guards `GET /status`, because function URLs are public |

### Schedule and deploy

Triggers are not part of the template, so add them beside `functions` in
`neon.ts`. Trigger names must be unique on the branch.

```ts
triggers: {
  "workos-tick": {
    type: "schedule",
    function: "workossync",
    cron: "* * * * *",
    functionPath: "/tick",
  },
  "workos-backfill": {
    type: "schedule",
    function: "workossync",
    cron: "*/5 * * * *",
    functionPath: "/backfill",
  },
  "workos-reconcile": {
    type: "schedule",
    function: "workossync",
    cron: "30 * * * *",
    functionPath: "/reconcile",
  },
  "workos-reconcile-fresh": {
    type: "schedule",
    function: "workossync",
    cron: "0 3 * * 0",
    functionPath: "/reconcile/fresh",
  },
},
```

```sh
neon deploy --env .env.local
neon functions get workossync   # prints the invocation_url
```

The first `/backfill` delivery creates the schema and starts the load;
`/tick` runs every minute from then on. Then, in the WorkOS Dashboard under
**Webhooks**, add an endpoint at the function's `invocation_url` plus
`/webhook`, select any events, copy its signing secret into `.env.local` as
`WORKOS_WEBHOOK_SECRET`, and deploy again. The event selection does not
matter: a delivery is only a nudge to poll, and the state still arrives
through the events stream.

```sh
curl -H "Authorization: Bearer $WORKOS_SYNC_STATUS_TOKEN" <invocation_url>/status
```

### What is different from running it yourself

- **There is no `listen`.** The mirror is kept current by a tick per minute,
  or within seconds of a change when the webhook nudges it. Every route is a
  bounded, resumable slice of the kind [Serverless hosts](serverless.md)
  describes. `MAX_PAGES` and `MAX_EVENTS_PER_TICK` at the top of the module
  are the budgets, and each response reports `pages` so you can size them
  against the 15-minute limit.
- **A reconcile cannot pause inside a stream.** A stream whose listing alone
  outlasts the budget runs to its last page. Reconcile that stream once from
  anywhere else with
  `npx workos-sync-postgres reconcile --repair --streams <name>` against the
  same `DATABASE_URL`, and let the scheduled slices cover the rest.
- **The function is branch-scoped.** Each branch that deploys it polls WorkOS
  separately into its own database, all within the one key's rate limit.
  Deploy it on the branches that need a live mirror; a branch for a preview
  or a test can keep the copy it inherited instead, see [Branches](#branches).
- **The compute stays awake here too.** The minute tick keeps the branch's
  compute from suspending, as described above.
- **Logs are branch logs.**
  `neon logs query --branch <branch> --source function --since 1h` shows the
  function's output, including the details behind any `500`.

## Branches

A Neon branch is a copy-on-write clone of its parent, and that includes the
mirror and its bookkeeping: the cursor, backfill checkpoints, the writer
lease and the schema fingerprint. In practice:

- **A child branch is a complete mirror as of the moment it was created**,
  with no backfill. For a preview or a test suite that only reads the mirror,
  that may be all it needs.
- **A sync started on the child resumes from the copied cursor** and catches
  up from the branch point instead of re-listing everything, provided it
  mirrors the same WorkOS environment under the same `_env` label. The copied
  lease row can still be unexpired for up to a minute after branching:
  `listen` waits for it and `tick` reports it as held, as with any other
  holder.
- **Resetting a branch from its parent** replaces the mirror and its cursor
  together, so the sync on that branch carries on from the parent's position
  with nothing to re-run.
- **Two branches syncing the same environment are two syncs on one key.**
  Each polls and backfills on its own; see
  [rate limits](operations.md#rate-limits) for the budget they share.

## Query it

Everything in [Querying the mirror](queries.md) applies. Use the SQL editor
in the Neon console, or open `psql` on the same URI:

```sh
neon connection-string --psql
```

If the branch has the Data API enabled, it exposes only `public` by default.
Leave `workos` out of its exposed schemas: the mirror holds raw event payloads
and deleted rows, and access from browser clients needs its own grants and
row-level security design. Neon Auth and the Data API keep their own schemas
and roles, which the engine never touches.
