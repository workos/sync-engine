# Serverless hosts

A host with a time limit cannot run `listen`: a scheduled function, a workflow
activity, a cron container that is stopped after a few minutes. It can run
everything else. Every other command is bounded, idempotent and resumable, so
a mirror is kept current by a handful of short invocations instead of one
long-lived process.

## What fits in a slot

| Command                     | Bounded by                             | Continues with                            |
| --------------------------- | -------------------------------------- | ----------------------------------------- |
| `tick`                      | `maxEventsPerTick` events, one poll    | the cursor, automatically                 |
| `backfill --max-pages <n>`  | `n` pages across every stream          | `--resume`                                |
| `refresh --max-pages <n>`   | `n` pages across the top-level streams | `--resume`; shares backfill's checkpoints |
| `reconcile --max-pages <n>` | `n` pages, stopping between streams    | `--resume`                                |

`tick` takes the writer lease and renews it while it runs. A second invocation
that overlaps it exits 3, or throws `LeaseHeldError` in the library, and should
simply stop: the sync is fine, something else is polling. The other three take
no lease and run safely alongside a tick, so their schedules need no
coordination with it.

A slice that stops short exits 0. The output says which streams are
`complete`, and the next slice with `--resume` carries on from the checkpoint.

## Schedules

One schedule per command. Each is a no-op once its work is done, so the
resumed ones stay scheduled and cost a few state reads.

```cron
* * * * *     workos-sync-postgres tick
*/5 * * * *   workos-sync-postgres backfill --resume --max-pages 50
30 * * * *    workos-sync-postgres reconcile --resume --repair --max-pages 50
0 3 * * 0     workos-sync-postgres reconcile --repair --max-pages 50
```

- **`tick` every minute** keeps the mirror current. Nothing waits behind it
  and it waits behind nothing.
- **`backfill --resume`** finishes the initial load one slice at a time. On a
  new mirror the first resumed slice starts from the beginning. Once every
  stream is complete it does nothing. Leave it scheduled: a later `backfill`
  without `--resume`, run once by hand to re-list everything, is picked up
  and finished the same way.
- **`reconcile`** starts fresh once a week and is finished by the hourly
  resumed slices, which do nothing once every stream is complete. Give both
  the same flags; `--repair` is applied per stream inside each slice. Without
  `--repair`, drift exits 2, which is the signal to alert on.
- **`refresh`** fits the same shape as `reconcile` when you use it.

Size the budget from the output. A slice's run time is its `pages` times the
time one page takes, and both are visible; start at 50 and raise it until a
slice fills most of the slot. `WORKOS_SYNC_MAX_EVENTS_PER_TICK` bounds `tick`
the same way.

## Metrics from a slot

Set `WORKOS_SYNC_OTEL=1` with the `OTEL_*` variables for your collector and
each invocation exports its metrics before it exits: the CLI starts the
OpenTelemetry SDK ahead of the engine and flushes it when the command
returns, which a periodic exporter on its own never gets to do in a slot this
short. The instruments are listed in [Operations](operations.md#opentelemetry).

## From the library

The same shape as exported handlers, for a runtime that calls functions on a
schedule and on HTTP requests:

```ts
import {
  SyncEngine,
  PostgresDestination,
  LeaseHeldError,
  verifyWebhook,
} from "@workos/sync-postgres";

const engine = new SyncEngine({
  destination: new PostgresDestination({
    databaseUrl: process.env.DATABASE_URL!,
  }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
  maxEventsPerTick: 2000,
});

// Every minute.
export async function tick() {
  try {
    return await engine.tick();
  } catch (err) {
    if (err instanceof LeaseHeldError)
      return { skipped: "another invocation is polling" };
    throw err;
  }
}

// Every few minutes. A no-op once every stream reports complete.
export const backfillSlice = () =>
  engine.backfill({ resume: true, maxPages: 50 });

// Weekly, fresh; then hourly with resume until every stream reports complete.
export const reconcileFresh = () =>
  engine.reconcile({ repair: true, maxPages: 50 });
export const reconcileSlice = () =>
  engine.reconcile({ repair: true, resume: true, maxPages: 50 });

// On a WorkOS webhook delivery: verify the raw body, then poll now. A held
// lease means another invocation is already polling, and the events stream
// carries the change either way, so the delivery is still acknowledged.
export async function webhook(rawBody: string, signature: string) {
  verifyWebhook({
    payload: rawBody,
    signature,
    secret: process.env.WORKOS_WEBHOOK_SECRET!,
  });
  try {
    return await engine.processWebhook();
  } catch (err) {
    if (err instanceof LeaseHeldError)
      return { skipped: "another invocation is polling" };
    throw err;
  }
}
```

Keep the engine at module scope so a warm runtime reuses its connection pool,
and size that pool as described under Connections below: a runtime that
scales out runs several copies of it.

Neon Functions has a ready-made version of this shape in its registry, with
the triggers, a webhook route and a token-guarded status route; see
[Neon](neon.md#run-the-sync-as-a-neon-function).

## Webhooks are a nudge, not a source

A verified delivery makes the engine poll now instead of at the next tick, so
latency drops to seconds without the webhook becoming the source of truth. The
state itself still flows through the ordered events stream, with its cursor
and replay. An unverified or late delivery is rejected by `verifyWebhook` and
changes nothing. Verify on the raw bytes, before any JSON parsing.

## Watching it

There is no `listen`, so there is no health server. `status` reads the
database and works from any invocation: `pollAgeSeconds` is the number to
alert on for a stalled sync, `rejected` for events the tail has quarantined,
and the per-stream rows give counts, tombstones, staleness and lag. Serve it
from a probe route, or run it on a schedule and alert on the result.

## A stream larger than the slot

`reconcile` cannot pause inside a stream, because a diff is taken against the
stream's whole listing. A stream whose listing alone outlasts the slot runs to
its last page regardless of the budget, and the slice overshoots. The `pages`
field shows it coming: when one stream's pages times the time per page
approaches the slot, run that stream's reconcile somewhere without a limit,
`engine.reconcile({ streams: [...] })` from the library or a one-off
`reconcile` from a container, and let the scheduled slices cover the rest.
`backfill` and `refresh` have no such floor; they checkpoint after every page.

## Connections

A pooled connection string from the provider is fine, including a transaction
pooler; see [Hosted Postgres setup](hosted-postgres.md).

The CLI opens a pool per invocation and the process exits, so nothing stays
open between slices. The library is different. `PostgresDestination` keeps
its pool until `close()`, and a warm runtime keeps that pool's idle
connections open until the runtime evicts it, in every copy it is running.
Keep the number small by owning the pool. The engine works in short
transactions and holds at most one connection per phase, so a pool of three
is plenty:

```ts
import { Pool } from "pg";
import {
  SyncEngine,
  PostgresDestination,
  fromPgPool,
} from "@workos/sync-postgres";

const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 3 });
// `pg` emits `error` when the server closes a connection sitting idle in the
// pool, and Node exits on an `error` event nobody listens for. A pool built
// from `databaseUrl` has this listener already; one you pass in needs its own.
pool.on("error", (err) => {
  console.warn("idle database connection closed; the pool will reconnect", err);
});
const engine = new SyncEngine({
  destination: new PostgresDestination({ db: fromPgPool(pool) }),
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});
```

A pool passed this way is not closed by `engine.close()`, carries its own TLS
settings, and must carry its own `error` listener, as above.
