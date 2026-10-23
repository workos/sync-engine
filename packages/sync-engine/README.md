# @workos/sync-engine

The engine-agnostic core for mirroring WorkOS data into a database of your
choosing. It knows WorkOS — the object catalog, event routing, the REST
backfill, the Events API tail, reconciliation — and nothing about any
particular database. Writes go through a small `Destination` interface.

> **Prototype.** A working proof of concept — not yet a supported product.
> The API may change between minor versions.

**You probably want a destination package instead.** Each one depends on this
package and re-exports it, so it is the only thing to install:

| Destination | Package                                                                        |
| ----------- | ------------------------------------------------------------------------------ |
| Postgres    | [`@workos/sync-postgres`](https://www.npmjs.com/package/@workos/sync-postgres) |

Install this package directly only to write a new destination.

## What is in here

- **Catalog** — `STREAMS`: every WorkOS object the engine can mirror, with its
  group, key, projected columns, list endpoint, and the event types that
  create, update and delete it. `defaultStreams()` is every stream that has
  events; `subscribedEventTypes(streams)` derives the `GET /events` filter.
- **Router** — `routeEvent(event)`: event name → upsert / delete / link /
  unlink / append / archive-only. An unknown event type is archived and
  reported, never thrown.
- **WorkOS client** — `WorkOSClient`: pagination, per-endpoint rate-limit
  buckets, retries on 429.
- **Events tail** — `fetchEvents`: a durable cursor, a lag window for a briefly
  non-monotonic feed, and a hard stop if `after` ever pages backwards.
- **Backfill and reconcile** — `backfillStream`, `diffStream`, `freshness`,
  `refreshStream`: the REST list endpoints as the source of pre-event state,
  and the id-set difference that is the only way to find a missed delete.
- **Webhook verification** — `verifyWebhook`: a verified delivery is a nudge
  to poll now; state still flows through the ordered events stream.
- **`SyncEngine`** — the lifecycle that ties them together.

## Using the engine

```ts
import { SyncEngine } from "@workos/sync-engine";

const engine = new SyncEngine({
  destination, // see below
  environments: [{ name: "production", apiKey: process.env.WORKOS_API_KEY! }],
});

await engine.backfill(); // 1. capture the stream position, 2. page the REST endpoints
await engine.listen(); // 3. replay events from the captured position, 4. tail forever
```

The lifecycle is deliberately overlapping: the cursor is captured _before_
backfill starts, so anything that changes mid-backfill is replayed afterwards
and the staleness gate decides which version wins.

| Option             | Default                             |                                                                                                                                               |
| ------------------ | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `destination`      | —                                   | A `Destination` implementation. Required.                                                                                                     |
| `environments`     | —                                   | `{ name, apiKey, apiUrl? }[]`. `name` is written to every row and must never change.                                                          |
| `streams`          | `defaultStreams()`                  | Stream names to sync: every stream with events. Poll-only streams are opt-in. `events` is always included.                                    |
| `extraColumns`     | —                                   | Extra typed columns to project out of `_raw`, keyed by stream. Add-only.                                                                      |
| `eventTypes`       | derived from `streams`              | The `GET /events` filter. Widen it to archive event families with no mirror table.                                                            |
| `hydrate`          | users and organizations `on_create` | Re-fetch objects on event receipt: `off`, `on_create`, `always`, or a per-stream map. Only streams with a get-by-id endpoint can be hydrated. |
| `pollIntervalMs`   | `5000`                              | Idle wait between polls.                                                                                                                      |
| `lagWindowMs`      | `5000`                              | Never advance the cursor past events younger than this.                                                                                       |
| `pageSize`         | `100`                               | Page size for list and event calls.                                                                                                           |
| `maxEventsPerTick` | one page                            | Events to apply per tick, across as many pages as needed. Raise it for destinations where every statement is expensive.                       |
| `leaseTtlMs`       | `60000`                             | How long the events-tail lease outlives a crashed process. `listen` renews it every third of this; a clean stop releases it immediately.      |
| `logger`           | JSON to stdout                      | `{ debug, info, warn, error }`.                                                                                                               |

Methods: `init()`, `backfill({ resume })`, `tick()`, `listen()`, `stop()`,
`reconcile({ repair, streams })`, `refresh()`, `status()`, `redact(userId)`,
`processWebhook()`, `close()`. Every method applies the schema first, so
`init()` is optional.

The tail has one writer per environment. `listen()` takes a lease in the
destination before polling and renews it while it runs; if another process
holds it, `listen()` waits, which is what a rolling deploy needs. `tick()`
takes the lease for one poll and throws `LeaseHeldError` (with `holder` and
`expiresAt`) instead of waiting. `backfill`, `reconcile`, `refresh`, `status`
and `redact` do not take it; they are idempotent and run safely alongside a
tail.

`status()` reports, per environment, the cursor, `pollAgeSeconds` (time since
the last successful poll; alert on this), `cursorAgeDays` (age of the newest
event applied; distance from the 90-day retention cliff, not liveness),
`lease` (who is tailing it now, or `null`), and per-stream row counts,
tombstones, staleness and lag.

A rejected credential (401 or 403) and a cursor that pages backwards are
fatal: the engine stops rather than retrying. Everything else is retried or
isolated by stream.

## Writing a destination

A destination is one class implementing `Destination`. The engine speaks to it
only in terms of stream definitions and JSON payloads; the physical schema is
entirely yours.

```ts
import type {
  Destination,
  FreshnessRow,
  Lease,
  LeaseResult,
  Logger,
  Row,
  StreamDef,
  WriteMode,
  WriteStats,
} from "@workos/sync-engine";

class MyDestination implements Destination {
  init(streams: StreamDef[], logger?: Logger): Promise<void>;
  close(): Promise<void>;

  upsert(
    stream: StreamDef,
    env: string,
    rows: Row[],
    mode: WriteMode,
  ): Promise<WriteStats>;
  delete(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats>;
  relink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats>;

  liveIds(stream: StreamDef, env: string): Promise<string[]>;
  rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]>;
  freshness(
    stream: StreamDef,
    env: string,
    lagWindowHours: number,
  ): Promise<FreshnessRow>;
  redact(
    streams: StreamDef[],
    env: string,
    userId: string,
  ): Promise<{ table: string; rows: number }[]>;

  getState<T>(env: string, stream: string): Promise<T | null>;
  setState(env: string, stream: string, state: unknown): Promise<void>;

  acquireLease(env: string, holder: string, ttlMs: number): Promise<LeaseResult>;
  releaseLease(env: string, holder: string): Promise<void>;
  getLease(env: string): Promise<Lease | null>;
}
```

The engine relies on six properties of your implementation. Each one exists
because of a documented failure in a comparable sync engine.

1. **Staleness gate, `>=` not `>`.** `upsert` rejects a row whose `updatedAt`
   is older than the stored one and accepts an equal one. With a strict `<`,
   two events sharing a timestamp drop the second forever.
2. **Merge vs replace.** `mode: 'merge'` shallow-merges the payload into the
   stored one; `'replace'` overwrites it. Event payloads are subsets of the
   REST objects, so event writes merge and a whole-row replace would null out
   exactly the columns people join on.
3. **Deletion is terminal.** `delete` tombstones without a staleness check and
   never clears the tombstone; only `relink` (a membership re-add) does.
4. **Durable on return.** The engine checkpoints its cursor as soon as a write
   resolves. A write must not be buffered past its own call.
5. **`init` is called before every operation.** It must be idempotent and
   cheap once the schema is current — cache that, rather than re-querying.
6. **`acquireLease` is atomic.** Of two processes asking for the same
   `(env)` at once, exactly one gets `acquired: true`; the other is told the
   holder. It succeeds when the lease is free, expired by the _database's_
   clock, or already held by `holder` (a renewal). `releaseLease` is a no-op
   for anyone but the holder, and `getLease` returns `null` for an expired
   row. Without this, a rolling deploy has two processes advancing one cursor.
   In SQL it is one `INSERT ... ON CONFLICT DO UPDATE ... WHERE` on a row
   keyed by `(syncId, env)`; see `db/lease.ts` in the Postgres destination.

`WriteStats` (`created` / `updated` / `skipped` / `deleted`) is a health metric
the engine logs but never branches on; a database that cannot tell an insert
from an update may approximate the split. `liveIds` means non-tombstoned rows,
in a stable order. `rawRows` returns stored payloads, tombstoned or not.

The reference implementation is
[`PostgresDestination`](https://github.com/workos/workos-pg-sync/blob/main/packages/sync-postgres/src/destination.ts)
in `@workos/sync-postgres` (under 200 lines).

## Documentation

- [How it works](https://github.com/workos/workos-pg-sync/blob/main/docs/how-it-works.md)
- [Consistency](https://github.com/workos/workos-pg-sync/blob/main/docs/consistency.md)
- [Operations](https://github.com/workos/workos-pg-sync/blob/main/docs/operations.md)

## License

MIT
