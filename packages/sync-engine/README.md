# @workos/sync-engine

The engine-agnostic core for mirroring WorkOS data into a database of your
choosing. It knows WorkOS — the object catalog, event routing, the REST
backfill, the Events API tail, reconciliation — and nothing about any
particular database. Writes go through a small `Destination` interface.

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
- **Quarantine** — `applyWithQuarantine`: a write that fails deterministically
  on two consecutive polls is set aside in the destination's rejected store,
  so one bad payload cannot stall every stream in the environment. Transient
  errors never qualify, and `replayRejected` puts an event back.
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

| Option             | Default                             |                                                                                                                                                         |
| ------------------ | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `destination`      | —                                   | A `Destination` implementation. Required.                                                                                                               |
| `environments`     | —                                   | `{ name, apiKey, apiUrl? }[]`. `name` is written to every row and must never change.                                                                    |
| `streams`          | `defaultStreams()`                  | Stream names to sync: every stream with events. Poll-only streams are opt-in. `events` is always included.                                              |
| `extraColumns`     | —                                   | Extra typed columns to project out of `_raw`, keyed by stream. Add-only.                                                                                |
| `eventTypes`       | derived from `streams`              | The `GET /events` filter. Widen it to archive event families with no mirror table.                                                                      |
| `hydrate`          | users and organizations `on_create` | Re-fetch objects on event receipt: `off`, `on_create`, `always`, or a per-stream map. Only streams with a get-by-id endpoint can be hydrated.           |
| `pollIntervalMs`   | `5000`                              | Idle wait between polls.                                                                                                                                |
| `lagWindowMs`      | `5000`                              | Never advance the cursor past events younger than this.                                                                                                 |
| `pageSize`         | `100`                               | Page size for list and event calls.                                                                                                                     |
| `maxEventsPerTick` | one page                            | Events to apply per tick, across as many pages as needed. Raise it for destinations where every statement is expensive.                                 |
| `leaseTtlMs`       | `60000`                             | How long the events-tail lease outlives a crashed process; at least 1. Renewed every third of this while polling; a clean stop releases it immediately. |
| `logger`           | JSON to stdout                      | `{ debug, info, warn, error }`.                                                                                                                         |

Methods: `init()`, `backfill({ resume })`, `tick()`, `listen()`, `stop()`,
`reconcile({ repair, streams })`, `refresh()`, `status()`, `health()`,
`redact(userId)`, `rejected({ ids })`, `replayRejected({ ids })`,
`processWebhook()`, `close()`. Every method except `health()` applies the
schema first, so `init()` is optional.

The tail has one writer per environment. `listen()` takes a lease in the
destination before polling and renews it while it runs; if another process
holds it, `listen()` waits, which is what a rolling deploy needs. `tick()`
takes the lease for one poll, renewing it while the poll runs, and throws
`LeaseHeldError` (with `holder` and `expiresAt`) instead of waiting; on an
engine that is also listening, it runs
after the listener's current poll, under the listener's lease. With several
environments, `listen()` claims all of them, in name order, before polling,
and gives partial claims back while it waits for the rest, so two engines
whose lists overlap cannot block each other. A process that stalls past the
TTL loses the lease to its replacement: it still applies the batch it had
fetched, which is idempotent, but the destination refuses its checkpoint, so
the replacement's cursor is never rewound. `backfill`, `reconcile`, `refresh`, `status` and
`redact` do not take the lease; they are idempotent and run safely alongside
a tail.

`status()` reports, per environment, the cursor, `pollAgeSeconds` (time since
the last successful poll; alert on this), `cursorAgeDays` (age of the newest
event applied; distance from the 90-day retention cliff, not liveness),
`lease` (who is tailing it now, or `null`), `rejected` (events quarantined by
the tail; alert on this too), and per-stream row counts, tombstones,
staleness and lag.

`health()` is the same liveness picture without the database: a synchronous
snapshot of in-memory state (`starting` / `listening` / `stopped`, any fatal
error, and per environment the last poll, last event, cursor, last error and
running counters). `startHealthServer(engine, { port, host?, maxPollAgeMs? })`
serves it on `GET /healthz` (`200` or `503`) and `GET /metrics` (Prometheus
text), which is what the `listen` command does when given a port. To mount
the same endpoints on a server you already run, `healthReport(engine.health(),
{ maxPollAgeMs })` gives the JSON body and verdict, and
`renderMetrics(engine.health())` the exposition text.

```ts
import { startHealthServer } from "@workos/sync-engine";

const health = await startHealthServer(engine, { port: 8080 });
await engine.listen();
await health.close();
```

A rejected credential (401 or 403) and a cursor that pages backwards are
fatal: the engine stops rather than retrying. Everything else is retried or
isolated by stream, with one exception: an event whose write fails
deterministically on two consecutive polls is quarantined, so the cursor can
move past it rather than stalling the environment. `tick()` reports how many
it set aside, `rejected()` lists them with the error and the whole event, and
`replayRejected()` re-applies them, oldest first, once the cause is fixed.
Hydration failures, API errors, and anything the destination's
`isTransientError` recognises never qualify; the two-sightings rule covers
transient errors it does not.

## Writing a destination

A destination is one class implementing `Destination`. The engine speaks to it
only in terms of stream definitions and JSON payloads; the physical schema is
entirely yours.

```ts
import type {
  Destination,
  FreshnessRow,
  IdDiff,
  Lease,
  LeaseResult,
  Logger,
  RejectedEvent,
  Row,
  TableDef,
  WorkOSEvent,
  WriteMode,
  WriteStats,
} from "@workos/sync-engine";

class MyDestination implements Destination {
  init(streams: TableDef[], logger?: Logger): Promise<void>;
  close(): Promise<void>;

  upsert(
    stream: TableDef,
    env: string,
    rows: Row[],
    mode: WriteMode,
  ): Promise<WriteStats>;
  delete(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>;
  relink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>;
  unlink(stream: TableDef, env: string, rows: Row[]): Promise<WriteStats>;

  liveIds(stream: TableDef, env: string): Promise<string[]>;
  rawRows(stream: TableDef, env: string): Promise<Record<string, unknown>[]>;
  diffIds(
    stream: TableDef,
    env: string,
    remote: AsyncIterable<Record<string, unknown>[]>,
  ): Promise<IdDiff>;
  freshness(
    stream: TableDef,
    env: string,
    lagWindowHours: number,
  ): Promise<FreshnessRow>;
  redact(
    streams: TableDef[],
    env: string,
    userId: string,
  ): Promise<{ table: string; rows: number }[]>;

  getState<T>(env: string, stream: string): Promise<T | null>;
  setState(env: string, stream: string, state: unknown): Promise<void>;
  setStateIfHeld(
    env: string,
    stream: string,
    state: unknown,
    holder: string,
    expected: unknown,
  ): Promise<boolean>;

  acquireLease(
    env: string,
    holder: string,
    ttlMs: number,
  ): Promise<LeaseResult>;
  releaseLease(env: string, holder: string): Promise<void>;
  getLease(env: string): Promise<Lease | null>;

  reject(
    env: string,
    rows: { event: WorkOSEvent; error: string }[],
  ): Promise<void>;
  rejected(env: string, opts?: { ids?: string[] }): Promise<RejectedEvent[]>;
  rejectedCount(env: string): Promise<number>;
  clearRejected(env: string, ids: string[]): Promise<number>;
  isTransientError?(err: unknown): boolean; // optional
}
```

A `TableDef` is the destination-facing half of a stream: `name`, `key`,
`mode`, `columns`, `group`, and a `description`. The WorkOS half — the list
endpoint, the events, hydration, the credential paths the engine strips —
lives on `StreamDef`, which extends it and is read only by the engine. The
contract is typed on `TableDef` so a destination cannot come to depend on
where rows come from: it is handed tables and payloads, nothing else.

The engine relies on seven properties of your implementation.

1. **Staleness gate, `>=` not `>`.** `upsert` rejects a row whose `updatedAt`
   is older than the stored one and accepts an equal one. With a strict `<`,
   two events sharing a timestamp drop the second forever.
2. **Merge vs replace.** `mode: 'merge'` shallow-merges the payload into the
   stored one; `'replace'` overwrites it. Event payloads are subsets of the
   REST objects, so event writes merge and a whole-row replace would null out
   exactly the columns people join on.
3. **Deletion is terminal.** `delete` tombstones without a staleness check and
   never clears the tombstone. Memberships are the exception, in both
   directions and only when the write is at least as new as the row: `relink`
   (a re-add) clears the tombstone and `unlink` (a removal) sets it. Events
   can be replayed from quarantine after newer ones have landed, and an older
   link or unlink must not undo what a newer one established.
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
7. **The checkpoint is fenced by the lease.** `setStateIfHeld` writes only
   if `holder` holds the unexpired lease for `env` _and_ the stored state
   still equals `expected` (`null` for no row), with both checked atomically
   with the write. The engine saves the tail cursor through it, so a process
   that stalled past its lease, or whose position a backfill re-captured
   meanwhile, is refused by the database rather than by a check made a moment
   earlier. Postgres uses a transaction: provisionally write the cursor,
   then lock and validate the lease after any cursor wait. A refused write
   rolls back to a savepoint; see `db/lease.ts`.

`WriteStats` (`created` / `updated` / `skipped` / `deleted`) is a health metric
the engine logs but never branches on; a database that cannot tell an insert
from an update may approximate the split. `liveIds` means non-tombstoned rows,
in a stable order. Each id represents the **whole** `stream.key`: a plain
string for a single-column key, or a JSON array of strings in key order for a
composite key (for example, `'["org_1","admin"]'` for an organization role).
Use the exported `encodeKey(values)` to serialize these consistently.
`rawRows` returns stored payloads, tombstoned or not.

`diffIds` is the reconcile set difference. The engine streams the stream's
list endpoint to it one page at a time; the destination must stage those pages
in the database and diff them there against the live rows, so memory is
bounded by the drift rather than the stream. It returns `matched`, the
`localOnly` keys in the same encoding as `liveIds` (only ever tombstoned) and
the `remoteOnly` payloads (only ever re-inserted). Compare every key component,
not just the first: two different roles in one organization are different
rows. `diffStream` uses the same encoding for both sides of its report and
reconstructs all key fields when repairing missed deletes.

The live set must be captured before the first page is consumed: listing
takes time and the tail keeps writing, so a row inserted
mid-listing must not become `localOnly` and be tombstoned by repair. The
engine reads nothing from the destination while `diffIds` runs, so an
implementation may hold one pinned connection for the duration. A destination
with nothing to stage into can delegate to the exported `diffIdsInMemory`,
which is the old in-process algorithm, and accept that its memory grows with
the stream.

The last five methods are the **quarantine store**, separate from the mirror.
`reject` stores whole raw events keyed by `(env, eventId)` — the archive write
may itself be what failed, so a reference into it is not enough — and replaces
the error when an id is rejected again. `rejected` returns them oldest first by
event id, `rejectedCount` is what `status()` reports on every monitoring poll
(a count, so it must not fetch the payloads), and `clearRejected` forgets them
after a replay. Because they embed payloads, `redact` must scrub this store as
well as the archive. `isTransientError`
is optional: return `true` for errors that mean the database had a bad moment (a
dropped connection, a deadlock, a full disk), never for a verdict on the row,
and the engine will retry instead of ever quarantining on them. Without it,
every error is presumed deterministic and only the two-sightings rule stands
between a flaky connection and a quarantine.

## License

MIT
