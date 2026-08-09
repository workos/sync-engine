# How the sync engine works

Keeping a copy of someone else's data correct is harder than it first appears.
This page explains the design and, more usefully, why each piece is there.

## Three subsystems, not one

A mirror needs all three of these. Most hand-rolled versions have only the
first, which is why they drift.

```
   ┌──────────┐   ┌──────────┐   ┌───────────┐
   │ BACKFILL │   │   TAIL   │   │ RECONCILE │
   └────┬─────┘   └────┬─────┘   └─────┬─────┘
        │              │               │
  list endpoints   Events API      set difference
        │              │               │
        └──────────────┴───────────────┘
                       ▼
                  workos.* tables
```

**Tail** keeps you current. **Backfill** gives you everything that happened
before you started, which the tail structurally cannot: the Events API retains
90 days. **Reconcile** proves the other two worked, and it is the only one that
can detect a missed delete — a deleted object is absent from every list
endpoint, so no amount of re-listing will ever surface its absence.

## Lifecycle

```
1. capture    record the current position in the event stream
2. backfill   page the list endpoints for everything that exists
3. drain      replay events from the captured cursor
4. tail       poll forever, checkpointing after each durable write
```

Steps 2 and 3 overlap deliberately. Anything that changes mid-backfill is
replayed in step 3, and the staleness gate decides which version wins. Trying
to make the two phases abut exactly is how gaps get created; letting them
overlap costs nothing, because every write is idempotent.

## Why the Events API rather than webhooks

WorkOS recommends the Events API for keeping a datastore in sync, and the
reasons are structural:

| | Events API | Webhooks |
|---|---|---|
| Order | Consistent | No guarantee on receipt |
| Recovery | Replayable from any cursor | Retries stop after 3 days |
| Pace | Set by your process | Set by the sender |
| Exposure | Authenticated pull | A public endpoint you must secure |

The failure that motivates this most clearly: a `user.deleted` processed before
the matching `user.created` leaves an active user in your database that should
not exist. Webhook delivery makes no ordering promise, so that reordering is
allowed to happen. Reading an ordered log removes the possibility.

### You must enumerate the event types you want

`GET /events` **requires** the `events` filter. Omitting it returns
`400 invalid_events_request`, and an event name that does not exist is
rejected the same way. This is worth stating because the published OpenAPI
spec marks the parameter optional — the live API and the Node SDK's types are
the accurate description, and this was confirmed against the API rather than
inferred.

The engine derives its subscription from the streams you sync, so the two stay
in step:

```ts
import { subscribedEventTypes } from '@workos-inc/sync-engine'
subscribedEventTypes()  // every event type the catalog knows about
```

The consequence is real and worth planning around: **you cannot subscribe to
"everything".** A newly introduced WorkOS event type does not start arriving
until the catalog is updated and redeployed. Archiving unknown events still
protects you against a payload whose shape changed or a type you configured by
hand, but it cannot conjure an event you never asked for. To widen the
subscription without waiting for a release, pass `eventTypes` explicitly:

```ts
import { subscribedEventTypes, ARCHIVE_ONLY_EVENT_TYPES } from '@workos-inc/sync-engine'

new SyncEngine({
  // ...
  eventTypes: [...subscribedEventTypes(), ...ARCHIVE_ONLY_EVENT_TYPES],
})
```

Webhooks remain useful as a *trigger*. If you configure one, a verified
delivery wakes the poller immediately rather than waiting for the next
interval — but the state still flows through the ordered stream.

```ts
import { verifyWebhook } from '@workos-inc/sync-engine'

app.post('/workos/webhook', async (req, res) => {
  verifyWebhook({
    payload: req.rawBody,          // raw bytes, not a re-serialized object
    signature: req.header('WorkOS-Signature')!,
    secret: process.env.WORKOS_WEBHOOK_SECRET!,
  })
  res.sendStatus(200)              // respond first
  await engine.processWebhook()    // then poll
})
```

## The write path

Every write goes through one statement shape:

```sql
insert into workos.users as t (_env, _raw, _updated_at)
values ($1, $2::jsonb, $3::timestamptz)
on conflict (_env, id) do update set
  _raw        = t._raw || excluded._raw,   -- merge (events) or replace (REST)
  _updated_at = excluded._updated_at,
  _synced_at  = now()
where excluded._updated_at >= t._updated_at
  and t._raw is distinct from (t._raw || excluded._raw)
returning (xmax = 0) as _created;
```

Four decisions are encoded there.

### The staleness gate uses `>=`, not `>`

`where excluded._updated_at >= t._updated_at` rejects a write that is older
than what is stored, which is what makes out-of-order delivery safe.

The comparison is `>=` rather than `>` because timestamps are not infinitely
precise, and two events genuinely can carry the same one. With a strict `>`,
the second is dropped permanently — the mirror gets stuck on a superseded state
with no error anywhere. The Stripe sync engine shipped `>` and spent a bug
report cycle on subscriptions that stayed "trialing" after converting, because
the conversion event shared a timestamp with its neighbour.

`>=` makes ties last-writer-wins. That is not a guarantee of correctness for
simultaneous events — nothing can be — so reconciliation is the backstop rather
than the comparison.

### The timestamp comes from the source, never the database

`_updated_at` is the object's own `updated_at` when present, otherwise the
event's `created_at`, otherwise (during backfill) the response time.

It is never `now()` and never a trigger. A destination-stamped timestamp
records when *you* wrote, which is not comparable across delivery paths — and
worse, a redelivered event would always look newer than the row it is
replacing, defeating the gate entirely.

### Event writes merge; REST writes replace

This is the WorkOS-specific one, and it matters more than anything else here.

**WorkOS event payloads are documented subsets of the REST objects.** A
`user.updated` event does not carry `external_id`, `metadata`,
`last_sign_in_at` or `locale`. `organization.*` events omit `metadata` and
`stripe_customer_id`. `role.*` events have no `id` at all.

A naive implementation replaces the whole row on every event and silently nulls
out exactly the columns people join on — `external_id` most of all. So event
writes shallow-merge into `_raw` (`t._raw || excluded._raw`), preserving what
backfill established, while REST writes replace it outright, because the REST
object is complete and a field removed upstream should disappear.

Two further safeguards close the gap for newly created objects:

- **Hydration**: `hydrate: { users: 'on_create' }` re-fetches an object by id
  when its create event arrives, so a new user has its full payload
  immediately. Creates are low-volume, which makes this cheap.
- **Reconciliation** re-lists on a schedule and refreshes the rest.

### Idempotency lives in the primary key

The key is the object id, so a redelivered event is a no-op by construction.
There is no event-id dedupe table — nothing to grow, prune, or get wrong.

An event-id table is also a hazard in a mirror: mark an event processed before
the row commits and you lose the update, and marking it at all suppresses the
redelivery that would otherwise have repaired a partial failure.

## Deletes

Deletes are tombstoned: `_deleted_at` is set and the last known payload is
kept, because it is the only remaining record of what was deleted.

Two rules apply:

- **Deletes are not staleness-gated.** A delete always lands.
- **`_deleted_at` is never cleared** on a mirror object. A late `updated` event
  arriving after a delete refreshes the payload but must not resurrect the row.
  WorkOS ids are ULIDs and are never reused, so this is safe.

Memberships are the exception: `group.member_added` after
`group.member_removed` is a real re-add, so relinking clears the tombstone.

A `404` on any hydration fetch is also treated as a delete — that repairs
deletes whose event you never received.

## The schema

`_raw` is the only column the engine writes. Everything typed is
`GENERATED ALWAYS AS ... STORED`, projected out of it, including the primary
key.

The payoff is that upstream schema changes cannot break ingest. A new field is
queryable as `_raw->>'field'` the moment it appears, and promoting it to a
typed column is one `ALTER TABLE ... ADD COLUMN`, computed from rows already
present — no backfill, no rewrite. The alternative is what the original Stripe
engine did: 42 hand-written migration files, one per upstream schema change,
each requiring a release.

### Timestamps need a custom parser

Postgres rejects `(_raw->>'created_at')::timestamptz` in a generated column,
because casting text to `timestamptz` depends on the session's `DateStyle` and
`TimeZone` settings and is therefore not immutable.

The engine installs `workos.parse_iso8601()`, built only from immutable
primitives (`regexp_match`, `make_timestamp`, `timezone('UTC', ...)`,
`make_interval`). It handles the formats WorkOS emits — with and without
fractional seconds, `Z` or a numeric offset — and returns `NULL` on anything it
cannot parse rather than raising, because a generated column that throws would
reject the whole row.

### No foreign keys

Events routinely arrive with children before parents: a `group.user_added` can
land before the `user.created` it refers to. A foreign key turns that ordering
artifact into a hard write failure and a stuck stream. Indexes yes, referential
constraints no.

## Cursors and checkpoints

Cursor state lives in `workos._sync_state`, in the same database as the data,
keyed by `(sync_id, env, stream)`.

The invariant that matters: **a cursor is never advanced before the writes it
covers are durable.** Get that backwards and a crash between the two loses data
silently.

Event ids are ULIDs, so lexicographic order is creation order, which makes the
id a safe monotonic watermark.

### The stream is always oldest-first, and `order` does nothing

Verified against the live API: `order=asc`, `order=desc`, and omitting the
parameter entirely return byte-identical pages, always in ascending creation
order. `after` pages forward in time. The documented default of `desc` is not
observable in the response.

This has one practical consequence. There is no cheap way to ask for "the id of
the newest event" — `order=desc&limit=1` returns the *oldest* event, not the
newest, so using it to record a starting position would set the cursor to the
beginning of the retention window and replay 90 days on every fresh start.

The engine therefore records a timestamp instead, and bootstraps with
`range_start`:

```
capture:  { cursor: null, rangeStart: "2026-08-06T20:47:20.857Z" }
first poll:  GET /events?range_start=...&events=...
thereafter:  GET /events?after=<last event id>&events=...
```

`range_start` and `after` are mutually exclusive — sending both is a
`400 "Only one of range_start and after may be provided"` — so the bootstrap
value is dropped as soon as a real cursor exists.

### The lag window

The tail holds back events younger than a few seconds before advancing the
cursor.

A change feed can be briefly non-monotonic: an event created moments ago may
not appear on a page covering its timestamp, but will on a later read. Sequin
found exactly this on Stripe's equivalent endpoint — events that were skipped
when first paginated past were retrievable on replay. Reprocessing an event
here costs nothing because writes are idempotent; skipping one is permanent.
The asymmetry justifies a few seconds of delay.

### The retention cliff

The cursor's age is checked on every poll. Past 75 days it warns; past 90 the
events are gone and only a fresh backfill can repair the gap.

This is the failure mode worth alerting on. A stalled sync is silent — nothing
errors, the tables just stop changing — and it becomes unrecoverable at a
deadline.

## Unknown events are archived, never thrown

Every event is written verbatim to `workos.events` before routing. An event
type this build has never seen is archived and reported at info level.

Throwing is the tempting alternative and it is a trap. The Stripe engine ended
its event switch with `default: throw`, which returned HTTP 400 upstream — so
every newly introduced event type turned an over-broad subscription into a
retry storm. Archiving means a new upstream event type is a no-op you can
backfill from your own table later.

## Failure isolation

A failure in one stream does not stop the others, and does not advance that
stream's cursor past data that was never written. Errors are handled by blast
radius: transient (retry with backoff), stream-scoped (isolate, continue), or
global (stop).

There is no dead-letter queue. The Events API's replayability is the retry
queue: a failed range stays un-advanced and is retried on the next run. That is
only safe with alerting on cursor age, which is why `status` reports it.
