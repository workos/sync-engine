# How the sync engine works

Keeping a copy of someone else's data correct is harder than it first appears.
This page explains the design and, more usefully, why each piece is there.

## Three subsystems, not one

A mirror needs all three of these functions:

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

- **Tail** keeps you current.
- **Backfill** gives you everything that happened before you started, which the tail structurally cannot: the Events API only retains 90 days.
- **Reconcile** proves the other two worked, and it is the only one that
  can detect a missed delete. A deleted object is absent from every list
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

|          | Events API                 | Webhooks                          |
| -------- | -------------------------- | --------------------------------- |
| Order    | Consistent                 | No guarantee on receipt           |
| Recovery | Replayable from any cursor | Retries stop after 3 days         |
| Pace     | Set by your process        | Set by the sender                 |
| Exposure | Authenticated pull         | A public endpoint you must secure |

The failure that motivates this most clearly: a `user.deleted` processed before
the matching `user.created` leaves an active user in your database that should
not exist. Webhook delivery makes no ordering promise, so that reordering is
allowed to happen. Reading an ordered log removes the possibility.

## The write path

Every write goes through one statement shape:

```sql
INSERT INTO workos.users AS t (_env, _raw, _updated_at)
   VALUES ($1, $2::jsonb, $3::timestamptz)
   ON CONFLICT (_env, id) DO UPDATE SET
      _raw        = t._raw || excluded._raw,   -- merge (events) or replace (REST)
      _updated_at = excluded._updated_at,
      _synced_at  = NOW()
   WHERE excluded._updated_at >= t._updated_at
      AND t._raw IS DISTINCT FROM (t._raw || excluded._raw)
   RETURNING (xmax = 0) AS _created;
```

Four decisions are encoded there.

### The staleness gate uses `>=`, not `>`

`where excluded._updated_at >= t._updated_at` rejects a write that is older
than what is stored, which is what makes out-of-order delivery safe.

The comparison is `>=` rather than `>` because timestamps are not infinitely
precise, and two events genuinely can carry the same one. With a strict `>`,
the second is dropped permanently and the mirror gets stuck on a superseded
state with no error anywhere.

`>=` makes ties last-writer-wins. That is not a guarantee of correctness for
simultaneous events, because nothing can be, so reconciliation is the backstop
rather than the comparison.

### The timestamp comes from the source, never the database

`_updated_at` is the object's own `updated_at` when present, otherwise the
event's `created_at`, otherwise (during backfill, for an object with neither)
the engine's wall clock at write time.

It is never a database `now()` and never a trigger. A destination-stamped
timestamp records when _you_ wrote, which is not comparable across delivery
paths. Worse, a redelivered event would always look newer than the row it is
replacing, defeating the gate entirely.

### Event writes merge; REST writes replace

WorkOS event payloads are documented subsets of the REST objects. A
`user.updated` event does not carry `external_id`, `metadata`,
`last_sign_in_at` or `locale`. `organization.*` events omit `metadata` and
`stripe_customer_id`. `role.*` events have no `id` at all.

A naive implementation replaces the whole row on every event and silently nulls
out exactly the columns people join on, `external_id` most of all. So event
writes shallow-merge into `_raw` (`t._raw || excluded._raw`), preserving what
backfill established, while REST writes replace it outright, because the REST
object is complete and a field removed upstream should disappear.

Two further mechanisms close the gap:

- **Hydration** re-fetches an object by id when its create event arrives, so
  a new user or organization has its full payload immediately. It is on by
  default for those two streams, which are the only ones with a get-by-id
  endpoint. Creates are low-volume, which makes this cheap. `hydrate: 'off'`
  disables it; `'always'` re-fetches on every event, which is rarely worth
  the requests.
- **Backfill and `refresh`** re-list from the REST endpoints and replace every
  row, which repairs any field an event left behind. Reconciliation does not:
  it compares ids and only writes the rows that are missing on one side.

### Idempotency lives in the primary key

The key is the object id, so a redelivered event is a no-op by construction.
There is no event-id dedupe table: nothing to grow, prune, or get wrong.

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

A `404` on any hydration fetch is also treated as a delete, which repairs
deletes whose event you never received.

## The schema

`_raw` is the only column the engine writes. Everything typed is
`GENERATED ALWAYS AS ... STORED`, projected out of it, including the primary
key.

The payoff is that upstream schema changes cannot break ingest. A new field is
queryable as `_raw->>'field'` the moment it appears, and promoting it to a
typed column is one `ALTER TABLE ... ADD COLUMN`, computed from rows already
present, with no backfill and no rewrite. The `extraColumns` option does this
without editing the catalog; see [Schema](schema.md#extending-it). The
alternative is what a comparable engine did: 42 hand-written migration files,
one per upstream schema change, each requiring a release.

### Timestamps need a custom parser

Postgres rejects `(_raw->>'created_at')::timestamptz` in a generated column,
because casting text to `timestamptz` depends on the session's `DateStyle` and
`TimeZone` settings and is therefore not immutable.

The engine installs `workos.parse_iso8601()`, built only from immutable
primitives (`regexp_match`, `make_timestamp`, `timezone('UTC', ...)`,
`make_interval`). It handles the formats WorkOS emits, with and without
fractional seconds, `Z` or a numeric offset, and returns `NULL` on anything it
cannot parse rather than raising, because a generated column that throws would
reject the whole row.

### No foreign keys

Events routinely arrive with children before parents: a `group.user_added` can
land before the `user.created` it refers to. A foreign key turns that ordering
artifact into a hard write failure and a stuck stream. Indexes yes, referential
constraints no.

## Cursors and checkpoints

Cursor state lives in `workos._sync_state`, in the same database as the data,
keyed by `(sync_id, env, stream)`. The tail position is one row per
environment, under the stream name `__tail__`. Backfill progress is one row
per stream.

**A cursor is never advanced before the writes it covers are durable.**

Event ids are ULIDs, so lexicographic order is creation order, which makes the
id a safe monotonic watermark.

### The stream is always oldest-first

The engine records events as a timestamp instead, and bootstraps with
`range_start`:

```
capture:  { cursor: null, rangeStart: "2026-08-06T20:47:20.857Z" }
first poll:  GET /events?range_start=...&events=...
thereafter:  GET /events?after=<last event id>&events=...
```

`range_start` and `after` are mutually exclusive. Sending both is a
`400 "Only one of range_start and after may be provided"`, so the bootstrap
value is dropped as soon as a real cursor exists. The API also limits a single
`range_start` query to 30 days, so a position captured long before the first
poll runs may need a fresh capture.

### Two clocks: liveness and retention

The tail state records two timestamps, and they answer different questions.

- **`lastPolledAt`** is when the last poll completed without error, whether or
  not it returned anything. If it stops moving, the sync has stopped. This is
  the liveness signal, reported by `status` as `pollAgeSeconds`.
- **`lastEventAt`** is the `created_at` of the newest event applied. Its age,
  reported as `cursorAgeDays`, is the distance from the retention cliff. On a
  quiet environment it grows for days while the sync is perfectly healthy, so
  it is not a stall detector.

### The lag window

The tail holds back events younger than a few seconds before advancing the
cursor.

A change feed can be briefly non-monotonic: an event created moments ago may
not appear on a page covering its timestamp, but will on a later read. Reprocessing an event
here costs nothing because writes are idempotent; skipping one is permanent.
The asymmetry justifies a few seconds of delay.

### The retention cliff

The cursor's age is checked on every poll. Past 75 days it warns; past 90 the
events are gone and only a fresh backfill can repair the gap. `backfill`
notices a cursor that is past retention and re-captures the starting position,
so the tail restarts from now rather than from events that no longer exist.

This is the failure mode worth alerting on. A stalled sync is silent: nothing
errors, the tables just stop changing, and it becomes unrecoverable at a
deadline.

## Unknown events are archived, never thrown

Every event is written verbatim to `workos.events` before routing, which is why
that table is always created whatever streams you select. An event type this
build has never seen is archived and reported at info level.

Throwing is the tempting alternative and it is a trap. A comparable engine ended
its event switch with `default: throw`, which returned HTTP 400 upstream, so
every newly introduced event type turned an over-broad subscription into a
retry storm. Archiving means a new upstream event type is a no-op you can
backfill from your own table later.

## Failure isolation

Errors are handled by blast radius, and the radius differs by phase.

- **Backfill and reconcile** are per-stream. A failure in one stream is logged
  and the others continue. Backfill leaves that stream's checkpoint where it
  was, so `--resume` picks it up.
- **The tail** is per-environment. There is one cursor per environment, and it
  is only advanced once every stream's writes in a batch are durable. A failed
  write anywhere in the batch means nothing is acknowledged, and the whole
  batch is retried on the next poll. That is the conservative choice: a
  per-stream cursor would need per-stream event filters, and partially
  acknowledging a batch is how data gets skipped.
- **Transient errors** (network, 5xx, 429) are retried with backoff inside the
  API client before any of the above applies.
- **Fatal errors** stop the engine: a rejected credential (401 or 403), and a
  cursor that pages backwards. Neither will fix itself, and a process that
  looks alive while writing nothing is the worse outcome.

There is no dead-letter queue. The Events API's replayability is the retry
queue: a failed batch stays un-advanced and is retried on the next run. That is
only safe with alerting on poll age, which is why `status` reports it.
