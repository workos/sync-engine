# Operations

Running the sync engine in production: what to watch, what to alert on, and how
to recover.

## The failure mode that matters

A stalled sync is silent. Nothing errors, no request fails — the tables simply
stop changing, and every query keeps returning plausible-looking results. It
becomes unrecoverable at a deadline, because the Events API retains 90 days of
history and a cursor older than that cannot be replayed.

So the single most important alert is **cursor age**, not error rate.

```bash
npx workos-sync status
```

```json
{
  "production": {
    "cursor": "event_01HXXXXXXXXXXXXXXXXXXXXXX",
    "cursorAgeDays": 0.0004,
    "streams": [
      { "stream": "users", "rows": 14203, "deleted": 87, "lagSeconds": 2.1 }
    ]
  }
}
```

The engine warns at 75 days and logs an error past 90. Do not wait for that:
alert when `cursorAgeDays` exceeds a few hours.

## What to monitor

Every table carries `_updated_at` (source time) and `_synced_at` (write time),
which makes the important signals plain SQL rather than an instrumentation
project.

**Lag** — how far behind the mirror is. Measure it only over rows that changed
recently:

```sql
select avg(_synced_at - _updated_at) as mean_lag,
       count(*) as sample
from workos.users
where _updated_at > now() - interval '1 hour';
```

The `where` clause is load-bearing. Across every row, `_synced_at -
_updated_at` reports how *old the objects are*, not how late the sync is: a
mirror that has just backfilled year-old users will report a lag of a year
while being perfectly current. `status` restricts the average to a 24-hour
window for the same reason, and returns `null` with `lagSampleSize: 0` when
nothing changed upstream — which means "no delay to measure", not "no delay".

**Staleness** — how long since anything changed upstream:

```sql
select max(now() - _updated_at) as staleness from workos.users;
```

**Liveness** — has this stream written anything recently:

```sql
select 'users' as stream, max(_synced_at) as last_write from workos.users
union all
select 'organizations', max(_synced_at) from workos.organizations
order by last_write;
```

**Ingest rate** — from the event archive:

```sql
select date_trunc('hour', created_at) as hour, count(*) as events
from workos.events
where created_at > now() - interval '24 hours'
group by 1 order by 1;
```

### Alerts worth having

| Alert | Condition | Why |
|---|---|---|
| Cursor stalled | `cursorAgeDays > 0.25` | The sync has stopped. Everything else is downstream of this. |
| Cursor near retention | `cursorAgeDays > 60` | Replay is still possible, but the window is closing. |
| Stream not writing | `max(_synced_at) < now() - interval '1 hour'` on a busy stream | One stream failing while others continue. |
| Skipped writes spiking | `skipped` in the logs | Out-of-order delivery or a backfill colliding with the tail. Usually benign, occasionally not. |
| Drift detected | `reconcile` exits 2 | Missed creates or deletes. |

The engine emits structured JSON logs with per-batch counts:

```json
{"level":"info","msg":"processed events","env":"production","events":42,
 "created":3,"updated":38,"skipped":1,"deleted":0,"cursor":"event_01H..."}
```

`skipped` is the health metric to graph. A steady trickle is normal — it is the
staleness gate doing its job. A sudden spike means something changed about
delivery ordering.

## Reconciliation schedule

Three tiers, cheapest first:

| Cadence | Command | Finds |
|---|---|---|
| Continuous | `status` | Stalls and lag |
| Daily | `reconcile` | Count-level drift |
| Weekly, and after any incident | `reconcile --repair` | Missed creates and deletes |

```bash
# Report only. Exits 2 when drift is found, so it works as a scheduled check.
npx workos-sync reconcile

# Fix what it finds.
npx workos-sync reconcile --repair
```

Reconciliation reports two numbers per stream, and they mean different things:

- **`localOnly`** — rows you have that no longer exist upstream. These are
  missed deletes. Nothing else in the system can find them: a deleted object is
  absent from every list endpoint, so re-listing can never reveal its absence.
- **`remoteOnly`** — rows upstream that you do not have. Missed creates, or a
  backfill that did not finish.

Streams that cannot be reconciled say so explicitly rather than reporting a
misleading zero:

```json
{ "stream": "events", "skippedReason": "append-only stream: nothing to reconcile" }
{ "stream": "organization_domains", "skippedReason": "no list endpoint upstream: drift cannot be detected for this stream" }
```

## Refreshing partial fields

Because event payloads are subsets of the REST objects, a row that has only
ever been touched by events will be missing `external_id`, `metadata`,
`last_sign_in_at` and `locale`.

Backfill and reconcile both fix this. To do it on its own:

```bash
npx workos-sync refresh
```

Or avoid the gap entirely for new objects by hydrating on create:

```ts
hydrate: { users: 'on_create', organizations: 'on_create' }
```

## Rate limits

The engine paces itself against the documented limits, but they shape how long
a backfill takes:

| Endpoint | Limit | Effect |
|---|---|---|
| Everything | 6,000 / 60s per key | Rarely binding |
| `/user_management/*` reads | 1,000 / 10s per environment | Comfortable |
| **`/directory_users`** | **4 / second per directory** | The real constraint |

The directory limit is **per directory and does not aggregate**, so a large
directory takes as long as it takes: roughly 400 users per second at best.
Backfill checkpoints after every page, so an interrupted run resumes:

```bash
npx workos-sync backfill --resume
```

## Recovery

**Backfill was interrupted.** `backfill --resume`.

**The sync was down for hours or days.** Nothing to do — start it again. The
cursor is where you left it and the engine replays from there, as long as the
cursor is inside the 90-day window.

**The cursor is past retention.** The events are gone. Run `backfill` to
re-establish current state, then `reconcile --repair` to clear out anything
deleted while you were away, then `listen`. Backfill re-captures the cursor
automatically.

**Suspected corruption in one stream.** `reconcile --repair` fixes membership
drift. To rebuild a stream from scratch:

```sql
delete from workos.users where _env = 'production';
delete from workos._sync_state where env = 'production' and stream = 'users';
```

```bash
npx workos-sync backfill --streams users
```

**A bad deploy wrote wrong data.** The event archive is a local, ordered log,
so you can inspect exactly what arrived without an API call:

```sql
select id, event, created_at, _raw->'data' as payload
from workos.events
where object_id = 'user_01H...' order by created_at;
```

## Deletion requests

The mirror inherits the deletion obligations of any other copy of the data.
Archived event payloads embed personal data too, so both have to go:

```bash
npx workos-sync redact --user user_01H...
```

This hard-deletes the subject's rows across every mirrored table and removes
matching rows from the event archive. Unlike a tombstone, it leaves nothing
behind.

## Deployment

**As a library, in your existing service** — the common case. No extra process,
and it uses the connection pool you already have.

```ts
const engine = new SyncEngine({ databaseUrl, environments })
await engine.init()
engine.listen().catch((err) => { logger.error(err); process.exit(1) })
```

**As a standalone container:**

```dockerfile
FROM node:22-slim
RUN npm install -g @workos-inc/sync-engine
CMD ["workos-sync", "listen"]
```

Run **one listener per environment set**. Two processes tailing the same
environment with the same `syncId` will both advance the same cursor and
duplicate work. If you genuinely need several, give each its own
`WORKOS_SYNC_ID`.

`listen` handles `SIGINT` and `SIGTERM` by finishing the current batch and
stopping, so a rolling deploy does not interrupt a write mid-flight.

**As a scheduled job**, if a few minutes of lag is acceptable:

```bash
*/5 * * * * workos-sync tick
```

## Database sizing

The mirror is small — it is your identity data, not your event firehose. The
one table that grows without bound is `workos.events`, which keeps every event
forever.

To trim it, keep the window you actually query:

```sql
delete from workos.events where created_at < now() - interval '180 days';
```

Do not prune `auth_attempts` on the same schedule unless you mean to: that
history exists nowhere else, and once deleted it cannot be recovered from the
API.
