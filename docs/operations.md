# Operations

Running the sync engine in production: what to watch, what to alert on, and how
to recover.

## The most important failure mode

A stalled sync is silent. Nothing errors, no request fails, the tables simply
stop changing, and every query keeps returning plausible-looking results. It
becomes unrecoverable at a deadline, because the Events API retains 90 days of
history and a cursor older than that cannot be replayed.

So, the single most important alert is **poll age**, not error rate.

```bash
npx workos-sync-postgres status
```

```json
{
  "production": {
    "cursor": "event_01HXXXXXXXXXXXXXXXXXXXXXX",
    "rangeStart": null,
    "started": true,
    "cursorAgeDays": 0.0004,
    "lastPolledAt": "2026-10-22T18:04:11.902Z",
    "pollAgeSeconds": 3.1,
    "lease": {
      "holder": "sync-7f9c4:1:a3f1b2",
      "expiresAt": "2026-10-22T18:05:11.902Z"
    },
    "streams": [
      {
        "stream": "users",
        "rows": 14203,
        "deleted": 87,
        "lastSyncedAt": "2026-10-22T18:04:09.471Z",
        "stalenessSeconds": 41.2,
        "lagSeconds": 2.1,
        "lagSampleSize": 12
      }
    ]
  }
}
```

Two of these numbers look similar but mean different things:

- **`pollAgeSeconds`** is how long since a poll last completed without error,
  event or no event. If it climbs, the process is down, stuck, or failing
  every request. **Alert on this.** It is `null` until the first poll.
- **`cursorAgeDays`** is the age of the newest event applied. It measures
  distance from the 90-day cliff, not liveness: an environment where nothing
  happens for a week will show seven days here while being perfectly healthy.
  It is `null` until the first event is applied. The engine warns at 75 days
  and logs an error past 90. Alert well before that.

`lease` names the process currently tailing this environment, as
`hostname:pid:suffix`, and when its claim lapses if it stops renewing. It is
`null` when nothing is listening, which alongside a climbing `pollAgeSeconds`
tells you the poller is gone rather than stuck.

## What to monitor

Every table carries `_updated_at` (source time) and `_synced_at` (write time),
which makes the important signals plain SQL rather than an instrumentation
project.

**Lag** is how far behind the mirror is. Measure it only over rows that changed
recently:

```sql
SELECT AVG(_synced_at - _updated_at) AS mean_lag,
       COUNT(*) AS sample
   FROM workos.users
   WHERE _updated_at > NOW() - INTERVAL '1 hour';
```

Across every row, `_synced_at -
_updated_at` reports how _old the objects are_, not how late the sync is: a
mirror that has just backfilled year-old users will report a lag of a year
while being perfectly current. `status` restricts the average to a 24-hour
window for the same reason, and returns `null` with `lagSampleSize: 0` when
nothing changed upstream, which means "no delay to measure", not "no delay".

**Staleness** is how long since anything changed upstream:

```sql
SELECT MAX(NOW() - _updated_at) AS staleness FROM workos.users;
```

**Liveness** is whether this stream has written anything recently:

```sql
SELECT 'users' AS stream, MAX(_synced_at) AS last_write FROM workos.users
UNION ALL
SELECT 'organizations', MAX(_synced_at) FROM workos.organizations
   ORDER BY last_write;
```

**Ingest rate**, from the event archive:

```sql
SELECT DATE_TRUNC('hour', created_at) AS hour, COUNT(*) AS events
   FROM workos.events
   WHERE created_at > NOW() - INTERVAL '24 hours'
   GROUP BY 1
   ORDER BY 1;
```

### Alerts worth having

| Alert                  | Condition                                                      | Why                                                                                            |
| ---------------------- | -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Sync stalled           | `pollAgeSeconds > 300`, or `null` long after deploy            | The poller has stopped.                                                                        |
| Cursor near retention  | `cursorAgeDays > 60`                                           | Replay is still possible, but the window is closing.                                           |
| Stream not writing     | `max(_synced_at) < now() - interval '1 hour'` on a busy stream | One stream failing while others continue.                                                      |
| Skipped writes spiking | `skipped` in the logs                                          | Out-of-order delivery or a backfill colliding with the tail. Usually benign, occasionally not. |
| Drift detected         | `reconcile` exits 2                                            | Missed creates or deletes.                                                                     |
| Process exited         | `listen` returned or crashed                                   | A rejected credential or a backwards-paging cursor stops the engine on purpose.                |
| Lease lost             | `lost the events tail lease` in the logs                       | The process stalled for longer than the lease TTL and another one took over. Check for a hung tick or an undersized `WORKOS_SYNC_LEASE_TTL_MS`. |

The engine emits structured JSON logs with per-batch counts:

```json
{
  "level": "info",
  "msg": "processed events",
  "env": "production",
  "events": 42,
  "pages": 1,
  "created": 3,
  "updated": 38,
  "skipped": 1,
  "deleted": 0,
  "cursor": "event_01H..."
}
```

`skipped` is the health metric to graph. A steady trickle is normal: it is the
staleness gate doing its job. A sudden spike means something changed about
delivery ordering.

## Reconciliation schedule

Three cadences, cheapest first:

| Cadence                        | Command              | Finds                      |
| ------------------------------ | -------------------- | -------------------------- |
| Continuous                     | `status`             | Stalls and lag             |
| Daily                          | `reconcile`          | Missed creates and deletes |
| Weekly, and after any incident | `reconcile --repair` | The same, and fixes them   |

```bash
# Report only. Exits 2 when drift is found, so it works as a scheduled check.
npx workos-sync-postgres reconcile

# Fix what it finds.
npx workos-sync-postgres reconcile --repair
```

Reconciliation reports two numbers per stream, and they mean different things:

- **`localOnly`**: rows you have that no longer exist upstream. These are
  missed deletes. Nothing else in the system can find them: a deleted object is
  absent from every list endpoint, so re-listing can never reveal its absence.
- **`remoteOnly`**: rows upstream that you do not have. Missed creates, or a
  backfill that did not finish.

Reconciliation compares ids. With `--repair` it tombstones the `localOnly`
rows and inserts the `remoteOnly` ones. It does not re-read rows that match,
so it is not the tool for refreshing fields; see below.

Streams that cannot be reconciled say so explicitly rather than reporting a
misleading zero:

```json
{ "stream": "events", "skippedReason": "append-only stream: nothing to reconcile" }
{ "stream": "organization_domains", "skippedReason": "no list endpoint upstream: drift cannot be detected for this stream" }
```

## Refreshing partial fields

Event payloads are subsets of the REST objects, so a row that has only ever
been touched by events is missing `external_id`, `metadata`, `last_sign_in_at`
and `locale`. Three things prevent or repair that:

- **Hydration**, on by default for users and organizations, re-fetches the
  object when its create event arrives. New rows are complete from the start.
- **`refresh`** re-lists every top-level stream from the REST API and replaces
  each row. It skips streams that are listed per parent (memberships,
  sessions, directory users and groups, groups, organization roles).
- **`backfill`** without `--resume` re-lists everything, including the
  per-parent streams, and replaces each row. This is the complete refresh.

```bash
npx workos-sync-postgres refresh     # top-level streams
npx workos-sync-postgres backfill    # everything
```

Hydration is configurable per stream:

```ts
hydrate: { users: 'on_create', organizations: 'off' }
```

or `WORKOS_SYNC_HYDRATE=off` to disable it everywhere.

## Rate limits

The engine paces itself against the documented limits, but they shape how long
a backfill takes:

| Endpoint                   | Limit                        | Effect             |
| -------------------------- | ---------------------------- | ------------------ |
| Everything                 | 6,000 / 60s per key          | Rarely binding     |
| `/user_management/*` reads | 1,000 / 10s per environment  | Comfortable        |
| **`/directory_users`**     | **4 / second per directory** | A tight constraint |

The directory limit is **per directory and does not aggregate**, so a large
directory takes as long as it takes: roughly 400 users per second at best.
Backfill checkpoints after every page, so an interrupted run resumes:

```bash
npx workos-sync-postgres backfill --resume
```

## Recovery

- **Backfill was interrupted.**
  - Call `backfill --resume`.

- **The sync was down for hours or days.**
  - There's nothing to do; start it again. The
    cursor is where you left it and the engine replays from there, as long as the
    cursor is inside the 90-day window.

- **The cursor is past retention.**
  - The events are gone. Run `backfill` to
    re-establish current state, then `reconcile --repair` to clear out anything
    deleted while you were away, then `listen`. Backfill sees that the stored
    cursor is older than retention and re-captures the starting position itself.

* **Start the tail over from now.** For any other reason, delete the tail row and
  run `backfill`, which captures a fresh position before it lists anything:

  ```sql
  DELETE FROM workos._sync_state
    WHERE sync_id = 'default' AND env = 'production' AND stream = '__tail__';
  ```

* **Suspected corruption in one stream.** `reconcile --repair` fixes membership
  drift. To rebuild a stream from scratch:

  ```sql
  DELETE FROM workos.users WHERE _env = 'production';
  DELETE FROM workos._sync_state
    WHERE sync_id = 'default' AND env = 'production' AND stream = 'users';
  ```

  Or:

  ```bash
  npx workos-sync-postgres backfill --streams users
  ```

* **A bad deploy wrote wrong data.** The event archive is a local, ordered log,
  so you can inspect exactly what arrived without an API call:

  ```sql
  SELECT id, event, created_at, data AS payload
    FROM workos.events
    WHERE object_id = 'user_01H...'
    ORDER BY created_at;
  ```

## Deletion requests

The mirror inherits the deletion obligations of any other copy of the data.
Archived event payloads embed personal data too, so both have to go:

```bash
npx workos-sync-postgres redact --user user_01H...
```

This hard-deletes, rather than tombstones, the user's row in `users` and every
row in a table with a `user_id` column (`organization_memberships`,
`sessions`, `auth_attempts`), and removes archived events whose object is the
user or whose payload carries that `user_id`. It leaves nothing behind in those
tables.

It does not reach rows that reference the person another way: an invitation
keeps `inviter_user_id`, `accepted_user_id` and the invitee's email, and a
directory user is a separate identity with its own id. Cover those with your
own `delete` statements if your obligation includes them.

## Deployment

**As a library, in your existing service** is the common case. No extra
process, and it uses the connection pool you already have.

```ts
const engine = new SyncEngine({
  destination: new PostgresDestination({ databaseUrl }),
  environments,
});
engine.listen().catch((err) => {
  logger.error(err);
  process.exit(1);
});
```

**As a standalone container**, from the published image:

```bash
docker run -d --restart unless-stopped \
  -e DATABASE_URL -e WORKOS_API_KEY -e WORKOS_ENVIRONMENT_NAME=production \
  ghcr.io/workos/sync-postgres:0.2.0 listen
```

The package and its production runs as a non-root user, and takes the same commands, flags and
environment as the CLI. It honors `HTTPS_PROXY` and `NO_PROXY` for calls to
the WorkOS API. To run the schema creation on each deploy,
run `init` as a one-shot container before starting `listen`.

Run **one listener per environment**, each with its own `WORKOS_API_KEY` and
`WORKOS_ENVIRONMENT_NAME`; they can share a database. The library form is the
exception: one embedded engine can hold several environments in its
`environments` array.

Only one process tails a given environment and `WORKOS_SYNC_ID` at a time. The
tail runs under a **single-writer lease** held in the database
(`_sync_lease`), so a rolling deploy is safe: the new process starts, finds
the lease held, logs a warning once a minute and waits; the old one receives
`SIGTERM`, finishes its current batch, releases the lease and exits; the new
one takes over on its next poll. Nothing is double-applied and the cursor has
one writer throughout. If the old process crashes instead, the lease expires
after `WORKOS_SYNC_LEASE_TTL_MS` (default 60 s) and the replacement takes
over then, which is the most lag a crash can add. Set the TTL comfortably
above the longest tick you expect: a process that stalls past it loses the
lease to its replacement and logs `lost the events tail lease` when it
notices. It still applies the batch it had already fetched, which is
idempotent, but it does not checkpoint it, so the replacement's cursor is
never rewound; it then waits for the lease like any other process. Two syncs
that genuinely must tail the same environment take different
`WORKOS_SYNC_ID`s, which gives each its own cursor and its own lease.

`backfill`, `reconcile`, `refresh`, `status` and `redact` do not take the
lease. They are idempotent and safe to run while a listener is tailing.

**As a scheduled job**, if a few minutes of lag is acceptable:

```bash
*/5 * * * * workos-sync-postgres tick
```

`tick` takes the lease for the duration of the poll and releases it on exit.
If a listener holds it, `tick` exits 3 without polling and names the holder
on stderr; that usually means both deployment styles are running at once.

## Database sizing

The mirror is small, as it is your identity data, not your event firehose. The
one table that grows without bound is `workos.events`, which keeps every event
forever.

To trim it, keep the window you actually query:

```sql
DELETE FROM workos.events WHERE created_at < NOW() - INTERVAL '180 days';
```

Do not prune `auth_attempts` on the same schedule unless you mean to: that
history exists nowhere else, and once deleted it cannot be recovered from the
API.
