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
    "rejected": 0,
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
- **`rejected`** is how many events the tail has set aside because they fail
  deterministically. The tail kept going, so poll age looks healthy, but the
  objects those events describe are missing or stale until you replay them.
  **Alert on this too.** See [Rejected events](#rejected-events).

`lease` names the process currently tailing this environment, as
`hostname:pid:suffix`, and when its claim lapses if it stops renewing. It is
`null` when nothing is listening, which alongside a climbing `pollAgeSeconds`
tells you the poller is gone rather than stuck.

`status` opens a database connection and queries every stream, which is the
wrong shape for something that runs every few seconds. The same poll age is
also served by `listen` itself, from memory, when `WORKOS_SYNC_HEALTH_PORT`
is set (the container image sets it to `8080`):

```bash
curl -s localhost:8080/healthz
```

```json
{
  "state": "listening",
  "fatal": null,
  "startedAt": "2026-10-22T17:58:02.114Z",
  "healthy": true,
  "environments": {
    "production": {
      "lastPolledAt": "2026-10-22T18:04:11.902Z",
      "pollAgeSeconds": 3.1,
      "lastEventAt": "2026-10-22T18:04:09.471Z",
      "cursor": "event_01HXXXXXXXXXXXXXXXXXXXXXX",
      "lastError": null,
      "counters": { "ticks": 71, "events": 42, "created": 3, "updated": 38, "skipped": 1, "deleted": 0, "errors": 0 }
    }
  }
}
```

The status code is the verdict: `200` while `state` is `starting` (the
process is up, the first poll has not finished) and while every environment
has polled successfully within `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` (default
60 seconds); `503` once any environment's poll is older than that, once the
engine has stopped on a fatal error, or once `listen` has returned. No
request to it touches the database, so it keeps answering when the database
is the thing that is down, and the answer is then a `503` from a climbing
poll age rather than a timeout.

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
| Sync stalled           | `pollAgeSeconds > 300`, or `null` long after deploy; `/healthz` returns 503 | The poller has stopped.                                                           |
| Cursor near retention  | `cursorAgeDays > 60`                                           | Replay is still possible, but the window is closing.                                           |
| Stream not writing     | `max(_synced_at) < now() - interval '1 hour'` on a busy stream | One stream failing while others continue.                                                      |
| Skipped writes spiking | `skipped` in the logs                                          | Out-of-order delivery or a backfill colliding with the tail. Usually benign, occasionally not. |
| Drift detected         | `reconcile` exits 2                                            | Missed creates or deletes.                                                                     |
| Column drift           | `init` exits 2 after a deploy                                  | A generated column no longer matches the catalog; the projection is stale until repaired.      |
| Events quarantined     | `rejected > 0` in `status`, or `rejected` exits 2              | One payload the mirror cannot apply. The tail kept going; the object waits on a replay.        |
| Process exited         | `listen` returned or crashed                                   | A rejected credential or a backwards-paging cursor stops the engine on purpose.                |
| Lease lost             | `lost the events tail lease` in the logs                       | The process stalled for longer than the lease TTL and another one took over. Check for a hung tick or an undersized `WORKOS_SYNC_LEASE_TTL_MS`. |
| Connections cycling    | `idle database connection closed by the server` repeating in the logs | The database, or a proxy in front of it, is closing connections the pool left idle. One is routine and the pool reconnects on its own; a steady stream points at the server side: a restart loop, a failover, or an idle timeout shorter than the poll interval. |

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

### Prometheus metrics

With `WORKOS_SYNC_HEALTH_PORT` set, `listen` also serves `GET /metrics` in
the Prometheus text format, from the same in-memory state as `/healthz`.
Every per-environment series carries an `env` label.

| Metric                         | Type    | What it is                                                                                            |
| ------------------------------ | ------- | ----------------------------------------------------------------------------------------------------- |
| `workos_sync_up`               | gauge   | `1` while the engine is running; `0` once it has stopped or hit a fatal error.                        |
| `workos_sync_poll_age_seconds` | gauge   | Seconds since the last successful poll. **Alert on this.** Absent until the first successful poll, so a listener that has never succeeded needs the `WorkOSSyncNeverPolled` rule below. |
| `workos_sync_cursor_age_days`  | gauge   | Age of the newest event applied: distance from the 90-day cliff, not liveness. Absent until the first event. |
| `workos_sync_ticks_total`      | counter | Polls attempted, successful or not.                                                                   |
| `workos_sync_events_total`     | counter | Events applied.                                                                                       |
| `workos_sync_writes_total`     | counter | Rows written, with a `kind` label of `created`, `updated`, `skipped` or `deleted`. Graph `skipped`.   |
| `workos_sync_errors_total`     | counter | Polls that failed with a retryable error. Fatal errors stop the process instead.                      |

```yaml
# prometheus.rules.yml
- alert: WorkOSSyncStalled
  expr: workos_sync_poll_age_seconds > 300 or workos_sync_up == 0
  for: 2m
# An environment with a ticks series but no poll-age series has never polled
# successfully: every request since startup failed, `/healthz` is 503, and
# nothing above matches. The `for` is the startup grace period.
- alert: WorkOSSyncNeverPolled
  expr: workos_sync_ticks_total unless workos_sync_poll_age_seconds
  for: 5m
- alert: WorkOSSyncCursorNearRetention
  expr: workos_sync_cursor_age_days > 60
```

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

The comparison runs in the database, not in the sync process. The upstream
list is streamed into a temporary table one page at a time and the set
difference is a pair of anti-joins in SQL, so the memory `reconcile` needs is
bounded by the size of the drift, not the size of the stream. A million-user
directory with ten missed deletes costs ten ids, plus one page of the list at
a time. The live ids are captured into a second temporary table before the
first page is requested, so a row the tail writes while the list is in
progress is never a deletion candidate: `--repair` only tombstones a row that
was already in the mirror when the listing began and is absent from the list.
Both tables live in a single transaction and are dropped when it commits, so
nothing is left behind in the schema. One gap remains, for streams listed
through a parent (memberships, directory users and groups): the parent ids
are read a few statements before the live snapshot, so a parent and its first
child that the tail writes in that gap put the child in the snapshot with no
parent to list it under, and `--repair` tombstones it. Run `--repair` for
those streams in a quiet moment, or run `reconcile` without it first and read
the report.

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

Nothing in a request exempts the sync from these limits. The global one is
enforced per API key, so the engine shares that budget with everything else
that uses its key; the per-environment and per-directory limits are shared
with everything in the environment whatever key is used. Give the engine a
key of its own even so. The per-key budget is then the engine's alone, and
every request the key makes is the engine's, which makes its traffic easy to
attribute.

## Telling the engine's requests apart

Every request carries a `User-Agent` of the form

```
workos-sync-engine/<version> (phase=<phase>)
```

The first token is the same `name/version` shape the WorkOS SDKs send. The
phase says which part of the engine made the request: `tail` is the events
poll, `backfill` a full listing, `reconcile` a drift check or refresh, and
`hydrate` the get-by-id fetch after an event. With a dedicated API key this is
enough to pick the engine's traffic, and any one phase of it, out of a request
log. The per-environment and per-directory limits are not isolated by the key, so
a backfill of a large directory still shares that directory's budget with your
application. The header is built by the engine from its own version and the
phase, and nothing a destination or an application passes in can change it.

## Backfill without a long-lived process

`backfill` runs every stream to completion by default, which can take hours
for a large directory. A host with a time limit — a cron job, a serverless
function, a workflow activity — runs it in slices instead:

```bash
npx workos-sync-postgres backfill --max-pages 50             # first slice
npx workos-sync-postgres backfill --resume --max-pages 50    # every later slice
```

Each slice fetches at most 50 pages across all streams, checkpoints after
every page, and prints one result per stream with `complete` set to `true` or
`false`. `--resume` continues each stream from its checkpoint, inside a
parent for the per-parent streams, and skips the ones already complete, so
repeated slices converge rather than re-listing what finished. Stop when every
stream reports `complete`. The exit code is 0 either way: a slice that stops
short is the outcome you asked for. The first slice runs without `--resume`,
because `--resume` on a mirror whose last backfill completed skips everything;
a fresh slice first forgets every stream's checkpoint, so a stream the budget
never reaches is not mistaken for complete on the next resumed slice. A
stream listed per parent is held back while its parent is incomplete, and a
resumed slice reaches it once the parent finishes.

From the library the loop is:

```ts
let results = await engine.backfill({ maxPages: 50 });
while (results.some((r) => !r.complete)) {
  results = await engine.backfill({ resume: true, maxPages: 50 });
}
```

The tail's starting position is captured on the first slice and kept, so
`tick` can run between slices and anything that changes during the backfill
is replayed from there. One backfill slice and one `tick` per invocation is
the natural shape for a scheduled host.

## Rejected events

There is one cursor per environment. Without a quarantine, one event that fails
deterministically — a payload a generated column cannot cast, an `updated_at`
the `::timestamptz` cast rejects, a router bug on a shape nobody has seen —
would fail every poll, the cursor would never move, and every stream in the
environment would stop, sign-in capture included, while the 90-day retention
clock ran down.

So the tail isolates such an event and, once it has failed on **two
consecutive polls**, sets it aside in `workos._rejected` and moves the cursor
past the batch. Transient failures never qualify: a hydration 5xx, any API
error, a dropped connection, a deadlock, a full disk all fail the tick and
retry as before. The rules are spelled out in
[Consistency](consistency.md#quarantine).

When it happens, you get three signals: an error log line per event, with the
event id, type, object id and the database's message; `rejected` in `status`;
and a non-zero exit from `rejected`, so it can run as a scheduled check next to
`reconcile`.

```bash
npx workos-sync-postgres rejected
```

```json
{
  "production": [
    {
      "eventId": "event_01HXXXXXXXXXXXXXXXXXXXXXX",
      "eventType": "user.updated",
      "objectId": "user_01HXXXXXXXXXXXXXXXXXXXXXX",
      "createdAt": "2026-10-22T18:04:09.471Z",
      "rejectedAt": "2026-10-22T18:04:21.118+00:00",
      "error": "error: invalid input syntax for type boolean: \"maybe\"",
      "raw": { "id": "event_01H...", "event": "user.updated", "data": { "...": "..." } }
    }
  ]
}
```

Then, in order:

1. **Read the error.** It is the database's own message, so it names the
   column or the cast. The whole event is in `raw`, and the same row is in
   `workos._rejected` if SQL is more convenient.
2. **Fix the cause.** Usually a release: a widened column type, a corrected
   `extraColumns` path, a router fix. Occasionally the payload upstream.
3. **Replay.** Everything, or one event by id:

   ```bash
   npx workos-sync-postgres rejected --replay
   npx workos-sync-postgres rejected --replay --id event_01H...
   ```

   Each event goes back through the normal write path, oldest first. Those
   that succeed are forgotten; those that fail again stay, with the error
   updated, and the command exits 2. The staleness gate still applies, so a
   replayed event older than the row it targets is a no-op: later events
   already carried the newer state, and the row is already correct. That
   holds for memberships in both directions: a link replayed after a newer
   removal leaves the membership removed, and a removal replayed after a
   newer re-add leaves it active.

The quarantine holds whole event payloads, which embed personal data; `redact`
scrubs it along with the `events` archive.

## Recovery

- **Backfill was interrupted.**
  - Call `backfill --resume`.

- **One event keeps failing.**
  - It is quarantined automatically after two polls, and `status` shows
    `rejected > 0`. Fix the cause, then `rejected --replay`. See
    [Rejected events](#rejected-events).

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

* **`init` exits 2 after a deploy.** A catalog column changed its path or type
  and the database still has the old projection; the rows are right in `_raw`,
  the typed column is stale. The log and the printed report carry the exact
  DDL. `init --repair` rebuilds non-key columns from `_raw`, setting the
  table's own `_live` view aside and restoring it with its grants; key
  columns, columns a view of yours depends on, and columns carrying an index
  or constraint of your own are left for you, with the DDL to run. The same
  flag rebuilds a generated view whose shape changed between releases. See
  [Changing a column](schema.md#changing-a-column) and
  [Live views](schema.md#live-views).

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

**TLS** follows the connection string: `sslmode=` is read the way `pg` reads
it, so a database whose certificate a public CA issued needs nothing beyond
the `sslmode` you would give any client. One whose certificate chains to a
private CA needs that CA supplied. Set `WORKOS_SYNC_PG_SSL_CA` (`sslCa` on
`PostgresDestination`) to the certificate, as PEM or as base64 of it for a
secret store that cannot hold a multi-line value. TLS is then required and
the server certificate verified against that CA and the host name, with or
without an `sslmode` in the string; `sslmode=no-verify` still turns
verification off, and `sslmode=disable` is refused as a contradiction. When
the certificate is a file the process can read, `sslrootcert=<path>` in the
connection string does the same without the variable.

The image sets `WORKOS_SYNC_HEALTH_PORT=8080`, exposes that port, and
declares a `HEALTHCHECK` that fetches `/healthz` every 30 seconds after a
30-second start period, so `docker ps` and Compose's `depends_on:
condition: service_healthy` reflect whether the tail is actually polling.
The check follows the port `listen` resolved, so changing it with either
`-e WORKOS_SYNC_HEALTH_PORT=<n>` or `listen --health-port <n>` keeps the
container healthy. The check only means something for `listen`: one-shot
commands exit before it would ever run. Publish the port (`-p 8080:8080`)
to scrape `/metrics` from outside the container.

**On Kubernetes**, point both probes at `/healthz`. The engine answers `200`
while it is still starting, so the liveness probe does not need a long
initial delay; what it needs is a `failureThreshold` and `periodSeconds` that
together exceed `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS`, or a transient API
outage restarts a pod that would have recovered on its own.

```yaml
containers:
  - name: sync
    image: ghcr.io/workos/sync-postgres:0.2.0
    args: ["listen"]
    ports:
      - name: health
        containerPort: 8080
    readinessProbe:
      httpGet: { path: /healthz, port: health }
      periodSeconds: 10
    livenessProbe:
      httpGet: { path: /healthz, port: health }
      periodSeconds: 30
      failureThreshold: 4
```

Add a `prometheus.io/scrape: "true"` annotation (or a `ServiceMonitor`) on
port `health` to collect `/metrics`.

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

`tick` takes the lease for the duration of the poll, renews it while the poll
runs, and releases it on exit.
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
