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
| Process exited         | `listen` returned or crashed                                   | A rejected credential or a backwards-paging cursor stops the engine on purpose.                |

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
nothing is left behind in the schema.

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
`WORKOS_ENVIRONMENT_NAME`; they can share a database. Two processes tailing the
same environment with the same `syncId` will both advance the same cursor and
duplicate work. If you genuinely need that, give each its own `WORKOS_SYNC_ID`.
The library form is the exception: one embedded engine can hold several
environments in its `environments` array.

`listen` handles `SIGINT` and `SIGTERM` by finishing the current batch and
stopping, so a rolling deploy does not interrupt a write mid-flight.

**As a scheduled job**, if a few minutes of lag is acceptable:

```bash
*/5 * * * * workos-sync-postgres tick
```

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
