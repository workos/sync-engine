# Consistency and guarantees

What the mirror promises, what it does not, and where to read instead.

This page exists because the equivalent question went unanswered for years in a
comparable project's issue tracker: "is this for analytics, or can it serve
regular interactions?" The ambiguity caused real bugs. So, plainly:

> **The mirror is a read model. It is eventually consistent, usually within
> seconds. It is not the system of record, and it must not be used as one for
> authorization decisions or read-your-writes flows.**

## What it guarantees

**Convergence.** Every write is idempotent and gated on a source-derived
timestamp, so the mirror converges on current upstream state regardless of the
order events arrive, how many times they are delivered, or when the process
restarts.

**Durability of what it has seen.** A cursor is never advanced before the
writes it covers are committed. A crash mid-batch replays that batch rather
than skipping it.

**No silent data loss on unknown input.** Every event is archived verbatim
before routing. An event type the engine does not recognise is stored and
reported, never dropped and never thrown.

**Detectable drift.** `reconcile` compares the mirror against the API and
reports differences in both directions. It is the only mechanism that can find
a missed delete.

## What it does not guarantee

**Read-your-writes.** If you create a user through the WorkOS API and
immediately query the mirror, the row may not be there yet. Typical lag is a
few seconds; there is no upper bound during an incident.

Use the API response you already have, or the session token, for anything in
that path, new-user onboarding especially.

```ts
// Wrong: the row may not exist yet.
const user = await workos.userManagement.createUser({ email });
const row = await db.query("select * from workos.users where id = $1", [
  user.id,
]);

// Right: you already have the object.
const user = await workos.userManagement.createUser({ email });
```

**Authorization decisions.** Do not gate access on the mirror. Roles and
permissions belong in the session token, which is signed, current, and does not
depend on a sync process being healthy. A mirror row can be stale, and during
an outage it can be stale for a long time without anything appearing wrong.

Use the mirror to _analyze_ authorization (who has which role, which
organizations have admins), not to _enforce_ it.

**Completeness for nearly every object.** Some streams cannot be complete:

| Stream                                             | Limitation                                                                                                                                         |
| -------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sessions`                                         | Only active sessions are listable, and only per user. Ended sessions older than the event window are unrecoverable.                                |
| `auth_attempts`                                    | No REST endpoint exists at all. History begins when you start syncing.                                                                             |
| `organization_domains`                             | No standalone list endpoint; sourced from the organization payload. `reconcile` cannot check it.                                                   |
| `group_memberships`, `directory_group_memberships` | Event-only. There is no list endpoint, so memberships that existed before you started syncing are not mirrored, and `reconcile` cannot check them. |
| `directories`                                      | No `dsync.updated` event exists. A directory going `linked → invalid_credentials` emits nothing; only `reconcile` sees the change.                 |
| `fga_resources`, `webhook_endpoints`               | No events at all. Opt-in, and exactly as fresh as your last `reconcile`.                                                                           |

**Ordering for events sharing a timestamp.** Two events can carry the same
`updated_at`. The staleness gate uses `>=`, so the last one applied wins. That
is deliberate, because a strict `>` would drop the second permanently, which is
worse. But it means simultaneous events are resolved by arrival, not by truth.
Reconciliation is the backstop.

## Vendor state is not business truth

The mirror faithfully reflects what WorkOS says. That is not always what you
mean.

A membership with `status = 'active'` means active in WorkOS. Whether that
person should still have access to your product is your question, answered by
your data. The mirror makes the join possible; it does not make the judgement.

This is the same trap as reading a payment provider's `subscription.status =
active` during a failed-payment grace period and concluding the customer is in
good standing. The field is accurate. The inference is yours to get right.

## Where to read from

| Need                               | Read from                                            |
| ---------------------------------- | ---------------------------------------------------- |
| Authorization decision             | Session token / JWT                                  |
| A change you just made             | The API response you already have                    |
| Current state of one known object  | WorkOS API, or the mirror if seconds of lag are fine |
| Lists, filters, joins, aggregates  | **The mirror**                                       |
| Anything joined to your own tables | **The mirror**                                       |
| Dashboards, analytics, reporting   | **The mirror**                                       |
| Agent or LLM access                | **The mirror**, via a read-only role                 |
| Sign-in history                    | **The mirror**, because it exists nowhere else       |

## Writes

There are none, and there will not be. The mirror is one-directional.

Send writes to the WorkOS API and let them flow back through the event stream.
Two-way sync is a different and much harder product.

If you want a change reflected immediately after writing it, use the API
response. It is the authoritative version of the object, and it is already in
your hand.

## Tombstones

Deleted objects are marked, not removed:

- `_deleted_at IS NULL`: live
- `_deleted_at IS NOT NULL`: deleted upstream, payload retained
- no row at all: never synced, or hard-deleted by `redact`

Filter on `_deleted_at IS NULL` for live data, or read the generated
`<table>_live` view, which is the table with that filter built in
([Live views](schema.md#live-views)). The distinction between "deleted" and
"never seen" is only visible on the base table, and is usually worth having:
the retained payload is the only record of what an object looked like when it
was removed.

Once set, `_deleted_at` is never cleared on a mirror object. A late `updated`
event refreshes the payload but does not resurrect the row, because WorkOS ids
are ULIDs and are never reused. Memberships are the exception, because a re-add
is a genuine event.

## Failure behaviour

| Failure                           | Behaviour                                                                                                                                                                |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Network error or 5xx              | Retried with backoff, up to five times, then the operation fails.                                                                                                        |
| 429                               | Retried, honouring `Retry-After`.                                                                                                                                        |
| One stream fails during backfill  | Isolated. Other streams continue; the failed stream's checkpoint stays where it was and `--resume` picks it up.                                                          |
| One stream fails during reconcile | Isolated. Other streams are still diffed; the failed one is logged and omitted from the report.                                                                          |
| A write fails during the tail     | Retried on the next poll when it could be transient: a dropped connection, a deadlock, a hydration 5xx. When it is deterministic, the engine finds the event at fault and, once it has failed on two consecutive polls, quarantines it in `_rejected` and moves the cursor past it, so one bad payload cannot stall the environment. See [Quarantine](#quarantine). |
| Invalid API key (401 or 403)      | Fatal. The engine stops rather than looping, because retrying will not fix a rejected credential and a healthy-looking process that writes nothing is the worse outcome. |
| Cursor pages backwards            | Fatal, with an explicit error. Continuing would overwrite current state with history.                                                                                    |
| Unknown event type                | Archived, logged at info, processing continues.                                                                                                                          |
| Process dies mid-batch            | The batch replays on restart. Writes are idempotent.                                                                                                                     |

The Events API's replayability is the retry queue for everything transient: a
failed range simply is not acknowledged, and the next poll retries it. That is
only safe if you alert on poll age, which is why `status` reports it and why it
is the first thing in [Operations](operations.md).

### Quarantine

There is one cursor per environment, not per stream. A batch that fails
deterministically — an `updated_at` the `::timestamptz` cast rejects, a
generated column that cannot cast one payload's value, a router bug on a shape
nobody has seen — would otherwise stop every stream in that environment,
sign-in capture included, while the 90-day retention clock runs. The quarantine
exists for exactly that case, and it is deliberately narrow, because skipping
an event is permanent and reprocessing one is free:

- **Only deterministic failures.** A hydration failure, any WorkOS API error,
  and any database error the destination calls transient (for Postgres:
  SQLSTATE classes `08`, `40`, `53`, `57`, `58` and `XX`, and socket errors)
  fail the tick and retry exactly as before. Nothing is set aside.
- **Only after two sightings.** When a batch fails for a reason that could be
  deterministic, the events are applied one at a time to find the one at
  fault. The first time it fails, the tick still fails and the cursor stays
  put; it is quarantined only if it fails again on the next poll. That costs
  one poll interval and protects against transient errors the classifier does
  not recognise.
- **Never a whole batch.** If every event in a batch of more than one fails,
  that is an outage, and the batch is retried.
- **Reversible.** The whole event is kept in `_rejected`, `status` reports
  the count, and `rejected --replay` puts it back through the normal write
  path once the cause is fixed. Until then the object it describes is missing
  or stale in the mirror: convergence, for that one object, waits on the
  replay. Alert on `rejected` for the same reason you alert on poll age.
