# Schema reference

Everything lives in a `workos` schema by default. Set `WORKOS_SYNC_SCHEMA` or
pass `--schema` to change it; the setting applies to both the DDL and every
query the engine issues.

## Columns every table has

| Column        | Type          | Meaning                                                                                                                    |
| ------------- | ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `_env`        | `text`        | The environment label from your config. Part of every primary key.                                                         |
| `_raw`        | `jsonb`       | The object as WorkOS returned it, minus any credential the stream declares in `redact`. The only column the engine writes. |
| `_updated_at` | `timestamptz` | Source time. The staleness gate compares this.                                                                             |
| `_synced_at`  | `timestamptz` | Write time. Lag is `_synced_at - _updated_at`.                                                                             |
| `_deleted_at` | `timestamptz` | Tombstone. `null` means live. Absent on append-only tables.                                                                |

Everything else is `GENERATED ALWAYS AS ... STORED`, projected out of `_raw`.

Two consequences worth internalising:

- **Filter `_deleted_at is null`** unless you specifically want deleted rows.
- **Join on `_env` as well as the id.** Ids are unique within an environment,
  and one database can mirror several.

## Bookkeeping tables

| Table         | Purpose                                                                                                                                                       |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `_sync_state` | Cursors and checkpoints, keyed by `(sync_id, env, stream)`. The tail position is the row with `stream = '__tail__'`; backfill progress is one row per stream. |
| `_migrations` | Fingerprints of the schema versions that have been applied, so `init` is a single `SELECT` when nothing changed.                                              |

## Stream modes

| Mode     | Behaviour                                                                |
| -------- | ------------------------------------------------------------------------ |
| `mirror` | Current state, upserted from events, repaired by backfill and reconcile. |
| `append` | Immutable log. Rows are never updated or deleted.                        |
| `poll`   | No events exist upstream. Only as fresh as the last `reconcile`.         |

## Which streams sync by default

Every stream that has events. That is everything below except the two
poll-only streams at the end, which cost a full re-list on every reconcile and
are only as fresh as that run, so they are opt-in:

```ts
import { defaultStreams } from "@workos/sync-postgres";

new SyncEngine({
  // ...
  streams: [...defaultStreams(), "fga_resources", "webhook_endpoints"],
});
```

Or on the command line, `--streams users,organizations,...`. The `events`
archive is always included, whatever you pass, because every event is written
there before it is routed.

`workos-sync-postgres streams` prints each stream with its group, mode,
whether it is on by default, and how it is backfilled.

## Tables

### Identity

| Table                      | Key  | Events                                       | Backfill                       | Notes                                                                                               |
| -------------------------- | ---- | -------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------- |
| `users`                    | `id` | `user.created/updated/deleted`               | `/user_management/users`       | Events omit `external_id`, `metadata`, `last_sign_in_at`, `locale`; hydrated on create by default   |
| `organizations`            | `id` | `organization.created/updated/deleted`       | `/organizations`               | Events omit `metadata`, `stripe_customer_id`; hydrated on create by default                         |
| `organization_memberships` | `id` | `organization_membership.*`                  | per organization               | List requires a filter and defaults to active only; the engine asks for all statuses                |
| `organization_domains`     | `id` | `organization_domain.*` (5 types)            | embedded in organizations      | No standalone list endpoint, so `reconcile` cannot check it                                         |
| `invitations`              | `id` | `invitation.created/accepted/resent/revoked` | `/user_management/invitations` | `token` and `accept_invitation_url` are stripped before the row is written; events never carry them |
| `sessions`                 | `id` | `session.created/revoked`                    | per user, active only          | Backfill is partial by construction                                                                 |

### Directory Sync and SSO

| Table                         | Key                                       | Events                                     | Backfill            | Notes                                                                                                                    |
| ----------------------------- | ----------------------------------------- | ------------------------------------------ | ------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `directories`                 | `id`                                      | `dsync.activated`, `dsync.deleted`         | `/directories`      | **No `dsync.updated` exists.** A directory going `linked → invalid_credentials` emits nothing. Only `reconcile` sees it. |
| `directory_users`             | `id`                                      | `dsync.user.*`                             | `/directory_users`  | 4 req/s per directory; the REST object's `groups` array is omitted from events                                           |
| `directory_groups`            | `id`                                      | `dsync.group.created/updated/deleted`      | `/directory_groups` |                                                                                                                          |
| `directory_group_memberships` | `(directory_group_id, directory_user_id)` | `dsync.group.user_added/removed`           | none                | The membership has no upstream id; the pair is the key. Memberships that predate the sync are not mirrored.              |
| `connections`                 | `id`                                      | `connection.activated/deactivated/deleted` | `/connections`      | Certificate events are archived to `events` only                                                                         |

### Authorization

| Table                | Key                                      | Events                          | Backfill                     | Notes                                                                                                      |
| -------------------- | ---------------------------------------- | ------------------------------- | ---------------------------- | ---------------------------------------------------------------------------------------------------------- |
| `groups`             | `id`                                     | `group.created/updated/deleted` | per organization             |                                                                                                            |
| `group_memberships`  | `(group_id, organization_membership_id)` | `group.member_added/removed`    | none                         | Payload is ids only. Memberships that predate the sync are not mirrored.                                   |
| `roles`              | `slug`                                   | `role.created/updated/deleted`  | `/authorization/roles`       | **Events carry no `id`**, so slug is the key. List is unpaginated.                                         |
| `organization_roles` | `(organization_id, slug)`                | `organization_role.*`           | per organization             |                                                                                                            |
| `permissions`        | `slug`                                   | `permission.*`                  | `/authorization/permissions` |                                                                                                            |
| `feature_flags`      | `slug`                                   | `flag.*`                        | `/feature-flags`             | Flags are defined per project, so every environment key lists the same flags; each appears once per `_env` |

### Logs

| Table           | Key        | Contents                                                    |
| --------------- | ---------- | ----------------------------------------------------------- |
| `events`        | `id`       | Every event, verbatim, including types with no mirror table |
| `auth_attempts` | `event_id` | All 15 `authentication.*` types                             |

`auth_attempts` deserves attention: **authentication events have no REST
endpoint anywhere in the WorkOS API.** There is no way to list sign-ins, so if
you do not capture them as they happen the history is gone after 90 days. Once
mirrored, it is an ordinary table:

```sql
SELECT email, COUNT(*) AS failures
   FROM workos.auth_attempts
   WHERE status = 'failed' AND created_at > NOW() - INTERVAL '1 hour'
   GROUP BY email
   HAVING COUNT(*) > 5;
```

### Poll-only (opt-in)

| Table               | Key  | Backfill                   | Notes                                                               |
| ------------------- | ---- | -------------------------- | ------------------------------------------------------------------- |
| `fga_resources`     | `id` | `/authorization/resources` | No events exist. Refreshed by `reconcile`, and exactly that stale.  |
| `webhook_endpoints` | `id` | `/webhook_endpoints`       | No events exist. The signing `secret` is stripped before any write. |

## What is not mirrored

| Object                                                | Why                                                                                                                      |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| SSO profiles                                          | No list endpoint, no get-by-id, and no events. Only returned during a live sign-in.                                      |
| Audit log events                                      | Not listable and not evented. Use per-organization CSV exports or Log Streams.                                           |
| Radar block/allow lists                               | Write-only endpoints.                                                                                                    |
| Vault values                                          | Should not leave Vault. Vault _activity_ events can be archived to `events` if you subscribe to them.                    |
| Portal links, widget tokens                           | Ephemeral credentials, not state.                                                                                        |
| Invitation acceptance tokens, webhook signing secrets | Live credentials. The REST objects carry them, so the engine strips them (`StreamDef.redact`) before any row is written. |

Events with no mirror table (certificate renewals, Vault activity, Radar,
agent, Pipes, API keys, password resets, and so on) are not fetched by default:
the subscription is only what the mirrored streams declare, because
`GET /events` requires an explicit list. Add their names to `eventTypes` and
they are archived verbatim to `workos.events`, where they stay queryable.

## Extending it

To project a field you care about that is not yet a typed column, pass it as
an extra column. The next `init` (or any engine method, since each applies the
schema first) issues `ALTER TABLE ... ADD COLUMN ... GENERATED ALWAYS AS ... STORED`,
which Postgres computes from data already in `_raw`. No backfill, no
downtime.

```ts
new SyncEngine({
  // ...
  extraColumns: {
    directory_users: [
      {
        name: "department",
        type: "text",
        path: ["custom_attributes", "department"],
        index: true,
      },
    ],
  },
});
```

Column types are `text`, `boolean`, `bigint`, `numeric`, `jsonb` and
`timestamptz`. The `path` is a top-level key or a list of keys to walk.

Until a column exists, query the field directly:

```sql
SELECT _raw->'custom_attributes'->>'department' AS department FROM workos.directory_users;
```

Changing or removing a column you added is covered next.

## Changing a column

The DDL is additive: `ADD COLUMN IF NOT EXISTS` does nothing for a column that
is already there. So when a release changes an existing catalog column's path
or type, or you edit an `extraColumns` entry, a database that already has the
column keeps the old projection, and nothing fails, because the engine only
ever writes `_raw`. That is **column drift**: a generated column that no longer
matches the catalog.

`init` (and every engine method, since each applies the schema first) checks
for it whenever the catalog fingerprint is new. It builds the catalog's columns
on a temp table, reads that and the live table back through `pg_get_expr`, and
compares. Three kinds are reported:

| Kind         | Meaning                                                                | With `--repair`                              |
| ------------ | ---------------------------------------------------------------------- | -------------------------------------------- |
| `expression` | The stored expression differs from the catalog's: a path changed.      | Rebuilt from `_raw`.                         |
| `type`       | The column type differs from the catalog's.                            | Rebuilt from `_raw`.                         |
| `orphan`     | A generated column the catalog no longer declares: an entry was removed. | Left alone. The `DROP COLUMN` DDL is printed. |

Without `--repair`, `init` logs every item at `error` with the exact repair
DDL, prints the report as JSON, exits 2, and **does not record the
fingerprint**, so every start keeps reporting it until it is fixed. Ingest
carries on meanwhile: `_raw` is still right, only the projection is stale.

```bash
npx workos-sync-postgres init            # report; exits 2 on drift
npx workos-sync-postgres init --repair   # rebuild drifted columns from _raw
```

`--repair` drops each drifted column and adds it back from the catalog, each
in its own transaction, then recreates its index. `STORED` recomputes the new
expression for every row already present, so this is also the backfill, and
nothing is lost because `_raw` holds all of it. As a library, pass
`repairColumns: true` to `PostgresDestination`.

Rebuilding a column also forgets every fingerprint recorded before it. A
fingerprint says the live columns were checked against that catalog, and that
is no longer true of any catalog checked before the rebuild. Rolling back to
an earlier release therefore runs the check again and reports the columns it
would need back, instead of trusting a fingerprint the table no longer
satisfies. Several processes sharing a database can start with `--repair` at
once: each column is rebuilt under a lock, and a process that arrives second
finds it already done and leaves it alone.

Four things are never repaired automatically. The DDL is printed and the
fingerprint stays withheld until an operator acts:

- **Key columns.** Dropping one drops the primary key, and between the drop
  and the re-add the table accepts duplicates. Run the printed DDL, which
  re-adds the key, in a maintenance window with ingest stopped.
- **A column with your own index or constraint on it.** `DROP COLUMN` takes
  a `UNIQUE`, a `CHECK`, or an index you added with it, silently, and the
  engine only knows how to put back the index the catalog declares. The
  report lists them under `dependents`. Drop them, repair, recreate them.
- **A column a view depends on.** `DROP COLUMN` fails rather than cascades,
  because cascading would silently remove the view. Drop the view, repair,
  recreate the view.
- **A plain column with a catalog column's name.** Reported as `expression`
  drift with `actual: null`. It may hold data that is not in `_raw`, so
  dropping it is your call.

Orphans are only ever reported: a column the catalog does not know about is
one it cannot vouch for. They do not hold back the fingerprint, so they are
reported once per catalog change rather than on every start.
