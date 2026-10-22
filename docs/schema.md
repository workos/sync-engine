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

## Stream modes

| Mode     | Behaviour                                                                |
| -------- | ------------------------------------------------------------------------ |
| `mirror` | Current state, upserted from events, repaired by backfill and reconcile. |
| `append` | Immutable log. Rows are never updated or deleted.                        |
| `poll`   | No events exist upstream. Only as fresh as the last `reconcile`.         |

## Tables

### Identity

| Table                      | Key  | Events                                       | Backfill                       | Notes                                                                                               |
| -------------------------- | ---- | -------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------- |
| `users`                    | `id` | `user.created/updated/deleted`               | `/user_management/users`       | Events omit `external_id`, `metadata`, `last_sign_in_at`, `locale`                                  |
| `organizations`            | `id` | `organization.created/updated/deleted`       | `/organizations`               | Events omit `metadata`, `stripe_customer_id`                                                        |
| `organization_memberships` | `id` | `organization_membership.*`                  | per organization               | List requires a filter and defaults to active only; the engine asks for all statuses                |
| `organization_domains`     | `id` | `organization_domain.*` (5 types)            | embedded in organizations      | No standalone list endpoint                                                                         |
| `invitations`              | `id` | `invitation.created/accepted/resent/revoked` | `/user_management/invitations` | `token` and `accept_invitation_url` are stripped before the row is written; events never carry them |
| `sessions`                 | `id` | `session.created/revoked`                    | per user, active only          | Backfill is partial by construction                                                                 |

### Directory Sync and SSO

| Table                         | Key                                       | Events                                     | Backfill            | Notes                                                                                                                     |
| ----------------------------- | ----------------------------------------- | ------------------------------------------ | ------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `directories`                 | `id`                                      | `dsync.activated`, `dsync.deleted`         | `/directories`      | **No `dsync.updated` exists** — a directory going `linked → invalid_credentials` emits nothing. Only `reconcile` sees it. |
| `directory_users`             | `id`                                      | `dsync.user.*`                             | `/directory_users`  | 4 req/s per directory; `groups` is omitted from events                                                                    |
| `directory_groups`            | `id`                                      | `dsync.group.created/updated/deleted`      | `/directory_groups` |                                                                                                                           |
| `directory_group_memberships` | `(directory_group_id, directory_user_id)` | `dsync.group.user_added/removed`           | —                   | The membership has no upstream id; the pair is the key                                                                    |
| `connections`                 | `id`                                      | `connection.activated/deactivated/deleted` | `/connections`      | Certificate events are archived to `events` only                                                                          |

### Authorization

| Table                | Key                                      | Events                          | Backfill                     | Notes                                                            |
| -------------------- | ---------------------------------------- | ------------------------------- | ---------------------------- | ---------------------------------------------------------------- |
| `groups`             | `id`                                     | `group.created/updated/deleted` | per organization             |                                                                  |
| `group_memberships`  | `(group_id, organization_membership_id)` | `group.member_added/removed`    | —                            | Payload is ids only                                              |
| `roles`              | `slug`                                   | `role.created/updated/deleted`  | `/authorization/roles`       | **Events carry no `id`** — slug is the key. List is unpaginated. |
| `organization_roles` | `(organization_id, slug)`                | `organization_role.*`           | per organization             |                                                                  |
| `permissions`        | `slug`                                   | `permission.*`                  | `/authorization/permissions` |                                                                  |
| `feature_flags`      | `slug`                                   | `flag.*`                        | `/feature-flags`             | Flags are project-scoped, shared across environments             |

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
select email, count(*) as failures
from workos.auth_attempts
where status = 'failed' and created_at > now() - interval '1 hour'
group by email having count(*) > 5;
```

### Poll-only

`fga_resources` and `webhook_endpoints` have list endpoints but no events at
all. They are refreshed by `reconcile`, and are exactly as stale as your last
run.

## What is not mirrored

Stated plainly, because knowing the boundary matters:

| Object                                                | Why                                                                                                                      |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| SSO profiles                                          | No list endpoint, no get-by-id, and no events. Only returned during a live sign-in.                                      |
| Audit log events                                      | Not listable and not evented. Use per-organization CSV exports or Log Streams.                                           |
| Radar block/allow lists                               | Write-only endpoints.                                                                                                    |
| Vault values                                          | Should not leave Vault. Vault _activity_ is archived to `events`.                                                        |
| Portal links, widget tokens                           | Ephemeral credentials, not state.                                                                                        |
| Invitation acceptance tokens, webhook signing secrets | Live credentials. The REST objects carry them, so the engine strips them (`StreamDef.redact`) before any row is written. |

Certificate, Vault, Radar, agent and Pipes events are archived to
`workos.events` even though they have no mirror table.

## Extending it

To project a field you care about that is not yet a typed column, add it to the
stream's `columns` and re-run `init`:

```ts
{ name: 'department', type: 'text', path: ['custom_attributes', 'department'], index: true }
```

The next run issues `ALTER TABLE ... ADD COLUMN ... GENERATED ALWAYS AS ...
STORED`, which Postgres computes from data already in `_raw`. No backfill, no
downtime. Until then, query it directly:

```sql
select _raw->'custom_attributes'->>'department' as department from workos.directory_users;
```
