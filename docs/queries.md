# Querying the mirror

Every example query below assumes a single environment; if you mirror more
than one, add `_env = 'production'` or join on `_env`.

There are two conventions throughout:

- Read the `_live` views (`workos.users_live`, `workos.organizations_live`,
  ...) for live rows. Each is its table minus the tombstoned rows, so no
  `_deleted_at is null` filter is needed. Read the base table when you want
  deleted rows too; see [Live views](schema.md#live-views).
- Join on `_env` as well as the id.

## Seats and adoption

**Provisioned but dormant seats.**

```sql
SELECT o.name AS organization,
       COUNT(*) AS seats,
       COUNT(*) FILTER (WHERE u.last_sign_in_at IS NULL
                           OR u.last_sign_in_at < NOW() - INTERVAL '30 days') AS dormant
   FROM workos.organization_memberships_live m
      JOIN workos.users_live u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations_live o ON o.id = m.organization_id AND o._env = m._env
   WHERE m.status = 'active'
   GROUP BY o.name
   HAVING COUNT(*) FILTER (WHERE u.last_sign_in_at IS NULL
                              OR u.last_sign_in_at < NOW() - INTERVAL '30 days') > 0
   ORDER BY dormant DESC;
```

`last_sign_in_at` is `null` for a user who has never signed in; the `is null`
branch keeps those seats counted as dormant.

**Users who signed up but never joined an organization.**

```sql
SELECT u.email, u.created_at, NOW() - u.created_at AS age
   FROM workos.users_live u
      LEFT JOIN workos.organization_memberships_live m
         ON m.user_id = u.id AND m._env = u._env
   WHERE m.id IS NULL
      AND u.created_at < NOW() - INTERVAL '7 days'
   ORDER BY u.created_at;
```

Note that the `_live` view on the outer side of the `LEFT JOIN` does the work
that `m._deleted_at IS NULL` in the `ON` clause would otherwise have to: a user
whose only membership was revoked still shows up here.

## Enterprise readiness

**Organizations with a verified domain but no directory.**

```sql
SELECT o.name AS organization, d.domain, o.created_at
   FROM workos.organizations_live o
      JOIN workos.organization_domains_live d
         ON d.organization_id = o.id AND d._env = o._env AND d.state = 'verified'
      LEFT JOIN workos.directories_live dir
         ON dir.organization_id = o.id AND dir._env = o._env
   WHERE dir.id IS NULL
   ORDER BY o.created_at;
```

**Directories in a broken state.** There is no `dsync.updated` event, so a
directory whose credentials expired emits nothing at all. This query — over a
mirror kept fresh by `reconcile` — is how you find them.

```sql
SELECT o.name AS organization, d.name AS directory, d.type, d.state, d.updated_at
   FROM workos.directories_live d
      JOIN workos.organizations_live o ON o.id = d.organization_id AND o._env = d._env
   WHERE d.state <> 'linked'
   ORDER BY d.updated_at;
```

**SSO adoption by connection type.**

```sql
SELECT connection_type, state, COUNT(*) AS connections
   FROM workos.connections_live
   GROUP BY connection_type, state
   ORDER BY connections DESC;
```

## Authentication

These read `auth_attempts`, which exists only because the mirror captured it —
there is no endpoint to ask for this history. It is append-only, so it has no
`_live` view and nothing to filter.

**Failed sign-ins by user, last hour.**

```sql
SELECT COALESCE(u.email, a.email) AS email,
       COUNT(*) AS failures,
       MAX(a.created_at) AS most_recent,
       COUNT(DISTINCT a.ip_address) AS distinct_ips
   FROM workos.auth_attempts a
      LEFT JOIN workos.users u ON u.id = a.user_id AND u._env = a._env
   WHERE a.status = 'failed' AND a.created_at > NOW() - INTERVAL '1 hour'
   GROUP BY 1
   HAVING COUNT(*) >= 5
   ORDER BY failures DESC;
```

This one joins the base `workos.users` table on purpose: an attempt against an
account that has since been deleted should still be reported under its email,
and the tombstone is where that email lives.

**Sign-in success rate by method.** The `type` column is the method
(`password`, `sso`, `oauth`, `magic_auth`, ...); `status` is derived from
whether the event name ended in `_succeeded` or `_failed`.

```sql
SELECT type AS method,
       COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE status = 'failed') AS failed,
       ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'succeeded') / COUNT(*), 1) AS success_rate
   FROM workos.auth_attempts
   WHERE created_at > NOW() - INTERVAL '7 days'
      AND status IS NOT NULL
   GROUP BY 1
   ORDER BY COUNT(*) DESC;
```

**Credential stuffing: one IP against many accounts.**

```sql
SELECT ip_address,
       COUNT(DISTINCT email) AS accounts_tried,
       COUNT(*) AS attempts
   FROM workos.auth_attempts
   WHERE status = 'failed' AND created_at > NOW() - INTERVAL '1 day'
   GROUP BY ip_address
   HAVING COUNT(DISTINCT email) > 10
   ORDER BY accounts_tried DESC;
```

## Joining your own tables

The mirror is only interesting next to your data. There are two ways to join,
and the column names should say which one you chose:

- Store the WorkOS ID on your side (`app.accounts.workos_user_id`) and join it
  to `u.id`.
- Set `external_id` on the WorkOS user to _your_ account ID, and join it to
  your primary key. This is the intended direction: `external_id` is the
  column WorkOS provides for exactly this.

```sql
SELECT u.email,
       u.metadata->>'plan' AS plan,
       COUNT(s.id) AS sessions_30d,
       MAX(s.created_at) AS last_active
   FROM workos.users_live u
      JOIN app.accounts a ON a.id = u.external_id
      LEFT JOIN app.sessions s ON s.account_id = a.id AND s.created_at > NOW() - INTERVAL '30 days'
   GROUP BY u.email, u.metadata->>'plan'
   ORDER BY sessions_30d DESC;
```

If you also mirror your billing provider, `organizations.stripe_customer_id` is
already on the organization object:

```sql
SELECT o.name, s.status AS subscription, COUNT(m.id) AS seats
   FROM workos.organizations_live o
      JOIN billing.subscriptions s ON s.customer = o.stripe_customer_id
      LEFT JOIN workos.organization_memberships_live m
         ON m.organization_id = o.id AND m._env = o._env
   GROUP BY o.name, s.status;
```

## Change history

The `events` table is an ordered local log, so you can answer "what happened to
this object" without an API call.

```sql
SELECT created_at, event, context->>'actor' AS actor
   FROM workos.events
   WHERE object_id = 'user_01HXXXXXXXXXXXXXXXXXXXXXX'
   ORDER BY created_at;
```

Membership churn over time:

```sql
SELECT DATE_TRUNC('day', created_at) AS day,
       COUNT(*) FILTER (WHERE event = 'organization_membership.created') AS joined,
       COUNT(*) FILTER (WHERE event = 'organization_membership.deleted') AS departed
   FROM workos.events
   WHERE event LIKE 'organization_membership.%' AND created_at > NOW() - INTERVAL '90 days'
   GROUP BY 1
   ORDER BY 1;
```

## Deleted objects

Tombstones mean you can still ask who left. This is the one kind of question
the `_live` views cannot answer, so these read the base table.

```sql
SELECT email, _deleted_at AS deleted_at, last_sign_in_at
   FROM workos.users
   WHERE _deleted_at IS NOT NULL AND _deleted_at > NOW() - INTERVAL '30 days'
   ORDER BY _deleted_at DESC;
```

The base table is also the only place that tells "deleted" apart from "never
synced": a row with `_deleted_at` set was seen and then removed upstream; no row
at all means the mirror never saw it.

## Convenience views

The `_live` views are generated for you, one per mirror table, and re-created
every time the schema is applied, so they always carry the table's current
columns. If you want them under different names or in a separate schema, build
your own views on top of them rather than editing them: the engine never drops
a view it did not create.

```sql
CREATE SCHEMA IF NOT EXISTS workos_live;

CREATE VIEW workos_live.users AS SELECT * FROM workos.users_live;
```

## Giving an agent read access

A read-only role over the mirror is a far better-scoped grant than an API key,
and one SQL tool replaces a pile of per-endpoint tools. `ALL TABLES` covers the
`_live` views as well:

```sql
CREATE ROLE analytics_reader LOGIN PASSWORD '...';
GRANT USAGE ON SCHEMA workos TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA workos TO analytics_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA workos GRANT SELECT ON TABLES TO analytics_reader;
```

To keep personal data out of reach, grant on a redacted view instead. A user
has no single organization, so bring memberships in as a count rather than a
column:

```sql
CREATE SCHEMA IF NOT EXISTS workos_safe;

CREATE VIEW workos_safe.users AS
   SELECT u.id, u._env, u.created_at, u.last_sign_in_at,
          COUNT(m.id) AS organizations
      FROM workos.users_live u
         LEFT JOIN workos.organization_memberships_live m
            ON m.user_id = u.id AND m._env = u._env
      GROUP BY u.id, u._env, u.created_at, u.last_sign_in_at;

GRANT USAGE ON SCHEMA workos_safe TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA workos_safe TO analytics_reader;
```
