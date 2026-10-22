# Querying the mirror

Every example query below assumes a single environment; if you mirror more
than one, add `_env = 'production'` or join on `_env`.

There are two conventions throughout:

- Filter `_deleted_at is null` for live rows.
- Join on `_env` as well as the id.

## Seats and adoption

**Provisioned but dormant seats.**

```sql
SELECT o.name AS organization,
       COUNT(*) AS seats,
       COUNT(*) FILTER (WHERE u.last_sign_in_at IS NULL
                           OR u.last_sign_in_at < NOW() - INTERVAL '30 days') AS dormant
   FROM workos.organization_memberships m
      JOIN workos.users u ON u.id = m.user_id AND u._env = m._env
      JOIN workos.organizations o ON o.id = m.organization_id AND o._env = m._env
   WHERE u._deleted_at IS NULL AND m._deleted_at IS NULL AND m.status = 'active'
   GROUP BY o.name
   HAVING COUNT(*) FILTER (WHERE u.last_sign_in_at IS NULL
                              OR u.last_sign_in_at < NOW() - INTERVAL '30 days') > 0
   ORDER BY dormant DESC;
```

`last_sign_in_at` is `null` for a user who has never signed in, and also for a
row that only events have touched, since `user.*` events omit it. Hydration on
create and `refresh` fill it in; the `is null` branch keeps the never-signed-in
seats counted either way.

**Users who signed up but never joined an organization.**

```sql
SELECT u.email, u.created_at, NOW() - u.created_at AS age
   FROM workos.users u
      LEFT JOIN workos.organization_memberships m
         ON m.user_id = u.id AND m._env = u._env AND m._deleted_at IS NULL
   WHERE u._deleted_at IS NULL
      AND m.id IS NULL
      AND u.created_at < NOW() - INTERVAL '7 days'
   ORDER BY u.created_at;
```

## Enterprise readiness

**Organizations with a verified domain but no directory.**

```sql
SELECT o.name AS organization, d.domain, o.created_at
   FROM workos.organizations o
      JOIN workos.organization_domains d
         ON d.organization_id = o.id AND d._env = o._env AND d.state = 'verified'
      LEFT JOIN workos.directories dir
         ON dir.organization_id = o.id AND dir._env = o._env AND dir._deleted_at IS NULL
   WHERE o._deleted_at IS NULL AND dir.id IS NULL
   ORDER BY o.created_at;
```

**Directories in a broken state.** There is no `dsync.updated` event, so a
directory whose credentials expired emits nothing at all. This query — over a
mirror kept fresh by `reconcile` — is how you find them.

```sql
SELECT o.name AS organization, d.name AS directory, d.type, d.state, d.updated_at
   FROM workos.directories d
      JOIN workos.organizations o ON o.id = d.organization_id AND o._env = d._env
   WHERE d._deleted_at IS NULL AND d.state <> 'linked'
   ORDER BY d.updated_at;
```

**SSO adoption by connection type.**

```sql
SELECT connection_type, state, COUNT(*) AS connections
   FROM workos.connections
   WHERE _deleted_at IS NULL
   GROUP BY connection_type, state
   ORDER BY connections DESC;
```

## Authentication

These read `auth_attempts`, which exists only because the mirror captured it —
there is no endpoint to ask for this history.

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
   FROM workos.users u
      JOIN app.accounts a ON a.id = u.external_id
      LEFT JOIN app.sessions s ON s.account_id = a.id AND s.created_at > NOW() - INTERVAL '30 days'
   WHERE u._deleted_at IS NULL
   GROUP BY u.email, u.metadata->>'plan'
   ORDER BY sessions_30d DESC;
```

If you also mirror your billing provider, `organizations.stripe_customer_id` is
already on the organization object:

```sql
SELECT o.name, s.status AS subscription, COUNT(m.id) AS seats
   FROM workos.organizations o
      JOIN billing.subscriptions s ON s.customer = o.stripe_customer_id
      LEFT JOIN workos.organization_memberships m
         ON m.organization_id = o.id AND m._env = o._env AND m._deleted_at IS NULL
   WHERE o._deleted_at IS NULL
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

Tombstones mean you can still ask who left.

```sql
SELECT email, _deleted_at AS deleted_at, last_sign_in_at
   FROM workos.users
   WHERE _deleted_at IS NOT NULL AND _deleted_at > NOW() - INTERVAL '30 days'
   ORDER BY _deleted_at DESC;
```

## Convenience views

If filtering tombstones on every query is tedious, create views once:

```sql
CREATE SCHEMA IF NOT EXISTS workos_live;

CREATE VIEW workos_live.users AS
   SELECT * FROM workos.users WHERE _deleted_at IS NULL;

CREATE VIEW workos_live.organizations AS
   SELECT * FROM workos.organizations WHERE _deleted_at IS NULL;
```

## Giving an agent read access

A read-only role over the mirror is a far better-scoped grant than an API key,
and one SQL tool replaces a pile of per-endpoint tools:

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
          COUNT(m.id) FILTER (WHERE m._deleted_at IS NULL) AS organizations
      FROM workos.users u
         LEFT JOIN workos.organization_memberships m
            ON m.user_id = u.id AND m._env = u._env
      WHERE u._deleted_at IS NULL
      GROUP BY u.id, u._env, u.created_at, u.last_sign_in_at;

GRANT USAGE ON SCHEMA workos_safe TO analytics_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA workos_safe TO analytics_reader;
```
