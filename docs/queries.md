# Querying the mirror

Worked examples. Every query assumes a single environment; if you mirror more
than one, add `_env = 'production'` or join on `_env`.

Two conventions throughout:

- Filter `_deleted_at is null` for live rows.
- Join on `_env` as well as the id.

## Seats and adoption

**Provisioned but dormant seats.** The question that pays for the whole
exercise — you cannot answer it from an API without listing every membership
and cross-referencing your own usage data.

```sql
select o.name as organization,
       count(*) as seats,
       count(*) filter (where u.last_sign_in_at < now() - interval '30 days') as dormant
from workos.organization_memberships m
join workos.users u on u.id = m.user_id and u._env = m._env
join workos.organizations o on o.id = m.organization_id and o._env = m._env
where u._deleted_at is null and m._deleted_at is null and m.status = 'active'
group by o.name
having count(*) filter (where u.last_sign_in_at < now() - interval '30 days') > 0
order by dormant desc;
```

**Users who signed up but never joined an organization.**

```sql
select u.email, u.created_at, now() - u.created_at as age
from workos.users u
left join workos.organization_memberships m
  on m.user_id = u.id and m._env = u._env and m._deleted_at is null
where u._deleted_at is null
  and m.id is null
  and u.created_at < now() - interval '7 days'
order by u.created_at;
```

## Enterprise readiness

**Organizations with a verified domain but no directory.** Half-finished
enterprise setups, which is usually a support conversation waiting to happen.

```sql
select o.name as organization, d.domain, o.created_at
from workos.organizations o
join workos.organization_domains d
  on d.organization_id = o.id and d._env = o._env and d.state = 'verified'
left join workos.directories dir
  on dir.organization_id = o.id and dir._env = o._env and dir._deleted_at is null
where o._deleted_at is null and dir.id is null
order by o.created_at;
```

**Directories in a broken state.** There is no `dsync.updated` event, so a
directory whose credentials expired emits nothing at all. This query — over a
mirror kept fresh by `reconcile` — is how you find them.

```sql
select o.name as organization, d.name as directory, d.type, d.state, d.updated_at
from workos.directories d
join workos.organizations o on o.id = d.organization_id and o._env = d._env
where d._deleted_at is null and d.state <> 'linked'
order by d.updated_at;
```

**SSO adoption by connection type.**

```sql
select connection_type, state, count(*) as connections
from workos.connections
where _deleted_at is null
group by connection_type, state
order by connections desc;
```

## Authentication

These read `auth_attempts`, which exists only because the mirror captured it —
there is no endpoint to ask for this history.

**Failed sign-ins by user, last hour.**

```sql
select coalesce(u.email, a.email) as email,
       count(*) as failures,
       max(a.created_at) as most_recent,
       count(distinct a.ip_address) as distinct_ips
from workos.auth_attempts a
left join workos.users u on u.id = a.user_id and u._env = a._env
where a.status = 'failed' and a.created_at > now() - interval '1 hour'
group by 1
having count(*) >= 5
order by failures desc;
```

**Sign-in success rate by method.**

```sql
select split_part(event, '.', 2) as method,
       count(*) filter (where status = 'succeeded') as succeeded,
       count(*) filter (where status = 'failed') as failed,
       round(100.0 * count(*) filter (where status = 'succeeded') / count(*), 1) as success_rate
from workos.auth_attempts
where created_at > now() - interval '7 days'
group by 1
order by succeeded + failed desc;
```

**Credential stuffing: one IP against many accounts.**

```sql
select ip_address,
       count(distinct email) as accounts_tried,
       count(*) as attempts
from workos.auth_attempts
where status = 'failed' and created_at > now() - interval '1 day'
group by ip_address
having count(distinct email) > 10
order by accounts_tried desc;
```

## Joining your own tables

The mirror is only interesting next to your data. `external_id` is the
intended join key — set it on the WorkOS user and it lands in the mirror.

```sql
select u.email,
       u.metadata->>'plan' as plan,
       count(s.id) as sessions_30d,
       max(s.created_at) as last_active
from workos.users u
join app.accounts a on a.workos_user_id = u.external_id
left join app.sessions s on s.account_id = a.id and s.created_at > now() - interval '30 days'
where u._deleted_at is null
group by u.email, u.metadata->>'plan'
order by sessions_30d desc;
```

If you also mirror your billing provider, `organizations.stripe_customer_id` is
already on the organization object:

```sql
select o.name, s.status as subscription, count(m.id) as seats
from workos.organizations o
join billing.subscriptions s on s.customer = o.stripe_customer_id
left join workos.organization_memberships m
  on m.organization_id = o.id and m._env = o._env and m._deleted_at is null
where o._deleted_at is null
group by o.name, s.status;
```

## Change history

The `events` table is an ordered local log, so you can answer "what happened to
this object" without an API call.

```sql
select created_at, event, _raw->'context'->>'actor' as actor
from workos.events
where object_id = 'user_01HXXXXXXXXXXXXXXXXXXXXXX'
order by created_at;
```

Membership churn over time:

```sql
select date_trunc('day', created_at) as day,
       count(*) filter (where event = 'organization_membership.created') as joined,
       count(*) filter (where event = 'organization_membership.deleted') as left
from workos.events
where event like 'organization_membership.%' and created_at > now() - interval '90 days'
group by 1 order by 1;
```

## Deleted objects

Tombstones mean you can still ask who left.

```sql
select email, _deleted_at as deleted_at, _raw->>'last_sign_in_at' as last_sign_in
from workos.users
where _deleted_at is not null and _deleted_at > now() - interval '30 days'
order by _deleted_at desc;
```

## Convenience views

If filtering tombstones on every query is tedious, create views once:

```sql
create schema if not exists workos_live;

create view workos_live.users as
  select * from workos.users where _deleted_at is null;

create view workos_live.organizations as
  select * from workos.organizations where _deleted_at is null;
```

## Giving an agent read access

A read-only role over the mirror is a far better-scoped grant than an API key,
and one SQL tool replaces a pile of per-endpoint tools:

```sql
create role analytics_reader login password '...';
grant usage on schema workos to analytics_reader;
grant select on all tables in schema workos to analytics_reader;
alter default privileges in schema workos grant select on tables to analytics_reader;
```

To keep personal data out of reach, grant on a redacted view instead:

```sql
create view workos_safe.users as
  select id, _env, organization_id_placeholder as org, created_at, last_sign_in_at
  from workos.users where _deleted_at is null;
```
