<!--
Proposed launch post for workos.com/blog

Slug:    postgres-sync
Byline:  single author
Date:    August 6, 2026
Length:  ~640 words
Voice:   2026 register — problem-first opening, no contractions, minimal code,
         closing "Available now" heading with a docs link.
-->

# Postgres Sync: Keep a copy of your WorkOS data in your own database

A sync engine that mirrors your users, organizations, and directory data into Postgres, so you can query them with SQL and join them to your own tables.

---

Every application built on WorkOS eventually needs to ask a question that
spans two systems. Which enterprise seats have we provisioned but nobody uses?
Which organizations verified a domain and then never finished setting up SCIM?
Which accounts saw a spike in failed sign-ins last night?

None of those questions have an endpoint. Each one is a join between data that
lives in WorkOS and data that lives in your database, and answering it through
an API means paginating one list, holding it in memory, paginating another, and
reconciling the two in application code. Teams do build this. It usually starts
as a webhook handler that writes a `users` table, and it grows a backfill
script, then a nightly repair job, then a second table, and at no point does
anyone feel confident it is correct.

Today we are shipping the thing that handler was trying to become. The WorkOS
Postgres Sync engine mirrors your WorkOS data into a database you control, and
keeps it current.

## What we are shipping

An open-source sync engine, available as a library and a command line tool. It
creates a `workos` schema in your database, loads everything that already
exists, and then follows the events API to stay current.

```
npx workos-sync backfill
npx workos-sync listen
```

After that, your identity data is ordinary tables:

```sql
select o.name as organization, u.email, m.role_slug
from workos.organization_memberships m
join workos.users u on u.id = m.user_id and u._env = m._env
join workos.organizations o on o.id = m.organization_id and o._env = m._env
where u._deleted_at is null;
```

It mirrors users, organizations, memberships, domains, invitations, and
sessions; directories, directory users, and groups; SSO connections; roles,
permissions, and feature flags. Every event is archived alongside them.

## How it works

Three parts, and the third is the one that hand-rolled versions leave out.

**Backfill** pages the list endpoints. The events API retains 90 days, so
everything older than that exists only here. This is the step a webhook-only
integration cannot do, and the reason those integrations begin life already
incomplete.

**The tail** follows the events API from a stored cursor. Events arrive in a
consistent order, the stream is replayable, and your process sets the pace
rather than absorbing whatever spike arrives. There is no public endpoint to
expose. Writes are gated on the object's own timestamp, so an out-of-order
delivery cannot overwrite newer state, and the object id is the primary key, so
a redelivery is a no-op.

**Reconciliation** compares the mirror against the API and repairs what it
finds. This is the part worth insisting on: a deleted object is not returned by
any list endpoint, so if you miss a delete event, no amount of re-listing will
ever reveal that the object is gone. Only a set difference finds it.

One detail is specific to our API and matters more than it sounds. Event
payloads are subsets of the corresponding objects — a `user.updated` event does
not carry `external_id` or `metadata`. A naive implementation replaces the row
on every event and quietly nulls out precisely the column you join on. The
engine merges event data into the stored object instead, and can re-fetch an
object on creation so a new user is complete immediately.

## A read model, deliberately

The mirror is eventually consistent, usually within seconds, and the sync runs
one way. Read from it for lists, joins, aggregates, dashboards, and analytics.
Keep authorization decisions on the session token, and read a user you just
created from the API response you already have. Send writes to the API and let
them flow back through the stream.

That boundary is why a read-only Postgres role over the `workos` schema is also
a good way to give an agent access to your identity data. One SQL connection
replaces a set of per-endpoint tools, and the grant is easy to reason about.

## Available now

Postgres Sync is available today under an MIT license. Install it with
`npm install @workos-inc/sync-engine`. Check out the docs for more information.
