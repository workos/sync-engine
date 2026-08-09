# WorkOS Sync Engine — Plan

**A utility that mirrors WorkOS objects into Postgres — via Events API backfill + tail — so applications can join identity data with their own data in SQL, without calling the WorkOS API on the read path.**

Modeled on the Stripe Sync Engine (born at Supabase, adopted first-party by Stripe in April 2026), adapted to WorkOS's data model and its Events API — which is a strictly better sync primitive than what Stripe offered.

---

## 1. Executive summary

Every serious WorkOS customer eventually hand-rolls a partial version of this: a `users` table kept in sync via webhooks, an `organizations` table populated by ad-hoc API calls, a cron job that patches up drift. The auth vendors themselves document the pattern (Clerk, Auth0, Okta all publish "sync to your DB" guides) while warning about its failure modes — and WorkOS's own docs argue at length that the Events API is the right way to do it, then leave the implementation to the customer.

This plan proposes **`workos-pg-sync`**: an open-source TypeScript library + single-container service that maintains a `workos` schema in the customer's Postgres, populated by:

1. **Backfill** — paginated REST list calls for initial/historical state,
2. **Tail** — continuous polling of the WorkOS Events API (ordered, replayable, 90-day retention) with a durable cursor,
3. **Reconcile** — scheduled count checks and ID-set diffs that detect and repair drift, including missed deletes.

The strongest precedent: the community-built Stripe sync engine reached ~125k npm downloads/month and was ultimately transferred to Stripe itself. Identity data is *more* join-central than billing data — users and organizations are the foreign keys of every multi-tenant application — and WorkOS already ships the ordered event log that Stripe's ecosystem had to approximate.

---

## 2. The argument

### 2.1 The problem

An application using WorkOS has its identity state (users, organizations, memberships, directory data, SSO connections) behind an HTTP API. That means:

- **No joins.** "Which orgs have SSO enabled but no active directory?" "Which users signed up 7+ days ago but never joined an org?" — these are one-line SQL queries against a mirror and miserable API-orchestration exercises otherwise. The Supabase/Stripe examples (MRR, churn risk, unconverted users) all translate directly: replace `stripe.customers ⋈ auth.users` with `workos.users ⋈ app.accounts`.
- **Rate limits on the read path.** WorkOS global limit is 6,000 req/60s per key; user-management reads are 1,000/10s per environment; directory-user listing is 4 req/s *per directory*. Any product that renders other users' data (dashboards, admin panels, social features) either hammers these limits or builds a cache — Val Town's post-mortem of Clerk (5 req/s account-wide) shows how this forces a hand-rolled mirror whether you planned one or not.
- **Latency and availability coupling.** Every API read adds a network round trip, and vendor downtime becomes your downtime. "Reliability is the minimum of the combined reliability of its critical parts."
- **Analytics can't reach it.** Product analytics, BI, and warehouse tooling can't query an API. There is no Fivetran or Airbyte connector for WorkOS; PostHog users filed a request for a managed WorkOS source (issue #53156, April 2026) specifically because they "have no easy way to correlate their identity and auth data with product usage data."

### 2.2 Why the hand-rolled version fails

Webhooks alone are structurally insufficient, per the vendors' own documentation:

- Delivery is at-least-once with **no ordering guarantee** — `user.deleted` can arrive before `user.created`, silently leaving an active user that should not exist (WorkOS's own blog example).
- Retries stop after 3 days; a down endpoint means **permanent silent gaps**.
- Webhooks **can't backfill** — they only capture events after configuration (the exact complaint in the PostHog issue).
- Dependent events (`dsync.group.user_added` before `dsync.user.created`) break naive handlers and foreign keys.

Sequin, who built this for a living: *"A system that relies purely on webhooks to stay in sync is bound to fail."* A correct mirror needs **events + backfill + reconciliation** — three coordinated subsystems that "take days or weeks to build correctly" (Supabase's words) and that every customer currently rebuilds independently, usually without the reconciliation part.

### 2.3 The 2026 argument: agents

The newest and possibly strongest driver:

- **Text-to-SQL over a local mirror beats API orchestration.** One read-only SQL tool over a `workos` schema replaces dozens of per-endpoint MCP tools. Documented MCP failure modes: tool definitions consuming 70%+ of the context window, 4–32× token overhead vs. direct queries, and upstream API rate limits described as "the single most common production failure mode for AI agents in 2026." The emerging recommended pattern is explicitly "land the SaaS data in Postgres; the agent queries the warehouse, not the live API."
- **Scoped grants.** `GRANT SELECT ON SCHEMA workos TO agent_role` is a far better-bounded credential than a WorkOS API key or dashboard access.
- **Freshness matters to agents** in a way it doesn't to dashboards: a batch ETL connector with 6-hour (Fivetran default) or even 15-minute cadence gives an agent silently stale state; an events-tail mirror is seconds behind.

### 2.4 Why WorkOS is uniquely positioned

- **The primitive already exists.** Sequin's published wishlist for a "syncable API" — a strictly ordered, replayable, cursor-paginated change feed — *is* the WorkOS Events API. Stripe's `/events` had to be reverse-engineered around 30-day retention and eventually-consistent pagination; WorkOS offers 90 days, documented ordering guarantees, and explicitly recommends this API for data syncing. The sync engine is the missing last mile of an investment WorkOS already made.
- **WorkOS has already published the argument** (docs/events/data-syncing, the "rethink your webhook strategy" blog) and left the implementation as an exercise. Shipping the implementation converts marketing content into product surface.
- **Strategic effects:** kills the "my data is locked in a vendor" objection that fuels Better Auth-style migrations (a continuously-synced local mirror *is* the exit hatch — offering it builds trust and, paradoxically, stickiness); deepens integration (customers build views, RLS policies, and dashboards on the schema); serves the enterprise ask directly (identity data next to product data for compliance and analytics).

### 2.5 Evidence of demand

| Signal | Detail |
|---|---|
| Stripe precedent | Community sync engine → ~125k npm downloads/month → adopted first-party as `stripe/sync-engine` (Apr 2026). Stripe had quietly forked it six weeks *before* Supabase's one-click launch — vendor interest preceded the public relaunch |
| **Stripe Database** | Two weeks after the transfer, Stripe Sessions 2026 previewed a *managed, hosted, read-only Postgres* of your Stripe data (`psql postgres://{id}.db.stripe.com`), sub-second latency, invite-only waitlist. The category endgame, validated by the most sophisticated API company in the industry: Sigma (SQL inside vendor) → Data Pipeline (push to warehouse) → **Database (managed Postgres mirror for app developers)** |
| Direct WorkOS ask | PostHog issue #53156 requesting a WorkOS warehouse source: wants users, orgs, memberships, invitations; dismisses ETL (cost/complexity) and webhooks (no backfill) |
| Convex component | `@convex-dev/workos-authkit` — "sync user data from WorkOS to your Convex database reliably and durably"; users-only, Convex-only; proves the shape |
| Hand-rolling ecosystem | Dozens of "sync Clerk/Auth0 users to your DB" tutorials, each rediscovering upserts, raw-body signature verification, and race conditions |
| FDW concession | `clerk_fdw`'s own README recommends storing results "in a local table for quick access" — the query-time alternative telling you to build the mirror |
| Whitespace | No Fivetran connector, no Airbyte connector, no OSS project, nothing commercial doing WorkOS→Postgres |

### 2.6 Counterarguments, answered in the design

| Objection | Answer |
|---|---|
| **Staleness / not read-your-writes** | Position the mirror for reads that tolerate seconds of lag (joins, lists, analytics, agents). Authoritative flows (post-signup onboarding) read the API or the session token — documented explicitly. Optional per-table hydration re-fetches objects on event receipt. |
| **Schema drift when the API evolves** | Raw JSONB payload is the only written column; typed columns are generated from it (§6). New upstream fields are queryable immediately; promoting them is DDL-only. The 42 hand-written migrations of the original Stripe engine are the cautionary tale this avoids. |
| **Silent drift** | First-class `reconcile` command: count checks + ID-set diffs + repair. Both Sequin and Stripe found their worst bugs only through reconciliation; it ships as a feature, not a runbook. |
| **GDPR / deletion propagation** | Deletes are mirrored (soft by default, hard-delete mode configurable); a `redact` command purges a subject's rows and raw-event copies on demand. Mirror inherits the deletion obligations of any replica — made tractable, not ignored. |
| **Write-back temptation** | Explicitly out of scope. Sequin nearly died building a Postgres wire-protocol proxy for writes. Reads from the mirror; writes to the API. Forever. |
| **"Just a cron script"** | The failure modes above (ordering, partial payloads, missed deletes, retention cliffs) are exactly what a cron script gets wrong silently. The counter is a correct, boring, well-tested implementation of a pattern everyone needs. |

---

## 3. Prior art — what to copy, what to avoid

| System | Lesson |
|---|---|
| **`stripe/sync-engine`** (née Supabase) | Copy: JSONB + generated columns (even the PK), staleness-gated upserts, no foreign keys, two timestamps on every row, cursor-checkpoint-after-flush invariant, reconciliation scripts, embeddable-library packaging, binary-subdivision backfill, 429/`Retry-After`-aware retries. Avoid (all shipped bugs in V1): synchronous webhook processing; hand-written per-field migrations (42 accumulated); **throwing on unknown event types** (returned HTTP 400 → vendor retry storms — unknown events must be logged and archived, never errored); config that applies to reads but not DDL (`SCHEMA` split-brain); truthy default API key (`'false'`); health checks that don't touch the DB; swallowed migration errors; nested-list truncation silently tombstoning rows; deletes exempt from *and* side effects outside the staleness gate. |
| **Issue #276, unanswered** | "Is this for analytics/sync, or an alternative way of handling regular Stripe interactions?" sat unanswered in their tracker. The README must answer this on line one: **a read model — never the system of record for transactional flows.** |
| **Sequin** | Copy: backfill/tail cutover pattern (capture cursor *before* backfill), cursor lag window (they caught Stripe's `/events` skipping same-timestamp events — advance cursor only past events >5s old), reconciliation as the only detector of missed deletes. Avoid: horizontal any-API platform (two Show HNs, ~7 points each; company pivoted twice), SQL write-back. |
| **Fivetran/Airbyte auth connectors** | They exist for Okta/Auth0 and catch deletes by *weekly or monthly full re-imports* — disqualifying for operational use and proof that the analytics-ETL shape doesn't serve this need. |
| **`@convex-dev/workos-authkit`** | Copy: idempotent, resumable `backfillUsers` (oldest-first, skip-if-synced). Extend: it covers users only, one destination, no drift repair. |
| **WorkOS's own docs** | The data-syncing guide already specifies the algorithm (store `updated_at` per object, skip stale events, sequential per-org processing, reconcile via state APIs including delete-diffing). The engine is those docs, implemented. |

---

## 4. Product definition

### 4.1 What it is

- An **open-source TypeScript library** (`@workos/pg-sync` shape) + **CLI** + **single-container service** — same code, three packagings. The library form won for Stripe (125k downloads/mo vs. 486/mo for the decomposed connector packages); keep the public surface coarse.
- **Read-only mirror.** One direction: WorkOS → Postgres. No write-back, ever.
- **Postgres-first.** One destination done deeply (RLS examples, logical-replication fan-out, Supabase recipe). No connector framework until a second destination proves necessary.

### 4.2 What it is not (naming hazards)

- Not **"Pipes"** — WorkOS Pipes is an unrelated third-party OAuth connector product. Avoid *pipes*, *connector*, and *data integration* in all naming.
- Not **"pgsync"** as a public name — collides with the established `toluaina/pgsync` (Postgres→Elasticsearch). Working name for the repo directory only; recommend **`workos-pg-sync`** publicly, mirroring `stripe/sync-engine`.
- Not an ETL/analytics product (though the mirror feeds warehouses via existing Postgres connectors for free), not an audit-log pipeline (Audit Logs have their own export/streaming products; see §7 Tier 4).

### 4.3 Primary interfaces

```
workos-pg-sync init        # create schema, tables, sync_state; validate API key
workos-pg-sync backfill    # full or per-object initial load; resumable
workos-pg-sync listen      # tail the Events API continuously (the daemon)
workos-pg-sync reconcile   # counts + ID-set diff + repair; --repair to fix
workos-pg-sync status      # cursor age, per-table freshness/lag, last errors
workos-pg-sync redact      # purge one subject's rows + raw events (GDPR)
```

Library equivalents: `createSyncEngine(config)`, `.backfill()`, `.start()`, `.reconcile()`, `.processWebhook(body, sig)` (optional webhook ingestion for the wake-up pattern, §5.4).

---

## 5. Sync architecture

### 5.1 Channel choice: Events API primary, webhooks optional

The Events API is the primary and default channel — per WorkOS's own comparison: consistent ordering guaranteed, replayable, consumer-paced, authenticated pull (no public endpoint to expose). Webhooks are supported only as an optional low-latency *trigger*: a signature-verified webhook receipt wakes the poller early, but state always flows through the ordered event stream. This sidesteps every webhook failure mode while keeping near-real-time latency.

### 5.2 Lifecycle: cursor-first backfill, then tail

```
1. CAPTURE   record the newest event ID (ULID) as the starting cursor
2. BACKFILL  paginated REST list per object type (limit=100, resumable
             per-stream state), freeze the time ceiling at run start
3. DRAIN     replay events from the captured cursor to now
4. TAIL      poll /events?after=<cursor> forever; checkpoint cursor
             in the same transaction as the writes it covers
```

Overlap between backfill and drain is deliberate and safe: staleness-gated idempotent upserts (§6.3) resolve races. Never try to make phases abut exactly — that's how gaps happen.

Tail mechanics:

- Poll interval configurable (default ~5s; sub-second possible — Sequin ran 500ms against Stripe). At `limit=100` and 6,000 req/min, the Events API is never the throughput bottleneck.
- **Lag window:** never advance the cursor past events younger than a few seconds, even though WorkOS documents strict ordering. Sequin proved Stripe's equivalent endpoint skipped same-timestamp events under concurrent ID generation; reprocessing is free (idempotent upserts), a skipped event is unrecoverable after 90 days. Trust, but verify — cheaply, forever.
- Event IDs are ULIDs; lexicographic order matches creation order (verified empirically, including same-millisecond ties). `MAX(id)` is the watermark.
- Start with a single worker (WorkOS's guidance). The documented scale-out path is partition-by-organization: parallel across orgs, strictly serial within one.

### 5.3 Reconciliation (the third subsystem, not an afterthought)

Three tiers on three cadences:

1. **Continuous:** cursor age, per-table `max(_synced_at)` liveness, `now() - max(_updated_at)` staleness, staleness-rejection rate. Alert on stall within minutes — the catastrophic failure mode is a *silent* multi-day stall followed by falling off the 90-day retention cliff.
2. **Daily:** per-table row counts vs. API-side counts. Catches gross drift and list-endpoint-default-filter bugs (the "canceled subscriptions missing" class).
3. **Weekly / post-incident:** full ID-set diff per table. `pg_only` rows = missed deletes; `api_only` rows = missed creates. This is the *only* mechanism that finds missed deletes — deleted objects are unlistable, so no amount of re-listing repairs them. `--repair` upserts the missing and tombstones the phantoms.

Reconciliation doubles as the answer to WorkOS event-payload gaps (§6.4): a periodic re-list refreshes fields that events don't carry.

### 5.4 Multi-environment

- One API key = one environment = one event stream = **one cursor**. Config accepts N environments (workspaces can have dozens, not just staging+production), each with a label; every row carries the label; primary keys are composite `(environment, id)`.
- Environment names repeat across projects — key config on the API key/client ID, never the display name.
- Feature flags are project-scoped (shared across environments) — the one documented exception; the flags table dedupes accordingly.

### 5.5 Rate-limit budget

- Global: 6,000/60s per key. User-management reads: 1,000/10s. **Directory users: 4 req/s per directory** — the binding constraint for large directories (~400 users/s/directory at `limit=100`); backfill paces per-directory.
- The `/organizations` 50/60s limit applies to DELETE requests (verified against the rate-limits reference), so organization *listing* runs at the global limit — but several resources (groups, org-scoped API keys, feature-flag targets) are only listable per-organization, making backfill N+1 in org count. Budgeted, resumable pagination with backoff is table stakes.
- Always set `limit=100` explicitly — the default is 10, a silent 10× backfill slowdown.

---

## 6. Schema design

### 6.1 Shape: raw JSONB + generated columns

Adopt the current `stripe/sync-engine` design wholesale — it resolves the typed-columns-vs-blob tradeoff:

```sql
create schema if not exists workos;

create table workos.users (
  _env         text not null,              -- environment label from config
  _raw         jsonb not null,             -- the only payload column ever written
  _updated_at  timestamptz not null,       -- SOURCE-derived (see 6.3)
  _synced_at   timestamptz not null default now(),
  _deleted_at  timestamptz,                -- soft delete tombstone
  id           text generated always as ((_raw->>'id')) stored,
  email        text generated always as ((_raw->>'email')) stored,
  email_verified boolean generated always as ((_raw->>'email_verified')::boolean) stored,
  first_name   text generated always as ((_raw->>'first_name')) stored,
  last_name    text generated always as ((_raw->>'last_name')) stored,
  external_id  text generated always as ((_raw->>'external_id')) stored,
  metadata     jsonb generated always as ((_raw->'metadata')) stored,
  created_at   timestamptz generated always as (((_raw->>'created_at'))::timestamptz) stored,
  updated_at   timestamptz generated always as (((_raw->>'updated_at'))::timestamptz) stored,
  primary key (_env, id)
);
create index on workos.users (external_id);
create index on workos.users (email);
```

- Upstream schema evolution never breaks ingest; new fields are queryable via `_raw->>'x'` immediately; promoting one to a typed column is pure DDL (`STORED` computes from existing data).
- DDL is **generated from the WorkOS OpenAPI spec** (`workos/openapi-spec` — the same source that generates the SDKs), fingerprint-gated so startup is a no-op when nothing changed. No hand-written per-field migrations.
- **No foreign keys** between mirror tables. Events arrive with children before parents; FKs convert ordering artifacts into poisoned writes. Indexes yes, constraints no.
- **63-byte identifier care** and longest-prefix-first ID dispatch (`conn_` is a proper prefix of `conn_app_`).

### 6.2 Table inventory (three shapes)

1. **Object mirrors** (upserted): users, organizations, organization_domains, organization_memberships, invitations, sessions, connections, directories, directory_users, directory_groups, groups, roles, org_roles, permissions, feature_flags, api_keys, fga_resources, fga_role_assignments…
2. **Join tables** (from reference-only events): `directory_group_memberships (directory_id, group_id, directory_user_id)`, `group_memberships (group_id, organization_membership_id)`.
3. **Append-only streams** (inserted, never updated): `events` (every raw event — local audit trail, replay source, and change history via `context.previous_attributes` / `context.actor`), `auth_attempts` (all 15 `authentication.*` types — sign-in success/failure log that has **no REST endpoint at all** and is otherwise lost after 90 days; mirroring it is a feature no one else can offer), `saml_certificate_events`, vault activity.

Slug-keyed objects (roles, permissions, flags — no IDs in their events) use natural keys: `(_env, slug)`, `(_env, organization_id, slug)`.

### 6.3 Write semantics

```sql
insert into workos.users as t (_env, _raw, _updated_at)
values ($1, $2, $3)
on conflict (_env, id) do update set
  _raw        = t._raw || excluded._raw,     -- merge mode (events); replace mode for REST-sourced rows
  _updated_at = excluded._updated_at,
  _synced_at  = now()
where excluded._updated_at >= t._updated_at;  -- staleness gate; >= not >
```

- **`_updated_at` is synthesized in the ingest layer, never by the database**: object's own `updated_at` if present, else the event's `created_at`, else (backfill) the HTTP Date header. A trigger-maintained timestamp records when *you* wrote — incomparable across delivery paths and actively wrong (this is DDR-009 from the Stripe engine, learned the hard way).
- `>=` not `>`: same-second ties are undecidable; last-writer-wins plus reconciliation beats dropping data (Stripe engine issue #268).
- `skipNoopUpdates` (`IS DISTINCT FROM` guard) to avoid WAL bloat when backfill/reconcile re-writes unchanged rows; `RETURNING (xmax = 0)` yields created/updated/skipped counts for free.
- Batched multi-row upserts per stream; **flush before every cursor checkpoint** — a checkpoint may never precede the durable commit of the rows it covers. Per-stream failure isolation: a poisoned batch in one table stalls that table's cursor only.
- Deletes: set `_deleted_at` (keep the last known `_raw` — it's the only record of what was deleted); **not** gated by the staleness check; never resurrected by late updates. Optional hard-delete mode + `redact` command for compliance. A 404 on any hydration fetch is treated as a delete signal.

### 6.4 The WorkOS-specific twist: partial event payloads

This is the biggest divergence from the Stripe model and the design's most consequential finding: **WorkOS event payloads are documented subsets of the REST objects.** `user.*` events omit `external_id`, `metadata`, `last_sign_in_at`, `locale`; `organization.*` events omit `metadata` and `stripe_customer_id`; `role.*` events carry no `id` at all; a dozen more (full matrix in research notes). `external_id` — the field customers use to join to their own tables — is *not in the user events*.

Blind whole-row upserts from events would silently null the most valuable columns. Three coordinated mitigations:

1. **Merge, don't replace** — event-sourced writes shallow-merge into `_raw` (`t._raw || excluded._raw`), preserving backfilled fields the event doesn't carry. REST-sourced writes (backfill, reconcile, hydration) replace `_raw` outright.
2. **Hydration mode, configurable per table** (`off | on_create | always`): re-fetch the object by ID on event receipt. Default `on_create` for users and organizations (creates are low-volume and it's the only way a *newly created* object gets its `external_id`/`metadata` before the next reconcile); `off` elsewhere.
3. **Reconciliation refresh** closes any remaining gap on schedule.

### 6.5 What lands where — decision from the coverage matrix

- Events with full-ish payloads + list endpoints → standard mirror tables (the core).
- Events but no/limited backfill (sessions: active-only per-user list; magic auth, email verification, password resets: get-by-ID only) → mirror from the stream, document the historical horizon, don't pretend to completeness.
- Lists but no events (FGA resources/assignments, identities, auth factors, webhook endpoints, Pipes configs) → **snapshot polling** on a schedule; tables carry `_snapshot_at`.
- Neither (SSO profiles, audit-log events, Radar lists) → out of scope; audit logs get at most a separate per-org CSV-export importer later.

---

## 7. Object coverage tiers

| Tier | Objects | Mechanism | Rationale |
|---|---|---|---|
| **1 — MVP** | users, organizations, organization_memberships, organization_domains, invitations; raw `events` table; `auth_attempts` stream | Events + backfill + reconcile | The join core. Alone satisfies the PostHog ask and 80% of use cases. `auth_attempts` is a headline feature: a queryable sign-in log that doesn't otherwise exist. |
| **2 — Enterprise surface** | directories, directory_users, directory_groups, directory_group_memberships, sso connections (+ SAML cert events), sessions | Events + backfill (directory-user backfill paced at 4 r/s/dir); sessions stream-first | The SKU-defining WorkOS data: SCIM and SSO state joined to app data. Note: no `dsync.updated` event exists — directory state changes (e.g. `linked → invalid_credentials`) require the directory poll. |
| **3 — Authorization** | roles, org roles, permissions, groups, group_memberships, feature_flags, api_keys | Events + backfill; slug natural keys | RBAC-in-SQL; flags are project-scoped. |
| **4 — Poll-only** | FGA resources + role assignments, identities, auth factors, webhook endpoints, connect apps | Scheduled snapshots | Zero events exist for these; honesty about staleness. |
| **Out of scope** | Audit-log events (own export/streaming products), SSO profiles (structurally unmirrorable), Radar lists (write-only), Vault values (shouldn't leave Vault), portal/widget tokens (ephemeral) | — | Documented explicitly. |

96 event types across 22 families as of today; the engine's event router is generated from the same catalog that populates `webhook_endpoint.events[]`.

---

## 8. Observability & operations

- **Two timestamps make lag a SQL query:** `_synced_at - _updated_at` per row; `now() - max(_updated_at)` staleness per table; `max(_synced_at)` liveness. `status` renders these; a `/metrics` endpoint exports them.
- Per-flush structured logs: table, batch size, inserted/updated/deleted/skipped, duration. **Alert on `skipped` spikes** (out-of-order or backfill/tail collisions) and on **cursor age** long before the 90-day cliff.
- Error classification by blast radius: transient (retry with backoff) / stream (isolate, others continue) / global (invalid key → hard stop).
- No custom dead-letter queue: failed ranges stay in per-stream resumable state for the next attempt; the events API's replayability *is* the retry queue. Legitimate only because stall alerting exists.
- Fan-out to the app: logical replication / CDC off the mirror tables (helped by no-op-skipping writes), or triggers the *customer* owns. No LISTEN/NOTIFY in the engine.

---

## 9. Implementation phases

**Phase 0 — Spike & verification (days).**
Repo scaffold (pnpm, TS, vitest). Resolve the three open API questions (§10) with a live key. Prototype: events poll loop → upsert `users` → cursor checkpoint. Schema generator PoC from `workos/openapi-spec`.

**Phase 1 — Tier 1 MVP (2–3 weeks).**
Schema generation + migration runner (fingerprint-gated). Upsert engine (merge/replace modes, staleness gate, batching, per-stream isolation). Backfill (resumable per-stream state, rate budget). Tail (cursor, lag window, drain-after-backfill). CLI: `init | backfill | listen | status`. Library API. Raw `events` + `auth_attempts` streams. Single-environment. Docker image. Honest README about consistency semantics.

**Phase 2 — Reliability (1–2 weeks).**
`reconcile` (counts, ID-set diff, `--repair`). Hydration mode. Multi-environment config + composite keys. Metrics endpoint + alert cookbook. `redact`. Soak test against a high-volume staging environment; chaos tests (kill mid-backfill, mid-flush, cursor at retention edge).

**Phase 3 — Coverage (2–3 weeks).**
Tier 2 (directory sync, SSO, sessions) with per-directory pacing. Tier 3 (RBAC, flags, API keys) incl. slug-keyed upserts and join tables. Tier 4 snapshot polling. Webhook wake-up trigger (`processWebhook`).

**Phase 4 — Distribution (ongoing).**
Docs site with the query cookbook (the Supabase blog's SQL examples, translated: dormant enterprise seats, SSO adoption by org, sign-in failure rates, directory drift). Supabase deploy recipe; RLS examples; agent recipe (read-only role + one SQL tool → replaces an MCP server). Example joins with `stripe.*` for customers running both engines — *organizations ⋈ stripe.customers on stripe_customer_id is on the org object already.* Launch post arguing this document's §2.

**Distribution lesson from the Stripe engine's numbers:** downloads sat at ~400/month for six months of "library on npm + blog post," then jumped to 56.8k the month the one-click dashboard integration shipped, and neither the launch nor the Stripe transfer produced any HN discussion. Buzz didn't distribute this category; **platform integration did.** The equivalent lever here is a setup path in the WorkOS dashboard and docs (an "Integrations → Postgres mirror" page generating config), not a launch post alone.

---

## 10. Open questions & pre-build verification

**RESOLVED against the live API on 2026-08-06** (see `docs/how-it-works.md`):

1. ~~`/events` cursor direction~~ — **`order` has no observable effect.** `order=asc`, `order=desc` and omitting it return byte-identical pages, always oldest-first; `after` pages forward in time. Consequence: `order=desc&limit=1` returns the *oldest* event, so it cannot be used to capture a starting position. The engine records a timestamp and bootstraps with `range_start` instead.
2. ~~`range_start` + `after`~~ — **mutually exclusive, enforced**: `400 "Only one of range_start and after may be provided."` `range_start=<now>` correctly returns zero events.
3. **`events` is required, not optional.** The published OpenAPI spec marks it optional; the live endpoint returns `400 invalid_events_request` without it, and rejects unknown event names the same way. All 63 catalog event names were validated individually. A consumer cannot subscribe to "everything" — new upstream event types require a catalog update.
4. Event payload subsets **confirmed** for Tier 1: `user.*` events do omit `external_id` and `metadata`, which the merge-not-replace write path preserves.

**Still open:** >30-day window and >90-day `range_start` behavior (needs an environment with older history than the test environment had).

**Design decisions to revisit with real data:**
4. Hydration defaults (create-only vs. off) once real event volumes are known.
5. Whether `context.client_id` should be persisted as an environment cross-check.
6. Soft-delete default vs. GDPR-conservative hard-delete default.
7. Whether Tier 1 ships with a minimal Postgres NOTIFY/outbox example or leaves fan-out entirely to CDC.

**Strategic:**
8. First-party (WorkOS-owned repo, `@workos/` scope) vs. community-first then adopt (the Stripe path). Given the Stripe precedent ended in first-party adoption anyway, starting first-party skips a step — but community-first derisks API commitments.
9. Relationship to a future hosted/managed offering. Stripe's trajectory names the ladder explicitly: OSS engine → one-click platform integration → **Stripe Database**, a managed hosted read-only Postgres, previewed two weeks after they took the engine over. The WorkOS equivalent ("WorkOS Database": a Postgres connection string to a managed mirror of your environment) is the plausible endgame; the OSS engine is both the on-ramp and the self-hosted tier. The managed tier needs the health-check surface built in Phase 2, and the Supabase one-click integration's issue tracker (cron misconfiguration, stale secrets, reinstall breakage) is the checklist of what a managed control plane must own.
10. Mirror semantics education: the mirror faithfully reflects *vendor* state, which is not the same as *business* truth (Stripe's grace-period example: a subscription reads `active` while payments fail). The query cookbook should encode the correct interpretations, not just raw joins.

---

## Appendix A — Key numbers

| Fact | Value |
|---|---|
| Events API retention / max query window | 90 days / 30 days per request |
| Event catalog | 96 types, 22 families |
| Global rate limit | 6,000 req/60s per API key |
| User-management reads | 1,000 req/10s per environment |
| Directory users | 4 req/s per directory |
| Organization DELETEs | 50/60s (reads fall under global limit) |
| List pagination | `limit` 1–100, **default 10** — always set 100 |
| Webhook retries | 6 attempts, exponential backoff, 3 days |
| Stripe engine adoption | ~125k npm downloads/month (library form) |

## Appendix B — Research provenance

Five research threads (full notes in session scratchpad): `stripe/sync-engine` code + design-decision records (repo cloned and read), WorkOS object/event coverage matrix (50+ reference pages), WorkOS Events API mechanics (OpenAPI spec + live ULID verification), market/prior-art survey (Sequin postmortems, vendor docs, demand signals), and sync-engine design patterns (Sequin/Svix/Hookdeck/Stripe engineering).
