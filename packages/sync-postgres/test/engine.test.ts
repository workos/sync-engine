/**
 * Whole-engine tests: backfill, tail, drift and repair, driven against the
 * fake WorkOS API and a real Postgres.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  SyncEngine,
  WorkOSClient,
  LeaseHeldError,
  StateStore,
  silentLogger,
  ENGINE_VERSION,
  type Destination,
} from '@workos/sync-engine'
import { createTestDb } from './helpers/pg.js'
import { FakeWorkOS, resetIds, id } from '../../sync-engine/test/helpers/fake-workos.js'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'
import { FaultyDestination, captureLogger } from './helpers/faulty.js'

const TIER1 = [
  'users',
  'organizations',
  'organization_memberships',
  'organization_domains',
  'invitations',
  'events',
  'auth_attempts',
]

function makeEngine(
  db: SqlClient,
  fake: FakeWorkOS,
  streams = TIER1,
  extra = {},
  /** Runs before every API call, so a test can make something happen mid-poll. */
  beforeFetch?: () => Promise<void>,
) {
  const fetchImpl: typeof fetch = beforeFetch
    ? async (input, init) => {
        await beforeFetch()
        return fake.fetchImpl(input, init)
      }
    : fake.fetchImpl
  return new SyncEngine(
    {
      destination: new PostgresDestination({ db, schema: 'workos' }),
      environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
      streams,
      logger: silentLogger,
      lagWindowMs: 0,
      ...extra,
    },
    {
      clientFor: () =>
        new WorkOSClient({
          apiKey: 'sk_test_1',
          apiUrl: 'http://fake.workos.local',
          fetchImpl,
          pageSize: 100,
        }),
    },
  )
}

describe('sync engine', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  const count = async (table: string) => {
    const r = await db.query<{ n: string }>(`select count(*)::text as n from workos.${table}`)
    return Number(r.rows[0]!.n)
  }

  it('backfills existing state, including fields events never carry', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')

    const engine = makeEngine(db, fake)
    await engine.backfill()

    expect(await count('users')).toBe(1)
    expect(await count('organizations')).toBe(1)
    expect(await count('organization_memberships')).toBe(1)

    // Every listing the backfill made says which phase it was.
    const listings = fake.requestLog
      .map((path, i) => [path, fake.userAgents[i]!] as const)
      .filter(([path]) => !path.startsWith('/events'))
    expect(listings.length).toBeGreaterThan(0)
    expect(listings.every(([, ua]) => ua.endsWith('(phase=backfill)'))).toBe(true)

    const u = await db.query<{ email: string; external_id: string; metadata: unknown }>(
      `select email, external_id, metadata from workos.users`,
    )
    // external_id and metadata only exist on the REST object, never on events.
    expect(u.rows[0]!.email).toBe('ada@acme.com')
    expect(u.rows[0]!.external_id).toMatch(/^ext_/)
    expect(u.rows[0]!.metadata).toEqual({ plan: 'pro' })

    const m = await db.query<{ role_slug: string }>(`select role_slug from workos.organization_memberships`)
    expect(m.rows[0]!.role_slug).toBe('admin')
  })

  it('captures the stream position before backfilling so nothing is missed', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.backfill()

    // The events endpoint always returns oldest-first, so there is no cheap
    // "newest event id" to record. The starting position is a timestamp, and
    // the tail bootstraps from it with range_start until a real cursor exists.
    const before = await engine.status()
    expect(before['prod']!.started).toBe(true)
    expect(before['prod']!.rangeStart).not.toBeNull()
    expect(before['prod']!.cursor).toBeNull()

    // Once events are applied, the position becomes a real cursor.
    await engine.tick()
    const after = await engine.status()
    expect(after['prod']!.cursor).not.toBeNull()
    expect(after['prod']!.rangeStart).toBeNull()
  })

  it('stops at the page budget and resumes from the checkpoint', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    // invitations sorts after users, so it is the stream the budget never reaches.
    const engine = makeEngine(db, fake, ['users', 'invitations'])

    const first = await engine.backfill({ maxPages: 2 })
    expect(first.find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })
    expect(await count('users')).toBe(200)
    // The budget belongs to the call, not the stream: a stream after the one
    // that spent it is left untouched, not started.
    expect(first.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })

    const before = fake.requestLog.length
    const second = await engine.backfill({ resume: true, maxPages: 10 })
    expect(second.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
    expect(second.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 1, complete: true })
    expect(await count('users')).toBe(250)
    // The resumed listing carried the cursor the first call stopped at rather
    // than starting the stream over.
    const users = fake.requestLog.slice(before).filter((r) => r.startsWith('/user_management/users'))
    expect(users).toHaveLength(1)
    expect(users[0]).toContain('after=')
  })

  it('skips streams already complete when resuming, and re-lists them otherwise', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake, ['users'])
    const listings = () => fake.requestLog.filter((r) => /^\/user_management\/users(\?|$)/.test(r)).length

    await engine.backfill()
    const once = listings()
    expect(once).toBeGreaterThan(0)

    // Resuming a finished stream is a no-op, which is what lets repeated
    // bounded calls converge instead of re-listing what already finished.
    const resumed = await engine.backfill({ resume: true })
    expect(resumed.find((r) => r.stream === 'users')).toMatchObject({ pages: 0, complete: true })
    expect(listings()).toBe(once)

    // Without resume, backfill is the complete refresh it always was.
    await engine.backfill()
    expect(listings()).toBe(once * 2)
  })

  it('pauses a per-parent stream inside a parent and resumes at that page', async () => {
    const org = fake.createOrganization()
    for (let i = 0; i < 150; i++) {
      fake.createMembership(fake.createUser()['id'] as string, org['id'] as string)
    }
    const engine = makeEngine(db, fake, ['users', 'organizations', 'organization_memberships'])

    // organizations: 1 page; users: 2 pages; memberships of the one organization: 2 pages.
    const first = await engine.backfill({ maxPages: 4 })
    expect(first.find((r) => r.stream === 'organization_memberships')).toMatchObject({ pages: 1, complete: false })
    expect(await count('organization_memberships')).toBe(100)

    const before = fake.requestLog.length
    const second = await engine.backfill({ resume: true })
    expect(second.find((r) => r.stream === 'organization_memberships')).toMatchObject({ pages: 1, complete: true })
    expect(await count('organization_memberships')).toBe(150)

    // Only the memberships listing ran, and it picked up inside the parent:
    // one request carrying the cursor, not the parent's first page again.
    const since = fake.requestLog.slice(before)
    const memberships = since.filter((r) => r.startsWith('/user_management/organization_memberships'))
    expect(memberships).toHaveLength(1)
    expect(memberships[0]).toContain('after=')
    expect(since.some((r) => /^\/(organizations|user_management\/users)(\?|$)/.test(r))).toBe(false)
  })

  it('refuses a page budget that is not a positive integer', async () => {
    const engine = makeEngine(db, fake, ['users'])
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      await expect(engine.backfill({ maxPages: bad })).rejects.toThrow(/maxPages/)
    }
  })

  /** The real engine over a destination with one write fault injected; see helpers/faulty.ts. */
  const faultyEngine = (destination: FaultyDestination, streams: string[], logger = silentLogger) =>
    new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams,
        hydrate: 'off',
        logger,
        lagWindowMs: 0,
      },
      {
        clientFor: () =>
          new WorkOSClient({
            apiKey: 'sk_test_1',
            apiUrl: 'http://fake.workos.local',
            fetchImpl: fake.fetchImpl,
            pageSize: 100,
          }),
      },
    )

  it('forgets every stream checkpoint on a fresh run, even those the budget never reaches', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()
    const listings = () => fake.requestLog.filter((r) => r.startsWith('/user_management/invitations')).length
    const before = listings()

    // A fresh bounded run spends its one page on users and never reaches invitations.
    const fresh = await engine.backfill({ maxPages: 1 })
    expect(fresh.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })
    expect(listings()).toBe(before)

    // Resuming must list invitations, not trust the completion the earlier run left behind.
    const resumed = await engine.backfill({ resume: true })
    expect(resumed.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 1, complete: true })
    expect(listings()).toBe(before + 1)
  })

  it('holds a child stream back while its parent is incomplete, so a resume fetches the missing children', async () => {
    const orgA = fake.createOrganization()
    const orgB = fake.createOrganization()
    fake.createMembership(fake.createUser()['id'] as string, orgA['id'] as string)
    fake.createMembership(fake.createUser()['id'] as string, orgB['id'] as string)
    const destination = new FaultyDestination({ db, schema: 'workos' })
    destination.poison = orgB['id'] as string
    const { logger, lines } = captureLogger()
    const engine = faultyEngine(destination, ['users', 'organizations', 'organization_memberships'], logger)

    const first = await engine.backfill()
    expect(first.find((r) => r.stream === 'organizations')).toMatchObject({ complete: false })
    const held = first.find((r) => r.stream === 'organization_memberships')!
    expect(held).toMatchObject({ pages: 0, complete: false })
    expect(held.skippedReason).toContain('organizations')
    // No budget was involved: this is a failure, and must not read as a pause.
    expect(lines.map((l) => l.msg)).not.toContain('backfill paused: page budget spent; resume to continue')

    destination.poison = null
    const second = await engine.backfill({ resume: true })
    expect(second.find((r) => r.stream === 'organizations')).toMatchObject({ complete: true })
    expect(second.find((r) => r.stream === 'organization_memberships')).toMatchObject({ complete: true })
    expect(await count('organization_memberships')).toBe(2)
  })

  it('charges a page to the budget even when its write fails', async () => {
    const user = fake.createUser()
    const destination = new FaultyDestination({ db, schema: 'workos' })
    destination.poison = user['id'] as string
    const engine = faultyEngine(destination, ['users', 'invitations'])

    const r = await engine.backfill({ maxPages: 1 })
    // The page was fetched before its write failed, so the failure reports it.
    expect(r.find((s) => s.stream === 'users')).toMatchObject({ pages: 1, complete: false })
    // The one page was fetched, so the budget is spent: invitations is not started.
    expect(r.find((s) => s.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })
    expect(fake.requestLog.some((q) => q.startsWith('/user_management/invitations'))).toBe(false)
  })

  it('checkpoints inside a parent after every page, so a crash mid-parent resumes at that page', async () => {
    const org = fake.createOrganization()
    const members: string[] = []
    for (let i = 0; i < 150; i++) {
      const m = fake.createMembership(fake.createUser()['id'] as string, org['id'] as string)
      members.push(m['id'] as string)
    }
    const destination = new FaultyDestination({ db, schema: 'workos' })
    // A membership on the second page of the one organization's listing.
    destination.poison = members[120]!
    const engine = faultyEngine(destination, ['users', 'organizations', 'organization_memberships'])

    const first = await engine.backfill()
    const failed = first.find((r) => r.stream === 'organization_memberships')!
    expect(failed.complete).toBe(false)
    expect(failed.skippedReason).toMatch(/failed/)
    expect(await count('organization_memberships')).toBe(100)

    destination.poison = null
    const before = fake.requestLog.length
    await engine.backfill({ resume: true })
    const since = fake.requestLog.slice(before).filter((q) => q.startsWith('/user_management/organization_memberships'))
    // Page one was checkpointed before page two failed, so the resume asks for page two alone.
    expect(since).toHaveLength(1)
    expect(since[0]).toContain('after=')
    expect(await count('organization_memberships')).toBe(150)
  })

  it('applies live events after backfill', async () => {
    const user = fake.createUser({ email: 'before@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()

    fake.updateUser(user['id'] as string, { email: 'after@example.com' })
    const org = fake.createOrganization({ name: 'New Co' })
    await engine.tick()

    const u = await db.query<{ email: string }>(`select email from workos.users`)
    expect(u.rows[0]!.email).toBe('after@example.com')
    expect(await count('organizations')).toBe(1)
    const o = await db.query<{ name: string }>(`select name from workos.organizations`)
    expect(o.rows[0]!.name).toBe('New Co')
    expect(org['id']).toBeTruthy()
  })

  it('does not null out backfilled fields when an event arrives', async () => {
    // The single most consequential WorkOS-specific behaviour: user.* event
    // payloads omit external_id, metadata, last_sign_in_at and locale.
    const user = fake.createUser({ email: 'ada@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()

    fake.updateUser(user['id'] as string, { first_name: 'Grace' })
    await engine.tick()

    const row = await db.query<{ first_name: string; external_id: string; metadata: unknown; locale: string }>(
      `select first_name, external_id, metadata, locale from workos.users`,
    )
    expect(row.rows[0]!.first_name).toBe('Grace')
    expect(row.rows[0]!.external_id).toMatch(/^ext_/)
    expect(row.rows[0]!.metadata).toEqual({ plan: 'pro' })
    expect(row.rows[0]!.locale).toBe('en')
  })

  it('is idempotent when the same events are delivered twice', async () => {
    fake = new FakeWorkOS({ duplicateEvents: true })
    fake.createUser()
    fake.createOrganization()

    const engine = makeEngine(db, fake)
    await engine.tick()
    await engine.tick()

    expect(await count('users')).toBe(1)
    expect(await count('organizations')).toBe(1)
  })

  it('converges regardless of the order events are applied in', async () => {
    const user = fake.createUser({ email: 'v1@example.com' })
    fake.updateUser(user['id'] as string, { email: 'v2@example.com' })
    fake.updateUser(user['id'] as string, { email: 'v3@example.com' })

    // Serve the batch reversed, as out-of-order delivery would.
    const shuffled = new FakeWorkOS({ shuffleEvents: true })
    shuffled.users = fake.users
    shuffled.events = fake.events

    const engine = makeEngine(db, shuffled)
    await engine.tick()

    const u = await db.query<{ email: string }>(`select email from workos.users`)
    // The staleness gate means the newest state wins whatever the arrival order.
    expect(u.rows[0]!.email).toBe('v3@example.com')
  })

  it('resolves events that share a timestamp as last-writer-wins', async () => {
    // Two events can carry the same updated_at, and no comparison can order
    // them. The gate is `>=` rather than `>` precisely so the second one is
    // still applied: dropping it is how a mirror gets permanently stuck on a
    // superseded state. Ties are settled by arrival, and reconcile is the
    // backstop — this documents that, rather than claiming a guarantee.
    const user = fake.createUser({ email: 'v1@example.com' })
    const uid = user['id'] as string
    const frozen = new Date(Date.now() - 30_000).toISOString()
    fake.users.set(uid, { ...fake.users.get(uid)!, email: 'tie-a@example.com', updated_at: frozen })
    fake.events.push({
      id: `event_tie_a`,
      event: 'user.updated',
      data: fake.userEventPayload({ ...fake.users.get(uid)!, email: 'tie-a@example.com', updated_at: frozen }),
      created_at: frozen,
    })
    fake.events.push({
      id: `event_tie_b`,
      event: 'user.updated',
      data: fake.userEventPayload({ ...fake.users.get(uid)!, email: 'tie-b@example.com', updated_at: frozen }),
      created_at: frozen,
    })

    const engine = makeEngine(db, fake)
    await engine.tick()

    const u = await db.query<{ email: string }>(`select email from workos.users`)
    expect(u.rows[0]!.email).toBe('tie-b@example.com')
  })

  it('tombstones a deleted user and keeps it out of live queries', async () => {
    const user = fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()

    fake.deleteUser(user['id'] as string)
    await engine.tick()

    const live = await db.query<{ n: string }>(
      `select count(*)::text as n from workos.users where "_deleted_at" is null`,
    )
    expect(Number(live.rows[0]!.n)).toBe(0)
    expect(await count('users')).toBe(1)
  })

  it('drops a deleted user from the live view while the base table keeps the tombstone', async () => {
    const gone = fake.createUser({ email: 'gone@example.com' })
    fake.createUser({ email: 'stays@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    expect(await count('users')).toBe(2)
    expect(await count('users_live')).toBe(2)

    fake.deleteUser(gone['id'] as string)
    await engine.tick()

    // No `where _deleted_at is null` needed: that is the point of the view.
    expect(await count('users')).toBe(2)
    expect(await count('users_live')).toBe(1)
    const live = await db.query<{ email: string }>(`select email from workos.users_live`)
    expect(live.rows.map((r) => r.email)).toEqual(['stays@example.com'])
  })

  it('archives subscribed events that have no mirror table, instead of throwing', async () => {
    fake.createUser()
    fake.emitUnknown()

    // A subscription can legitimately include types this build has no handler
    // for: the archive-only families (vault, radar, agent, pipes), or a type
    // added upstream after release and configured by hand. Those must land in
    // the archive rather than throwing — an exception here is what turns a new
    // event type into a stuck stream.
    const engine = makeEngine(db, fake, TIER1, {
      eventTypes: ['user.created', 'user.updated', 'user.deleted', 'quantum_widget.entangled'],
    })
    await engine.tick()

    const archived = await db.query<{ event: string }>(`select event from workos.events order by event`)
    const names = archived.rows.map((r) => r.event)
    expect(names).toContain('user.created')
    expect(names).toContain('quantum_widget.entangled')

    // It was archived only — no mirror row was invented for it.
    const users = await db.query<{ n: string }>(`select count(*)::text as n from workos.users`)
    expect(Number(users.rows[0]!.n)).toBe(1)
  })

  it('subscribes only to the event types its selected streams declare', async () => {
    // GET /events requires the filter, so the subscription is derived from the
    // catalog. Selecting fewer streams must narrow it, not widen it.
    const usersOnly = makeEngine(db, fake, ['users', 'events'])
    expect(usersOnly.eventTypes).toEqual(['user.created', 'user.deleted', 'user.updated'])

    const withAuth = makeEngine(db, fake, TIER1)
    expect(withAuth.eventTypes).toContain('authentication.password_failed')
    expect(withAuth.eventTypes).toContain('organization_membership.created')
    expect(withAuth.eventTypes).not.toContain('dsync.user.created')
  })

  it('records sign-in attempts that exist nowhere else in the API', async () => {
    const user = fake.createUser()
    fake.emitAuthAttempt(user['id'] as string, true)
    fake.emitAuthAttempt(user['id'] as string, false)
    fake.emitAuthAttempt(user['id'] as string, false)

    const engine = makeEngine(db, fake)
    await engine.tick()

    const r = await db.query<{ status: string; n: string }>(
      `select status, count(*)::text as n from workos.auth_attempts group by status order by status`,
    )
    expect(r.rows).toEqual([
      { status: 'failed', n: '2' },
      { status: 'succeeded', n: '1' },
    ])

    const failed = await db.query<{ error_code: string; email: string }>(
      `select error_code, email from workos.auth_attempts where status = 'failed' limit 1`,
    )
    expect(failed.rows[0]!.error_code).toBe('invalid_credentials')
  })

  it('resumes from the stored cursor rather than replaying', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    const first = await engine.tick()
    expect(first.processed).toBe(1)

    const second = await engine.tick()
    expect(second.processed).toBe(0)

    fake.createUser()
    const third = await engine.tick()
    expect(third.processed).toBe(1)
  })

  it('drains several pages in one tick when maxEventsPerTick allows it', async () => {
    for (let i = 0; i < 5; i++) fake.createUser()

    // Default: one page per tick, so a backlog takes as many ticks as pages.
    const paged = makeEngine(db, fake, TIER1, { pageSize: 2 })
    expect((await paged.tick()).processed).toBe(2)

    // Budgeted: the remaining 3 arrive across two pages but one tick.
    const budgeted = makeEngine(db, fake, TIER1, { pageSize: 2, maxEventsPerTick: 10 })
    expect((await budgeted.tick()).processed).toBe(3)
    expect((await budgeted.tick()).processed).toBe(0)
    expect(await count('users')).toBe(5)
  })

  it('explodes organization domains out of the organization payload', async () => {
    // organization_domains has events but no list endpoint of its own.
    fake.createOrganization({
      domains: [
        { object: 'organization_domain', id: 'org_domain_1', domain: 'acme.com', state: 'verified' },
        { object: 'organization_domain', id: 'org_domain_2', domain: 'acme.io', state: 'pending' },
      ],
    })
    const engine = makeEngine(db, fake)
    await engine.backfill()

    const r = await db.query<{ domain: string; state: string; organization_id: string }>(
      `select domain, state, organization_id from workos.organization_domains order by domain`,
    )
    expect(r.rows.map((x) => x.domain)).toEqual(['acme.com', 'acme.io'])
    expect(r.rows[0]!.state).toBe('verified')
    expect(r.rows[0]!.organization_id).toMatch(/^org_/)
  })

  it('never mirrors invitation acceptance tokens, however the row arrives', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const invite = fake.createInvitation(org['id'] as string)
    const token = invite['token'] as string

    const stored = async () => {
      const r = await db.query<{ raw: Record<string, unknown>; email: string; state: string }>(
        `select "_raw" as raw, email, state from workos.invitations where id = $1`,
        [invite['id']],
      )
      return r.rows[0]!
    }

    // Backfill writes the REST object, which is where the credential lives.
    const engine = makeEngine(db, fake)
    await engine.backfill()
    let row = await stored()
    expect(row.email).toBe(invite['email'])
    expect(row.raw['token']).toBeUndefined()
    expect(row.raw['accept_invitation_url']).toBeUndefined()
    expect(JSON.stringify(row.raw)).not.toContain(token)

    // An event merges into the stored payload; it must not reintroduce it.
    fake.acceptInvitation(invite['id'] as string, 'user_1')
    await engine.tick()
    row = await stored()
    expect(row.state).toBe('accepted')
    expect(JSON.stringify(row.raw)).not.toContain(token)

    // Refresh and repair re-list the REST object; same guarantee.
    await engine.refresh()
    await db.query(`delete from workos.invitations where id = $1`, [invite['id']])
    await engine.reconcile({ repair: true, streams: ['invitations'] })
    row = await stored()
    expect(row.state).toBe('accepted')
    expect(JSON.stringify(row.raw)).not.toContain(token)

    const archive = await db.query<{ n: string }>(
      `select count(*)::text as n from workos.events where "_raw"::text like $1`,
      [`%${token}%`],
    )
    expect(Number(archive.rows[0]!.n)).toBe(0)
  })

  it('never mirrors domain verification tokens, however the row arrives', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const dom = fake.createDomain(org['id'] as string, 'acme.com')
    const token = dom['verification_token'] as string
    const prefix = dom['verification_prefix'] as string

    // Every place a copy of the object can land, as text: both mirror tables
    // (their _raw and the generated `domains` column) and the event archive.
    const copies = async (needle: string) => {
      const r = await db.query<{ n: string }>(
        `select (
           (select count(*) from workos.organizations where "_raw"::text like $1 or domains::text like $1)
           + (select count(*) from workos.organization_domains where "_raw"::text like $1)
           + (select count(*) from workos.events where "_raw"::text like $1)
         )::text as n`,
        [`%${needle}%`],
      )
      return Number(r.rows[0]!.n)
    }
    const stored = async () => {
      const r = await db.query<{ state: string; raw: Record<string, unknown> }>(
        `select state, "_raw" as raw from workos.organization_domains where id = $1`,
        [dom['id']],
      )
      return r.rows[0]!
    }

    // Backfill writes the REST organization, domains inline, then explodes them.
    const engine = makeEngine(db, fake)
    await engine.backfill()
    expect((await stored()).state).toBe('pending')
    expect((await stored()).raw['domain']).toBe('acme.com')
    expect(await copies(token)).toBe(0)
    expect(await copies(prefix)).toBe(0)

    // The create events carry the token, unlike an invitation's. The archive
    // and the merge into both tables have to drop it.
    await engine.tick()
    expect(await count('events')).toBeGreaterThan(0)
    expect(await copies(token)).toBe(0)
    expect(await copies(prefix)).toBe(0)

    fake.verifyDomain(dom['id'] as string)
    await engine.tick()
    expect((await stored()).state).toBe('verified')
    expect(await copies(token)).toBe(0)

    // verification_failed wraps the object: the archived event keeps the
    // reason and the object, minus the token.
    fake.failDomainVerification(dom['id'] as string)
    await engine.tick()
    expect((await stored()).state).toBe('failed')
    const failed = await db.query<{ raw: { data: Record<string, unknown> } }>(
      `select "_raw" as raw from workos.events where event = 'organization_domain.verification_failed'`,
    )
    expect(failed.rows).toHaveLength(1)
    expect(failed.rows[0]!.raw.data['reason']).toBe('domain_verification_period_expired')
    expect((failed.rows[0]!.raw.data['organization_domain'] as Record<string, unknown>)['id']).toBe(dom['id'])
    expect(await copies(token)).toBe(0)

    // A new organization's create event is hydrated from the REST object,
    // whose domains carry the token. The domain is created with the
    // organization so that the create is the only event in its batch and
    // the row under test is the hydrated snapshot alone.
    const otherId = id('org_')
    const other = fake.createOrganization({
      id: otherId,
      name: 'Initech',
      domains: [
        {
          object: 'organization_domain',
          id: id('org_domain_'),
          organization_id: otherId,
          domain: 'initech.com',
          state: 'pending',
          verification_strategy: 'dns',
          verification_token: 'vt_initech_challenge',
          verification_prefix: 'superapp-domain-verification-initech',
        },
      ],
    })
    expect((await engine.tick()).processed).toBe(1)
    const hydrated = await db.query<{ raw: Record<string, unknown> }>(
      `select "_raw" as raw from workos.organizations where id = $1`,
      [otherId],
    )
    // Only the REST object carries stripe_customer_id, so its presence proves the hydration ran.
    expect(hydrated.rows[0]!.raw['stripe_customer_id']).toBe(other['stripe_customer_id'])
    expect(await copies('vt_initech_challenge')).toBe(0)
    expect(await copies('superapp-domain-verification-initech')).toBe(0)

    // Refresh and repair re-list the REST organization; same guarantee.
    await engine.refresh()
    await db.query(`delete from workos.organizations where id = $1`, [org['id']])
    await engine.reconcile({ repair: true, streams: ['organizations'] })
    expect(await count('organizations')).toBe(2)
    expect(await copies(token)).toBe(0)
    expect(await copies(prefix)).toBe(0)
  })

  it('keeps two environments apart in one database', async () => {
    const staging = new FakeWorkOS()
    staging.createUser({ email: 'staging@example.com' })
    fake.createUser({ email: 'prod@example.com' })

    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [
          { name: 'prod', apiKey: 'sk_live' },
          { name: 'staging', apiKey: 'sk_test' },
        ],
        streams: TIER1,
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: (env) =>
          new WorkOSClient({
            apiKey: env.apiKey,
            apiUrl: 'http://fake.workos.local',
            fetchImpl: env.name === 'prod' ? fake.fetchImpl : staging.fetchImpl,
          }),
      },
    )
    await engine.backfill()

    const r = await db.query<{ _env: string; email: string }>(
      `select "_env", email from workos.users order by "_env"`,
    )
    expect(r.rows).toEqual([
      { _env: 'prod', email: 'prod@example.com' },
      { _env: 'staging', email: 'staging@example.com' },
    ])

    // Cursors are per environment and are not comparable across them.
    const status = await engine.status()
    expect(Object.keys(status)).toEqual(['prod', 'staging'])
  })

  it('always includes the events archive, whatever streams are selected', async () => {
    // Every event is archived before it is routed, so a stream list without
    // `events` used to fail every tick with "relation workos.events does not exist".
    fake.createUser()
    const engine = makeEngine(db, fake, ['users'])
    expect(engine.streams.map((s) => s.name)).toEqual(['users', 'events'])

    const r = await engine.tick()
    expect(r.processed).toBe(1)
    expect(await count('events')).toBe(1)
  })

  it('rejects a stream name it does not know', () => {
    expect(() => makeEngine(db, fake, ['users', 'customers'])).toThrow(/unknown stream: customers/)
  })

  it('records the last successful poll even when nothing arrived', async () => {
    const engine = makeEngine(db, fake)
    await engine.backfill()
    const fresh = await engine.status()
    expect(fresh['prod']!.lastPolledAt).toBeNull()
    expect(fresh['prod']!.pollAgeSeconds).toBeNull()
    // Nothing happened upstream, so there is no event to advance the cursor
    // with. The poll still succeeded, and that is the liveness signal.
    expect(fresh['prod']!.cursorAgeDays).toBeNull()

    await engine.tick()
    const polled = await engine.status()
    expect(polled['prod']!.lastPolledAt).not.toBeNull()
    expect(polled['prod']!.pollAgeSeconds).toBeGreaterThanOrEqual(0)
    expect(polled['prod']!.pollAgeSeconds).toBeLessThan(5)
    expect(polled['prod']!.cursorAgeDays).toBeNull()
  })

  it('stops on a rejected credential instead of retrying forever', async () => {
    const strict = new FakeWorkOS({ apiKey: 'sk_test_1' })
    strict.createUser()
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [{ name: 'prod', apiKey: 'sk_test_WRONG' }],
        streams: TIER1,
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: () =>
          new WorkOSClient({
            apiKey: 'sk_test_WRONG',
            apiUrl: 'http://fake.workos.local',
            fetchImpl: strict.fetchImpl,
            maxRetries: 0,
          }),
      },
    )
    // A 401 is a configuration error. Retrying every poll interval would hide
    // it behind a process that looks alive, so the engine stops instead.
    await expect(engine.tick()).rejects.toThrow(/401/)
    await expect(engine.backfill()).rejects.toThrow(/401/)
    await expect(engine.reconcile()).rejects.toThrow(/401/)
  })

  it('re-captures the starting position when the stored cursor is past retention', async () => {
    const engine = makeEngine(db, fake)
    await engine.init()
    const stale = new Date(Date.now() - 100 * 86_400_000).toISOString()
    await db.query(
      `insert into workos."_sync_state" (sync_id, env, stream, state)
       values ('default', 'prod', '__tail__', $1::jsonb)`,
      [JSON.stringify({ kind: 'tail', cursor: 'event_000', lastEventAt: stale })],
    )

    // Replaying from a cursor the API no longer has is impossible, and this
    // backfill re-establishes current state, so the tail restarts from now.
    await engine.backfill()
    const status = await engine.status()
    expect(status['prod']!.cursor).toBeNull()
    expect(status['prod']!.rangeStart).not.toBeNull()
    expect(status['prod']!.cursorAgeDays).toBeNull()
  })

  it('projects extra columns out of _raw without touching the catalog', async () => {
    fake.createUser({ metadata: { plan: 'pro', region: 'eu' } })
    const engine = makeEngine(db, fake, TIER1, {
      extraColumns: {
        users: [{ name: 'region', type: 'text', path: ['metadata', 'region'], index: true }],
      },
    })
    await engine.backfill()

    const r = await db.query<{ region: string; is_generated: string }>(
      `select u.region, c.is_generated
       from workos.users u, information_schema.columns c
       where c.table_schema = 'workos' and c.table_name = 'users' and c.column_name = 'region'`,
    )
    expect(r.rows[0]!.region).toBe('eu')
    expect(r.rows[0]!.is_generated).toBe('ALWAYS')
  })

  it('hydrates users and organizations on create by default', async () => {
    const engine = makeEngine(db, fake)
    expect(engine.hydrate).toEqual({ users: 'on_create', organizations: 'on_create' })
    await engine.backfill()

    fake.createUser({ email: 'default@example.com' })
    await engine.tick()
    const r = await db.query<{ external_id: string }>(
      `select external_id from workos.users where email = 'default@example.com'`,
    )
    expect(r.rows[0]!.external_id).toMatch(/^ext_/)
    // The re-fetch is the last request of the tick, and says it is one.
    expect(fake.userAgents.at(-1)).toMatch(/\(phase=hydrate\)$/)
  })

  it('keeps the hydrated fields when the create and a later update share a tick', async () => {
    const engine = makeEngine(db, fake)
    await engine.backfill()

    // Both events land in one poll. The create is hydrated from the REST
    // object, which alone carries external_id and metadata; the update's
    // payload is the documented subset. They become one write, and neither
    // may lose to the other.
    const user = fake.createUser({ email: 'both@example.com' })
    fake.updateUser(user['id'] as string, { first_name: 'Grace' })
    expect((await engine.tick()).processed).toBe(2)

    const r = await db.query<{ first_name: string; external_id: string | null; metadata: unknown }>(
      `select first_name, external_id, metadata from workos.users where email = 'both@example.com'`,
    )
    expect(r.rows[0]).toEqual({ first_name: 'Grace', external_id: user['external_id'], metadata: { plan: 'pro' } })
  })

  it('leaves event-only fields empty when hydration is off', async () => {
    const engine = makeEngine(db, fake, TIER1, { hydrate: 'off' })
    await engine.backfill()

    fake.createUser({ email: 'raw@example.com' })
    await engine.tick()
    const r = await db.query<{ external_id: string | null }>(
      `select external_id from workos.users where email = 'raw@example.com'`,
    )
    // user.created does not carry external_id; without hydration it stays
    // null until backfill or refresh.
    expect(r.rows[0]!.external_id).toBeNull()
  })

  it('hydrates on create so a new user has its full payload immediately', async () => {
    const engine = makeEngine(db, fake, TIER1, { hydrate: { users: 'on_create' } })
    await engine.backfill()

    fake.createUser({ email: 'fresh@example.com' })
    await engine.tick()

    const r = await db.query<{ external_id: string; metadata: unknown }>(
      `select external_id, metadata from workos.users where email = 'fresh@example.com'`,
    )
    // Without hydration these would be null until the next reconcile, because
    // user.created does not carry them.
    expect(r.rows[0]!.external_id).toMatch(/^ext_/)
    expect(r.rows[0]!.metadata).toEqual({ plan: 'pro' })
  })

  it('joins mirrored identity data with application tables', async () => {
    // The point of the whole exercise.
    const org = fake.createOrganization({ name: 'Acme' })
    const u1 = fake.createUser({ email: 'active@acme.com' })
    const u2 = fake.createUser({ email: 'dormant@acme.com' })
    fake.createMembership(u1['id'] as string, org['id'] as string)
    fake.createMembership(u2['id'] as string, org['id'] as string)

    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    fake.emitAuthAttempt(u1['id'] as string, true)
    await engine.tick()

    await db.exec(`create table app_events (user_id text, occurred_at timestamptz)`)
    await db.query(`insert into app_events values ($1, now())`, [u1['id']])

    const r = await db.query<{ email: string; sign_ins: string; app_events: string }>(
      `select u.email,
              count(distinct a.event_id)::text as sign_ins,
              count(distinct e.user_id)::text as app_events
       from workos.users u
       join workos.organization_memberships m on m.user_id = u.id and m."_env" = u."_env"
       left join workos.auth_attempts a on a.user_id = u.id
       left join app_events e on e.user_id = u.id
       where u."_deleted_at" is null and m.organization_id = $1
       group by u.email order by u.email`,
      [org['id']],
    )
    expect(r.rows).toEqual([
      { email: 'active@acme.com', sign_ins: '1', app_events: '1' },
      { email: 'dormant@acme.com', sign_ins: '0', app_events: '0' },
    ])
  })
})

describe('reconciliation', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  it('reports a clean mirror as matching with no drift', async () => {
    fake.createUser()
    fake.createOrganization()
    const engine = makeEngine(db, fake)
    await engine.backfill()

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([])
    expect(users.remoteOnly).toEqual([])
    expect(users.matched).toBe(1)
  })

  it('detects a missed delete, which nothing else can', async () => {
    const user = fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.backfill()

    // Delete upstream without the engine ever seeing the event: the object is
    // now absent from every list endpoint, so re-listing can never reveal it.
    fake.users.delete(user['id'] as string)

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([user['id']])
    expect(users.remoteOnly).toEqual([])
  })

  it('detects a missed create', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.backfill()

    // Appears upstream while the engine is not looking.
    const missed = fake.createUser({ email: 'missed@example.com' })
    fake.events = []

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.remoteOnly).toEqual([missed['id']])
  })

  it('repairs drift in both directions', async () => {
    const stale = fake.createUser({ email: 'gone@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()

    fake.users.delete(stale['id'] as string)
    const added = fake.createUser({ email: 'added@example.com' })
    fake.events = []

    await engine.reconcile({ repair: true })

    const live = await db.query<{ email: string }>(
      `select email from workos.users where "_deleted_at" is null order by email`,
    )
    expect(live.rows.map((r) => r.email)).toEqual(['added@example.com'])

    const tombstoned = await db.query<{ email: string }>(
      `select email from workos.users where "_deleted_at" is not null`,
    )
    expect(tombstoned.rows[0]!.email).toBe('gone@example.com')
    expect(added['id']).toBeTruthy()

    // A second pass finds nothing left to fix.
    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([])
    expect(users.remoteOnly).toEqual([])
  })

  it('says plainly when a stream cannot be reconciled', async () => {
    const engine = makeEngine(db, fake)
    await engine.backfill()
    const { diffs } = await engine.reconcile()

    const events = diffs['prod']!.find((d) => d.stream === 'events')!
    expect(events.skippedReason).toMatch(/append-only/)
    const domains = diffs['prod']!.find((d) => d.stream === 'organization_domains')!
    expect(domains.skippedReason).toMatch(/no list endpoint/)
  })

  it('reports freshness and lag per stream', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.backfill()

    const status = await engine.status()
    const users = status['prod']!.streams.find((s) => s.stream === 'users')!
    expect(users.rows).toBe(1)
    expect(users.lastSyncedAt).toBeTruthy()
    expect(users.lagSeconds).not.toBeNull()
  })

  it('refreshes fields that event payloads omit', async () => {
    const user = fake.createUser()
    const engine = makeEngine(db, fake, TIER1, { hydrate: 'off' })
    await engine.tick()

    // Event-only: the row exists but the omitted columns are empty.
    let row = await db.query<{ external_id: string | null }>(`select external_id from workos.users`)
    expect(row.rows[0]!.external_id).toBeNull()

    await engine.refresh()
    row = await db.query<{ external_id: string | null }>(`select external_id from workos.users`)
    expect(row.rows[0]!.external_id).toMatch(/^ext_/)
    expect(user['id']).toBeTruthy()
  })
})

describe('single-writer lease', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
  const until = async (ok: () => Promise<boolean>, timeoutMs = 5_000) => {
    const deadline = Date.now() + timeoutMs
    while (!(await ok())) {
      if (Date.now() > deadline) throw new Error('condition not met in time')
      await sleep(10)
    }
  }
  const count = async (table: string) => {
    const r = await db.query<{ n: string }>(`select count(*)::text as n from workos.${table}`)
    return Number(r.rows[0]!.n)
  }
  const deferred = () => {
    let resolve!: () => void
    const promise = new Promise<void>((r) => {
      resolve = r
    })
    return { promise, resolve }
  }
  /**
   * A destination whose lease calls the test can slow down or watch: a
   * database that answers late at exactly the wrong moment.
   */
  const instrumented = (
    dest: PostgresDestination,
    hooks: { beforeAcquire?: () => Promise<void>; onRelease?: () => void },
  ): Destination =>
    new Proxy(dest, {
      get(target, prop) {
        if (prop === 'acquireLease') {
          return async (...args: Parameters<Destination['acquireLease']>) => {
            await hooks.beforeAcquire?.()
            return target.acquireLease(...args)
          }
        }
        if (prop === 'releaseLease') {
          return async (...args: Parameters<Destination['releaseLease']>) => {
            hooks.onRelease?.()
            return target.releaseLease(...args)
          }
        }
        const v = Reflect.get(target, prop, target)
        return typeof v === 'function' ? v.bind(target) : v
      },
    })

  it('tick() refuses to run while another process holds the lease', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.init()
    // Another process, as far as the database can tell.
    const other = new PostgresDestination({ db, schema: 'workos' })
    const theirs = await other.acquireLease('prod', 'other-host:42:abc', 60_000)

    const err = await engine.tick().catch((e: unknown) => e)
    expect(err).toBeInstanceOf(LeaseHeldError)
    expect((err as LeaseHeldError).holder).toBe('other-host:42:abc')
    expect((err as LeaseHeldError).expiresAt).toBe(theirs.expiresAt)
    expect((err as LeaseHeldError).message).toContain('other-host:42:abc')
    // Nothing was applied, and the other process still holds the lease.
    expect(await count('users')).toBe(0)
    expect(await other.getLease('prod')).toMatchObject({ holder: 'other-host:42:abc' })

    // Once it lets go, a cron tick runs and leaves the lease free behind it.
    await other.releaseLease('prod', 'other-host:42:abc')
    expect((await engine.tick()).processed).toBe(1)
    expect(await other.getLease('prod')).toBeNull()
  })

  it('listen() waits for the lease, takes over when it is released, and gives it back on stop', async () => {
    fake.createUser()
    const warnings: Record<string, unknown>[] = []
    const engine = makeEngine(db, fake, TIER1, {
      pollIntervalMs: 20,
      leaseTtlMs: 2_000,
      logger: { ...silentLogger, warn: (_m: string, f?: Record<string, unknown>) => warnings.push(f ?? {}) },
    })
    await engine.init()
    const other = new PostgresDestination({ db, schema: 'workos' })
    await other.acquireLease('prod', 'old-process', 60_000)

    // The rolling-deploy case: the new process starts while the old one is
    // still tailing. It must idle, not fail and not write.
    const listening = engine.listen()
    await sleep(150)
    expect(await count('users')).toBe(0)
    expect(warnings).toContainEqual(expect.objectContaining({ env: 'prod', holder: 'old-process' }))
    // Waiting is logged once, not once per poll.
    expect(warnings.filter((w) => w['holder'] === 'old-process')).toHaveLength(1)

    // The old process stops and releases; the new one picks up within a poll.
    await other.releaseLease('prod', 'old-process')
    await until(async () => (await count('users')) === 1)
    const mine = await other.getLease('prod')
    expect(mine?.holder).toMatch(/^.+:\d+:[0-9a-f]{6}$/)
    expect((await engine.status())['prod']!.lease).toEqual(mine)

    // While it is tailing, a cron tick elsewhere is refused.
    expect((await other.acquireLease('prod', 'cron', 60_000)).holder).toBe(mine!.holder)

    engine.stop()
    await listening
    // Released immediately, not left to expire: the replacement need not wait.
    expect(await other.getLease('prod')).toBeNull()
    expect((await engine.status())['prod']!.lease).toBeNull()
  })

  it('refuses a lease TTL that would expire on arrival', () => {
    // A zero or NaN TTL makes every acquire succeed, which is no lease at all.
    for (const leaseTtlMs of [0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => makeEngine(db, fake, TIER1, { leaseTtlMs })).toThrow(/leaseTtlMs/)
    }
    expect(() => makeEngine(db, fake, TIER1, { leaseTtlMs: 1 })).not.toThrow()
  })

  it('does not checkpoint after losing the lease mid-poll', async () => {
    fake.createUser()
    const other = new PostgresDestination({ db, schema: 'workos' })
    let stalled = false
    // The poll stalls past the TTL while fetching and a replacement takes
    // over in the meantime. The database clock is moved rather than the test.
    const engine = makeEngine(db, fake, TIER1, {}, async () => {
      if (stalled) return
      stalled = true
      await db.query(`update workos."_sync_lease" set expires_at = now() - interval '1 second'`)
      await other.acquireLease('prod', 'replacement', 60_000)
    })
    await engine.init()
    const r = await engine.tick()

    // The batch it had already fetched is applied, which is idempotent, but
    // the cursor is left where the replacement will look for it.
    expect(await count('users')).toBe(1)
    expect(r.processed).toBe(0)
    const status = await engine.status()
    expect(status['prod']!.cursor).toBeNull()
    expect(status['prod']!.lastPolledAt).toBeNull()
    expect(await other.getLease('prod')).toMatchObject({ holder: 'replacement' })
  })

  it('keeps a slow standalone tick covered so it can still checkpoint', async () => {
    fake.createUser()
    const other = new PostgresDestination({ db, schema: 'workos' })
    let slowed = false
    // A cron tick with no listener: nothing else renews for it. The fetch
    // outlasts the TTL, which must not cost it the lease and its checkpoint.
    const engine = makeEngine(db, fake, TIER1, { leaseTtlMs: 150 }, async () => {
      if (slowed) return
      slowed = true
      await sleep(400)
    })
    await engine.init()
    const r = await engine.tick()

    expect(r.processed).toBe(1)
    expect((await engine.status())['prod']!.cursor).not.toBeNull()
    // And it gives the lease back when it is done, as any tick does.
    expect(await other.getLease('prod')).toBeNull()
  })

  it('does not overwrite a position that moved under it, even while it holds the lease', async () => {
    fake.createUser()
    const other = new PostgresDestination({ db, schema: 'workos' })
    let moved = false
    // The lease is intact throughout, so it cannot be the guard. A backfill
    // re-capturing the position mid-poll, or a replacement that advanced the
    // cursor and let go before this process woke, both look like this.
    const engine = makeEngine(db, fake, TIER1, {}, async () => {
      if (moved) return
      moved = true
      await new StateStore(other).setTail('prod', { cursor: 'event_from_elsewhere' })
    })
    await engine.init()
    const r = await engine.tick()

    expect(r.processed).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBe('event_from_elsewhere')
    expect(await other.getLease('prod')).toBeNull()
  })

  it('two listeners with the same environments in different orders do not block each other', async () => {
    fake.createUser()
    const engineFor = (environments: { name: string; apiKey: string }[]) =>
      new SyncEngine(
        {
          destination: new PostgresDestination({ db, schema: 'workos' }),
          environments,
          streams: TIER1,
          logger: silentLogger,
          lagWindowMs: 0,
          pollIntervalMs: 20,
        },
        {
          clientFor: () =>
            new WorkOSClient({ apiKey: 'k', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl }),
        },
      )
    const ab = engineFor([
      { name: 'a', apiKey: 'k' },
      { name: 'b', apiKey: 'k' },
    ])
    const ba = engineFor([
      { name: 'b', apiKey: 'k' },
      { name: 'a', apiKey: 'k' },
    ])
    await ab.init()
    const runs = [ab.listen(), ba.listen()]

    // Claimed in list order, each would take its first environment and wait
    // for the other's forever. In one shared order, one of them gets both.
    await until(async () => (await count('users')) === 2)
    const other = new PostgresDestination({ db, schema: 'workos' })
    const [a, b] = await Promise.all([other.getLease('a'), other.getLease('b')])
    expect(a).not.toBeNull()
    expect(b?.holder).toBe(a!.holder)

    ab.stop()
    ba.stop()
    await Promise.all(runs)
    expect(await other.getLease('a')).toBeNull()
    expect(await other.getLease('b')).toBeNull()
  })

  it("a tick overlapping listen() startup does not release the listener's lease", async () => {
    fake.createUser()
    let acquires = 0
    const listenerAcquiring = deferred()
    const dest = instrumented(new PostgresDestination({ db, schema: 'workos' }), {
      // Every acquire answers late, so the listener and the nudge would
      // otherwise both be mid-acquire at once, under the same holder.
      beforeAcquire: async () => {
        if (++acquires === 1) listenerAcquiring.resolve()
        await sleep(30)
      },
    })
    const engine = makeEngine(db, fake, TIER1, { destination: dest, pollIntervalMs: 500 })
    await engine.init()
    const other = new PostgresDestination({ db, schema: 'workos' })

    const listening = engine.listen()
    await listenerAcquiring.promise
    // A webhook nudge arriving as the listener starts.
    await engine.processWebhook()
    // It ran under the listener's claim and left it in place.
    expect(await other.getLease('prod')).not.toBeNull()
    expect(await count('users')).toBe(1)

    engine.stop()
    await listening
    expect(await other.getLease('prod')).toBeNull()
  })

  it('stop() waits for a renewal in flight rather than leaving a lease behind', async () => {
    fake.createUser()
    let gate: Promise<void> | undefined
    let acquires = 0
    let releases = 0
    const dest = instrumented(new PostgresDestination({ db, schema: 'workos' }), {
      beforeAcquire: async () => {
        acquires++
        await gate
      },
      onRelease: () => {
        releases++
      },
    })
    const engine = makeEngine(db, fake, TIER1, { destination: dest, pollIntervalMs: 300, leaseTtlMs: 150 })
    await engine.init()
    const other = new PostgresDestination({ db, schema: 'workos' })

    const listening = engine.listen()
    await until(async () => (await engine.status())['prod']!.lastPolledAt !== null)
    // The loop is idle for a poll interval, so the next acquire is the
    // heartbeat's. It hangs, as on a database that answers late.
    const slow = deferred()
    gate = slow.promise
    const before = acquires
    await until(async () => acquires > before)

    engine.stop()
    // Long enough for the loop to wake and reach its release. Acquiring is an
    // upsert, so releasing while a renewal is pending would put the row back.
    await sleep(400)
    expect(releases).toBe(0)

    slow.resolve()
    await listening
    expect(await other.getLease('prod')).toBeNull()
  })
})

describe('redaction', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  it('erases a subject from the mirror and the event archive', async () => {
    const user = fake.createUser({ email: 'forget@example.com' })
    fake.emitAuthAttempt(user['id'] as string, true)
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()

    const removed = await engine.redact(user['id'] as string)
    expect(removed.length).toBeGreaterThan(0)

    const remaining = await db.query<{ n: string }>(`select count(*)::text as n from workos.users`)
    expect(Number(remaining.rows[0]!.n)).toBe(0)

    // The archived payloads carry the same personal data and must go too.
    const events = await db.query<{ n: string }>(
      `select count(*)::text as n from workos.events where "_raw"::text like $1`,
      [`%${user['id']}%`],
    )
    expect(Number(events.rows[0]!.n)).toBe(0)
  })
})

describe('poison events', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  const count = async (table: string) => {
    const r = await db.query<{ n: string }>(`select count(*)::text as n from workos."${table}"`)
    return Number(r.rows[0]!.n)
  }
  const emails = async () => {
    const r = await db.query<{ email: string }>(`select email from workos.users order by email`)
    return r.rows.map((x) => x.email)
  }
  const eventFor = (objectId: string) => fake.events.find((e) => e.data['id'] === objectId)!

  /** The engine over a FaultyDestination, with a capturing logger and no client retries. */
  function makeFaulty(extra: Record<string, unknown> = {}, fetchImpl = fake.fetchImpl) {
    const destination = new FaultyDestination({ db, schema: 'workos' })
    const { logger, lines } = captureLogger()
    const engine = new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: TIER1,
        hydrate: 'off',
        logger,
        lagWindowMs: 0,
        ...extra,
      },
      {
        clientFor: () =>
          new WorkOSClient({
            apiKey: 'sk_test_1',
            apiUrl: 'http://fake.workos.local',
            fetchImpl,
            maxRetries: 0,
          }),
      },
    )
    return { engine, destination, lines }
  }

  it('quarantines one event after two sightings and keeps the rest of the environment moving', async () => {
    fake.createUser({ email: 'good1@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    fake.createUser({ email: 'good2@example.com' })
    const badEvent = eventFor(bad['id'] as string)
    const { engine, destination, lines } = makeFaulty()
    destination.poison = bad['id'] as string

    // Poll 1: first sighting. The tick fails as it always did, the cursor
    // stays put, and the suspect is only remembered in memory.
    const first = await engine.tick()
    expect(first).toMatchObject({ processed: 0, rejected: 0 })
    let status = await engine.status()
    expect(status['prod']!.cursor).toBeNull()
    expect(status['prod']!.rejected).toBe(0)
    expect(await count('_rejected')).toBe(0)
    const warned = lines.find((l) => l.level === 'warn' && l.fields['eventId'] === badEvent.id)
    expect(warned?.msg).toMatch(/quarantined if it fails again/)
    expect(warned?.fields['error']).toMatch(/invalid input syntax for type boolean/)

    // Poll 2: the same batch, the same failure. The good events are written,
    // the bad one is set aside, and the cursor moves past all three.
    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 3, rejected: 1 })
    status = await engine.status()
    expect(status['prod']!.cursor).toBe(fake.events[fake.events.length - 1]!.id)
    expect(status['prod']!.rejected).toBe(1)
    expect(await emails()).toEqual(['good1@example.com', 'good2@example.com'])

    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({
      eventId: badEvent.id,
      eventType: 'user.created',
      objectId: bad['id'],
      createdAt: badEvent.created_at,
      raw: badEvent,
    })
    expect(rejected[0]!.error).toMatch(/invalid input syntax for type boolean: "maybe"/)
    expect(rejected[0]!.rejectedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/)
    const logged = lines.find((l) => l.level === 'error' && l.msg.includes('quarantined'))
    expect(logged?.fields).toMatchObject({ eventId: badEvent.id, eventType: 'user.created', objectId: bad['id'] })
    expect(String(logged?.fields['hint'])).toContain('rejected')

    // The tail is not stalled: the next poll is a no-op rather than a retry.
    expect((await engine.tick()).processed).toBe(0)

    // The archive still has every event, including the one set aside.
    expect(await count('events')).toBe(3)
  })

  it('replays a quarantined event through the normal path once the cause is fixed', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = bad['id'] as string
    await engine.tick()
    await engine.tick()
    expect((await engine.status())['prod']!.rejected).toBe(1)

    // Still broken: the row stays, with its error refreshed.
    destination.fault = () => Object.assign(new Error('still broken'), { code: '22P02' })
    const again = await engine.replayRejected()
    expect(again.replayed).toBe(0)
    expect(again.stillRejected).toEqual([{ eventId: eventFor(bad['id'] as string).id, error: 'Error: still broken' }])
    expect((await engine.rejected())['prod']![0]!.error).toBe('Error: still broken')

    // Fixed: the event is applied and forgotten.
    destination.poison = null
    const fixed = await engine.replayRejected()
    expect(fixed).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count('_rejected')).toBe(0)
    expect((await engine.status())['prod']!.rejected).toBe(0)
    expect(await emails()).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('replays a quarantined create after a later update, restoring the fields only hydration carries', async () => {
    const user = fake.createUser({ email: 'late@example.com' })
    const { engine, destination } = makeFaulty({ hydrate: 'on_create' })
    destination.poison = user['id'] as string
    await engine.tick()
    await engine.tick()
    expect((await engine.status())['prod']!.rejected).toBe(1)

    // The cause is gone, and the object moves on before anyone replays: the
    // update creates the row from the event payload alone, which carries
    // neither external_id nor metadata.
    destination.poison = null
    fake.updateUser(user['id'] as string, { first_name: 'Late' })
    await engine.tick()
    const before = await db.query<{ external_id: string | null; metadata: unknown }>(
      `select external_id, metadata from workos.users where id = $1`,
      [user['id']],
    )
    expect(before.rows[0]).toEqual({ external_id: null, metadata: null })

    // The replayed create hydrates, and the snapshot it writes must win over
    // the row the update made, or clearing the rejection would be a lie.
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    const after = await db.query<{ first_name: string; external_id: string | null; metadata: unknown }>(
      `select first_name, external_id, metadata from workos.users where id = $1`,
      [user['id']],
    )
    expect(after.rows[0]).toEqual({ first_name: 'Late', external_id: user['external_id'], metadata: { plan: 'pro' } })
  })

  it('never quarantines on an error the destination classifies as transient', async () => {
    const u1 = fake.createUser()
    fake.createUser()
    const { engine, destination, lines } = makeFaulty()
    destination.poison = u1['id'] as string

    // A dropped connection, then a socket error: both are the database having
    // a bad moment, and the tick must retry, however many times it happens.
    destination.fault = () => Object.assign(new Error('connection failure'), { code: '08006' })
    await engine.tick()
    await engine.tick()
    destination.fault = () => Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })
    await engine.tick()
    await engine.tick()

    expect(await count('_rejected')).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()
    expect(lines.filter((l) => l.msg === 'tick failed')).toHaveLength(4)
    expect(lines.some((l) => l.level === 'warn')).toBe(false)
    // It failed fast each time, without ever isolating event by event.
    expect(destination.faults).toBe(4)

    // Once the database recovers the batch applies in full.
    destination.poison = null
    expect((await engine.tick()).processed).toBe(2)
    expect(await count('users')).toBe(2)
  })

  it('treats a whole batch failing as an outage, not a batch of poison events', async () => {
    fake.createUser()
    fake.createUser()
    fake.createUser()
    const { engine, destination, lines } = makeFaulty()
    // An unclassified error, so only the all-fail rule stands in the way.
    destination.fault = () => new Error('relation "workos.users" does not exist')
    destination.failAll = true

    await engine.tick()
    await engine.tick()
    await engine.tick()

    expect(await count('_rejected')).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()
    expect(lines.filter((l) => l.msg === 'tick failed')).toHaveLength(3)
    expect(lines.some((l) => l.level === 'warn')).toBe(false)
  })

  it('treats a hydration failure as an outage, never as a poison event', async () => {
    fake.createUser()
    fake.createUser()
    // The API answers the events page but not the user GET that hydration
    // needs. The client has already given up retrying by the time the engine
    // sees it, so what the engine sees is an outage.
    const failingGet: typeof fetch = ((input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input.toString())
      if (/^\/user_management\/users\/[^/]+$/.test(url.pathname)) {
        return Promise.resolve(new Response('{"message":"upstream"}', { status: 503 }))
      }
      return fake.fetchImpl(input, init)
    }) as typeof fetch
    const { engine, lines } = makeFaulty({ hydrate: { users: 'on_create' } }, failingGet)

    await engine.tick()
    await engine.tick()

    expect(await count('_rejected')).toBe(0)
    expect(await count('users')).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()
    const failures = lines.filter((l) => l.msg === 'tick failed')
    expect(failures).toHaveLength(2)
    expect(String(failures[0]!.fields['error'])).toMatch(/hydration failed .* 503/)
    expect(lines.some((l) => l.level === 'warn')).toBe(false)
  })

  it('stops when hydration is denied, not only when the events page is', async () => {
    fake.createUser()
    // The events page answers; the object GET that hydration needs is
    // forbidden. That is the same configuration error as a 403 on the events
    // page, and wrapping it in a hydration error must not turn it into a
    // retry every poll interval.
    const denied: typeof fetch = ((input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input.toString())
      if (/^\/user_management\/users\/[^/]+$/.test(url.pathname)) {
        return Promise.resolve(new Response('{"message":"forbidden"}', { status: 403 }))
      }
      return fake.fetchImpl(input, init)
    }) as typeof fetch
    const { engine, lines } = makeFaulty({ hydrate: { users: 'on_create' } }, denied)

    await expect(engine.tick()).rejects.toThrow(/403/)
    expect(await count('_rejected')).toBe(0)
    expect(lines.some((l) => l.msg === 'tick failed')).toBe(false)
    expect(lines.some((l) => l.msg === 'authentication failed; stopping')).toBe(true)
  })

  it('forgets a suspect that succeeds on its second poll', async () => {
    const flaky = fake.createUser({ email: 'flaky@example.com' })
    fake.createUser({ email: 'steady@example.com' })
    const { engine, destination } = makeFaulty()
    // An unclassified error that happens once: exactly what two sightings is for.
    destination.fault = () => new Error('Connection terminated unexpectedly')
    destination.poison = flaky['id'] as string
    await engine.tick()
    destination.poison = null

    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 2, rejected: 0 })
    expect(await count('_rejected')).toBe(0)
    expect(await emails()).toEqual(['flaky@example.com', 'steady@example.com'])
  })

  it('forgets a suspect that succeeds alone on a poll that fails for another reason', async () => {
    const flaky = fake.createUser({ email: 'flaky@example.com' })
    fake.createUser({ email: 'steady@example.com' })
    const { engine, destination } = makeFaulty()
    destination.fault = () => new Error('Connection terminated unexpectedly')

    // Poll 1: the event fails alone. First sighting.
    destination.poison = flaky['id'] as string
    await engine.tick()

    // Poll 2: the batch fails, but every event — this one included — succeeds
    // alone. Nothing is quarantined and the tick still fails, but whatever
    // was wrong with the event last time is not wrong now.
    destination.poison = null
    destination.failBatches = true
    expect(await engine.tick()).toMatchObject({ processed: 0, rejected: 0 })

    // Poll 3: it fails alone again. Two failures, but not on consecutive
    // polls: this is a new first sighting, and nothing is quarantined.
    destination.failBatches = false
    destination.poison = flaky['id'] as string
    expect(await engine.tick()).toMatchObject({ processed: 0, rejected: 0 })
    expect(await count('_rejected')).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()

    // Poll 4: now it has failed twice in a row.
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 1 })
    expect(await count('_rejected')).toBe(1)
  })

  it('does not resurrect a membership removed after a quarantined link', async () => {
    // A membership re-add is the one write that clears a tombstone, which
    // makes it the one place an out-of-order replay could undo a removal that
    // happened after the replayed event.
    const at = (ms: number) => new Date(Date.now() - 50_000 + ms).toISOString()
    const pair = { group_id: 'group_01', organization_membership_id: 'om_01' }
    const push = (event: string, created_at: string) => {
      const e = { id: id('event_'), event, data: pair, created_at }
      fake.events.push(e)
      return e
    }
    push('group.member_added', at(0))
    push('group.member_removed', at(1_000))
    const readd = push('group.member_added', at(2_000))
    push('group.member_removed', at(3_000))
    const { engine, destination } = makeFaulty({ streams: ['group_memberships', 'events'] })
    const live = async () => {
      const r = await db.query<{ n: string }>(
        `select count(*)::text as n from workos.group_memberships where "_deleted_at" is null`,
      )
      return Number(r.rows[0]!.n)
    }

    // The archive write for the re-add fails, so it alone is set aside; the
    // removal that followed it is applied, and the membership ends up removed.
    destination.poison = readd.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 4, rejected: 1 })
    expect(await count('group_memberships')).toBe(1)
    expect(await live()).toBe(0)

    // Replayed now, the re-add is older than the removal it would undo. It is
    // a no-op, like any stale event, and is forgotten like any replayed one.
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(0)
    expect(await count('_rejected')).toBe(0)
  })

  it('does not hide a membership re-added after a quarantined removal', async () => {
    // The mirror image: an older removal replayed after a newer re-add must
    // not tombstone a membership that is active now. Object deletes are
    // terminal and not staleness-gated; membership removals have to be.
    const at = (ms: number) => new Date(Date.now() - 50_000 + ms).toISOString()
    const pair = { group_id: 'group_01', organization_membership_id: 'om_01' }
    const push = (event: string, created_at: string) => {
      const e = { id: id('event_'), event, data: pair, created_at }
      fake.events.push(e)
      return e
    }
    push('group.member_added', at(0))
    const removal = push('group.member_removed', at(1_000))
    push('group.member_added', at(2_000))
    const { engine, destination } = makeFaulty({ streams: ['group_memberships', 'events'] })
    const live = async () => {
      const r = await db.query<{ n: string }>(
        `select count(*)::text as n from workos.group_memberships where "_deleted_at" is null`,
      )
      return Number(r.rows[0]!.n)
    }

    destination.poison = removal.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 3, rejected: 1 })
    expect(await live()).toBe(1)

    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(1)
    expect(await count('_rejected')).toBe(0)

    // A removal that really is newer than the re-add still lands.
    push('group.member_removed', at(3_000))
    expect(await engine.tick()).toMatchObject({ processed: 1, rejected: 0 })
    expect(await live()).toBe(0)
  })

  it('quarantines an event whose own timestamp Postgres cannot store', async () => {
    // A `created_at` that has the ISO-8601 shape but is not a real instant
    // fails the archive write's cast. The quarantine row stores the same
    // value, and must not fail on it: that would leave the tail stuck on the
    // very event the quarantine exists to set aside.
    const bad = fake.createUser({ email: 'bad@example.com' })
    const badEvent = eventFor(bad['id'] as string)
    badEvent.created_at = '2026-02-30T12:00:00Z'
    fake.createUser({ email: 'good@example.com' })
    const { engine } = makeFaulty()

    expect((await engine.tick()).processed).toBe(0)
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 1 })
    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({ eventId: badEvent.id, createdAt: '2026-02-30T12:00:00Z' })
    expect(rejected[0]!.error).toMatch(/out of range/)
    // The typed column is only for SQL; the value as it arrived is in `_raw`.
    const stored = await db.query<{ created_at: string | null }>(`select created_at from workos."_rejected"`)
    expect(stored.rows[0]!.created_at).toBeNull()
    expect(await emails()).toEqual(['good@example.com'])
    expect((await engine.status())['prod']!.cursor).not.toBeNull()
  })

  it('is scrubbed by redact like the event archive', async () => {
    fake.createUser({ email: 'keep@example.com' })
    const subject = fake.createUser({ email: 'forget@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = subject['id'] as string
    await engine.tick()
    await engine.tick()
    expect(await count('_rejected')).toBe(1)

    const removed = await engine.redact(subject['id'] as string)
    expect(removed).toContainEqual({ table: '_rejected', rows: 1 })
    expect(await count('_rejected')).toBe(0)
    const leftovers = await db.query<{ n: string }>(
      `select count(*)::text as n from workos."_rejected" where "_raw"::text like $1`,
      [`%${subject['id']}%`],
    )
    expect(Number(leftovers.rows[0]!.n)).toBe(0)
  })

  it('quarantines a payload that Postgres itself cannot cast, with no fault injected', async () => {
    // The realistic case: an operator projects a boolean column, and one
    // object carries a string there. The generated column raises on insert,
    // the batch fails, and without quarantine every stream would stop here.
    fake.createUser({ email: 'good@example.com', metadata: { beta: true } })
    const bad = fake.createUser({ email: 'bad@example.com', metadata: { beta: 'maybe' } })
    const engine = makeEngine(db, fake, TIER1, {
      // Hydration is on by default for users, so the REST object — the one
      // with `metadata` — is what gets written.
      extraColumns: { users: [{ name: 'beta', type: 'boolean', path: ['metadata', 'beta'] }] },
    })

    expect((await engine.tick()).processed).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()

    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 2, rejected: 1 })
    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]!.objectId).toBe(bad['id'])
    expect(rejected[0]!.error).toMatch(/invalid input syntax for type boolean: "maybe"/)

    const rows = await db.query<{ email: string; beta: boolean }>(`select email, beta from workos.users`)
    expect(rows.rows).toEqual([{ email: 'good@example.com', beta: true }])
    expect((await engine.status())['prod']!.cursor).not.toBeNull()
  })
})

describe('User-Agent', () => {
  it("is the engine's name and version alone in the engine's own client", () => {
    const engine = new SyncEngine({
      destination: new PostgresDestination({ db: {} as SqlClient, schema: 'workos' }),
      environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
      logger: silentLogger,
    })
    // The client the engine builds for itself when no `clientFor` is injected.
    const client = (
      engine as unknown as { clientFor(env: { name: string; apiKey: string }): WorkOSClient }
    ).clientFor({ name: 'prod', apiKey: 'sk_test_1' })
    expect(client.userAgent).toBe(`workos-sync-engine/${ENGINE_VERSION}`)
  })
})

describe('reconcile page budget', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  const listings = (prefix: string) => fake.requestLog.filter((r) => r.startsWith(prefix)).length
  const diffFor = (r: Awaited<ReturnType<SyncEngine['reconcile']>>, stream: string) =>
    r.diffs['prod']!.find((d) => d.stream === stream)!

  it('runs a started stream to its last page, then leaves the rest for a resume', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()

    // Two pages are budgeted and users needs three. The diff is a set
    // difference against the whole listing, so the stream finishes anyway;
    // the budget's job is to keep the next stream from starting.
    const first = await engine.reconcile({ maxPages: 2 })
    expect(diffFor(first, 'users')).toMatchObject({ pages: 3, complete: true, matched: 250 })
    expect(diffFor(first, 'invitations')).toMatchObject({ pages: 0, complete: false })
    expect(diffFor(first, 'invitations').skippedReason).toMatch(/resume/)

    const usersBefore = listings('/user_management/users')
    const second = await engine.reconcile({ resume: true, maxPages: 2 })
    expect(diffFor(second, 'users')).toMatchObject({ pages: 0, complete: true })
    expect(diffFor(second, 'invitations')).toMatchObject({ pages: 1, complete: true })
    // The resumed call did not list users again.
    expect(listings('/user_management/users')).toBe(usersBefore)
  })

  it('forgets completion on a fresh run, so a resume diffs what the budget skipped', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()
    await engine.reconcile()
    const before = listings('/user_management/invitations')

    // A fresh bounded run spends its one page on users and never reaches invitations.
    const fresh = await engine.reconcile({ maxPages: 1 })
    expect(diffFor(fresh, 'invitations')).toMatchObject({ pages: 0, complete: false })
    expect(listings('/user_management/invitations')).toBe(before)

    // Resuming must diff invitations, not trust the completion the earlier run left behind.
    const resumed = await engine.reconcile({ resume: true })
    expect(diffFor(resumed, 'invitations')).toMatchObject({ pages: 1, complete: true })
    expect(listings('/user_management/invitations')).toBe(before + 1)
  })

  it('still finds and repairs drift inside a bounded call', async () => {
    const stale = fake.createUser({ email: 'gone@example.com' })
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()
    fake.users.delete(stale['id'] as string)
    fake.events = []

    const r = await engine.reconcile({ repair: true, maxPages: 1 })
    expect(diffFor(r, 'users')).toMatchObject({ complete: true, localOnly: [stale['id']], repaired: { deleted: 1, inserted: 0 } })
    expect(diffFor(r, 'invitations')).toMatchObject({ complete: false })
  })

  it('keeps its progress apart from the backfill checkpoint', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users'])

    // A backfill paused inside users...
    const paused = await engine.backfill({ maxPages: 2 })
    expect(paused.find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })

    // ...survives a reconcile that records users as complete for its own purposes.
    const rec = await engine.reconcile()
    expect(diffFor(rec, 'users')).toMatchObject({ complete: true })

    const resumed = await engine.backfill({ resume: true })
    expect(resumed.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
  })

  it('reports a failed stream as incomplete instead of leaving it out', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()
    const added = fake.createUser({ email: 'added@example.com' })
    fake.events = []

    // Repairing the missed create writes a row, and that write fails.
    const destination = new FaultyDestination({ db, schema: 'workos' })
    destination.poison = added['id'] as string
    const faulty = new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users', 'invitations'],
        hydrate: 'off',
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: () =>
          new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl }),
      },
    )
    const r = await faulty.reconcile({ repair: true })
    // The listing was fetched before the repair failed, and that page came
    // off the shared budget, so the failure still reports it.
    expect(diffFor(r, 'users')).toMatchObject({ pages: 1, complete: false })
    expect(diffFor(r, 'users').skippedReason).toMatch(/^failed: /)
    // The other stream was still diffed.
    expect(diffFor(r, 'invitations')).toMatchObject({ complete: true })
    // A resume comes back to the failed stream rather than skipping it.
    expect(diffFor(await engine.reconcile({ resume: true }), 'users')).toMatchObject({ pages: 1, complete: true })
  })

  it('reports one result per stream when recording its completion fails', async () => {
    fake.createUser()
    class Unrecordable extends PostgresDestination {
      override async setState(env: string, stream: string, state: unknown): Promise<void> {
        if (stream.startsWith('__reconcile__:') && (state as { status?: string }).status === 'complete') {
          throw new Error('state write refused')
        }
        return super.setState(env, stream, state)
      }
    }
    const engine = makeEngine(db, fake, ['users'], { destination: new Unrecordable({ db, schema: 'workos' }) })
    await engine.backfill()

    const r = await engine.reconcile()
    const users = r.diffs['prod']!.filter((d) => d.stream === 'users')
    // The diff itself succeeded, but the stream is reported once, as the
    // failure, so a caller using find() cannot pick up a success beside it.
    expect(users).toHaveLength(1)
    expect(users[0]).toMatchObject({ pages: 1, complete: false })
    expect(users[0]!.skippedReason).toMatch(/^failed: /)
  })

  it('resets every environment before listing, so a fatal error in the first cannot leave the second stale', async () => {
    fake.createUser()
    let denyA = false
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [
          { name: 'a', apiKey: 'sk_a' },
          { name: 'b', apiKey: 'sk_b' },
        ],
        streams: ['users'],
        hydrate: 'off',
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: (env) =>
          new WorkOSClient({
            apiKey: env.apiKey,
            apiUrl: 'http://fake.workos.local',
            pageSize: 100,
            fetchImpl: async (input, init) =>
              denyA && env.name === 'a'
                ? new Response('{"message":"Unauthorized"}', {
                    status: 401,
                    headers: { 'content-type': 'application/json' },
                  })
                : fake.fetchImpl(input, init),
          }),
      },
    )
    await engine.backfill()
    const full = await engine.reconcile()
    expect(full.diffs['a']!.find((d) => d.stream === 'users')).toMatchObject({ complete: true })
    expect(full.diffs['b']!.find((d) => d.stream === 'users')).toMatchObject({ complete: true })

    // A fresh run dies on the first environment's credential before it
    // lists anything in the second.
    denyA = true
    await expect(engine.reconcile()).rejects.toThrow(/401/)
    denyA = false

    // The second environment must be diffed again, not skipped on the
    // completion the earlier full run recorded.
    const resumed = await engine.reconcile({ resume: true })
    const b = resumed.diffs['b']!.find((d) => d.stream === 'users')!
    expect(b).toMatchObject({ pages: 1, complete: true })
    expect(b.skippedReason).toBeUndefined()
  })

  it('refuses a page budget that is not a positive integer', async () => {
    const engine = makeEngine(db, fake, ['users'])
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      await expect(engine.reconcile({ maxPages: bad })).rejects.toThrow(/maxPages/)
    }
  })
})

describe('backfill reset order', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  it('resets every environment before listing, so a fatal error in the first cannot leave the second stale', async () => {
    fake.createUser()
    let denyA = false
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [
          { name: 'a', apiKey: 'sk_a' },
          { name: 'b', apiKey: 'sk_b' },
        ],
        streams: ['users'],
        hydrate: 'off',
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: (env) =>
          new WorkOSClient({
            apiKey: env.apiKey,
            apiUrl: 'http://fake.workos.local',
            pageSize: 100,
            fetchImpl: async (input, init) =>
              denyA && env.name === 'a'
                ? new Response('{"message":"Unauthorized"}', {
                    status: 401,
                    headers: { 'content-type': 'application/json' },
                  })
                : fake.fetchImpl(input, init),
          }),
      },
    )
    // Results come back in environment order: users for a, then users for b.
    const full = await engine.backfill()
    expect(full.filter((r) => r.stream === 'users').map((r) => r.complete)).toEqual([true, true])

    // A fresh run dies on the first environment's credential before it
    // lists anything in the second.
    denyA = true
    await expect(engine.backfill()).rejects.toThrow(/401/)
    denyA = false

    // The second environment must be listed again, not skipped on the
    // completion the earlier full run recorded.
    const resumed = await engine.backfill({ resume: true })
    const users = resumed.filter((r) => r.stream === 'users')
    expect(users).toHaveLength(2)
    expect(users[1]).toMatchObject({ pages: 1, complete: true })
    expect(users[1]!.skippedReason).toBeUndefined()
  })
})

describe('refresh page budget', () => {
  let db: SqlClient
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  const count = async (table: string) => {
    const r = await db.query<{ n: string }>(`select count(*)::text as n from workos.${table}`)
    return Number(r.rows[0]!.n)
  }
  const userListings = (since: number) =>
    fake.requestLog.slice(since).filter((r) => r.startsWith('/user_management/users'))

  it('stops at the page budget, resumes from the checkpoint, and lists under the reconcile phase', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()
    const before = fake.requestLog.length

    const first = await engine.refresh({ maxPages: 2 })
    expect(first.find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })
    expect(first.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })
    // Every listing a refresh makes is counted under the reconcile phase
    // upstream, as it was before it could be sliced.
    const phases = fake.userAgents.slice(before)
    expect(phases.length).toBeGreaterThan(0)
    expect(phases.every((ua) => ua.endsWith('(phase=reconcile)'))).toBe(true)

    const second = await engine.refresh({ resume: true })
    expect(second.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
    expect(second.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 1, complete: true })
    const users = userListings(before)
    expect(users).toHaveLength(3)
    expect(users[2]).toContain('after=')
  })

  it('shares its checkpoint with backfill, so either command resumes the other', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users'])

    const paused = await engine.refresh({ maxPages: 2 })
    expect(paused.find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })
    expect(await count('users')).toBe(200)

    const before = fake.requestLog.length
    const finished = await engine.backfill({ resume: true })
    expect(finished.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
    expect(await count('users')).toBe(250)
    // One request, carrying the cursor the refresh stopped at.
    const users = userListings(before)
    expect(users).toHaveLength(1)
    expect(users[0]).toContain('after=')
  })

  it('records the tail starting point before listing, like backfill', async () => {
    for (let i = 0; i < 150; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users'], { hydrate: 'off' })

    // A refresh on a new mirror pauses after its first page with the tail's
    // starting point already recorded: anything that changes from here on is
    // the tail's to replay, whether or not a later listing sees it.
    const paused = await engine.refresh({ maxPages: 1 })
    expect(paused.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: false })
    const captured = (await engine.status())['prod']!
    expect(captured.started).toBe(true)
    expect(captured.rangeStart).not.toBeNull()

    // The resumed backfill keeps that position rather than recording a later
    // one, which would leave a change between the two invisible to both the
    // listing and the tail.
    await engine.backfill({ resume: true })
    expect((await engine.status())['prod']!.rangeStart).toBe(captured.rangeStart)
  })

  it('leaves an expired cursor for a fresh backfill to recover', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake, ['users'])
    await engine.init()
    const stale = new Date(Date.now() - 100 * 86_400_000).toISOString()
    await db.query(
      `insert into workos."_sync_state" (sync_id, env, stream, state)
       values ('default', 'prod', '__tail__', $1::jsonb)`,
      [JSON.stringify({ kind: 'tail', cursor: 'event_000', lastEventAt: stale })],
    )

    // A refresh re-lists only the top-level streams, and a resumed backfill
    // skips what is complete, so neither re-establishes enough state to make
    // restarting the tail from now harmless. Both leave the cursor where it
    // is, for the retention alarm to report.
    await engine.refresh()
    expect((await engine.status())['prod']!.cursor).toBe('event_000')
    await engine.backfill({ resume: true })
    expect((await engine.status())['prod']!.cursor).toBe('event_000')

    // A fresh backfill re-lists everything, so it restarts the tail from now.
    await engine.backfill()
    const status = (await engine.status())['prod']!
    expect(status.cursor).toBeNull()
    expect(status.rangeStart).not.toBeNull()
  })

  it('leaves the per-parent streams to backfill', async () => {
    const org = fake.createOrganization()
    fake.createMembership(fake.createUser()['id'] as string, org['id'] as string)
    const engine = makeEngine(db, fake, ['users', 'organizations', 'organization_memberships'])
    await engine.backfill()
    const before = fake.requestLog.length

    const r = await engine.refresh()
    expect(r.map((s) => s.stream).sort()).toEqual(['organizations', 'users'])
    expect(r.every((s) => s.complete)).toBe(true)
    const since = fake.requestLog.slice(before)
    expect(since.some((p) => p.startsWith('/user_management/organization_memberships'))).toBe(false)
  })

  it('refuses a page budget that is not a positive integer', async () => {
    const engine = makeEngine(db, fake, ['users'])
    for (const bad of [0, -1, 1.5, Number.NaN]) {
      await expect(engine.refresh({ maxPages: bad })).rejects.toThrow(/maxPages/)
    }
  })
})
