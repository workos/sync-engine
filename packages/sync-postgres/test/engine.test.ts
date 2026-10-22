/**
 * Whole-engine tests: backfill, tail, drift and repair, driven against the
 * fake WorkOS API and a real Postgres.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { SyncEngine, WorkOSClient, silentLogger } from '@workos/sync-engine'
import { createTestDb } from './helpers/pg.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'

const TIER1 = [
  'users',
  'organizations',
  'organization_memberships',
  'organization_domains',
  'invitations',
  'events',
  'auth_attempts',
]

function makeEngine(db: SqlClient, fake: FakeWorkOS, streams = TIER1, extra = {}) {
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
          fetchImpl: fake.fetchImpl,
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
