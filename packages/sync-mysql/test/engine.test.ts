/**
 * Whole-engine tests: backfill, tail, drift and repair, driven against the
 * fake WorkOS API and a real MySQL. The engine's own behaviour is covered
 * exhaustively in `@workos/sync-postgres`; what is exercised here is every
 * path through this destination, with the same fake, so that a MySQL-only
 * difference in a write cannot hide behind a unit test.
 */
import { it, expect, beforeEach, afterEach } from 'vitest'
import { SyncEngine, WorkOSClient, LeaseHeldError, silentLogger } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { FakeWorkOS, resetIds, id } from '../../sync-engine/test/helpers/fake-workos.js'
import { MysqlDestination } from '../src/destination.js'
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

function makeEngine(db: TestDb, fake: FakeWorkOS, streams = TIER1, extra = {}) {
  return new SyncEngine(
    {
      destination: new MysqlDestination({ db, schema: db.schema }),
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

describeDb('sync engine', () => {
  let db: TestDb
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
    const r = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.\`${table}\``)
    return Number(r.rows[0]!.n)
  }
  const emails = async (table = 'users') => {
    const r = await db.query<{ email: string }>(`select email from \`${db.schema}\`.\`${table}\` order by email`)
    return r.rows.map((x) => x.email)
  }

  it('backfills existing state, including the field events omit', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')

    const engine = makeEngine(db, fake)
    await engine.backfill()

    expect(await count('users')).toBe(1)
    expect(await count('organizations')).toBe(1)
    expect(await count('organization_memberships')).toBe(1)

    const u = await db.query<{ email: string; external_id: string; metadata: unknown }>(
      `select email, external_id, metadata from \`${db.schema}\`.users`,
    )
    expect(u.rows[0]!.email).toBe('ada@acme.com')
    expect(u.rows[0]!.external_id).toMatch(/^ext_/)
    expect(u.rows[0]!.metadata).toEqual({ plan: 'pro' })

    const o = await db.query<{ allow_profiles_outside_organization: number | null }>(
      `select allow_profiles_outside_organization from \`${db.schema}\`.organizations`,
    )
    expect(o.rows[0]!.allow_profiles_outside_organization).toBe(0)

    const m = await db.query<{ role_slug: string }>(`select role_slug from \`${db.schema}\`.organization_memberships`)
    expect(m.rows[0]!.role_slug).toBe('admin')
  })

  it('applies live events after backfill', async () => {
    const user = fake.createUser({ email: 'before@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()

    fake.updateUser(user['id'] as string, { email: 'after@example.com' })
    fake.createOrganization({ name: 'New Co' })
    await engine.tick()

    expect(await emails()).toEqual(['after@example.com'])
    const o = await db.query<{ name: string }>(`select name from \`${db.schema}\`.organizations`)
    expect(o.rows[0]!.name).toBe('New Co')
  })

  it('does not null out backfilled fields when an event arrives', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()

    fake.updateOrganization(org['id'] as string, { name: 'Acme Corp' })
    await engine.tick()

    const row = await db.query<{ name: string; allow_profiles_outside_organization: number | null }>(
      `select name, allow_profiles_outside_organization from \`${db.schema}\`.organizations`,
    )
    expect(row.rows[0]!.name).toBe('Acme Corp')
    expect(row.rows[0]!.allow_profiles_outside_organization).toBe(0)
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
    const shuffled = new FakeWorkOS({ shuffleEvents: true })
    shuffled.users = fake.users
    shuffled.events = fake.events

    const engine = makeEngine(db, shuffled)
    await engine.tick()

    expect(await emails()).toEqual(['v3@example.com'])
  })

  it('tombstones a deleted user, drops it from the live view, and keeps the base row', async () => {
    const gone = fake.createUser({ email: 'gone@example.com' })
    fake.createUser({ email: 'stays@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    expect(await count('users_live')).toBe(2)

    fake.deleteUser(gone['id'] as string)
    await engine.tick()

    expect(await count('users')).toBe(2)
    expect(await count('users_live')).toBe(1)
    expect(await emails('users_live')).toEqual(['stays@example.com'])
  })

  it('records sign-in attempts that exist nowhere else in the API', async () => {
    const user = fake.createUser()
    fake.emitAuthAttempt(user['id'] as string, true)
    fake.emitAuthAttempt(user['id'] as string, false, 'password')
    const engine = makeEngine(db, fake)
    await engine.tick()
    expect(await count('auth_attempts')).toBe(2)
    const failed = await db.query<{ n: number }>(
      `select count(*) as n from \`${db.schema}\`.auth_attempts where status = 'failed'`,
    )
    expect(Number(failed.rows[0]!.n)).toBe(1)
  })

  it('resumes from the stored cursor rather than replaying', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.tick()
    const first = await engine.status()
    fake.createUser()
    await engine.tick()
    const second = await engine.status()
    expect(second['prod']!.cursor).not.toBe(first['prod']!.cursor)
    expect(await count('users')).toBe(2)
    // A fresh engine over the same database carries on from the same cursor.
    const again = makeEngine(db, fake)
    expect((await again.tick()).processed).toBe(0)
  })

  it('explodes organization domains out of the organization payload', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    fake.createDomain(org['id'] as string, 'acme.com')
    const engine = makeEngine(db, fake)
    await engine.backfill()
    const d = await db.query<{ domain: string; organization_id: string }>(
      `select domain, organization_id from \`${db.schema}\`.organization_domains`,
    )
    expect(d.rows).toEqual([{ domain: 'acme.com', organization_id: org['id'] }])
  })

  it('never mirrors invitation acceptance tokens, however the row arrives', async () => {
    const org = fake.createOrganization()
    fake.createInvitation(org['id'] as string, { email: 'new@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    const rows = await db.query<{ raw: Record<string, unknown> }>(
      `select \`_raw\` as raw from \`${db.schema}\`.invitations`,
    )
    expect(rows.rows).toHaveLength(1)
    expect(rows.rows[0]!.raw).not.toHaveProperty('token')
    expect(rows.rows[0]!.raw).not.toHaveProperty('accept_invitation_url')
    const archived = await db.query<{ raw: Record<string, unknown> }>(
      `select \`_raw\` as raw from \`${db.schema}\`.events where event like 'invitation.%'`,
    )
    for (const e of archived.rows) expect(e.raw['data']).not.toHaveProperty('token')
  })

  it('keeps two environments apart in one database', async () => {
    fake.createUser({ email: 'prod@example.com' })
    const other = new FakeWorkOS()
    other.createUser({ email: 'staging@example.com' })
    const engine = new SyncEngine(
      {
        destination: new MysqlDestination({ db, schema: db.schema }),
        environments: [
          { name: 'prod', apiKey: 'sk_prod' },
          { name: 'staging', apiKey: 'sk_staging' },
        ],
        streams: ['users'],
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: (env) =>
          new WorkOSClient({
            apiKey: env.apiKey,
            apiUrl: 'http://fake.workos.local',
            fetchImpl: env.apiKey === 'sk_prod' ? fake.fetchImpl : other.fetchImpl,
          }),
      },
    )
    await engine.backfill()
    const rows = await db.query<{ _env: string; email: string }>(
      `select \`_env\`, email from \`${db.schema}\`.users order by \`_env\``,
    )
    expect(rows.rows).toEqual([
      { _env: 'prod', email: 'prod@example.com' },
      { _env: 'staging', email: 'staging@example.com' },
    ])
    const status = await engine.status()
    expect(Object.keys(status).sort()).toEqual(['prod', 'staging'])
  })

  it('projects extra columns out of _raw without touching the catalog', async () => {
    fake.createUser({ email: 'ada@example.com', metadata: { plan: 'pro', seats: 12 } })
    const engine = makeEngine(db, fake, TIER1, {
      extraColumns: {
        users: [
          { name: 'plan', type: 'text', path: ['metadata', 'plan'], index: true },
          { name: 'seats', type: 'bigint', path: ['metadata', 'seats'] },
        ],
      },
    })
    await engine.backfill()
    const r = await db.query<{ plan: string; seats: number }>(`select plan, seats from \`${db.schema}\`.users`)
    expect(r.rows[0]).toEqual({ plan: 'pro', seats: 12 })
    const idx = await db.query<{ index_name: string }>(
      `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = 'users'`,
      [db.schema],
    )
    expect(idx.rows.map((i) => i.index_name)).toContain('users_plan_idx')
  })

  it('lands a payload a typed column cannot cast, with the column NULL and the value in _raw', async () => {
    // Where Postgres quarantines the event, MySQL's `NULL ON ERROR` keeps the
    // row flowing: nothing is lost, and the bad value is still in `_raw`.
    fake.createUser({ email: 'good@example.com', metadata: { beta: true } })
    const bad = fake.createUser({ email: 'bad@example.com', metadata: { beta: 'maybe' } })
    const engine = makeEngine(db, fake, TIER1, {
      extraColumns: { users: [{ name: 'beta', type: 'boolean', path: ['metadata', 'beta'] }] },
    })
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 0 })
    const rows = await db.query<{ email: string; beta: number | null; raw: Record<string, unknown> }>(
      `select email, beta, \`_raw\` as raw from \`${db.schema}\`.users order by email`,
    )
    expect(rows.rows.map((r) => [r.email, r.beta])).toEqual([
      ['bad@example.com', null],
      ['good@example.com', 1],
    ])
    expect((rows.rows[0]!.raw['metadata'] as Record<string, unknown>)['beta']).toBe('maybe')
    expect(bad['id']).toBeTruthy()
  })

  it('joins mirrored identity data with application tables', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')
    const engine = makeEngine(db, fake)
    await engine.backfill()

    await db.exec(
      `create table \`${db.schema}\`.app_accounts (workos_org_id varchar(191) primary key, plan varchar(32))`,
    )
    await db.query(`insert into \`${db.schema}\`.app_accounts values (?, 'enterprise')`, [org['id']])
    const r = await db.query<{ email: string; plan: string; role_slug: string }>(
      `select u.email, a.plan, m.role_slug
       from \`${db.schema}\`.organization_memberships_live m
       join \`${db.schema}\`.users_live u on u.id = m.user_id and u.\`_env\` = m.\`_env\`
       join \`${db.schema}\`.app_accounts a on a.workos_org_id = m.organization_id`,
    )
    expect(r.rows).toEqual([{ email: 'ada@acme.com', plan: 'enterprise', role_slug: 'admin' }])
  })
})

describeDb('reconciliation', () => {
  let db: TestDb
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
    expect(users).toMatchObject({ matched: 1, localOnly: [], remoteOnly: [] })
  })

  it('detects a missed delete and a missed create, and repairs both', async () => {
    const stale = fake.createUser({ email: 'gone@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()

    fake.users.delete(stale['id'] as string)
    const added = fake.createUser({ email: 'added@example.com' })
    fake.events = []

    const first = await engine.reconcile()
    const users = first.diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([stale['id']])
    expect(users.remoteOnly).toEqual([added['id']])

    await engine.reconcile({ repair: true })
    const live = await db.query<{ email: string }>(`select email from \`${db.schema}\`.users_live order by email`)
    expect(live.rows.map((r) => r.email)).toEqual(['added@example.com'])
    const tombstoned = await db.query<{ email: string }>(
      `select email from \`${db.schema}\`.users where \`_deleted_at\` is not null`,
    )
    expect(tombstoned.rows[0]!.email).toBe('gone@example.com')

    const again = await engine.reconcile()
    expect(again.diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({ localOnly: [], remoteOnly: [] })
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
    expect(users.stalenessSeconds).not.toBeNull()
  })

  it('refreshes fields that event payloads omit', async () => {
    fake.createOrganization()
    const engine = makeEngine(db, fake)
    await engine.tick()
    const read = async () =>
      (
        await db.query<{ allow_profiles_outside_organization: number | null }>(
          `select allow_profiles_outside_organization from \`${db.schema}\`.organizations`,
        )
      ).rows[0]!.allow_profiles_outside_organization
    expect(await read()).toBeNull()
    await engine.refresh()
    expect(await read()).toBe(0)
  })
})

describeDb('single-writer lease', () => {
  let db: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await db.close()
  })

  it('tick() refuses to run while another process holds the lease', async () => {
    fake.createUser()
    const holder = makeEngine(db, fake)
    await holder.init()
    const dest = new MysqlDestination({ db, schema: db.schema })
    await dest.init([])
    expect((await dest.acquireLease('prod', 'someone-else', 60_000)).acquired).toBe(true)
    await expect(holder.tick()).rejects.toBeInstanceOf(LeaseHeldError)
    await dest.releaseLease('prod', 'someone-else')
    expect((await holder.tick()).processed).toBe(1)
    // A clean tick gives the lease back.
    expect(await dest.getLease('prod')).toBeNull()
  })

  it('listen() waits for the lease, takes over when it is released, and gives it back on stop', async () => {
    fake.createUser()
    const dest = new MysqlDestination({ db, schema: db.schema })
    await dest.init([])
    await dest.acquireLease('prod', 'old-process', 60_000)
    const engine = makeEngine(db, fake, TIER1, { pollIntervalMs: 20 })
    const listening = engine.listen()
    await new Promise((r) => setTimeout(r, 150))
    const r = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.users`)
    expect(Number(r.rows[0]!.n)).toBe(0)

    await dest.releaseLease('prod', 'old-process')
    const deadline = Date.now() + 5_000
    while (Date.now() < deadline) {
      const c = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.users`)
      if (Number(c.rows[0]!.n) === 1) break
      await new Promise((r) => setTimeout(r, 20))
    }
    expect(Number((await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.users`)).rows[0]!.n)).toBe(
      1,
    )
    expect((await dest.getLease('prod'))?.holder).not.toBe('old-process')

    engine.stop()
    await listening
    expect(await dest.getLease('prod')).toBeNull()
  })
})

describeDb('redaction', () => {
  let db: TestDb
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
    expect(removed).toContainEqual({ table: 'users', rows: 1 })
    expect(removed).toContainEqual({ table: 'auth_attempts', rows: 1 })
    expect(removed.find((r) => r.table === 'events')!.rows).toBeGreaterThan(0)

    const remaining = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.users`)
    expect(Number(remaining.rows[0]!.n)).toBe(0)
    const events = await db.query<{ n: number }>(
      `select count(*) as n from \`${db.schema}\`.events where cast(\`_raw\` as char) like ?`,
      [`%${user['id']}%`],
    )
    expect(Number(events.rows[0]!.n)).toBe(0)
  })
})

describeDb('poison events', () => {
  let db: TestDb
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
    const r = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.\`${table}\``)
    return Number(r.rows[0]!.n)
  }
  const emails = async () => {
    const r = await db.query<{ email: string }>(`select email from \`${db.schema}\`.users order by email`)
    return r.rows.map((x) => x.email)
  }
  const eventFor = (objectId: string) => fake.events.find((e) => e.data['id'] === objectId)!

  function makeFaulty(extra: Record<string, unknown> = {}) {
    const destination = new FaultyDestination({ db, schema: db.schema })
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
            fetchImpl: fake.fetchImpl,
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

    const first = await engine.tick()
    expect(first).toMatchObject({ processed: 0, rejected: 0 })
    expect(await count('_rejected')).toBe(0)
    const warned = lines.find((l) => l.level === 'warn' && l.fields['eventId'] === badEvent.id)
    expect(warned?.msg).toMatch(/quarantined if it fails again/)

    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 3, rejected: 1 })
    const status = await engine.status()
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
    expect(rejected[0]!.error).toMatch(/Incorrect datetime value/)
    expect((await engine.tick()).processed).toBe(0)
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

    destination.fault = () => Object.assign(new Error('still broken'), { code: 'ER_DATA_TOO_LONG', errno: 1406 })
    const again = await engine.replayRejected()
    expect(again.replayed).toBe(0)
    expect(again.stillRejected).toEqual([{ eventId: eventFor(bad['id'] as string).id, error: 'Error: still broken' }])

    destination.poison = null
    const fixed = await engine.replayRejected()
    expect(fixed).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count('_rejected')).toBe(0)
    expect(await emails()).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('never quarantines on an error the destination classifies as transient', async () => {
    fake.createUser({ email: 'a@example.com' })
    const { engine, destination } = makeFaulty()
    destination.failAll = true
    destination.fault = () => Object.assign(new Error('deadlock'), { code: 'ER_LOCK_DEADLOCK', errno: 1213 })
    for (let i = 0; i < 3; i++) expect((await engine.tick()).processed).toBe(0)
    expect(await count('_rejected')).toBe(0)
    destination.failAll = false
    expect((await engine.tick()).processed).toBe(1)
  })

  it('does not resurrect a membership removed after a quarantined link', async () => {
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
    const live = () => count('group_memberships_live')

    destination.poison = readd.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 4, rejected: 1 })
    expect(await count('group_memberships')).toBe(1)
    expect(await live()).toBe(0)

    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(0)
    expect(await count('_rejected')).toBe(0)
  })

  it('does not hide a membership re-added after a quarantined removal', async () => {
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
    const live = () => count('group_memberships_live')

    destination.poison = removal.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 3, rejected: 1 })
    expect(await live()).toBe(1)

    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(1)

    push('group.member_removed', at(3_000))
    expect(await engine.tick()).toMatchObject({ processed: 1, rejected: 0 })
    expect(await live()).toBe(0)
  })

  it('quarantines an event whose own timestamp cannot be stored, with no fault injected', async () => {
    // A `created_at` with the ISO-8601 shape but no such instant: the archive
    // write refuses it before the statement, and the quarantine row stores the
    // same value without failing on it.
    const bad = fake.createUser({ email: 'bad@example.com' })
    const badEvent = eventFor(bad['id'] as string)
    badEvent.created_at = '2026-13-01T00:00:00Z'
    fake.createUser({ email: 'good@example.com' })
    const { engine } = makeFaulty()

    expect((await engine.tick()).processed).toBe(0)
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 1 })
    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({ eventId: badEvent.id, createdAt: '2026-13-01T00:00:00Z' })
    expect(rejected[0]!.error).toMatch(/invalid updatedAt timestamp/)
    const stored = await db.query<{ created_at: Date | null }>(`select created_at from \`${db.schema}\`.\`_rejected\``)
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
  })
})
