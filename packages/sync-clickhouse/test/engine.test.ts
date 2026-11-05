/**
 * Whole-engine tests: backfill, tail, drift and repair, driven against the
 * fake WorkOS API and a real ClickHouse. The engine's own behaviour is
 * covered exhaustively in `@workos/sync-postgres`; what is exercised here is
 * every path through this destination, with the same fake, so that a
 * ClickHouse-only difference in a write cannot hide behind a unit test.
 */
import { it, expect, beforeEach, afterEach } from 'vitest'
import { SyncEngine, WorkOSClient, LeaseHeldError, silentLogger } from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { FakeWorkOS, resetIds, id } from '../../sync-engine/test/helpers/fake-workos.js'
import { ClickHouseDestination } from '../src/destination.js'
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
      destination: new ClickHouseDestination({ db, schema: db.schema }),
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

const count = async (db: TestDb, table: string, final = true) =>
  Number(
    (
      await db.query<{ n: string }>(
        `SELECT toString(count()) AS n FROM \`${db.schema}\`.\`${table}\`${final ? ' FINAL' : ''}`,
      )
    )[0]!.n,
  )
const emails = async (db: TestDb, table = 'users_live') =>
  (await db.query<{ email: string }>(`SELECT email FROM \`${db.schema}\`.\`${table}\` ORDER BY email`)).map(
    (x) => x.email,
  )

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

  it('backfills existing state, including the field events omit', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')
    const engine = makeEngine(db, fake)
    await engine.backfill()

    expect(await count(db, 'users')).toBe(1)
    expect(await count(db, 'organizations')).toBe(1)
    expect(await count(db, 'organization_memberships')).toBe(1)
    const u = await db.query<{ email: string; external_id: string; metadata: string }>(
      `SELECT email, external_id, metadata FROM \`${db.schema}\`.users_live`,
    )
    expect(u[0]!.email).toBe('ada@acme.com')
    expect(u[0]!.external_id).toMatch(/^ext_/)
    expect(JSON.parse(u[0]!.metadata)).toEqual({ plan: 'pro' })
    const o = await db.query<{ allow: boolean | null }>(
      `SELECT allow_profiles_outside_organization AS allow FROM \`${db.schema}\`.organizations_live`,
    )
    expect(o[0]!.allow).toBe(false)
    const m = await db.query<{ role_slug: string }>(
      `SELECT role_slug FROM \`${db.schema}\`.organization_memberships_live`,
    )
    expect(m[0]!.role_slug).toBe('admin')
  })

  it('applies live events after backfill', async () => {
    const user = fake.createUser({ email: 'before@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    fake.updateUser(user['id'] as string, { email: 'after@example.com' })
    fake.createOrganization({ name: 'New Co' })
    await engine.tick()
    expect(await emails(db)).toEqual(['after@example.com'])
    expect((await db.query<{ name: string }>(`SELECT name FROM \`${db.schema}\`.organizations_live`))[0]!.name).toBe(
      'New Co',
    )
  })

  it('does not null out backfilled fields when an event arrives', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    fake.updateOrganization(org['id'] as string, { name: 'Acme Corp' })
    await engine.tick()
    const row = await db.query<{ name: string; allow: boolean | null }>(
      `SELECT name, allow_profiles_outside_organization AS allow FROM \`${db.schema}\`.organizations_live`,
    )
    expect(row[0]).toEqual({ name: 'Acme Corp', allow: false })
  })

  it('is idempotent when the same events are delivered twice', async () => {
    fake = new FakeWorkOS({ duplicateEvents: true })
    fake.createUser()
    fake.createOrganization()
    const engine = makeEngine(db, fake)
    await engine.tick()
    await engine.tick()
    expect(await count(db, 'users')).toBe(1)
    expect(await count(db, 'organizations')).toBe(1)
    expect(await count(db, 'events', false)).toBe(2)
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
    expect(await emails(db)).toEqual(['v3@example.com'])
  })

  it('tombstones a deleted user, drops it from the live view, and keeps the base row', async () => {
    const gone = fake.createUser({ email: 'gone@example.com' })
    fake.createUser({ email: 'stays@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    expect(await count(db, 'users_live')).toBe(2)
    fake.deleteUser(gone['id'] as string)
    await engine.tick()
    expect(await count(db, 'users')).toBe(2)
    expect(await count(db, 'users_live')).toBe(1)
    expect(await emails(db)).toEqual(['stays@example.com'])
  })

  it('records sign-in attempts that exist nowhere else in the API', async () => {
    const user = fake.createUser()
    fake.emitAuthAttempt(user['id'] as string, true)
    fake.emitAuthAttempt(user['id'] as string, false, 'password')
    const engine = makeEngine(db, fake)
    await engine.tick()
    expect(await count(db, 'auth_attempts', false)).toBe(2)
    const failed = await db.query<{ n: string }>(
      `SELECT toString(countIf(status = 'failed')) AS n FROM \`${db.schema}\`.auth_attempts`,
    )
    expect(Number(failed[0]!.n)).toBe(1)
  })

  it('resumes from the stored cursor rather than replaying', async () => {
    fake.createUser()
    const engine = makeEngine(db, fake)
    await engine.tick()
    const first = await engine.status()
    fake.createUser()
    await engine.tick()
    expect((await engine.status())['prod']!.cursor).not.toBe(first['prod']!.cursor)
    expect(await count(db, 'users')).toBe(2)
    expect((await makeEngine(db, fake).tick()).processed).toBe(0)
  })

  it('explodes organization domains out of the organization payload', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    fake.createDomain(org['id'] as string, 'acme.com')
    const engine = makeEngine(db, fake)
    await engine.backfill()
    const d = await db.query<{ domain: string; organization_id: string }>(
      `SELECT domain, organization_id FROM \`${db.schema}\`.organization_domains_live`,
    )
    expect(d).toEqual([{ domain: 'acme.com', organization_id: org['id'] }])
  })

  it('never mirrors invitation acceptance tokens, however the row arrives', async () => {
    const org = fake.createOrganization()
    fake.createInvitation(org['id'] as string, { email: 'new@example.com' })
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    const rows = await db.query<{ raw: string }>(`SELECT \`_raw\` AS raw FROM \`${db.schema}\`.invitations_live`)
    expect(rows).toHaveLength(1)
    expect(JSON.parse(rows[0]!.raw)).not.toHaveProperty('token')
    const archived = await db.query<{ raw: string }>(
      `SELECT \`_raw\` AS raw FROM \`${db.schema}\`.events WHERE event LIKE 'invitation.%'`,
    )
    for (const e of archived) expect(JSON.parse(e.raw)['data']).not.toHaveProperty('token')
  })

  it('keeps two environments apart in one database', async () => {
    fake.createUser({ email: 'prod@example.com' })
    const other = new FakeWorkOS()
    other.createUser({ email: 'staging@example.com' })
    const engine = new SyncEngine(
      {
        destination: new ClickHouseDestination({ db, schema: db.schema }),
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
      `SELECT \`_env\`, email FROM \`${db.schema}\`.users_live ORDER BY \`_env\``,
    )
    expect(rows).toEqual([
      { _env: 'prod', email: 'prod@example.com' },
      { _env: 'staging', email: 'staging@example.com' },
    ])
    expect(Object.keys(await engine.status()).sort()).toEqual(['prod', 'staging'])
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
    const r = await db.query<{ plan: string; seats: string }>(`SELECT plan, seats FROM \`${db.schema}\`.users_live`)
    expect(r[0]!.plan).toBe('pro')
    expect(Number(r[0]!.seats)).toBe(12)
    const idx = await db.query<{ name: string }>(
      `SELECT name FROM system.data_skipping_indices WHERE database = {s:String} AND table = 'users'`,
      { s: db.schema },
    )
    expect(idx.map((i) => i.name)).toContain('users_plan_idx')
  })

  it('lands a payload a typed column cannot cast, with the column NULL and the value in _raw', async () => {
    fake.createUser({ email: 'good@example.com', metadata: { beta: true } })
    fake.createUser({ email: 'bad@example.com', metadata: { beta: 'maybe' } })
    const engine = makeEngine(db, fake, TIER1, {
      extraColumns: { users: [{ name: 'beta', type: 'boolean', path: ['metadata', 'beta'] }] },
    })
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 0 })
    const rows = await db.query<{ email: string; beta: boolean | null; raw: string }>(
      `SELECT email, beta, \`_raw\` AS raw FROM \`${db.schema}\`.users_live ORDER BY email`,
    )
    expect(rows.map((r) => [r.email, r.beta])).toEqual([
      ['bad@example.com', null],
      ['good@example.com', true],
    ])
    expect(JSON.parse(rows[0]!.raw)['metadata']['beta']).toBe('maybe')
  })

  it('joins mirrored identity data with the sign-in log', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')
    fake.emitAuthAttempt(user['id'] as string, false)
    fake.emitAuthAttempt(user['id'] as string, true)
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    const r = await db.query<{ email: string; organization: string; failures: string }>(
      `SELECT u.email AS email, o.name AS organization, toString(countIf(a.status = 'failed')) AS failures
       FROM \`${db.schema}\`.auth_attempts a
       JOIN \`${db.schema}\`.users_live u ON u.id = a.user_id AND u.\`_env\` = a.\`_env\`
       JOIN \`${db.schema}\`.organization_memberships_live m ON m.user_id = u.id AND m.\`_env\` = u.\`_env\`
       JOIN \`${db.schema}\`.organizations_live o ON o.id = m.organization_id AND o.\`_env\` = m.\`_env\`
       GROUP BY email, organization`,
    )
    expect(r).toEqual([{ email: 'ada@acme.com', organization: 'Acme', failures: '1' }])
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
    expect(diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({
      matched: 1,
      localOnly: [],
      remoteOnly: [],
    })
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
    expect(await emails(db)).toEqual(['added@example.com'])
    const tombstoned = await db.query<{ email: string }>(
      `SELECT email FROM \`${db.schema}\`.users FINAL WHERE \`_deleted_at\` IS NOT NULL`,
    )
    expect(tombstoned[0]!.email).toBe('gone@example.com')
    expect((await engine.reconcile()).diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({
      localOnly: [],
      remoteOnly: [],
    })
  })

  it('keeps an oversized verdict across runs, and forgets it once the stream completes', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(db, fake, ['users', 'invitations'])
    await engine.backfill()

    // Three pages upstream and two budgeted: abandoned on the third page,
    // with the verdict written to the stream's state in `_sync`.
    const first = await engine.reconcile({ maxPages: 2 })
    expect(first.diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({
      pages: 3,
      complete: false,
      oversized: true,
    })
    expect(await engine.status()).toMatchObject({ prod: { oversized: ['users'] } })

    // A resumed slice under the same budget reads the verdict back and skips the stream.
    const second = await engine.reconcile({ resume: true, maxPages: 2 })
    expect(second.diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({ pages: 0, oversized: true })
    expect(second.diffs['prod']!.find((d) => d.stream === 'invitations')).toMatchObject({ complete: true })

    // A larger budget diffs it in full and clears the verdict.
    const third = await engine.reconcile({ resume: true, maxPages: 3 })
    expect(third.diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({ complete: true, matched: 250 })
    expect(await engine.status()).toMatchObject({ prod: { oversized: [] } })
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
    fake.createOrganization()
    const engine = makeEngine(db, fake)
    await engine.tick()
    const read = async () =>
      (
        await db.query<{ allow: boolean | null }>(
          `SELECT allow_profiles_outside_organization AS allow FROM \`${db.schema}\`.organizations_live`,
        )
      )[0]!.allow
    expect(await read()).toBeNull()
    await engine.refresh()
    expect(await read()).toBe(false)
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
    const dest = new ClickHouseDestination({ db, schema: db.schema })
    await dest.init([])
    expect((await dest.acquireLease('prod', 'someone-else', 60_000)).acquired).toBe(true)
    await expect(holder.tick()).rejects.toBeInstanceOf(LeaseHeldError)
    await dest.releaseLease('prod', 'someone-else')
    expect((await holder.tick()).processed).toBe(1)
    expect(await dest.getLease('prod')).toBeNull()
  })

  it('listen() waits for the lease, takes over when it is released, and gives it back on stop', async () => {
    fake.createUser()
    const dest = new ClickHouseDestination({ db, schema: db.schema })
    await dest.init([])
    await dest.acquireLease('prod', 'old-process', 60_000)
    const engine = makeEngine(db, fake, TIER1, { pollIntervalMs: 20 })
    await engine.init()
    const listening = engine.listen()
    await new Promise((r) => setTimeout(r, 200))
    expect(await count(db, 'users')).toBe(0)
    await dest.releaseLease('prod', 'old-process')
    const deadline = Date.now() + 10_000
    while (Date.now() < deadline && (await count(db, 'users')) < 1) await new Promise((r) => setTimeout(r, 50))
    expect(await count(db, 'users')).toBe(1)
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

  it('erases a subject from the mirror, the sign-in log and the event archive', async () => {
    const user = fake.createUser({ email: 'forget@example.com' })
    fake.emitAuthAttempt(user['id'] as string, true)
    const engine = makeEngine(db, fake)
    await engine.backfill()
    await engine.tick()
    const removed = await engine.redact(user['id'] as string)
    expect(removed).toContainEqual({ table: 'users', rows: 1 })
    expect(removed).toContainEqual({ table: 'auth_attempts', rows: 1 })
    expect(removed.find((r) => r.table === 'events')!.rows).toBeGreaterThan(0)
    expect(await count(db, 'users', false)).toBe(0)
    const events = await db.query<{ n: string }>(
      `SELECT toString(count()) AS n FROM \`${db.schema}\`.events WHERE \`_raw\` LIKE {p:String}`,
      { p: `%${user['id']}%` },
    )
    expect(Number(events[0]!.n)).toBe(0)
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

    expect(await engine.tick()).toMatchObject({ processed: 0, rejected: 0 })
    expect(await count(db, '_rejected')).toBe(0)
    expect(lines.find((l) => l.level === 'warn' && l.fields['eventId'] === badEvent.id)?.msg).toMatch(
      /quarantined if it fails again/,
    )

    expect(await engine.tick()).toMatchObject({ processed: 3, rejected: 1 })
    const status = await engine.status()
    expect(status['prod']!.cursor).toBe(fake.events[fake.events.length - 1]!.id)
    expect(status['prod']!.rejected).toBe(1)
    expect(await emails(db)).toEqual(['good1@example.com', 'good2@example.com'])
    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({
      eventId: badEvent.id,
      eventType: 'user.created',
      objectId: bad['id'],
      raw: badEvent,
    })
    expect(rejected[0]!.error).toMatch(/Cannot parse string/)
    expect((await engine.tick()).processed).toBe(0)
    expect(await count(db, 'events', false)).toBe(3)
  })

  it('replays a quarantined event through the normal path once the cause is fixed', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = bad['id'] as string
    await engine.tick()
    await engine.tick()
    expect((await engine.status())['prod']!.rejected).toBe(1)
    destination.fault = () => Object.assign(new Error('still broken'), { code: '36', type: 'BAD_ARGUMENTS' })
    const again = await engine.replayRejected()
    expect(again.replayed).toBe(0)
    expect(again.stillRejected).toEqual([{ eventId: eventFor(bad['id'] as string).id, error: 'Error: still broken' }])
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count(db, '_rejected')).toBe(0)
    expect(await emails(db)).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('never quarantines on an error the destination classifies as transient', async () => {
    fake.createUser({ email: 'a@example.com' })
    const { engine, destination } = makeFaulty()
    destination.failAll = true
    destination.fault = () =>
      Object.assign(new Error('Keeper session expired'), { code: '999', type: 'KEEPER_EXCEPTION' })
    for (let i = 0; i < 3; i++) expect((await engine.tick()).processed).toBe(0)
    expect(await count(db, '_rejected')).toBe(0)
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
    destination.poison = readd.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 4, rejected: 1 })
    expect(await count(db, 'group_memberships')).toBe(1)
    expect(await count(db, 'group_memberships_live')).toBe(0)
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count(db, 'group_memberships_live')).toBe(0)
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
    destination.poison = removal.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 3, rejected: 1 })
    expect(await count(db, 'group_memberships_live')).toBe(1)
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count(db, 'group_memberships_live')).toBe(1)
    push('group.member_removed', at(3_000))
    expect(await engine.tick()).toMatchObject({ processed: 1, rejected: 0 })
    expect(await count(db, 'group_memberships_live')).toBe(0)
  })

  it('quarantines an event whose own timestamp cannot be stored, with no fault injected', async () => {
    const bad = fake.createUser({ email: 'bad@example.com' })
    const badEvent = eventFor(bad['id'] as string)
    badEvent.created_at = '2026-13-01T00:00:00Z'
    fake.createUser({ email: 'good@example.com' })
    const { engine } = makeFaulty()
    expect((await engine.tick()).processed).toBe(0)
    expect(await engine.tick()).toMatchObject({ processed: 2, rejected: 1 })
    const rejected = (await engine.rejected())['prod']!
    expect(rejected[0]).toMatchObject({ eventId: badEvent.id, createdAt: '2026-13-01T00:00:00Z' })
    expect(rejected[0]!.error).toMatch(/invalid updatedAt timestamp/)
    expect(await emails(db)).toEqual(['good@example.com'])
  })

  it('is scrubbed by redact like the event archive', async () => {
    fake.createUser({ email: 'keep@example.com' })
    const subject = fake.createUser({ email: 'forget@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = subject['id'] as string
    await engine.tick()
    await engine.tick()
    expect(await count(db, '_rejected')).toBe(1)
    const removed = await engine.redact(subject['id'] as string)
    expect(removed).toContainEqual({ table: '_rejected', rows: 1 })
    expect(await count(db, '_rejected')).toBe(0)
  })
})
