/**
 * Whole-engine tests: backfill, tail, drift and repair, driven against the
 * fake WorkOS API and a real MongoDB.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  SyncEngine,
  WorkOSClient,
  LeaseHeldError,
  StateStore,
  silentLogger,
  type Destination,
} from '@workos/sync-engine'
import { MongoNetworkError } from 'mongodb'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { FakeWorkOS, resetIds, id } from '../../sync-engine/test/helpers/fake-workos.js'
import { MongoDestination } from '../src/destination.js'
import type { MirrorDoc } from '../src/db/upsert.js'
import type { SyncDoc } from '../src/db/lease.js'
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
  t: TestDb,
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
      destination: new MongoDestination({ client: t.client, database: t.name }),
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

const coll = (t: TestDb, stream: string) => t.db.collection<MirrorDoc>(`workos_${stream}`)
const count = (t: TestDb, stream: string, filter = {}) => coll(t, stream).countDocuments(filter)
const raws = async (t: TestDb, stream: string, filter = {}, sort: Record<string, 1 | -1> = { '_raw.id': 1 }) =>
  (await coll(t, stream).find(filter).sort(sort).toArray()).map((d) => d._raw)
const emails = async (t: TestDb) => (await raws(t, 'users', {}, { '_raw.email': 1 })).map((r) => r['email'] as string)

describe('sync engine', () => {
  let t: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await t.close()
  })

  it('backfills existing state, including the field events omit', async () => {
    const org = fake.createOrganization({ name: 'Acme' })
    const user = fake.createUser({ email: 'ada@acme.com' })
    fake.createMembership(user['id'] as string, org['id'] as string, 'admin')

    const engine = makeEngine(t, fake)
    await engine.backfill()

    expect(await count(t, 'users')).toBe(1)
    expect(await count(t, 'organizations')).toBe(1)
    expect(await count(t, 'organization_memberships')).toBe(1)

    // Every listing the backfill made says which phase it was.
    const listings = fake.requestLog
      .map((path, i) => [path, fake.userAgents[i]!] as const)
      .filter(([path]) => !path.startsWith('/events'))
    expect(listings.length).toBeGreaterThan(0)
    expect(listings.every(([, ua]) => ua.endsWith('(phase=backfill)'))).toBe(true)

    const [u] = await raws(t, 'users')
    expect(u!['email']).toBe('ada@acme.com')
    expect(u!['external_id']).toMatch(/^ext_/)
    expect(u!['metadata']).toEqual({ plan: 'pro' })

    const [o] = await raws(t, 'organizations')
    // Deprecated fields are left out of events; only the REST object carries it.
    expect(o!['allow_profiles_outside_organization']).toBe(false)

    const [m] = await raws(t, 'organization_memberships')
    expect((m!['role'] as { slug: string }).slug).toBe('admin')
    // Every document carries the bookkeeping fields and the environment label.
    const doc = (await coll(t, 'users').findOne({}))!
    expect(doc._env).toBe('prod')
    expect(doc._updated_at).toBeInstanceOf(Date)
    expect(doc._synced_at).toBeInstanceOf(Date)
  })

  it('captures the stream position before backfilling so nothing is missed', async () => {
    fake.createUser()
    const engine = makeEngine(t, fake)
    await engine.backfill()

    const before = await engine.status()
    expect(before['prod']!.started).toBe(true)
    expect(before['prod']!.rangeStart).not.toBeNull()
    expect(before['prod']!.cursor).toBeNull()

    await engine.tick()
    const after = await engine.status()
    expect(after['prod']!.cursor).not.toBeNull()
    expect(after['prod']!.rangeStart).toBeNull()
  })

  it('stops at the page budget and resumes from the checkpoint', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(t, fake, ['users', 'invitations'])

    const first = await engine.backfill({ maxPages: 2 })
    expect(first.find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })
    expect(await count(t, 'users')).toBe(200)
    expect(first.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })

    const before = fake.requestLog.length
    const second = await engine.backfill({ resume: true, maxPages: 10 })
    expect(second.find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
    expect(second.find((r) => r.stream === 'invitations')).toMatchObject({ pages: 1, complete: true })
    expect(await count(t, 'users')).toBe(250)
    const users = fake.requestLog.slice(before).filter((r) => r.startsWith('/user_management/users'))
    expect(users).toHaveLength(1)
    expect(users[0]).toContain('after=')
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

  it('holds a child stream back while its parent is incomplete, so a resume fetches the missing children', async () => {
    const orgA = fake.createOrganization()
    const orgB = fake.createOrganization()
    fake.createMembership(fake.createUser()['id'] as string, orgA['id'] as string)
    fake.createMembership(fake.createUser()['id'] as string, orgB['id'] as string)
    const destination = new FaultyDestination({ client: t.client, database: t.name })
    destination.poison = orgB['id'] as string
    const { logger, lines } = captureLogger()
    const engine = faultyEngine(destination, ['users', 'organizations', 'organization_memberships'], logger)

    const first = await engine.backfill()
    expect(first.find((r) => r.stream === 'organizations')).toMatchObject({ complete: false })
    const held = first.find((r) => r.stream === 'organization_memberships')!
    expect(held).toMatchObject({ pages: 0, complete: false })
    expect(held.skippedReason).toContain('organizations')
    expect(lines.map((l) => l.msg)).not.toContain('backfill paused: page budget spent; resume to continue')

    destination.poison = null
    const second = await engine.backfill({ resume: true })
    expect(second.find((r) => r.stream === 'organizations')).toMatchObject({ complete: true })
    expect(second.find((r) => r.stream === 'organization_memberships')).toMatchObject({ complete: true })
    expect(await count(t, 'organization_memberships')).toBe(2)
  })

  it('applies live events after backfill', async () => {
    const user = fake.createUser({ email: 'before@example.com' })
    const engine = makeEngine(t, fake)
    await engine.backfill()

    fake.updateUser(user['id'] as string, { email: 'after@example.com' })
    const org = fake.createOrganization({ name: 'New Co' })
    await engine.tick()

    expect(await emails(t)).toEqual(['after@example.com'])
    expect(await count(t, 'organizations')).toBe(1)
    expect((await raws(t, 'organizations'))[0]!['name']).toBe('New Co')
    expect(org['id']).toBeTruthy()
  })

  it('does not drop backfilled fields when an event arrives', async () => {
    // Events leave out deprecated fields, allow_profiles_outside_organization
    // here. The event write has to merge into the backfilled document, not replace it.
    const org = fake.createOrganization({ name: 'Acme' })
    const engine = makeEngine(t, fake)
    await engine.backfill()
    await engine.tick()

    fake.updateOrganization(org['id'] as string, { name: 'Acme Corp' })
    await engine.tick()

    const [row] = await raws(t, 'organizations')
    expect(row!['name']).toBe('Acme Corp')
    expect(row!['allow_profiles_outside_organization']).toBe(false)
  })

  it('is idempotent when the same events are delivered twice', async () => {
    fake = new FakeWorkOS({ duplicateEvents: true })
    fake.createUser()
    fake.createOrganization()

    const engine = makeEngine(t, fake)
    await engine.tick()
    await engine.tick()

    expect(await count(t, 'users')).toBe(1)
    expect(await count(t, 'organizations')).toBe(1)
  })

  it('converges regardless of the order events are applied in', async () => {
    const user = fake.createUser({ email: 'v1@example.com' })
    fake.updateUser(user['id'] as string, { email: 'v2@example.com' })
    fake.updateUser(user['id'] as string, { email: 'v3@example.com' })

    // Serve the batch reversed, as out-of-order delivery would.
    const shuffled = new FakeWorkOS({ shuffleEvents: true })
    shuffled.users = fake.users
    shuffled.events = fake.events

    const engine = makeEngine(t, shuffled)
    await engine.tick()

    // The staleness gate means the newest state wins whatever the arrival order.
    expect(await emails(t)).toEqual(['v3@example.com'])
  })

  it('resolves events that share a timestamp as last-writer-wins', async () => {
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

    const engine = makeEngine(t, fake)
    await engine.tick()

    expect(await emails(t)).toEqual(['tie-b@example.com'])
  })

  it('tombstones a deleted user: the live view drops it while the collection keeps it', async () => {
    const gone = fake.createUser({ email: 'gone@example.com' })
    fake.createUser({ email: 'stays@example.com' })
    const engine = makeEngine(t, fake)
    await engine.backfill()
    await engine.tick()
    expect(await count(t, 'users')).toBe(2)
    expect(await count(t, 'users_live')).toBe(2)

    fake.deleteUser(gone['id'] as string)
    await engine.tick()

    // No `_deleted_at: null` needed: that is the point of the view.
    expect(await count(t, 'users')).toBe(2)
    expect(await count(t, 'users', { _deleted_at: null })).toBe(1)
    expect(await count(t, 'users_live')).toBe(1)
    expect((await raws(t, 'users_live'))[0]!['email']).toBe('stays@example.com')
  })

  it('archives subscribed events that have no mirror collection, instead of throwing', async () => {
    fake.createUser()
    fake.emitUnknown()

    const engine = makeEngine(t, fake, TIER1, {
      eventTypes: ['user.created', 'user.updated', 'user.deleted', 'quantum_widget.entangled'],
    })
    await engine.tick()

    const names = (await raws(t, 'events')).map((r) => r['event'])
    expect(names).toContain('user.created')
    expect(names).toContain('quantum_widget.entangled')
    // It was archived only — no mirror document was invented for it.
    expect(await count(t, 'users')).toBe(1)
  })

  it('subscribes only to the event types its selected streams declare', async () => {
    const usersOnly = makeEngine(t, fake, ['users', 'events'])
    expect(usersOnly.eventTypes).toEqual(['user.created', 'user.deleted', 'user.updated'])

    const withAuth = makeEngine(t, fake, TIER1)
    expect(withAuth.eventTypes).toContain('authentication.password_failed')
    expect(withAuth.eventTypes).toContain('organization_membership.created')
    expect(withAuth.eventTypes).not.toContain('dsync.user.created')
  })

  it('records sign-in attempts that exist nowhere else in the API', async () => {
    const user = fake.createUser()
    fake.emitAuthAttempt(user['id'] as string, true)
    fake.emitAuthAttempt(user['id'] as string, false)
    fake.emitAuthAttempt(user['id'] as string, false)

    const engine = makeEngine(t, fake)
    await engine.tick()

    const byStatus = await coll(t, 'auth_attempts')
      .aggregate<{ _id: string; n: number }>([
        { $group: { _id: '$_raw.status', n: { $sum: 1 } } },
        { $sort: { _id: 1 } },
      ])
      .toArray()
    expect(byStatus).toEqual([
      { _id: 'failed', n: 2 },
      { _id: 'succeeded', n: 1 },
    ])
    const failed = (await coll(t, 'auth_attempts').findOne({ '_raw.status': 'failed' }))!
    expect((failed._raw['error'] as { code: string }).code).toBe('invalid_credentials')
  })

  it('resumes from the stored cursor rather than replaying', async () => {
    fake.createUser()
    const engine = makeEngine(t, fake)
    const first = await engine.tick()
    expect(first.processed).toBe(1)

    const second = await engine.tick()
    expect(second.processed).toBe(0)

    fake.createUser()
    const third = await engine.tick()
    expect(third.processed).toBe(1)
  })

  it('keeps two environments apart in one database', async () => {
    const staging = new FakeWorkOS()
    staging.createUser({ email: 'staging@example.com' })
    fake.createUser({ email: 'prod@example.com' })

    const engine = new SyncEngine(
      {
        destination: new MongoDestination({ client: t.client, database: t.name }),
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

    const docs = await coll(t, 'users').find().sort({ _env: 1 }).toArray()
    expect(docs.map((d) => [d._env, d._raw['email']])).toEqual([
      ['prod', 'prod@example.com'],
      ['staging', 'staging@example.com'],
    ])

    // Cursors are per environment and are not comparable across them.
    const status = await engine.status()
    expect(Object.keys(status)).toEqual(['prod', 'staging'])
  })

  it('always includes the events archive, whatever streams are selected', async () => {
    fake.createUser()
    const engine = makeEngine(t, fake, ['users'])
    expect(engine.streams.map((s) => s.name)).toEqual(['users', 'events'])

    const r = await engine.tick()
    expect(r.processed).toBe(1)
    expect(await count(t, 'events')).toBe(1)
  })

  it('rejects a stream name it does not know', () => {
    expect(() => makeEngine(t, fake, ['users', 'customers'])).toThrow(/unknown stream: customers/)
  })

  it('turns an extra column into an index on _raw without touching the catalog', async () => {
    fake.createUser({ metadata: { plan: 'pro', region: 'eu' } })
    const engine = makeEngine(t, fake, TIER1, {
      extraColumns: {
        users: [{ name: 'region', type: 'text', path: ['metadata', 'region'], index: true }],
      },
    })
    await engine.backfill()

    const indexes = await coll(t, 'users').listIndexes().toArray()
    expect(indexes.map((i) => i.name)).toContain('_env_1__raw.metadata.region_1')
    // There is no projected field to keep in step: the value is read where it lives.
    expect(await count(t, 'users', { _env: 'prod', '_raw.metadata.region': 'eu' })).toBe(1)
    // Every index leads with _env, so a query that names the environment is served by it.
    const plan = await coll(t, 'users').find({ _env: 'prod', '_raw.metadata.region': 'eu' }).explain('queryPlanner')
    expect(JSON.stringify(plan['queryPlanner']['winningPlan'])).toContain('_env_1__raw.metadata.region_1')
  })

  it('keeps the hydrated fields when the create and a later update share a tick', async () => {
    const engine = makeEngine(t, fake, TIER1, { hydrate: { organizations: 'on_create' } })
    await engine.backfill()

    const org = fake.createOrganization({ name: 'Both' })
    fake.updateOrganization(org['id'] as string, { name: 'Both Corp' })
    expect((await engine.tick()).processed).toBe(2)

    const [row] = await raws(t, 'organizations', { '_raw.id': org['id'] })
    expect(row).toMatchObject({ name: 'Both Corp', allow_profiles_outside_organization: false })
  })

  it('hydrates on create so a new organization has its full payload immediately', async () => {
    const engine = makeEngine(t, fake, TIER1, { hydrate: { organizations: 'on_create' } })
    await engine.backfill()

    fake.createOrganization({ name: 'Fresh' })
    await engine.tick()

    const [row] = await raws(t, 'organizations', { '_raw.name': 'Fresh' })
    expect(row!['allow_profiles_outside_organization']).toBe(false)
    expect(fake.userAgents.at(-1)).toMatch(/\(phase=hydrate\)$/)

    // Without hydration the field the event omits stays absent until a refresh.
    const plain = makeEngine(t, fake)
    fake.createOrganization({ name: 'Raw' })
    await plain.tick()
    const [raw] = await raws(t, 'organizations', { '_raw.name': 'Raw' })
    expect(raw!['allow_profiles_outside_organization']).toBeUndefined()
  })

  it('looks up mirrored identity data next to application collections', async () => {
    // The point of the whole exercise.
    const org = fake.createOrganization({ name: 'Acme' })
    const u1 = fake.createUser({ email: 'active@acme.com' })
    const u2 = fake.createUser({ email: 'dormant@acme.com' })
    fake.createMembership(u1['id'] as string, org['id'] as string)
    fake.createMembership(u2['id'] as string, org['id'] as string)

    const engine = makeEngine(t, fake)
    await engine.backfill()
    await engine.tick()
    fake.emitAuthAttempt(u1['id'] as string, true)
    await engine.tick()

    await t.db.collection('app_events').insertOne({ user_id: u1['id'], occurred_at: new Date() })

    const rows = await t.db
      .collection('workos_organization_memberships_live')
      .aggregate<{ email: string; sign_ins: number; app_events: number }>([
        { $match: { _env: 'prod', '_raw.organization_id': org['id'] } },
        {
          $lookup: {
            from: 'workos_users_live',
            let: { uid: '$_raw.user_id', env: '$_env' },
            pipeline: [{ $match: { $expr: { $and: [{ $eq: ['$_raw.id', '$$uid'] }, { $eq: ['$_env', '$$env'] }] } } }],
            as: 'user',
          },
        },
        { $unwind: '$user' },
        {
          $lookup: {
            from: 'workos_auth_attempts',
            localField: '_raw.user_id',
            foreignField: '_raw.user_id',
            as: 'attempts',
          },
        },
        { $lookup: { from: 'app_events', localField: '_raw.user_id', foreignField: 'user_id', as: 'app' } },
        {
          $project: {
            _id: 0,
            email: '$user._raw.email',
            sign_ins: { $size: '$attempts' },
            app_events: { $size: '$app' },
          },
        },
        { $sort: { email: 1 } },
      ])
      .toArray()
    expect(rows).toEqual([
      { email: 'active@acme.com', sign_ins: 1, app_events: 1 },
      { email: 'dormant@acme.com', sign_ins: 0, app_events: 0 },
    ])
  })
})

describe('reconciliation', () => {
  let t: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await t.close()
  })

  it('reports a clean mirror as matching with no drift', async () => {
    fake.createUser()
    fake.createOrganization()
    const engine = makeEngine(t, fake)
    await engine.backfill()

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([])
    expect(users.remoteOnly).toEqual([])
    expect(users.matched).toBe(1)
    // Nothing staged is left behind.
    expect(await t.db.listCollections({ name: /_reconcile_/ }).toArray()).toEqual([])
  })

  it('detects a missed delete, which nothing else can', async () => {
    const user = fake.createUser()
    const engine = makeEngine(t, fake)
    await engine.backfill()

    fake.users.delete(user['id'] as string)

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([user['id']])
    expect(users.remoteOnly).toEqual([])
  })

  it('detects a missed create', async () => {
    fake.createUser()
    const engine = makeEngine(t, fake)
    await engine.backfill()

    const missed = fake.createUser({ email: 'missed@example.com' })
    fake.events = []

    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.remoteOnly).toEqual([missed['id']])
  })

  it('repairs drift in both directions', async () => {
    const stale = fake.createUser({ email: 'gone@example.com' })
    const engine = makeEngine(t, fake)
    await engine.backfill()

    fake.users.delete(stale['id'] as string)
    const added = fake.createUser({ email: 'added@example.com' })
    fake.events = []

    await engine.reconcile({ repair: true })

    expect((await raws(t, 'users', { _deleted_at: null })).map((r) => r['email'])).toEqual(['added@example.com'])
    expect((await raws(t, 'users', { _deleted_at: { $type: 'date' } }))[0]!['email']).toBe('gone@example.com')
    expect(added['id']).toBeTruthy()

    // A second pass finds nothing left to fix.
    const { diffs } = await engine.reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users.localOnly).toEqual([])
    expect(users.remoteOnly).toEqual([])
  })

  it('says plainly when a stream cannot be reconciled', async () => {
    const engine = makeEngine(t, fake)
    await engine.backfill()
    const { diffs } = await engine.reconcile()

    const events = diffs['prod']!.find((d) => d.stream === 'events')!
    expect(events.skippedReason).toMatch(/append-only/)
    const domains = diffs['prod']!.find((d) => d.stream === 'organization_domains')!
    expect(domains.skippedReason).toMatch(/no list endpoint/)
  })

  it('reports freshness and lag per stream', async () => {
    fake.createUser()
    const engine = makeEngine(t, fake)
    await engine.backfill()

    const status = await engine.status()
    const users = status['prod']!.streams.find((s) => s.stream === 'users')!
    expect(users.rows).toBe(1)
    expect(users.lastSyncedAt).toBeTruthy()
    expect(users.lagSeconds).not.toBeNull()
    expect(status['prod']!.rejected).toBe(0)
    expect(status['prod']!.lease).toBeNull()
  })

  it('refreshes fields that event payloads omit', async () => {
    const org = fake.createOrganization()
    const engine = makeEngine(t, fake)
    await engine.tick()

    let [row] = await raws(t, 'organizations')
    expect(row!['allow_profiles_outside_organization']).toBeUndefined()

    await engine.refresh()
    ;[row] = await raws(t, 'organizations')
    expect(row!['allow_profiles_outside_organization']).toBe(false)
    expect(org['id']).toBeTruthy()
  })
})

describe('single-writer lease', () => {
  let t: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await t.close()
  })

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
  const until = async (ok: () => Promise<boolean>, timeoutMs = 5_000) => {
    const deadline = Date.now() + timeoutMs
    while (!(await ok())) {
      if (Date.now() > deadline) throw new Error('condition not met in time')
      await sleep(10)
    }
  }
  const expireLease = () =>
    t.db
      .collection<SyncDoc>('workos__sync')
      .updateOne({ sync_id: 'default', env: 'prod' }, { $set: { 'lease.expires_at': new Date(Date.now() - 1000) } })

  it('tick() refuses to run while another process holds the lease', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    expect((await other.acquireLease('prod', 'someone-else', 60_000)).acquired).toBe(true)

    const engine = makeEngine(t, fake)
    await expect(engine.tick()).rejects.toThrow(LeaseHeldError)
    expect(await count(t, 'users')).toBe(0)

    await other.releaseLease('prod', 'someone-else')
    expect((await engine.tick()).processed).toBe(1)
    // A tick gives the lease back when it is done.
    expect(await other.getLease('prod')).toBeNull()
  })

  it('listen() waits for the lease, takes over when it is released, and gives it back on stop', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    await other.acquireLease('prod', 'someone-else', 60_000)

    const engine = makeEngine(t, fake, TIER1, { pollIntervalMs: 10 })
    const listening = engine.listen()
    await sleep(100)
    expect(await count(t, 'users')).toBe(0)
    expect(await other.getLease('prod')).toMatchObject({ holder: 'someone-else' })

    await other.releaseLease('prod', 'someone-else')
    await until(async () => (await count(t, 'users')) === 1)
    const lease = await other.getLease('prod')
    expect(lease).not.toBeNull()
    expect(lease!.holder).not.toBe('someone-else')

    engine.stop()
    await listening
    expect(await other.getLease('prod')).toBeNull()
  })

  it('does not checkpoint after losing the lease mid-poll', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    let stalled = false
    // The poll stalls past the TTL while fetching and a replacement takes
    // over in the meantime. The database clock is moved rather than the test.
    const engine = makeEngine(t, fake, TIER1, {}, async () => {
      if (stalled) return
      stalled = true
      await expireLease()
      await other.acquireLease('prod', 'replacement', 60_000)
    })
    await engine.init()
    const r = await engine.tick()

    // The batch it had already fetched is applied, which is idempotent, but
    // the cursor is left where the replacement will look for it.
    expect(await count(t, 'users')).toBe(1)
    expect(r.processed).toBe(0)
    const status = await engine.status()
    expect(status['prod']!.cursor).toBeNull()
    expect(status['prod']!.lastPolledAt).toBeNull()
    expect(await other.getLease('prod')).toMatchObject({ holder: 'replacement' })
  })

  it('keeps a slow standalone tick covered so it can still checkpoint', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    let slowed = false
    const engine = makeEngine(t, fake, TIER1, { leaseTtlMs: 150 }, async () => {
      if (slowed) return
      slowed = true
      await sleep(400)
    })
    await engine.init()
    const r = await engine.tick()

    expect(r.processed).toBe(1)
    expect((await engine.status())['prod']!.cursor).not.toBeNull()
    expect(await other.getLease('prod')).toBeNull()
  })

  it('does not overwrite a position that moved under it, even while it holds the lease', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    let moved = false
    const engine = makeEngine(t, fake, TIER1, {}, async () => {
      if (moved) return
      moved = true
      await new StateStore(other as Destination).setTail('prod', { cursor: 'event_from_elsewhere' })
    })
    await engine.init()
    const r = await engine.tick()

    expect(r.processed).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBe('event_from_elsewhere')
    expect(await other.getLease('prod')).toBeNull()
  })
})

describe('redaction', () => {
  let t: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await t.close()
  })

  it('erases a subject from the mirror and the event archive', async () => {
    const user = fake.createUser({ email: 'forget@example.com' })
    fake.emitAuthAttempt(user['id'] as string, true)
    const engine = makeEngine(t, fake)
    await engine.backfill()
    await engine.tick()

    const removed = await engine.redact(user['id'] as string)
    expect(removed.length).toBeGreaterThan(0)
    expect(await count(t, 'users')).toBe(0)
    expect(await count(t, 'auth_attempts')).toBe(0)
    // The archived payloads carry the same personal data and must go too.
    const leftovers = await coll(t, 'events').find().toArray()
    expect(leftovers.some((d) => JSON.stringify(d).includes(user['id'] as string))).toBe(false)
  })
})

describe('poison events', () => {
  let t: TestDb
  let fake: FakeWorkOS

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
  })
  afterEach(async () => {
    await t.close()
  })

  const rejectedCount = () => t.db.collection('workos__rejected').countDocuments()
  const eventFor = (objectId: string) => fake.events.find((e) => e.data['id'] === objectId)!

  /** The engine over a FaultyDestination, with a capturing logger and no client retries. */
  function makeFaulty(extra: Record<string, unknown> = {}, fetchImpl = fake.fetchImpl) {
    const destination = new FaultyDestination({ client: t.client, database: t.name })
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

    // Poll 1: first sighting. The tick fails, the cursor stays put, and the
    // suspect is only remembered in memory.
    const first = await engine.tick()
    expect(first).toMatchObject({ processed: 0, rejected: 0 })
    let status = await engine.status()
    expect(status['prod']!.cursor).toBeNull()
    expect(status['prod']!.rejected).toBe(0)
    expect(await rejectedCount()).toBe(0)
    const warned = lines.find((l) => l.level === 'warn' && l.fields['eventId'] === badEvent.id)
    expect(warned?.msg).toMatch(/quarantined if it fails again/)
    expect(warned?.fields['error']).toMatch(/Document failed validation/)

    // Poll 2: the same batch, the same failure. The good events are written,
    // the bad one is set aside, and the cursor moves past all three.
    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 3, rejected: 1 })
    status = await engine.status()
    expect(status['prod']!.cursor).toBe(fake.events[fake.events.length - 1]!.id)
    expect(status['prod']!.rejected).toBe(1)
    expect(await emails(t)).toEqual(['good1@example.com', 'good2@example.com'])

    const rejected = (await engine.rejected())['prod']!
    expect(rejected).toHaveLength(1)
    expect(rejected[0]).toMatchObject({
      eventId: badEvent.id,
      eventType: 'user.created',
      objectId: bad['id'],
      createdAt: badEvent.created_at,
      raw: badEvent,
    })
    expect(rejected[0]!.error).toMatch(/Document failed validation/)
    expect(rejected[0]!.rejectedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/)
    const logged = lines.find((l) => l.level === 'error' && l.msg.includes('quarantined'))
    expect(logged?.fields).toMatchObject({ eventId: badEvent.id, eventType: 'user.created', objectId: bad['id'] })

    // The tail is not stalled: the next poll is a no-op rather than a retry.
    expect((await engine.tick()).processed).toBe(0)
    // The archive still has every event, including the one set aside.
    expect(await count(t, 'events')).toBe(3)
  })

  it('replays a quarantined event through the normal path once the cause is fixed', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = bad['id'] as string
    await engine.tick()
    await engine.tick()
    expect((await engine.status())['prod']!.rejected).toBe(1)

    // Still broken: the document stays, with its error refreshed.
    destination.fault = () => Object.assign(new Error('still broken'), { code: 121 })
    const again = await engine.replayRejected()
    expect(again.replayed).toBe(0)
    expect(again.stillRejected).toEqual([{ eventId: eventFor(bad['id'] as string).id, error: 'Error: still broken' }])
    expect((await engine.rejected())['prod']![0]!.error).toBe('Error: still broken')

    // Fixed: the event is applied and forgotten.
    destination.poison = null
    const fixed = await engine.replayRejected()
    expect(fixed).toEqual({ replayed: 1, stillRejected: [] })
    expect(await rejectedCount()).toBe(0)
    expect((await engine.status())['prod']!.rejected).toBe(0)
    expect(await emails(t)).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('never quarantines on an error the destination classifies as transient', async () => {
    const u1 = fake.createUser()
    fake.createUser()
    const { engine, destination, lines } = makeFaulty()
    destination.poison = u1['id'] as string

    // A dead socket, a write conflict, then a socket error: all the database
    // having a bad moment, and the tick must retry, however often it happens.
    destination.fault = () => new MongoNetworkError('connection closed')
    await engine.tick()
    await engine.tick()
    destination.fault = () => Object.assign(new Error('WriteConflict'), { code: 112 })
    await engine.tick()
    destination.fault = () => Object.assign(new Error('read ECONNRESET'), { code: 'ECONNRESET' })
    await engine.tick()

    expect(await rejectedCount()).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()
    expect(lines.filter((l) => l.msg === 'tick failed')).toHaveLength(4)
    expect(lines.some((l) => l.level === 'warn')).toBe(false)
    // It failed fast each time, without ever isolating event by event.
    expect(destination.faults).toBe(4)

    // Once the database recovers the batch applies in full.
    destination.poison = null
    expect((await engine.tick()).processed).toBe(2)
    expect(await count(t, 'users')).toBe(2)
  })

  it('treats a whole batch failing as an outage, not a batch of poison events', async () => {
    fake.createUser()
    fake.createUser()
    fake.createUser()
    const { engine, destination, lines } = makeFaulty()
    // An unclassified error, so only the all-fail rule stands in the way.
    destination.fault = () => new Error('ns not found: workos_users')
    destination.failAll = true

    await engine.tick()
    await engine.tick()
    await engine.tick()

    expect(await rejectedCount()).toBe(0)
    expect((await engine.status())['prod']!.cursor).toBeNull()
    expect(lines.filter((l) => l.msg === 'tick failed')).toHaveLength(3)
    expect(lines.some((l) => l.level === 'warn')).toBe(false)
  })

  it('forgets a suspect that succeeds on its second poll', async () => {
    const flaky = fake.createUser({ email: 'flaky@example.com' })
    fake.createUser({ email: 'steady@example.com' })
    const { engine, destination } = makeFaulty()
    // An unclassified error that happens once: exactly what two sightings is for.
    destination.fault = () => new Error('connection closed unexpectedly')
    destination.poison = flaky['id'] as string
    await engine.tick()
    destination.poison = null

    const second = await engine.tick()
    expect(second).toMatchObject({ processed: 2, rejected: 0 })
    expect(await rejectedCount()).toBe(0)
    expect(await emails(t)).toEqual(['flaky@example.com', 'steady@example.com'])
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
    const live = () => count(t, 'group_memberships', { _deleted_at: null })

    // The archive write for the re-add fails, so it alone is set aside; the
    // removal that followed it is applied, and the membership ends up removed.
    destination.poison = readd.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 4, rejected: 1 })
    expect(await count(t, 'group_memberships')).toBe(1)
    expect(await live()).toBe(0)

    // Replayed now, the re-add is older than the removal it would undo. It is
    // a no-op, like any stale event, and is forgotten like any replayed one.
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(0)
    expect(await rejectedCount()).toBe(0)
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
    const live = () => count(t, 'group_memberships', { _deleted_at: null })

    destination.poison = removal.id
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ processed: 3, rejected: 1 })
    expect(await live()).toBe(1)

    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await live()).toBe(1)
    expect(await rejectedCount()).toBe(0)

    // A removal that really is newer than the re-add still lands.
    push('group.member_removed', at(3_000))
    expect(await engine.tick()).toMatchObject({ processed: 1, rejected: 0 })
    expect(await live()).toBe(0)
  })

  it('quarantines an event whose own timestamp is not an instant, with no fault injected', async () => {
    // A `created_at` that has the ISO-8601 shape but is not a real instant
    // fails the archive write's parse. The quarantine document stores the
    // same value, and must not fail on it: that would leave the tail stuck on
    // the very event the quarantine exists to set aside.
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
    const stored = await t.db.collection('workos__rejected').findOne({ event_id: badEvent.id })
    expect(stored!['created_at']).toBeNull()
    expect(await emails(t)).toEqual(['good@example.com'])
    expect((await engine.status())['prod']!.cursor).not.toBeNull()
  })

  it('is scrubbed by redact like the event archive', async () => {
    fake.createUser({ email: 'keep@example.com' })
    const subject = fake.createUser({ email: 'forget@example.com' })
    const { engine, destination } = makeFaulty()
    destination.poison = subject['id'] as string
    await engine.tick()
    await engine.tick()
    expect(await rejectedCount()).toBe(1)

    const removed = await engine.redact(subject['id'] as string)
    expect(removed).toContainEqual({ table: '_rejected', rows: 1 })
    expect(await rejectedCount()).toBe(0)
  })
})
