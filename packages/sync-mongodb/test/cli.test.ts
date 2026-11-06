import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SyncEngine, WorkOSClient, silentLogger, parseExcludeStreams } from '@workos/sync-engine'
import { main } from '../src/cli.js'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { FaultyDestination } from './helpers/faulty.js'
import { MongoDestination } from '../src/destination.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'

describe('init --repair', () => {
  it('is in the help text, which names this destination', async () => {
    const out: string[] = []
    const spy = vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
    try {
      expect(await main(['help'])).toBe(0)
      expect(await main(['init', '--help'])).toBe(0)
    } finally {
      spy.mockRestore()
    }
    const help = out.join('')
    expect(help).toContain('init [--repair]')
    expect(help).toContain('workos-sync-mongodb')
    expect(help).toContain('--prefix')
    expect(help).toContain('WORKOS_SYNC_MONGODB_TLS_CA')
    expect(help).not.toContain('--print-sql')
    expect(help).not.toContain('Postgres')
  })
})

/** Every command below is run through `main` against a real server. */
describe('commands', () => {
  let t: TestDb
  let fake: FakeWorkOS
  let out: string[]
  const env = { ...process.env }

  beforeEach(async () => {
    t = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
    process.env['WORKOS_API_KEY'] = 'sk_test_1'
    delete process.env['DATABASE_URL']
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    process.env = { ...env }
    await t.close()
  })

  const lastJson = () => JSON.parse(out[out.length - 1]!) as Record<string, unknown>
  interface Diff {
    stream: string
    pages: number
    complete: boolean
    oversized?: boolean
    localOnly: string[]
    remoteOnly: string[]
    skippedReason?: string
  }
  const diffs = () => (lastJson() as { diffs: Record<string, Diff[]> }).diffs['prod']!
  const engineOver = (destination: MongoDestination, streams: string[]) =>
    new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams,
        hydrate: 'off',
        logger: silentLogger,
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

  it('init creates the schema from the environment and the flags, and is quiet when nothing changed', async () => {
    expect(
      await main(['init', '--database', t.name, '--streams', 'users', '--prefix', 'mirror_'], { db: t.client }),
    ).toBe(0)
    expect(out.join('')).toContain('schema ready')
    const names = (await t.db.listCollections().toArray())
      .map((c) => c.name)
      .filter((n) => !n.startsWith('system.'))
      .sort()
    expect(names).toEqual([
      'mirror__migrations',
      'mirror__rejected',
      'mirror__sync',
      'mirror_events',
      'mirror_users',
      'mirror_users_live',
    ])
    // The views it made are reported the first time, not the second.
    expect(out.some((o) => o.includes('mirror_users_live'))).toBe(true)
    out.length = 0
    expect(
      await main(['init', '--database', t.name, '--streams', 'users', '--prefix', 'mirror_'], { db: t.client }),
    ).toBe(0)
    expect(out.join('')).toBe('schema ready\n')
    // The environment works where the flags do.
    process.env['WORKOS_SYNC_DATABASE'] = t.name
    process.env['WORKOS_SYNC_PREFIX'] = 'other_'
    expect(await main(['init', '--streams', 'users'], { db: t.client })).toBe(0)
    expect(await t.db.listCollections({ name: 'other_users' }).toArray()).toHaveLength(1)
  })

  it('init --repair redefines a generated view whose pipeline changed, and plain init refuses to', async () => {
    expect(await main(['init', '--database', t.name, '--streams', 'users'], { db: t.client })).toBe(0)
    await t.db.command({
      collMod: 'workos_users_live',
      viewOn: 'workos_users',
      pipeline: [{ $match: { _env: 'prod' } }],
    })
    // The fingerprint is recorded, so a plain init does not look; a catalog change makes it look.
    await expect(
      main(['init', '--database', t.name, '--streams', 'users,organizations'], { db: t.client }),
    ).rejects.toThrow(/init --repair/)
    expect(
      await main(['init', '--repair', '--database', t.name, '--streams', 'users,organizations'], { db: t.client }),
    ).toBe(0)
    const info = (await t.db.listCollections({ name: 'workos_users_live' }, { nameOnly: false }).toArray())[0]!
    expect(info.options?.['pipeline']).toEqual([{ $match: { _deleted_at: null } }])
  })

  it('needs a connection string, a key, and a known stream', async () => {
    await expect(main(['init'])).rejects.toThrow(/DATABASE_URL/)
    delete process.env['WORKOS_API_KEY']
    await expect(main(['init', '--database', t.name], { db: t.client })).rejects.toThrow(/WORKOS_API_KEY/)
    process.env['WORKOS_API_KEY'] = 'sk_test_1'
    await expect(main(['init', '--database', t.name, '--streams', 'customers'], { db: t.client })).rejects.toThrow(
      /unknown stream: customers/,
    )
    await expect(main(['init', '--database', t.name, '--prefix', 'bad prefix'], { db: t.client })).rejects.toThrow(
      /invalid collection prefix/,
    )
  })

  it('streams lists the catalog without touching a database; an unknown command exits 1', async () => {
    expect(await main(['streams'])).toBe(0)
    const rows = JSON.parse(out.join('')) as { stream: string; default: string }[]
    expect(rows.find((r) => r.stream === 'users')).toMatchObject({ default: 'yes' })
    const err: string[] = []
    vi.spyOn(process.stderr, 'write').mockImplementation((chunk) => {
      err.push(String(chunk))
      return true
    })
    expect(await main(['frobnicate', '--database', t.name], { db: t.client })).toBe(1)
    expect(err.join('')).toContain('unknown command: frobnicate')
  })

  it('backfill, status and redact report what they did', async () => {
    const user = fake.createUser({ email: 'ada@example.com' })
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users'])
    expect(await main(['backfill'], { engine })).toBe(0)
    expect(
      (lastJson() as unknown as { stream: string; complete: boolean }[]).find((r) => r.stream === 'users'),
    ).toMatchObject({ complete: true })
    expect(await t.db.collection('workos_users').countDocuments()).toBe(1)

    expect(await main(['status'], { engine })).toBe(0)
    const status = lastJson() as Record<
      string,
      { started: boolean; rejected: number; streams: { stream: string; rows: number }[] }
    >
    expect(status['prod']!.started).toBe(true)
    expect(status['prod']!.rejected).toBe(0)
    expect(status['prod']!.streams.find((s) => s.stream === 'users')).toMatchObject({ rows: 1 })

    await expect(main(['redact'], { engine })).rejects.toThrow(/--user/)
    expect(await main(['redact', '--user', user['id'] as string], { engine })).toBe(0)
    expect(lastJson()).toEqual([{ table: 'users', rows: 1 }])
    expect(await t.db.collection('workos_users').countDocuments()).toBe(0)
  })

  it('tick exits 3 while a listener holds the lease', async () => {
    fake.createUser()
    const other = new MongoDestination({ client: t.client, database: t.name })
    await other.init([])
    await other.acquireLease('prod', 'listener', 60_000)
    const err: string[] = []
    vi.spyOn(process.stderr, 'write').mockImplementation((chunk) => {
      err.push(String(chunk))
      return true
    })
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users'])
    expect(await main(['tick'], { engine })).toBe(3)
    expect(err.join('')).toContain('listener')
    await other.releaseLease('prod', 'listener')
    expect(
      await main(['tick'], {
        engine: engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users']),
      }),
    ).toBe(0)
    expect(lastJson()).toMatchObject({ processed: 1 })
  })

  it('rejected exits 2 while quarantined events exist, and 0 once they replay', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const destination = new FaultyDestination({ client: t.client, database: t.name })
    destination.poison = bad['id'] as string
    const engine = engineOver(destination, ['users'])

    // Nothing quarantined yet: a clean exit, so a scheduled check stays quiet.
    expect(await main(['rejected'], { engine })).toBe(0)

    // Two polls: first sighting, then quarantine.
    await engine.tick()
    await engine.tick()
    expect(await main(['rejected'], { engine })).toBe(2)
    const listed = lastJson() as { prod: { eventId: string; eventType: string; error: string }[] }
    expect(listed.prod).toHaveLength(1)
    expect(listed.prod[0]!.eventType).toBe('user.created')
    expect(listed.prod[0]!.error).toMatch(/Document failed validation/)

    // Still broken: a replay reports it and keeps the non-zero exit.
    expect(await main(['rejected', '--replay'], { engine })).toBe(2)
    expect((lastJson() as { stillRejected: unknown[] }).stillRejected).toHaveLength(1)

    // Fixed: replaying one by id clears it, and the check goes quiet again.
    destination.poison = null
    expect(await main(['rejected', '--replay', '--id', listed.prod[0]!.eventId], { engine })).toBe(0)
    expect(lastJson()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await main(['rejected'], { engine })).toBe(0)

    const users = await t.db.collection('workos_users').find().sort({ '_raw.email': 1 }).toArray()
    expect(users.map((u) => u['_raw']['email'])).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('reconcile exits 2 on drift, 0 after a repair, and slices with --max-pages', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const gone = fake.createUser({ email: 'gone@example.com' })
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users', 'invitations'])
    await engine.backfill()
    fake.users.delete(gone['id'] as string)

    // Users needs exactly the three pages budgeted: the stream completes and
    // spends the budget, so invitations is left for a resume.
    const code = await main(['reconcile', '--max-pages', '3'], { engine })
    const users = diffs().find((d) => d.stream === 'users')!
    // A stream whose diff threw is reported with the error here, not thrown.
    expect(users.skippedReason).toBeUndefined()
    expect(users).toMatchObject({ pages: 3, complete: true, localOnly: [gone['id']], remoteOnly: [] })
    expect(diffs().find((d) => d.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })
    // A slice that stops short is the outcome asked for, not an error, but drift is.
    expect(code).toBe(2)

    expect(await main(['reconcile', '--repair'], { engine })).toBe(0)
    expect(await main(['reconcile'], { engine })).toBe(0)
    expect(await t.db.collection('workos_users').countDocuments({ _deleted_at: { $type: 'date' } })).toBe(1)
  })

  it('abandons a stream whose listing passes --max-pages and reports it oversized, still exiting 0', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users'])
    await engine.backfill()

    expect(await main(['reconcile', '--max-pages', '2'], { engine })).toBe(0)
    expect(diffs().find((d) => d.stream === 'users')).toMatchObject({ pages: 3, complete: false, oversized: true })
    expect(await main(['status'], { engine })).toBe(0)
    expect(lastJson()).toMatchObject({ prod: { oversized: ['users'] } })
  })

  it('leaves out --exclude-streams, and refuses a bare flag or an unknown name', async () => {
    fake.createUser()
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users', 'invitations'])
    await engine.backfill()

    expect(await main(['reconcile', '--exclude-streams', 'users'], { engine })).toBe(0)
    const names = diffs().map((d) => d.stream)
    expect(names).not.toContain('users')
    expect(names).toContain('invitations')

    await expect(main(['reconcile', '--exclude-streams', 'customers'], { engine })).rejects.toThrow(
      /unknown stream: customers/,
    )
    // A bare flag parses as `true`; it must not become an exclusion of nothing.
    expect(() => parseExcludeStreams(true)).toThrow(/invalid --exclude-streams/)
    expect(parseExcludeStreams('users, invitations')).toEqual(['users', 'invitations'])
  })

  it('refresh slices with --max-pages and converges with --resume', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = engineOver(new MongoDestination({ client: t.client, database: t.name }), ['users'])
    await engine.backfill()

    const results = () => lastJson() as unknown as { stream: string; pages: number; complete: boolean }[]
    expect(await main(['refresh', '--max-pages', '2'], { engine })).toBe(0)
    expect(results().find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })
    expect(await main(['refresh', '--resume'], { engine })).toBe(0)
    expect(results().find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
  })
})
