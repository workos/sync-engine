import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SyncEngine, WorkOSClient, parseExcludeStreams, selectStreams, silentLogger } from '@workos/sync-engine'
import { main, printSchemaSql } from '../src/cli.js'
import { fingerprint, migrate } from '../src/schema/migrate.js'
import { buildSchemaSql } from '../src/schema/ddl.js'
import { createTestDb } from './helpers/pg.js'
import { FaultyDestination } from './helpers/faulty.js'
import { PostgresDestination } from '../src/destination.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'
import type { SqlClient } from '../src/db/client.js'

describe('init --repair', () => {
  it('is in the help text', async () => {
    const out: string[] = []
    const spy = vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
    try {
      expect(await main(['help'])).toBe(0)
    } finally {
      spy.mockRestore()
    }
    expect(out.join('')).toContain('init [--repair]')
  })
})

describe('init --print-sql', () => {
  let out: string[]
  let spy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    out = []
    spy = vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string | Uint8Array) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
  })

  afterEach(() => {
    spy.mockRestore()
  })

  it('prints the DDL init would run, without a database or an API key', async () => {
    const saved = { url: process.env['DATABASE_URL'], key: process.env['WORKOS_API_KEY'] }
    delete process.env['DATABASE_URL']
    delete process.env['WORKOS_API_KEY']
    try {
      expect(await main(['init', '--print-sql'])).toBe(0)
    } finally {
      if (saved.url !== undefined) process.env['DATABASE_URL'] = saved.url
      if (saved.key !== undefined) process.env['WORKOS_API_KEY'] = saved.key
    }
    const sql = out.join('')
    expect(sql).toBe(printSchemaSql(undefined, 'workos'))
    expect(sql).toContain('create schema if not exists "workos"')
    expect(sql).toContain('create table if not exists "workos"."users"')
    expect(sql).toContain('create or replace view "workos"."users_live"')
    expect(sql).toContain(`fingerprint ${fingerprint(selectStreams(), 'workos')}`)
    // Left to `init`, which records it only after checking the live columns.
    expect(sql).not.toMatch(/insert into [^\n]*_migrations/)
  })

  it('honors --schema and --streams the way init does', async () => {
    expect(await main(['init', '--print-sql', '--schema', 'mirror', '--streams', 'users'])).toBe(0)
    const sql = out.join('')
    expect(sql).toContain('create table if not exists "mirror"."users"')
    // Always included, whatever --streams says.
    expect(sql).toContain('create table if not exists "mirror"."events"')
    expect(sql).not.toContain('"organizations"')
  })

  it('refuses a stream the catalog does not know, like init', async () => {
    await expect(main(['init', '--print-sql', '--streams', 'customers'])).rejects.toThrow(/unknown stream: customers/)
  })

  it('applies for schema names that would break naive quoting', async () => {
    const db = await createTestDb()
    try {
      // A line break would end the header comment early; `$guard$` would end
      // a fixed dollar quote; `%` would be a format placeholder in `raise`.
      for (const schema of ['team\nmirror', 'team$guard$mirror', 'team%mirror', "o'brien", 'say "hi"']) {
        const sql = printSchemaSql(['users'], schema)
        for (const line of sql.split('\n\n')[0]!.split('\n')) expect(line).toMatch(/^-- /)
        await db.exec(sql)
        await db.exec(sql) // the guard meets its own view and lets it through
        const found = await db.query<{ n: number }>(
          `select count(*)::int as n from pg_views where schemaname = $1 and viewname = 'users_live'`,
          [schema],
        )
        expect(found.rows).toEqual([{ n: 1 }])
      }
    } finally {
      await db.close()
    }
  })

  it('leaves a foreign _live view alone even when applied statement by statement', async () => {
    // psql without ON_ERROR_STOP reports an error and carries on, so the
    // check and the create have to be one statement for a failed check to
    // mean anything there.
    const db = await createTestDb()
    try {
      await db.exec(printSchemaSql(['users'], 'workos'))
      await db.exec(`
        create or replace view workos.users_live as
          select * from workos.users where "_deleted_at" is null and email like '%@example.com';
        comment on view workos.users_live is null;
      `)
      const errors: string[] = []
      for (const statement of buildSchemaSql(selectStreams(['users']), 'workos', { guardViews: true })) {
        try {
          await db.exec(statement)
        } catch (err) {
          errors.push(String(err))
        }
      }
      expect(errors).toHaveLength(1)
      expect(errors[0]).toMatch(/workos\.users_live exists but was not generated/)
      const def = await db.query<{ def: string }>(`select pg_get_viewdef('workos.users_live'::regclass) as def`)
      expect(def.rows[0]!.def).toContain('example.com')
    } finally {
      await db.close()
    }
  })

  it('refuses to replace a _live view it did not generate, like init', async () => {
    const db = await createTestDb()
    try {
      expect(await main(['init', '--print-sql'])).toBe(0)
      const script = out.join('')
      await db.exec(script)
      // Somebody narrows the view by hand and drops the generated comment:
      // same shape, so Postgres alone would let the next apply overwrite it,
      // and the comment that follows would claim it as ours.
      await db.exec(`
        create or replace view workos.users_live as
          select * from workos.users where "_deleted_at" is null and email like '%@example.com';
        comment on view workos.users_live is null;
      `)
      await expect(db.exec(script)).rejects.toThrow(/workos\.users_live exists but was not generated/)
      const def = await db.query<{ def: string }>(`select pg_get_viewdef('workos.users_live'::regclass) as def`)
      expect(def.rows[0]!.def).toContain('example.com')
    } finally {
      await db.close()
    }
  })

  it('prints a script a database accepts, after which init records the fingerprint', async () => {
    const db = await createTestDb()
    try {
      expect(await main(['init', '--print-sql'])).toBe(0)
      // Twice: the second apply meets the generated views, and the guard lets them through.
      await db.exec(out.join(''))
      await db.exec(out.join(''))
      // What `init` does next: finds the DDL in place, checks the live
      // columns, and records the fingerprint the script left out.
      const result = await migrate(db, selectStreams(), 'workos')
      expect(result.applied).toBe(true)
      expect(result.drift).toEqual([])
      const recorded = await db.query<{ fingerprint: string }>('select fingerprint from workos."_migrations"')
      expect(recorded.rows).toEqual([{ fingerprint: fingerprint(selectStreams(), 'workos') }])
    } finally {
      await db.close()
    }
  })
})

describe('rejected command', () => {
  let db: SqlClient
  let fake: FakeWorkOS
  let out: string[]

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    await db.close()
  })

  const lastJson = () => JSON.parse(out[out.length - 1]!) as Record<string, unknown>

  it('exits 2 while quarantined events exist, and 0 once they replay', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const destination = new FaultyDestination({ db, schema: 'workos' })
    destination.poison = bad['id'] as string
    const engine = new SyncEngine(
      {
        destination,
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users'],
        hydrate: 'off',
        logger: silentLogger,
        lagWindowMs: 0,
      },
      {
        clientFor: () =>
          new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl }),
      },
    )

    // Nothing quarantined yet: a clean exit, so a scheduled check stays quiet.
    expect(await main(['rejected'], { engine })).toBe(0)

    // Two polls: first sighting, then quarantine.
    await engine.tick()
    await engine.tick()
    expect(await main(['rejected'], { engine })).toBe(2)
    const listed = lastJson() as { prod: { eventId: string; eventType: string; error: string }[] }
    expect(listed.prod).toHaveLength(1)
    expect(listed.prod[0]!.eventType).toBe('user.created')
    expect(listed.prod[0]!.error).toMatch(/invalid input syntax/)

    // Still broken: a replay reports it and keeps the non-zero exit.
    expect(await main(['rejected', '--replay'], { engine })).toBe(2)
    expect((lastJson() as { stillRejected: unknown[] }).stillRejected).toHaveLength(1)

    // Fixed: replaying one by id clears it, and the check goes quiet again.
    destination.poison = null
    expect(await main(['rejected', '--replay', '--id', listed.prod[0]!.eventId], { engine })).toBe(0)
    expect(lastJson()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await main(['rejected'], { engine })).toBe(0)

    const users = await db.query<{ email: string }>(`select email from workos.users order by email`)
    expect(users.rows.map((r) => r.email)).toEqual(['bad@example.com', 'good@example.com'])
  })
})

describe('reconcile command', () => {
  let db: SqlClient
  let fake: FakeWorkOS
  let out: string[]

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    await db.close()
  })

  const lastDiffs = () =>
    (
      JSON.parse(out[out.length - 1]!) as {
        diffs: Record<string, { stream: string; pages: number; complete: boolean; oversized?: boolean }[]>
      }
    ).diffs['prod']!

  it('slices with --max-pages and converges with --resume, exiting 0 either way', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users', 'invitations'],
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
    await engine.backfill()

    // A slice that stops short is the outcome asked for, not an error.
    expect(await main(['reconcile', '--max-pages', '3'], { engine })).toBe(0)
    let diffs = lastDiffs()
    expect(diffs.find((d) => d.stream === 'users')).toMatchObject({ pages: 3, complete: true })
    expect(diffs.find((d) => d.stream === 'invitations')).toMatchObject({ pages: 0, complete: false })

    expect(await main(['reconcile', '--resume', '--max-pages', '2'], { engine })).toBe(0)
    diffs = lastDiffs()
    expect(diffs.find((d) => d.stream === 'users')).toMatchObject({ pages: 0, complete: true })
    expect(diffs.find((d) => d.stream === 'invitations')).toMatchObject({ pages: 1, complete: true })
  })

  it('abandons a stream whose listing passes --max-pages and reports it oversized, still exiting 0', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users'],
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
    await engine.backfill()

    expect(await main(['reconcile', '--max-pages', '2'], { engine })).toBe(0)
    expect(lastDiffs().find((d) => d.stream === 'users')).toMatchObject({ pages: 3, complete: false, oversized: true })
    expect(await main(['status'], { engine })).toBe(0)
    expect(JSON.parse(out[out.length - 1]!)).toMatchObject({ prod: { oversized: ['users'] } })
  })

  it('leaves out --exclude-streams, and refuses a bare flag or an unknown name', async () => {
    fake.createUser()
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users', 'invitations'],
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
    await engine.backfill()

    expect(await main(['reconcile', '--exclude-streams', 'users'], { engine })).toBe(0)
    const names = lastDiffs().map((d) => d.stream)
    expect(names).not.toContain('users')
    expect(names).toContain('invitations')

    await expect(main(['reconcile', '--exclude-streams', 'customers'], { engine })).rejects.toThrow(
      /unknown stream: customers/,
    )
    // A bare flag parses as `true`; it must not become an exclusion of nothing.
    expect(() => parseExcludeStreams(true)).toThrow(/invalid --exclude-streams/)
    expect(parseExcludeStreams('users, invitations')).toEqual(['users', 'invitations'])
  })
})

describe('refresh command', () => {
  let db: SqlClient
  let fake: FakeWorkOS
  let out: string[]

  beforeEach(async () => {
    db = await createTestDb()
    resetIds()
    fake = new FakeWorkOS()
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    await db.close()
  })

  const lastResults = () => JSON.parse(out[out.length - 1]!) as { stream: string; pages: number; complete: boolean }[]

  it('slices with --max-pages and converges with --resume, exiting 0 either way', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = new SyncEngine(
      {
        destination: new PostgresDestination({ db, schema: 'workos' }),
        environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
        streams: ['users'],
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
    await engine.backfill()

    expect(await main(['refresh', '--max-pages', '2'], { engine })).toBe(0)
    expect(lastResults().find((r) => r.stream === 'users')).toMatchObject({ pages: 2, complete: false })

    expect(await main(['refresh', '--resume'], { engine })).toBe(0)
    expect(lastResults().find((r) => r.stream === 'users')).toMatchObject({ pages: 1, complete: true })
  })
})
