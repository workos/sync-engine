import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SyncEngine, WorkOSClient, selectStreams, silentLogger } from '@workos/sync-engine'
import {
  main,
  parseHydrate,
  parseExcludeStreams,
  parseLeaseTtl,
  parseMaxPages,
  parseSslCa,
  printSchemaSql,
  repairRequested,
} from '../src/cli.js'
import { fingerprint, migrate } from '../src/schema/migrate.js'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { FaultyDestination } from './helpers/faulty.js'
import { TEST_CA_PEM } from './helpers/ca.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'

describe('WORKOS_SYNC_HYDRATE', () => {
  it('leaves the engine default in place when unset', () => {
    expect(parseHydrate(undefined)).toBeUndefined()
    expect(parseHydrate('')).toBeUndefined()
  })

  it('accepts one mode for every stream, or per-stream overrides', () => {
    expect(parseHydrate('always')).toBe('always')
    expect(parseHydrate('users=off, organizations=off')).toEqual({ users: 'off', organizations: 'off' })
  })

  it('rejects anything else loudly', () => {
    expect(() => parseHydrate('sometimes')).toThrow(/invalid WORKOS_SYNC_HYDRATE/)
  })
})

describe('backfill --max-pages', () => {
  it('means unbounded when absent and accepts a positive integer', () => {
    expect(parseMaxPages(undefined)).toBeUndefined()
    expect(parseMaxPages('25')).toBe(25)
  })

  it('refuses a bare flag, zero, a fraction, and anything that is not a number', () => {
    for (const bad of [true, '', '0', '-3', '1.5', 'ten'])
      expect(() => parseMaxPages(bad)).toThrow(/invalid --max-pages/)
  })
})

describe('reconcile --exclude-streams', () => {
  it('means nothing excluded when absent and splits a list', () => {
    expect(parseExcludeStreams(undefined)).toBeUndefined()
    expect(parseExcludeStreams('users, invitations')).toEqual(['users', 'invitations'])
  })

  it('refuses a bare flag: an exclusion of nothing is the mistake it exists to prevent', () => {
    expect(() => parseExcludeStreams(true)).toThrow(/invalid --exclude-streams/)
    expect(() => parseExcludeStreams(' , ')).toThrow(/invalid --exclude-streams/)
  })
})

describe('init --repair', () => {
  it('asks for repair on init and nothing else', () => {
    expect(repairRequested('init', { repair: true })).toBe(true)
    expect(repairRequested('reconcile', { repair: true })).toBe(false)
  })

  it('is in the help text', async () => {
    const out: string[] = []
    const spy = vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string) => {
      out.push(String(chunk))
      return true
    }) as typeof process.stdout.write)
    try {
      expect(await main(['help'])).toBe(0)
    } finally {
      spy.mockRestore()
    }
    const help = out.join('')
    expect(help).toContain('workos-sync-clickhouse')
    expect(help).toContain('init [--repair] [--print-sql]')
    expect(help).toContain('reconcile [--repair] [--resume] [--max-pages <n>] [--exclude-streams <a,b>]')
    expect(help).toContain('WORKOS_SYNC_CLICKHOUSE_TLS_CA')
    expect(help).not.toMatch(/MySQL|Postgres/)
  })
})

describe('WORKOS_SYNC_LEASE_TTL_MS', () => {
  it('accepts a number of milliseconds and refuses a value that would expire on arrival', () => {
    expect(parseLeaseTtl(undefined)).toBeUndefined()
    expect(parseLeaseTtl('250')).toBe(250)
    for (const bad of ['0', '-1', 'abc']) expect(() => parseLeaseTtl(bad)).toThrow(/invalid WORKOS_SYNC_LEASE_TTL_MS/)
  })
})

describe('WORKOS_SYNC_CLICKHOUSE_TLS_CA', () => {
  it('accepts PEM as is or as base64, and refuses anything that is not a certificate', () => {
    expect(parseSslCa(undefined)).toBeUndefined()
    expect(parseSslCa(TEST_CA_PEM)).toBe(TEST_CA_PEM.trim())
    expect(parseSslCa(Buffer.from(TEST_CA_PEM).toString('base64'))).toBe(TEST_CA_PEM)
    expect(() => parseSslCa('not a cert')).toThrow(/invalid WORKOS_SYNC_CLICKHOUSE_TLS_CA/)
  })
})

describe('init --print-sql', () => {
  let out: string[]
  let spy: ReturnType<typeof vi.spyOn>

  beforeEach(() => {
    out = []
    spy = vi.spyOn(process.stdout, 'write').mockImplementation(((chunk: string) => {
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
    expect(sql).toContain('CREATE DATABASE IF NOT EXISTS `workos`')
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS `workos`.`users`')
    expect(sql).toContain('CREATE OR REPLACE VIEW `workos`.`users_live`')
    expect(sql).toContain(`fingerprint ${fingerprint(selectStreams(), 'workos')}`)
    expect(sql).not.toMatch(/INSERT INTO [^\n]*_migrations/)
  })

  it('honors --schema and --streams the way init does', async () => {
    expect(await main(['init', '--print-sql', '--schema', 'mirror', '--streams', 'users'])).toBe(0)
    const sql = out.join('')
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS `mirror`.`users`')
    expect(sql).toContain('CREATE TABLE IF NOT EXISTS `mirror`.`events`')
    expect(sql).not.toContain('`organizations`')
  })

  it('refuses a stream the catalog does not know, like init', async () => {
    await expect(main(['init', '--print-sql', '--streams', 'customers'])).rejects.toThrow(/unknown stream: customers/)
  })

  describeDb('applied by hand', () => {
    let db: TestDb
    beforeEach(async () => {
      db = await createTestDb()
    })
    afterEach(async () => {
      await db.close()
    })

    it('prints a script a server accepts twice, after which init records the fingerprint', async () => {
      expect(await main(['init', '--print-sql', '--schema', db.schema])).toBe(0)
      const statements = out
        .join('')
        .split(/;\n\n/)
        .map((s) =>
          s
            .replace(/^--.*$/gm, '')
            .trim()
            .replace(/;$/, ''),
        )
        .filter(Boolean)
      for (const sql of statements) await db.command(sql)
      for (const sql of statements) await db.command(sql)
      const result = await migrate(db, selectStreams(), db.schema)
      expect(result.applied).toBe(true)
      expect(result.drift).toEqual([])
      const recorded = await db.query<{ fingerprint: string }>(
        `SELECT fingerprint FROM \`${db.schema}\`.\`_migrations\``,
      )
      expect(recorded).toEqual([{ fingerprint: fingerprint(selectStreams(), db.schema) }])
    })
  })
})

describeDb('commands against a server', () => {
  let db: TestDb
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
  const makeEngine = (destination: FaultyDestination, streams = ['users']) =>
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
          new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl }),
      },
    )

  it('exits 2 while quarantined events exist, and 0 once they replay', async () => {
    fake.createUser({ email: 'good@example.com' })
    const bad = fake.createUser({ email: 'bad@example.com' })
    const destination = new FaultyDestination({ db, schema: db.schema })
    destination.poison = bad['id'] as string
    const engine = makeEngine(destination)

    expect(await main(['rejected'], { engine })).toBe(0)
    await engine.tick()
    await engine.tick()
    expect(await main(['rejected'], { engine })).toBe(2)
    const listed = lastJson() as { prod: { eventId: string; eventType: string; error: string }[] }
    expect(listed.prod).toHaveLength(1)
    expect(listed.prod[0]!.error).toMatch(/Cannot parse string/)

    expect(await main(['rejected', '--replay'], { engine })).toBe(2)
    destination.poison = null
    expect(await main(['rejected', '--replay', '--id', listed.prod[0]!.eventId], { engine })).toBe(0)
    expect(lastJson()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await main(['rejected'], { engine })).toBe(0)

    const users = await db.query<{ email: string }>(`SELECT email FROM \`${db.schema}\`.users_live ORDER BY email`)
    expect(users.map((r) => r.email)).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('runs backfill, status, reconcile and streams end to end', async () => {
    fake.createUser({ email: 'ada@example.com' })
    const engine = makeEngine(new FaultyDestination({ db, schema: db.schema }), ['users', 'organizations'])

    expect(await main(['backfill'], { engine })).toBe(0)
    const backfill = lastJson() as unknown as { stream: string; complete: boolean }[]
    expect(backfill.find((s) => s.stream === 'users')).toMatchObject({ complete: true })

    expect(await main(['status'], { engine })).toBe(0)
    const status = lastJson() as Record<string, { streams: { stream: string; rows: number }[]; rejected: number }>
    expect(status['prod']!.streams.find((s) => s.stream === 'users')!.rows).toBe(1)
    expect(status['prod']!.rejected).toBe(0)

    fake.users.clear()
    expect(await main(['reconcile'], { engine })).toBe(2)
    expect(await main(['reconcile', '--repair'], { engine })).toBe(0)
    expect(await main(['reconcile'], { engine })).toBe(0)

    out = []
    expect(await main(['streams'])).toBe(0)
    expect((JSON.parse(out.join('')) as { stream: string }[]).some((s) => s.stream === 'users')).toBe(true)
  })

  it('leaves out --exclude-streams on reconcile, and refuses a name the catalog does not know', async () => {
    fake.createUser()
    const engine = makeEngine(new FaultyDestination({ db, schema: db.schema }), ['users', 'organizations'])
    await engine.backfill()

    expect(await main(['reconcile', '--exclude-streams', 'users'], { engine })).toBe(0)
    const names = (lastJson() as { diffs: Record<string, { stream: string }[]> }).diffs['prod']!.map((d) => d.stream)
    expect(names).not.toContain('users')
    expect(names).toContain('organizations')

    await expect(main(['reconcile', '--exclude-streams', 'customers'], { engine })).rejects.toThrow(
      /unknown stream: customers/,
    )
  })

  it('reports a stream reconcile abandoned as oversized, in the diff and in status', async () => {
    for (let i = 0; i < 250; i++) fake.createUser()
    const engine = makeEngine(new FaultyDestination({ db, schema: db.schema }))
    await engine.backfill()

    // Three pages upstream, two budgeted: abandoned on the third, exit 0.
    expect(await main(['reconcile', '--max-pages', '2'], { engine })).toBe(0)
    const users = (
      lastJson() as {
        diffs: Record<string, { stream: string; pages: number; complete: boolean; oversized?: boolean }[]>
      }
    ).diffs['prod']!.find((d) => d.stream === 'users')
    expect(users).toMatchObject({ pages: 3, complete: false, oversized: true })
    expect(await main(['status'], { engine })).toBe(0)
    expect(lastJson()).toMatchObject({ prod: { oversized: ['users'] } })
  })
})
