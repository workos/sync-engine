import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SyncEngine, WorkOSClient, selectStreams, silentLogger } from '@workos/sync-engine'
import {
  main,
  parseHydrate,
  parseLeaseTtl,
  parseMaxPages,
  parseSslCa,
  printSchemaSql,
  repairRequested,
} from '../src/cli.js'
import { fingerprint, migrate } from '../src/schema/migrate.js'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { FaultyDestination } from './helpers/faulty.js'
import { TEST_CA_PEM } from './helpers/ca.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'

describe('WORKOS_SYNC_HYDRATE', () => {
  it('leaves the engine default in place when unset', () => {
    expect(parseHydrate(undefined)).toBeUndefined()
    expect(parseHydrate('')).toBeUndefined()
  })

  it('accepts one mode for every stream', () => {
    expect(parseHydrate('off')).toBe('off')
    expect(parseHydrate('always')).toBe('always')
  })

  it('accepts per-stream overrides on top of the default', () => {
    expect(parseHydrate('users=always')).toEqual({ users: 'always' })
    expect(parseHydrate('users=off, organizations=off')).toEqual({ users: 'off', organizations: 'off' })
  })

  it('rejects anything else loudly', () => {
    expect(() => parseHydrate('sometimes')).toThrow(/invalid WORKOS_SYNC_HYDRATE/)
    expect(() => parseHydrate('users=maybe')).toThrow(/invalid WORKOS_SYNC_HYDRATE/)
  })
})

describe('backfill --max-pages', () => {
  it('means unbounded when absent', () => {
    expect(parseMaxPages(undefined)).toBeUndefined()
  })

  it('accepts a positive integer', () => {
    expect(parseMaxPages('25')).toBe(25)
    expect(parseMaxPages(' 1 ')).toBe(1)
  })

  it('refuses a bare flag, zero, a fraction, and anything that is not a number', () => {
    for (const bad of [true, '', '0', '-3', '1.5', 'ten']) {
      expect(() => parseMaxPages(bad)).toThrow(/invalid --max-pages/)
    }
  })
})

describe('init --repair', () => {
  it('asks for repair on init and nothing else', () => {
    expect(repairRequested('init', { repair: true })).toBe(true)
    expect(repairRequested('init', {})).toBe(false)
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
    expect(help).toContain('workos-sync-mysql')
    expect(help).toContain('init [--repair] [--print-sql]')
    expect(help).toContain('WORKOS_SYNC_MYSQL_SSL_CA')
    expect(help).not.toContain('Postgres')
  })
})

describe('WORKOS_SYNC_LEASE_TTL_MS', () => {
  it('leaves the engine default in place when unset or empty', () => {
    expect(parseLeaseTtl(undefined)).toBeUndefined()
    expect(parseLeaseTtl('')).toBeUndefined()
  })

  it('accepts a number of milliseconds', () => {
    expect(parseLeaseTtl('250')).toBe(250)
  })

  it('refuses a value that would expire on arrival', () => {
    for (const bad of ['0', '-1', '0.5', 'abc', 'Infinity']) {
      expect(() => parseLeaseTtl(bad)).toThrow(/invalid WORKOS_SYNC_LEASE_TTL_MS/)
    }
  })
})

describe('WORKOS_SYNC_MYSQL_SSL_CA', () => {
  const pem = TEST_CA_PEM

  it('leaves the driver defaults in place when unset or empty', () => {
    expect(parseSslCa(undefined)).toBeUndefined()
    expect(parseSslCa('  ')).toBeUndefined()
  })

  it('accepts PEM as is, as a bundle, with escaped newlines, or as base64', () => {
    expect(parseSslCa(pem)).toBe(pem.trim())
    expect(parseSslCa(pem + pem)).toBe((pem + pem).trim())
    expect(parseSslCa(pem.trim().replace(/\n/g, '\\n'))).toBe(pem.trim())
    expect(parseSslCa(Buffer.from(pem).toString('base64'))).toBe(pem)
  })

  it('refuses anything that is not a certificate', () => {
    for (const bad of ['/etc/ssl/ca.crt', 'not a cert', Buffer.from('nope').toString('base64')]) {
      expect(() => parseSslCa(bad)).toThrow(/invalid WORKOS_SYNC_MYSQL_SSL_CA/)
    }
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
    expect(sql).toContain('create database if not exists `workos`')
    expect(sql).toContain('create table if not exists `workos`.`users`')
    expect(sql).toContain('create or replace view `workos`.`users_live`')
    expect(sql).toContain(`fingerprint ${fingerprint(selectStreams(), 'workos')}`)
    expect(sql).not.toMatch(/insert into [^\n]*_migrations/)
  })

  it('honors --schema and --streams the way init does', async () => {
    expect(await main(['init', '--print-sql', '--schema', 'mirror', '--streams', 'users'])).toBe(0)
    const sql = out.join('')
    expect(sql).toContain('create table if not exists `mirror`.`users`')
    expect(sql).toContain('create table if not exists `mirror`.`events`')
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

    it('prints a script a database accepts, after which init records the fingerprint', async () => {
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
      // Twice: the second apply meets everything already there.
      for (const sql of statements) await db.exec(sql)
      for (const sql of statements) await db.exec(sql)
      // What `init` does next must be DML only: a host that gates DDL
      // (PlanetScale's safe migrations) is exactly where the script is used.
      const ddl: string[] = []
      const watched: typeof db = {
        ...db,
        exec: async (sql: string) => {
          ddl.push(sql)
          return db.exec(sql)
        },
        pinned: (fn) =>
          db.pinned((conn) =>
            fn({
              ...conn,
              exec: async (sql: string) => {
                ddl.push(sql)
                return conn.exec(sql)
              },
            }),
          ),
      }
      const result = await migrate(watched, selectStreams(), db.schema)
      expect(result.applied).toBe(true)
      expect(result.drift).toEqual([])
      expect(ddl).toEqual([])
      const recorded = await db.query<{ fingerprint: string }>(
        `select fingerprint from \`${db.schema}\`.\`_migrations\``,
      )
      expect(recorded.rows).toEqual([{ fingerprint: fingerprint(selectStreams(), db.schema) }])
    })
  })
})

describeDb('commands against a database', () => {
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
    expect(listed.prod[0]!.eventType).toBe('user.created')
    expect(listed.prod[0]!.error).toMatch(/Incorrect datetime value/)

    expect(await main(['rejected', '--replay'], { engine })).toBe(2)
    expect((lastJson() as { stillRejected: unknown[] }).stillRejected).toHaveLength(1)

    destination.poison = null
    expect(await main(['rejected', '--replay', '--id', listed.prod[0]!.eventId], { engine })).toBe(0)
    expect(lastJson()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await main(['rejected'], { engine })).toBe(0)

    const users = await db.query<{ email: string }>(`select email from \`${db.schema}\`.users order by email`)
    expect(users.rows.map((r) => r.email)).toEqual(['bad@example.com', 'good@example.com'])
  })

  it('runs init, backfill, status, reconcile and streams end to end', async () => {
    fake.createUser({ email: 'ada@example.com' })
    const engine = makeEngine(new FaultyDestination({ db, schema: db.schema }), ['users', 'organizations'])

    expect(await main(['backfill'], { engine })).toBe(0)
    const backfill = JSON.parse(out[out.length - 1]!) as { stream: string; complete: boolean }[]
    expect(backfill.find((s) => s.stream === 'users')).toMatchObject({ complete: true })

    expect(await main(['status'], { engine })).toBe(0)
    const status = lastJson() as Record<string, { streams: { stream: string; rows: number }[]; rejected: number }>
    expect(status['prod']!.streams.find((s) => s.stream === 'users')!.rows).toBe(1)
    expect(status['prod']!.rejected).toBe(0)

    // Drift the mirror and let reconcile find it: exit 2 without --repair.
    fake.users.clear()
    expect(await main(['reconcile'], { engine })).toBe(2)
    expect(await main(['reconcile', '--repair'], { engine })).toBe(0)
    expect(await main(['reconcile'], { engine })).toBe(0)

    out = []
    expect(await main(['streams'])).toBe(0)
    const streams = JSON.parse(out.join('')) as { stream: string }[]
    expect(streams.some((s) => s.stream === 'users')).toBe(true)
  })
})
