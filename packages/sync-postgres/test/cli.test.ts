import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { SyncEngine, WorkOSClient, selectStreams, silentLogger } from '@workos/sync-engine'
import { main, parseHydrate, parseLeaseTtl, parseMaxPages, parseSslCa, printSchemaSql, repairRequested } from '../src/cli.js'
import { fingerprint, migrate } from '../src/schema/migrate.js'
import { createTestDb } from './helpers/pg.js'
import { FaultyDestination } from './helpers/faulty.js'
import { TEST_CA_PEM } from './helpers/ca.js'
import { FakeWorkOS, resetIds } from '../../sync-engine/test/helpers/fake-workos.js'
import type { SqlClient } from '../src/db/client.js'

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
    expect(parseHydrate('users=always')).toEqual({ users: 'always', organizations: 'on_create' })
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
    // `--max-pages` with no value parses as `true`; it must not become a budget of nothing.
    for (const bad of [true, '', '0', '-3', '1.5', 'ten']) {
      expect(() => parseMaxPages(bad)).toThrow(/invalid --max-pages/)
    }
  })
})

describe('init --repair', () => {
  it('asks for repair on init and nothing else', () => {
    expect(repairRequested('init', { repair: true })).toBe(true)
    expect(repairRequested('init', {})).toBe(false)
    // `reconcile --repair` repairs rows, not views.
    expect(repairRequested('reconcile', { repair: true })).toBe(false)
  })

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

describe('WORKOS_SYNC_LEASE_TTL_MS', () => {
  it('leaves the engine default in place when unset or empty', () => {
    expect(parseLeaseTtl(undefined)).toBeUndefined()
    expect(parseLeaseTtl('')).toBeUndefined()
    expect(parseLeaseTtl('  ')).toBeUndefined()
  })

  it('accepts a number of milliseconds', () => {
    expect(parseLeaseTtl('250')).toBe(250)
  })

  it('refuses a value that would expire on arrival or fail in SQL', () => {
    // Number('') is 0 and Number('abc') is NaN. A zero TTL is a lease that
    // excludes nobody, so neither may reach the engine.
    for (const bad of ['0', '-1', '0.5', 'abc', 'Infinity']) {
      expect(() => parseLeaseTtl(bad)).toThrow(/invalid WORKOS_SYNC_LEASE_TTL_MS/)
    }
  })
})

describe('WORKOS_SYNC_PG_SSL_CA', () => {
  const pem = TEST_CA_PEM

  it('leaves the driver defaults in place when unset or empty', () => {
    expect(parseSslCa(undefined)).toBeUndefined()
    expect(parseSslCa('')).toBeUndefined()
    expect(parseSslCa('  ')).toBeUndefined()
  })

  it('accepts PEM as is', () => {
    expect(parseSslCa(pem)).toBe(pem.trim())
  })

  it('accepts a bundle of several certificates', () => {
    expect(parseSslCa(pem + pem)).toBe((pem + pem).trim())
  })

  it('straightens a PEM whose newlines arrived as the two characters backslash-n', () => {
    expect(parseSslCa(pem.trim().replace(/\n/g, '\\n'))).toBe(pem.trim())
  })

  it('accepts the PEM encoded as base64, line-wrapped or not', () => {
    const b64 = Buffer.from(pem).toString('base64')
    expect(parseSslCa(b64)).toBe(pem)
    expect(parseSslCa(b64.replace(/(.{20})/g, '$1\n'))).toBe(pem)
  })

  it('refuses anything that is not a certificate', () => {
    // A path, prose, and base64 of something else: each would otherwise reach
    // the driver and fail there as a handshake error against the server.
    for (const bad of ['/etc/ssl/ca.crt', 'not a cert', Buffer.from('nope').toString('base64')]) {
      expect(() => parseSslCa(bad)).toThrow(/invalid WORKOS_SYNC_PG_SSL_CA/)
    }
  })

  it('refuses a damaged certificate rather than letting the driver fail on it', () => {
    const lines = pem.trim().split('\n')
    const header = lines[0]!
    const footer = lines[lines.length - 1]!
    const unterminated = lines.slice(0, -1).join('\n')
    const truncated = [header, ...lines.slice(1, 4), footer].join('\n')
    const garbage = [header, Buffer.from('not a cert').toString('base64'), footer].join('\n')
    for (const bad of [header, unterminated, truncated, garbage, Buffer.from(truncated).toString('base64')]) {
      expect(() => parseSslCa(bad)).toThrow(/invalid WORKOS_SYNC_PG_SSL_CA/)
    }
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

  it('prints a script a database accepts, after which init records the fingerprint', async () => {
    const db = await createTestDb()
    try {
      expect(await main(['init', '--print-sql'])).toBe(0)
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
