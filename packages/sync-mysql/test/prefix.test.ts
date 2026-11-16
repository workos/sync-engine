/**
 * The table prefix.
 *
 * It exists for a database the mirror has to share with the application: a
 * PlanetScale branch is one database, and most applications already have a
 * `users` table in it. What is held here is that with a prefix set, nothing
 * the engine names is left without it — one call site that still said
 * `users` or `_sync_state` would read or write somebody else's table — and
 * that with none set, the names, the fingerprint and the lock are the ones
 * existing mirrors already have.
 */
import { createHash } from 'node:crypto'
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import {
  SyncEngine,
  WorkOSClient,
  LeaseHeldError,
  STREAMS,
  selectStreams,
  silentLogger,
  streamByName,
  type TableDef,
} from '@workos/sync-engine'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { FaultyDestination } from './helpers/faulty.js'
import { FakeWorkOS, resetIds, id } from '../../sync-engine/test/helpers/fake-workos.js'
import { MysqlDestination } from '../src/destination.js'
import { main, printSchemaSql } from '../src/cli.js'
import { DDL_VERSION, fingerprint, lockName, migrate } from '../src/schema/migrate.js'
import { bookkeepingSql, buildSchemaSql, createTableSql, indexSpecs } from '../src/schema/ddl.js'
import { MAX_PREFIX_LENGTH, objectName, parseTablePrefix, qualified } from '../src/schema/identifiers.js'
import { liveViewName, liveViewSql } from '../src/schema/views.js'
import type { SqlClient } from '../src/db/client.js'

const users = streamByName('users')!
const PREFIX = 'workos_'
const BOOKKEEPING = ['_sync_state', '_migrations', '_sync_lease', '_rejected', '_columns']

/** What a mirror of `streams` under `prefix` should consist of, spelled out rather than asked of the code under test. */
const objectsOf = (streams: TableDef[], prefix: string): string[] =>
  [
    ...BOOKKEEPING.map((name) => `${prefix}${name}`),
    ...streams.flatMap((s) =>
      s.mode === 'append' ? [`${prefix}${s.name}`] : [`${prefix}${s.name}`, `${prefix}${s.name}_live`],
    ),
  ].sort()

/**
 * `db`, with every statement kept, on whatever connection it ran. Temporary
 * tables never show in `information_schema`, so the statements are the only
 * place their names can be read.
 */
function recording(db: SqlClient): { client: SqlClient; statements: string[]; ddl: string[] } {
  const statements: string[] = []
  const ddl: string[] = []
  const wrap = (c: SqlClient): SqlClient => ({
    query: <T>(sql: string, params?: unknown[]) => {
      statements.push(sql)
      return c.query<T>(sql, params)
    },
    exec: (sql: string) => {
      statements.push(sql)
      ddl.push(sql)
      return c.exec(sql)
    },
    transaction: (fn) => c.transaction((tx) => fn(wrap(tx))),
    pinned: (fn) => c.pinned((conn) => fn(wrap(conn))),
    close: () => c.close(),
  })
  return { client: wrap(db), statements, ddl }
}

/** Every name a statement qualifies with the database: the `name` of `` `db`.`name` ``. */
function namesIn(statements: string[], schema: string): string[] {
  const qualifiedName = new RegExp(`\`${schema}\`\\.\`([^\`]+)\``, 'g')
  return [...new Set(statements.flatMap((sql) => [...sql.matchAll(qualifiedName)].map((m) => m[1]!)))].sort()
}

describe('table prefix: names', () => {
  it('puts the prefix on every object and leaves a bare database name as it was', () => {
    const prefixed = { schema: 'app', prefix: PREFIX }
    expect(objectName('app', 'users')).toBe('users')
    expect(objectName(prefixed, 'users')).toBe('workos_users')
    // The rule is prefix plus the existing name, so bookkeeping keeps its underscore.
    expect(objectName(prefixed, '_sync_state')).toBe('workos__sync_state')
    expect(qualified('app', 'users')).toBe('`app`.`users`')
    expect(qualified(prefixed, 'users')).toBe('`app`.`workos_users`')
    expect(qualified({ schema: 'app', prefix: '' }, 'users')).toBe(qualified('app', 'users'))

    expect(bookkeepingSql(prefixed).map(([name]) => name)).toEqual(BOOKKEEPING.map((n) => `workos_${n}`))
    for (const [name, sql] of bookkeepingSql(prefixed)) {
      expect(sql).toContain(`create table if not exists \`app\`.\`${name}\` (`)
    }
    expect(createTableSql(users, prefixed)).toContain('create table if not exists `app`.`workos_users` (')
    expect(liveViewName(users, prefixed)).toBe('workos_users_live')
    expect(liveViewSql(users, prefixed)).toEqual([
      'create or replace view `app`.`workos_users_live` as select * from `app`.`workos_users` where `_deleted_at` is null',
    ])
    // Callable the way they were before the prefix existed.
    expect(liveViewName(users)).toBe('users_live')
    expect(liveViewSql(users, 'app')).toEqual([
      'create or replace view `app`.`users_live` as select * from `app`.`users` where `_deleted_at` is null',
    ])
  })

  it('leaves index names alone, since MySQL scopes them to their table', () => {
    const sql = createTableSql(users, { schema: 'app', prefix: PREFIX })
    for (const idx of indexSpecs(users)) {
      expect(idx.name).not.toContain(PREFIX)
      expect(sql).toContain(`index \`${idx.name}\` (`)
    }
    expect(indexSpecs(users).map((i) => i.name)).toContain('users_email_idx')
  })

  it('shortens a prefixed name that would not fit, keeping the prefix', () => {
    const long = { ...users, name: 'x'.repeat(60) }
    const name = objectName({ schema: 'app', prefix: PREFIX }, long.name)
    expect(name).toHaveLength(64)
    expect(name.startsWith(PREFIX)).toBe(true)
    expect(liveViewName(long, { schema: 'app', prefix: PREFIX })).toHaveLength(64)
    // The longest prefix still leaves every catalog name whole, opt-in streams included.
    const widest = { schema: 'app', prefix: 'p'.repeat(MAX_PREFIX_LENGTH) }
    for (const s of STREAMS) {
      expect(liveViewName(s, widest)).toBe(`${widest.prefix}${s.name}_live`)
    }
  })

  it('emits the DDL it always did when there is no prefix', () => {
    const streams = selectStreams()
    expect(buildSchemaSql(streams, { schema: 'workos', prefix: '' })).toEqual(buildSchemaSql(streams, 'workos'))
    expect(bookkeepingSql('workos').map(([name]) => name)).toEqual(BOOKKEEPING)
    expect(printSchemaSql(undefined, 'workos', '')).toBe(printSchemaSql(undefined, 'workos'))
  })
})

describe('table prefix: fingerprint and lock', () => {
  it('keeps the fingerprint of a mirror without a prefix, and gives a prefixed one its own', () => {
    const streams = selectStreams()
    const bare = fingerprint(streams, 'workos')
    expect(fingerprint(streams, { schema: 'workos', prefix: '' })).toBe(bare)
    const prefixed = fingerprint(streams, { schema: 'workos', prefix: PREFIX })
    expect(prefixed).not.toBe(bare)
    expect(fingerprint(streams, { schema: 'workos', prefix: 'other_' })).not.toBe(prefixed)

    // The hash is of the same canonical JSON as before the prefix existed:
    // no `prefix` key at all without one, or every existing database would
    // see a new fingerprint and run its DDL again.
    const thing: TableDef = {
      name: 'things',
      description: 'A fixture',
      mode: 'mirror',
      group: 'logs',
      key: ['id'],
      columns: [{ name: 'id', type: 'text', path: 'id' }],
    }
    const canonical = (extra: Record<string, unknown>) =>
      createHash('sha256')
        .update(
          JSON.stringify({
            ddl: DDL_VERSION,
            schema: 'workos',
            ...extra,
            streams: [{ n: 'things', k: ['id'], m: 'mirror', c: [['id', 'text', 'id', false]] }],
          }),
        )
        .digest('hex')
        .slice(0, 16)
    expect(fingerprint([thing], 'workos')).toBe(canonical({}))
    expect(fingerprint([thing], { schema: 'workos', prefix: PREFIX })).toBe(canonical({ prefix: PREFIX }))
  })

  it('scopes the migration lock by prefix only when there is one', () => {
    expect(lockName({ schema: 'workos', prefix: '' })).toBe('workos-sync-mysql:migrate:workos')
    expect(lockName({ schema: 'workos', prefix: '' })).toBe(lockName('workos'))
    const prefixed = lockName({ schema: 'workos', prefix: PREFIX })
    expect(prefixed).toBe('workos-sync-mysql:migrate:workos:workos_')
    expect(lockName({ schema: 'workos', prefix: 'other_' })).not.toBe(prefixed)
    const long = lockName({ schema: 'x'.repeat(64), prefix: 'p'.repeat(MAX_PREFIX_LENGTH) })
    expect(long.length).toBeLessThanOrEqual(64)
    expect(long).not.toBe(lockName('x'.repeat(64)))
  })
})

describe('table prefix: validation', () => {
  const build = (tablePrefix: string | undefined) =>
    new MysqlDestination({ databaseUrl: 'mysql://localhost/unused', tablePrefix })

  it('means no prefix when absent, empty or blank', () => {
    for (const none of [undefined, '', '   ', '\t\n']) {
      expect(parseTablePrefix(none)).toBe('')
      expect(build(none).tablePrefix).toBe('')
    }
  })

  it('accepts lowercase letters, digits and underscores, and drops the whitespace around them', () => {
    for (const ok of ['workos_', 'w', 'workos_2', '_', '9lives_', 'p'.repeat(MAX_PREFIX_LENGTH)]) {
      expect(build(ok).tablePrefix).toBe(ok)
    }
    expect(build('  workos_\n').tablePrefix).toBe('workos_')
  })

  it('refuses anything else when the destination is built, before a connection is made', () => {
    for (const bad of [
      // Mixed case: a server with lower_case_table_names stores it lowered,
      // and the names read back would no longer match the ones asked for.
      'WorkOS_',
      'work-os_',
      'work os_',
      'workos.',
      'workos`',
      "workos'",
      'wörkos_',
      'a;drop table x',
      '%',
      '\\',
    ]) {
      expect(() => build(bad), bad).toThrow(/invalid table prefix .*letters, digits and underscores/)
    }
    expect(MAX_PREFIX_LENGTH).toBe(24)
    expect(() => build('p'.repeat(MAX_PREFIX_LENGTH + 1))).toThrow(/invalid table prefix .*at most 24 characters/)
  })
})

describe('table prefix: command line', () => {
  let out: string[]

  beforeEach(() => {
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
    // Whatever the shell running the suite has set, these tests start from neither.
    vi.stubEnv('WORKOS_SYNC_TABLE_PREFIX', undefined)
    vi.stubEnv('WORKOS_SYNC_SCHEMA', undefined)
  })
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllEnvs()
  })

  const printed = async (...args: string[]) => {
    out = []
    expect(await main(['init', '--print-sql', '--streams', 'users', ...args])).toBe(0)
    return out.join('')
  }

  it('lists the flag and the variable in the help', async () => {
    expect(await main(['help'])).toBe(0)
    const help = out.join('')
    expect(help).toContain('--table-prefix <prefix>')
    expect(help).toContain('WORKOS_SYNC_TABLE_PREFIX')
    // Under Flags and under Environment, each beside its neighbour.
    expect(help).toMatch(/--schema <name>[^\n]*\n\s+--table-prefix <prefix>\n/)
    expect(help).toMatch(/\n\s+WORKOS_SYNC_SCHEMA[^\n]*\n\s+WORKOS_SYNC_TABLE_PREFIX\s+Prefix on every table/)
  })

  it('reads --table-prefix, then WORKOS_SYNC_TABLE_PREFIX, and the flag wins', async () => {
    expect(await printed()).toContain('create table if not exists `workos`.`users`')

    expect(await printed('--table-prefix', 'flag_')).toContain('create table if not exists `workos`.`flag_users`')

    vi.stubEnv('WORKOS_SYNC_TABLE_PREFIX', 'env_')
    const fromEnv = await printed()
    expect(fromEnv).toContain('create table if not exists `workos`.`env_users`')
    expect(fromEnv).toBe(printSchemaSql(['users'], 'workos', 'env_'))

    const both = await printed('--table-prefix', 'flag_')
    expect(both).toContain('create table if not exists `workos`.`flag_users`')
    expect(both).not.toContain('env_')

    // Set but empty is how a deployment template says "none".
    vi.stubEnv('WORKOS_SYNC_TABLE_PREFIX', '')
    expect(await printed()).toBe(printSchemaSql(['users'], 'workos'))
  })

  it('refuses a prefix it cannot use, and a flag left without a value', async () => {
    await expect(main(['init', '--print-sql', '--table-prefix', 'work-os_'])).rejects.toThrow(/invalid table prefix/)
    // `--table-prefix "$UNSET"`: running on would mirror into unprefixed tables.
    await expect(main(['init', '--print-sql', '--table-prefix'])).rejects.toThrow(/invalid --table-prefix/)
    await expect(main(['init', '--table-prefix', '--print-sql'])).rejects.toThrow(/invalid --table-prefix/)
    vi.stubEnv('WORKOS_SYNC_TABLE_PREFIX', 'work-os_')
    await expect(main(['init', '--print-sql'])).rejects.toThrow(/invalid table prefix/)
  })

  it('prints prefixed names only with init --print-sql --table-prefix', async () => {
    expect(await main(['init', '--print-sql', '--table-prefix', PREFIX])).toBe(0)
    const sql = out.join('')
    const streams = selectStreams()
    expect(sql).toBe(printSchemaSql(undefined, 'workos', PREFIX))
    expect(sql).toContain('-- Schema "workos", table prefix "workos_", catalog fingerprint ')
    expect(sql).toContain(`fingerprint ${fingerprint(streams, { schema: 'workos', prefix: PREFIX })}.`)
    // Everything created, and everything a view reads from, and nothing else.
    expect(namesIn([sql], 'workos')).toEqual(objectsOf(streams, PREFIX))
    const created = [...sql.matchAll(/^create (?:table if not exists|or replace view) `workos`\.`([^`]+)`/gm)]
    expect(created.map((m) => m[1]!).sort()).toEqual(objectsOf(streams, PREFIX))
    // Without the flag the header says nothing of a prefix, as before.
    expect(printSchemaSql(undefined, 'workos')).toContain('-- Schema "workos", catalog fingerprint ')
  })

  describeDb('against a database', () => {
    let db: TestDb
    beforeEach(async () => {
      db = await createTestDb()
      vi.stubEnv('WORKOS_API_KEY', 'sk_test_1')
      vi.stubEnv('LOG_LEVEL', 'silent')
    })
    afterEach(async () => {
      await db.close()
    })

    const objects = async () => {
      const r = await db.query<{ table_name: string }>(
        `select table_name as table_name from information_schema.tables where table_schema = ? order by table_name`,
        [db.schema],
      )
      return r.rows.map((row) => row.table_name)
    }

    it('creates the mirror under the prefix the flag or the variable names', async () => {
      const init = (...flags: string[]) => main(['init', '--schema', db.schema, '--streams', 'users', ...flags], { db })
      const streams = selectStreams(['users'])

      expect(await init('--table-prefix', 'flag_')).toBe(0)
      expect(await objects()).toEqual(objectsOf(streams, 'flag_'))

      vi.stubEnv('WORKOS_SYNC_TABLE_PREFIX', 'env_')
      expect(await init()).toBe(0)
      expect(await objects()).toEqual([...objectsOf(streams, 'env_'), ...objectsOf(streams, 'flag_')].sort())

      // The flag wins: nothing new appears under either.
      expect(await init('--table-prefix', 'flag_')).toBe(0)
      expect(await objects()).toEqual([...objectsOf(streams, 'env_'), ...objectsOf(streams, 'flag_')].sort())
    })

    it('prints a prefixed script a database accepts, after which init creates and alters nothing', async () => {
      expect(await main(['init', '--print-sql', '--schema', db.schema, '--table-prefix', PREFIX])).toBe(0)
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
      const streams = selectStreams()
      expect(await objects()).toEqual(objectsOf(streams, PREFIX))

      // A host that gates DDL is where the script is used, so `init` must
      // create and alter nothing after it. Every connection is watched here,
      // so what does show is the temporary table drift detection reads the
      // server's own spelling from, made and dropped in one session. It has
      // to carry the prefix like everything else.
      const target = { schema: db.schema, prefix: PREFIX }
      const watched = recording(db)
      const result = await migrate(watched.client, streams, target)
      expect(result.applied).toBe(true)
      expect(result.drift).toEqual([])
      expect(watched.ddl.length).toBeGreaterThan(0)
      for (const sql of watched.ddl) {
        expect(sql).toMatch(
          new RegExp(`^(create temporary table|drop temporary table if exists) \`${db.schema}\`\\.\`workos__probe_`),
        )
      }
      expect(namesIn(watched.statements, db.schema).filter((name) => !name.startsWith(PREFIX))).toEqual([])
      expect(await objects()).toEqual(objectsOf(streams, PREFIX))

      const recorded = await db.query<{ fingerprint: string }>(
        `select fingerprint from \`${db.schema}\`.\`workos__migrations\``,
      )
      expect(recorded.rows).toEqual([{ fingerprint: fingerprint(streams, target) }])
      // And from then on the fingerprint is found, and nothing is looked at.
      expect((await migrate(db, streams, target)).applied).toBe(false)
    })
  })
})

describeDb('table prefix: against real MySQL', () => {
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

  const makeEngine = (destination: MysqlDestination, streams: string[], api = fake) =>
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
            fetchImpl: api.fetchImpl,
            maxRetries: 0,
          }),
      },
    )

  const objects = async () => {
    const r = await db.query<{ table_name: string }>(
      `select table_name as table_name from information_schema.tables where table_schema = ? order by table_name`,
      [db.schema],
    )
    return r.rows.map((row) => row.table_name)
  }
  const count = async (table: string, where = '1 = 1') => {
    const r = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.\`${table}\` where ${where}`)
    return Number(r.rows[0]!.n)
  }
  const emails = async (table: string) => {
    const r = await db.query<{ email: string }>(`select email from \`${db.schema}\`.\`${table}\` order by email`)
    return r.rows.map((row) => row.email)
  }

  it('runs a whole lifecycle with every table, view and temporary table under the prefix', async () => {
    const names = ['users', 'organizations', 'organization_memberships', 'group_memberships', 'auth_attempts']
    const streams = selectStreams(names)
    const { client, statements } = recording(db)
    const destination = new FaultyDestination({ db: client, schema: db.schema, tablePrefix: PREFIX })
    const engine = makeEngine(destination, names)

    // Before anything exists: `status` asks, and creates nothing.
    expect((await engine.status())['prod']).toMatchObject({ initialized: false })
    expect(await objects()).toEqual([])
    await engine.init()
    expect(await objects()).toEqual(objectsOf(streams, PREFIX))
    expect((await engine.status())['prod']).toMatchObject({ initialized: true })

    const org = fake.createOrganization({ name: 'Acme' })
    const ada = fake.createUser({ email: 'ada@example.com' })
    const leaver = fake.createUser({ email: 'leaver@example.com' })
    fake.createMembership(ada['id'] as string, org['id'] as string, 'admin')
    await engine.backfill()
    expect(await emails('workos_users_live')).toEqual(['ada@example.com', 'leaver@example.com'])
    expect(await count('workos_organization_memberships_live')).toBe(1)

    // The tail: an update, a delete, a sign-in, an organization only events
    // have described, a group membership that goes and comes back (unlink,
    // relink), and one event whose write fails until it is quarantined.
    fake.updateUser(ada['id'] as string, { email: 'ada@acme.com' })
    fake.deleteUser(leaver['id'] as string)
    fake.emitAuthAttempt(ada['id'] as string, true)
    const late = fake.createOrganization({ name: 'Late' })
    const pair = { group_id: 'group_01', organization_membership_id: 'om_01' }
    for (const [i, event] of ['group.member_added', 'group.member_removed', 'group.member_added'].entries()) {
      const created_at = new Date(Date.now() - 50_000 + i * 1_000).toISOString()
      fake.events.push({ id: id('event_'), event, data: pair, created_at })
    }
    const bad = fake.createUser({ email: 'bad@example.com' })
    destination.poison = bad['id'] as string
    await engine.tick()
    expect(await engine.tick()).toMatchObject({ rejected: 1 })
    expect((await engine.status())['prod']).toMatchObject({ rejected: 1 })
    expect((await engine.rejected())['prod']!.map((r) => r.objectId)).toEqual([bad['id']])
    expect(await count('workos__rejected')).toBe(1)
    destination.poison = null
    expect(await engine.replayRejected()).toEqual({ replayed: 1, stillRejected: [] })
    expect(await count('workos__rejected')).toBe(0)

    expect(await emails('workos_users_live')).toEqual(['ada@acme.com', 'bad@example.com'])
    expect(await count('workos_users')).toBe(3)
    expect(await count('workos_group_memberships_live')).toBe(1)
    expect(await count('workos_auth_attempts')).toBe(1)
    expect(await count('workos_events')).toBeGreaterThan(0)
    const lateOrg = async () =>
      (
        await db.query<{ allow_profiles_outside_organization: number | null }>(
          `select allow_profiles_outside_organization from \`${db.schema}\`.workos_organizations where id = ?`,
          [late['id']],
        )
      ).rows[0]!.allow_profiles_outside_organization
    expect(await lateOrg()).toBeNull()
    // The cursor is in the prefixed state table, for this sync and environment.
    expect(await count('workos__sync_state', `sync_id = 'default' and env = 'prod'`)).toBeGreaterThan(0)

    // Reconcile: a delete and a create the tail never saw.
    fake.users.delete(bad['id'] as string)
    const added = fake.createUser({ email: 'added@example.com' })
    fake.events = []
    const found = await engine.reconcile()
    expect(found.diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({
      matched: 1,
      localOnly: [bad['id']],
      remoteOnly: [added['id']],
    })
    await engine.reconcile({ repair: true })
    expect(await emails('workos_users_live')).toEqual(['ada@acme.com', 'added@example.com'])
    expect((await engine.reconcile()).diffs['prod']!.find((d) => d.stream === 'users')).toMatchObject({
      localOnly: [],
      remoteOnly: [],
    })

    // Status reports streams by their own names, whatever the tables are called.
    const status = (await engine.status())['prod']!
    expect(status.initialized).toBe(true)
    expect(status.streams.find((s) => s.stream === 'users')).toMatchObject({ rows: 4, deleted: 2 })

    await engine.refresh()
    expect(await lateOrg()).toBe(0)

    // The reads the contract offers besides the diff.
    expect(await destination.liveIds(users, 'prod')).toEqual([ada['id'], added['id']].sort())
    expect(await destination.rawRows(users, 'prod')).toHaveLength(4)

    const removed = await engine.redact(ada['id'] as string)
    expect(removed).toContainEqual({ table: 'users', rows: 1 })
    expect(removed).toContainEqual({ table: 'organization_memberships', rows: 1 })
    expect(removed).toContainEqual({ table: 'auth_attempts', rows: 1 })
    expect(removed.find((r) => r.table === 'events')!.rows).toBeGreaterThan(0)
    expect(await emails('workos_users_live')).toEqual(['added@example.com'])
    expect(await count('workos_events', `cast(\`_raw\` as char) like '%${String(ada['id'])}%'`)).toBe(0)

    // The lease: taken, refused to a second holder, renewed, seen by the tail, given back.
    expect(await destination.acquireLease('prod', 'old-process', 60_000)).toMatchObject({ acquired: true })
    expect(await destination.acquireLease('prod', 'new-process', 60_000)).toMatchObject({
      acquired: false,
      holder: 'old-process',
    })
    expect(await destination.acquireLease('prod', 'old-process', 60_000)).toMatchObject({ acquired: true })
    await expect(engine.tick()).rejects.toBeInstanceOf(LeaseHeldError)
    expect((await destination.getLease('prod'))?.holder).toBe('old-process')
    expect(await count('workos__sync_lease')).toBe(1)
    await destination.releaseLease('prod', 'old-process')
    expect(await destination.getLease('prod')).toBeNull()
    await engine.tick()
    expect(await count('workos__sync_lease')).toBe(0)

    // The engine called `init()` before every one of those; all but the
    // first found the fingerprint in the prefixed `_migrations`.
    expect(destination.lastMigration).toMatchObject({ applied: false })
    expect(await count('workos__migrations')).toBe(1)

    // Nothing was created beside the mirror, and no statement named anything
    // else: the staging tables of the diff are temporary, so only the
    // statements can show what they were called.
    expect(await objects()).toEqual(objectsOf(streams, PREFIX))
    for (const name of await objects()) expect(name.startsWith(PREFIX)).toBe(true)
    const named = namesIn(statements, db.schema)
    expect(named.filter((name) => !name.startsWith(PREFIX))).toEqual([])
    expect(named).toEqual(expect.arrayContaining(['workos__reconcile_local', 'workos__reconcile_remote']))
    expect(statements.some((sql) => sql.includes('`workos__sync_lease`.holder'))).toBe(true)
    expect(statements.join('\n')).not.toMatch(/`_(sync_state|sync_lease|migrations|rejected|columns|reconcile_\w+)`/)
  })

  it('maintains a prefixed schema: a new column, a drifted one, a view that is not its own', async () => {
    const target = { schema: db.schema, prefix: PREFIX }
    await migrate(db, [users], target)
    expect((await migrate(db, [users], target)).applied).toBe(false)
    await db.query(
      `insert into \`${db.schema}\`.workos_users (\`_env\`, \`_raw\`, \`_updated_at\`) values ('prod', ?, now(6))`,
      [JSON.stringify({ id: 'user_1', email: 'a@b.com', primary_email: 'primary@b.com', nickname: 'ada' })],
    )

    // Additive: the column is computed from the rows there, and the view,
    // recognised as the engine's own by its prefixed table, picks it up.
    const extended = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text' as const, path: 'nickname' }],
    }
    const added = await migrate(db, [extended], target)
    expect(added).toMatchObject({ applied: true, drift: [] })
    const viewed = await db.query<{ nickname: string }>(`select nickname from \`${db.schema}\`.workos_users_live`)
    expect(viewed.rows).toEqual([{ nickname: 'ada' }])

    // Drift: found against the prefixed `_columns`, rebuilt on the prefixed
    // table, and the engine's index goes back under its unprefixed name.
    const moved = {
      ...extended,
      columns: extended.columns.map((c) =>
        c.name === 'email' ? { name: 'email', type: 'text' as const, path: 'primary_email', index: true } : c,
      ),
    }
    const drifted = await migrate(db, [moved], target)
    expect(drifted.drift).toHaveLength(1)
    expect(drifted.drift[0]).toMatchObject({ stream: 'users', column: 'email', kind: 'expression' })
    const repaired = await migrate(db, [moved], target, undefined, { repair: true })
    expect(repaired).toMatchObject({ drift: [], repaired: [{ column: 'email' }] })
    expect(await emails('workos_users')).toEqual(['primary@b.com'])
    const indexes = await db.query<{ index_name: string }>(
      `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = 'workos_users'`,
      [db.schema],
    )
    expect(indexes.rows.map((i) => i.index_name)).toContain('users_email_idx')
    const recorded = await db.query<{ table_name: string }>(
      `select distinct table_name as table_name from \`${db.schema}\`.\`workos__columns\``,
    )
    expect(recorded.rows).toEqual([{ table_name: 'workos_users' }])

    // A view under the prefixed name that is somebody else's is refused, by that name.
    await db.exec(
      `create or replace view \`${db.schema}\`.workos_users_live as select id from \`${db.schema}\`.workos_users where email like '%@acme.com'`,
    )
    await db.query(`delete from \`${db.schema}\`.\`workos__migrations\``)
    await expect(migrate(db, [moved], target)).rejects.toThrow(
      new RegExp(`${db.schema}\\.workos_users_live is a view the engine did not generate`),
    )
    expect(await migrate(db, [moved], target, undefined, { repairViews: true })).toMatchObject({ applied: true })
    expect(await count('workos_users_live')).toBe(1)
    expect(await objects()).toEqual(objectsOf([users], PREFIX))
  })

  describe('beside an application that already has a users table', () => {
    // What PlanetScale reported: a branch is one database, the application's
    // `users` is in it, and `init` took that table for the mirror's.
    const application = async () => {
      await db.exec(`create database \`${db.schema}\``)
      await db.exec(`create table \`${db.schema}\`.users (id int primary key, email varchar(255), name varchar(255))`)
      await db.query(`insert into \`${db.schema}\`.users values (1, 'owner@app.example', 'App Owner')`)
    }
    const applicationTable = async () => {
      const columns = await db.query<{ column_name: string; column_type: string }>(
        `select column_name as column_name, column_type as column_type from information_schema.columns
         where table_schema = ? and table_name = 'users' order by ordinal_position`,
        [db.schema],
      )
      const rows = await db.query(`select * from \`${db.schema}\`.users`)
      return { columns: columns.rows, rows: rows.rows }
    }
    const untouched = {
      columns: [
        { column_name: 'id', column_type: 'int' },
        { column_name: 'email', column_type: 'varchar(255)' },
        { column_name: 'name', column_type: 'varchar(255)' },
      ],
      rows: [{ id: 1, email: 'owner@app.example', name: 'App Owner' }],
    }

    it('fails without a prefix, on the column the application table does not have', async () => {
      await application()
      fake.createUser({ email: 'ada@example.com' })
      const engine = makeEngine(new MysqlDestination({ db, schema: db.schema }), ['users'])
      await expect(engine.init()).rejects.toThrow(/Unknown column '_raw'/)
      await expect(engine.backfill()).rejects.toThrow(/_raw/)
      expect(await applicationTable()).toEqual(untouched)
    })

    it('mirrors beside it with a prefix, and leaves it as it was', async () => {
      await application()
      fake.createUser({ email: 'ada@example.com' })
      const engine = makeEngine(new MysqlDestination({ db, schema: db.schema, tablePrefix: PREFIX }), ['users'])
      await engine.init()
      await engine.backfill()
      fake.createUser({ email: 'grace@example.com' })
      await engine.tick()

      expect(await emails('workos_users_live')).toEqual(['ada@example.com', 'grace@example.com'])
      expect(await applicationTable()).toEqual(untouched)
      expect(await objects()).toEqual(['users', ...objectsOf(selectStreams(['users']), PREFIX)].sort())
      // Which is the point of sharing the database: one join away.
      await db.query(`update \`${db.schema}\`.users set email = 'ada@example.com' where id = 1`)
      const joined = await db.query<{ name: string; workos_id: string }>(
        `select a.name, w.id as workos_id from \`${db.schema}\`.users a
         join \`${db.schema}\`.workos_users_live w on w.email = a.email`,
      )
      expect(joined.rows).toHaveLength(1)
      expect(joined.rows[0]!.name).toBe('App Owner')
      expect(joined.rows[0]!.workos_id).toMatch(/^user_/)
    })
  })

  it('keeps two mirrors under different prefixes apart, rows and bookkeeping', async () => {
    const other = new FakeWorkOS()
    fake.createUser({ email: 'a@example.com' })
    other.createUser({ email: 'b@example.com' })
    // Same database, same sync id, same environment name: only the prefix differs.
    const a = new MysqlDestination({ db, schema: db.schema, tablePrefix: 'a_' })
    const b = new MysqlDestination({ db, schema: db.schema, tablePrefix: 'b_' })
    const engineA = makeEngine(a, ['users'], fake)
    const engineB = makeEngine(b, ['users'], other)
    await engineA.backfill()
    await engineB.backfill()
    fake.createUser({ email: 'a2@example.com' })
    await engineA.tick()

    const streams = selectStreams(['users'])
    expect(await objects()).toEqual([...objectsOf(streams, 'a_'), ...objectsOf(streams, 'b_')].sort())
    expect(await emails('a_users_live')).toEqual(['a2@example.com', 'a@example.com'])
    expect(await emails('b_users_live')).toEqual(['b@example.com'])
    expect((await engineA.status())['prod']!.streams.find((s) => s.stream === 'users')!.rows).toBe(2)
    expect((await engineB.status())['prod']!.streams.find((s) => s.stream === 'users')!.rows).toBe(1)

    // Cursors and run state.
    const statusA = (await engineA.status())['prod']!
    const statusB = (await engineB.status())['prod']!
    expect(statusA.cursor).not.toBeNull()
    expect(statusA.cursor).not.toBe(statusB.cursor)
    await a.setState('prod', 'probe', { n: 1 })
    expect(await a.getState('prod', 'probe')).toEqual({ n: 1 })
    expect(await b.getState('prod', 'probe')).toBeNull()

    // The lease: one mirror holding it does not hold the other's.
    expect((await a.acquireLease('prod', 'holder-a', 60_000)).acquired).toBe(true)
    expect(await b.getLease('prod')).toBeNull()
    expect((await b.acquireLease('prod', 'holder-b', 60_000)).acquired).toBe(true)
    expect((await a.getLease('prod'))?.holder).toBe('holder-a')
    await a.releaseLease('prod', 'holder-a')
    expect((await b.getLease('prod'))?.holder).toBe('holder-b')
    await b.releaseLease('prod', 'holder-b')

    // The quarantine.
    const event = { id: 'event_01', event: 'user.created', data: { id: 'user_x' }, created_at: '2026-01-01T00:00:00Z' }
    await a.reject('prod', [{ event, error: 'boom' }])
    expect(await a.rejectedCount('prod')).toBe(1)
    expect(await b.rejectedCount('prod')).toBe(0)
    expect(await b.rejected('prod')).toEqual([])
    expect(await b.clearRejected('prod', ['event_01'])).toBe(0)
    expect(await a.clearRejected('prod', ['event_01'])).toBe(1)

    // Each recorded its own fingerprint, in its own table.
    const fingerprints = async (table: string) =>
      (await db.query<{ fingerprint: string }>(`select fingerprint from \`${db.schema}\`.\`${table}\``)).rows.map(
        (r) => r.fingerprint,
      )
    expect(await fingerprints('a__migrations')).toEqual([fingerprint(streams, { schema: db.schema, prefix: 'a_' })])
    expect(await fingerprints('b__migrations')).toEqual([fingerprint(streams, { schema: db.schema, prefix: 'b_' })])

    // Erasing a subject from one leaves the other's rows alone.
    const subject = [...fake.users.keys()][0]!
    expect(await engineB.redact(subject)).toEqual([])
    expect(await engineA.redact(subject)).toContainEqual({ table: 'users', rows: 1 })
    expect(await count('a_users')).toBe(1)
    expect(await count('b_users')).toBe(1)
  })
})
