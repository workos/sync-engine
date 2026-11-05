import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { migrate, fingerprint, DDL_VERSION, lockName } from '../src/schema/migrate.js'
import {
  buildSchemaSql,
  safeIdentifier,
  columnExpression,
  createTableSql,
  iso8601Expression,
  jsonPath,
  indexSpecs,
  generatedColumnSql,
} from '../src/schema/ddl.js'
import { liveViewName, liveViewSql, isGeneratedDefinition } from '../src/schema/views.js'
import { STREAMS, streamByName, type Logger } from '@workos/sync-engine'

const users = streamByName('users')!

describe('DDL generation', () => {
  it('projects every typed column out of _raw, never writing them directly', () => {
    const sql = createTableSql(users, 'workos')
    expect(sql).toContain('`_raw`       json not null')
    for (const col of users.columns) {
      expect(sql).toMatch(new RegExp(`\`${col.name}\`[^,]*generated always as`))
    }
  })

  it('bounds key columns and leaves other text columns unbounded', () => {
    expect(generatedColumnSql({ name: 'id', type: 'text', path: 'id' }, users)).toContain('`id` varchar(191)')
    expect(generatedColumnSql({ name: 'email', type: 'text', path: 'email' }, users)).toContain('`email` text')
    // Indexed TEXT columns get a prefix; VARCHAR keys and other types do not.
    expect(indexSpecs(users)).toContainEqual({ name: 'users_email_idx', column: 'email', prefix: 255 })
    expect(indexSpecs(streamByName('events')!)).toContainEqual({ name: 'events_created_at_idx', column: 'created_at' })
  })

  it('reads JSON with json_value so a JSON null is a SQL NULL, and never raises on a bad cast', () => {
    for (const type of ['text', 'boolean', 'bigint', 'numeric', 'timestamptz'] as const) {
      const expr = columnExpression({ name: 'x', type, path: 'x' }, false)
      expect(expr).toContain('json_value(')
      expect(expr).toContain('null on error')
    }
    expect(columnExpression({ name: 'x', type: 'jsonb', path: 'x' }, false)).toBe('(json_extract(`_raw`, \'$."x"\'))')
  })

  it('walks nested paths for columns like role.slug, quoting every step', () => {
    expect(jsonPath(['role', 'slug'])).toBe(`'$."role"."slug"'`)
    expect(jsonPath('a.b c')).toBe(`'$."a.b c"'`)
    expect(jsonPath(`it's`)).toBe(`'$."it''s"'`)
    expect(jsonPath('say "hi"')).toBe(`'$."say \\\\"hi\\\\""'`)
    const expr = columnExpression({ name: 'role_slug', type: 'text', path: ['role', 'slug'] }, false)
    expect(expr).toContain(`'$."role"."slug"'`)
  })

  it('builds the timestamp expression from deterministic functions only', () => {
    const expr = iso8601Expression('v')
    for (const banned of ['convert_tz', 'now(', 'sysdate', 'rand(', 'uuid(']) {
      expect(expr.toLowerCase()).not.toContain(banned)
    }
    expect(expr).toContain('str_to_date')
    expect(expr).toContain('timestampadd')
  })

  it('declares no foreign keys between mirror tables', () => {
    const all = buildSchemaSql(STREAMS, 'workos').join('\n')
    expect(all.toLowerCase()).not.toContain('references')
    expect(all.toLowerCase()).not.toContain('foreign key')
  })

  it('shortens identifiers longer than MySQL allows', () => {
    const long = 'a'.repeat(80)
    const safe = safeIdentifier(long)
    expect(safe.length).toBe(64)
    expect(safe).not.toBe(safeIdentifier(long + 'b'))
    expect(safeIdentifier('users')).toBe('users')
    expect(lockName('x'.repeat(100)).length).toBeLessThanOrEqual(64)
    expect(lockName('workos')).toBe('workos-sync-mysql:migrate:workos')
  })

  it('changes the fingerprint when the catalog changes', () => {
    const a = fingerprint([users], 'workos')
    const b = fingerprint([{ ...users, columns: [...users.columns, { name: 'x', type: 'text', path: 'x' }] }], 'workos')
    expect(a).not.toBe(b)
    expect(fingerprint([users], 'other')).not.toBe(a)
    expect(typeof DDL_VERSION).toBe('number')
  })

  it('generates a live view for every stream that has tombstones, and no other', () => {
    for (const s of STREAMS) {
      const sql = liveViewSql(s, 'workos')
      if (s.mode === 'append') expect(sql).toEqual([])
      else {
        expect(sql).toHaveLength(1)
        expect(sql[0]).toBe(
          `create or replace view \`workos\`.\`${s.name}_live\` as select * from \`workos\`.\`${s.name}\` where \`_deleted_at\` is null`,
        )
      }
    }
    expect(liveViewName({ ...users, name: 'x'.repeat(70) })).toHaveLength(64)
  })

  it('recognises its own view definition by shape', () => {
    const ours =
      'select `workos`.`users`.`id` AS `id` from `workos`.`users` where (`workos`.`users`.`_deleted_at` is null)'
    expect(isGeneratedDefinition(ours, 'workos', 'users')).toBe(true)
    expect(isGeneratedDefinition(ours.replace('is null', 'is not null'), 'workos', 'users')).toBe(false)
    expect(isGeneratedDefinition(ours, 'workos', 'organizations')).toBe(false)
  })
})

describeDb('migrations against real MySQL', () => {
  let db: TestDb

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  const insertUser = (raw: Record<string, unknown>) =>
    db.query(`insert into \`${db.schema}\`.users (\`_env\`, \`_raw\`, \`_updated_at\`) values ('prod', ?, now(6))`, [
      JSON.stringify(raw),
    ])

  it('creates every stream table with working generated columns', async () => {
    await migrate(db, STREAMS, db.schema)
    const tables = await db.query<{ table_name: string }>(
      `select table_name as table_name from information_schema.tables where table_schema = ? order by table_name`,
      [db.schema],
    )
    const names = tables.rows.map((r) => r.table_name)
    for (const s of STREAMS) expect(names).toContain(s.name)
    for (const t of ['_sync_state', '_migrations', '_sync_lease', '_rejected', '_columns']) expect(names).toContain(t)
    // Every generated column was recorded, so drift has something to compare against.
    const recorded = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.\`_columns\``)
    expect(Number(recorded.rows[0]!.n)).toBe(STREAMS.reduce((n, s) => n + s.columns.length, 0))
  })

  it('is a no-op on the second run', async () => {
    const first = await migrate(db, STREAMS, db.schema)
    const second = await migrate(db, STREAMS, db.schema)
    expect(first.applied).toBe(true)
    expect(second.applied).toBe(false)
    expect(second.statements).toBe(0)
  })

  it('creates the database when it is missing and leaves it alone when it is not', async () => {
    await db.exec(`create database \`${db.schema}\` character set utf8mb4 collate utf8mb4_bin`)
    await db.exec(`create table \`${db.schema}\`.keep (id int primary key)`)
    await migrate(db, [users], db.schema)
    const keep = await db.query(`select 1 from \`${db.schema}\`.keep`)
    expect(keep.rows).toEqual([])
  })

  it('adds a newly projected column without a backfill, and the view picks it up', async () => {
    await migrate(db, [users], db.schema)
    await insertUser({ id: 'user_1', email: 'a@b.com', nickname: 'ada' })
    const extended = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text' as const, path: 'nickname' }],
    }
    const r = await migrate(db, [extended], db.schema)
    expect(r.applied).toBe(true)
    expect(r.drift).toEqual([])
    const row = await db.query<{ nickname: string }>(`select nickname from \`${db.schema}\`.users_live`)
    expect(row.rows[0]?.nickname).toBe('ada')
  })

  it('adds a missing index on a table that already exists', async () => {
    await migrate(db, [users], db.schema)
    await db.exec(`alter table \`${db.schema}\`.users drop index users_email_idx`)
    // The fingerprint hashes the catalog, not the live schema, so a new
    // catalog (or a forgotten fingerprint) is what makes init look again.
    await db.query(`delete from \`${db.schema}\`.\`_migrations\``)
    const r = await migrate(db, [users], db.schema)
    expect(r.applied).toBe(true)
    const idx = await db.query<{ index_name: string }>(
      `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = 'users'`,
      [db.schema],
    )
    expect(idx.rows.map((i) => i.index_name)).toContain('users_email_idx')
  })

  it('parses the ISO-8601 formats WorkOS emits, and nulls anything else', async () => {
    await migrate(db, [users], db.schema)
    const cases: [string | null, string | null][] = [
      ['2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'],
      ['2026-08-06T17:44:03.481Z', '2026-08-06T17:44:03.481Z'],
      ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00.000Z'],
      ['2026-01-01T00:00:00.123456789Z', '2026-01-01T00:00:00.123Z'],
      ['2026-01-01T05:00:00+02:00', '2026-01-01T03:00:00.000Z'],
      ['2026-01-01T05:00:00-0130', '2026-01-01T06:30:00.000Z'],
      ['2026-01-01 05:00:00', '2026-01-01T05:00:00.000Z'],
      ['2024-02-29T00:00:00Z', '2024-02-29T00:00:00.000Z'],
      ['2000-02-29T00:00:00Z', '2000-02-29T00:00:00.000Z'],
      ['not-a-timestamp', null],
      [null, null],
      // These match the pattern but are not real instants. `str_to_date`
      // raises on them under strict mode, and a generated column that raises
      // rejects the whole row — including the quarantine write that would set
      // that row aside. So each field is range-checked first.
      ['2026-02-30T12:00:00Z', null],
      ['2026-02-29T00:00:00Z', null],
      ['1900-02-29T00:00:00Z', null],
      ['2026-04-31T00:00:00Z', null],
      ['2026-13-01T00:00:00Z', null],
      ['2026-00-01T00:00:00Z', null],
      ['2026-01-00T00:00:00Z', null],
      ['2026-01-01T24:00:00Z', null],
      ['2026-01-01T23:60:00Z', null],
      ['2026-01-01T23:59:60Z', null],
      ['0000-01-01T00:00:00Z', null],
    ]
    for (const [input, expected] of cases) {
      const id = `user_${String(input)}`
      await insertUser({ id, created_at: input })
      const r = await db.query<{ created_at: Date | null }>(
        `select created_at from \`${db.schema}\`.users where id = ?`,
        [id],
      )
      const got = r.rows[0]?.created_at
      expect([input, got ? new Date(got).toISOString() : null]).toEqual([input, expected])
    }
  })

  it('projects booleans, integers, decimals and JSON the way the catalog types say', async () => {
    const stream = {
      ...users,
      columns: [
        ...users.columns,
        { name: 'n', type: 'bigint' as const, path: 'n' },
        { name: 'd', type: 'numeric' as const, path: 'd' },
        { name: 'flag', type: 'boolean' as const, path: ['meta', 'flag'] },
      ],
    }
    await migrate(db, [stream], db.schema)
    await insertUser({
      id: 'u1',
      n: 9007199254740993n.toString(),
      d: '12.50',
      meta: { flag: false, k: [1, 'two'] },
      metadata: { a: 1 },
    })
    await insertUser({ id: 'u2', n: 'twelve', d: 'cheap', meta: { flag: 'maybe' } })
    const rows = await db.query<Record<string, unknown>>(
      `select id, n, d, flag, metadata from \`${db.schema}\`.users order by id`,
    )
    expect(String(rows.rows[0]!['n'])).toBe('9007199254740993')
    expect(Number(rows.rows[0]!['d'])).toBe(12.5)
    expect(rows.rows[0]!['flag']).toBe(0)
    expect(rows.rows[0]!['metadata']).toEqual({ a: 1 })
    expect(rows.rows[1]).toMatchObject({ n: null, d: null, flag: null, metadata: null })
  })

  it('indexes the columns marked for it', async () => {
    await migrate(db, [users], db.schema)
    const idx = await db.query<{ index_name: string }>(
      `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = 'users'`,
      [db.schema],
    )
    const names = idx.rows.map((r) => r.index_name)
    expect(names).toContain('users_email_idx')
    expect(names).toContain('users_external_id_idx')
    expect(names).toContain('users_synced_at_idx')
    expect(names).toContain('users_deleted_at_idx')
  })

  it('serialises concurrent migrations with a named lock', async () => {
    const results = await Promise.all([migrate(db, STREAMS, db.schema), migrate(db, STREAMS, db.schema)])
    expect(results.map((r) => r.applied).sort()).toEqual([false, true])
  })
})

describeDb('live views against real MySQL', () => {
  let db: TestDb

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  const views = async () => {
    const r = await db.query<{ table_name: string }>(
      `select table_name as table_name from information_schema.views where table_schema = ? order by table_name`,
      [db.schema],
    )
    return r.rows.map((x) => x.table_name)
  }

  it('creates a view for every mirror stream and none for the append-only logs', async () => {
    await migrate(db, STREAMS, db.schema)
    const names = await views()
    for (const s of STREAMS) {
      if (s.mode === 'append') expect(names).not.toContain(`${s.name}_live`)
      else expect(names).toContain(`${s.name}_live`)
    }
  })

  it('excludes tombstoned rows and nothing else', async () => {
    await migrate(db, [users], db.schema)
    await db.query(
      `insert into \`${db.schema}\`.users (\`_env\`, \`_raw\`, \`_updated_at\`, \`_deleted_at\`) values ('prod', ?, now(6), null), ('prod', ?, now(6), now(6))`,
      [JSON.stringify({ id: 'live' }), JSON.stringify({ id: 'gone' })],
    )
    const live = await db.query<{ id: string }>(`select id from \`${db.schema}\`.users_live`)
    expect(live.rows.map((r) => r.id)).toEqual(['live'])
  })

  it('re-creates a view that was dropped once the schema is applied again', async () => {
    await migrate(db, [users], db.schema)
    await db.exec(`drop view \`${db.schema}\`.users_live`)
    await db.query(`delete from \`${db.schema}\`.\`_migrations\``)
    await migrate(db, [users], db.schema)
    expect(await views()).toContain('users_live')
  })

  it('refuses to replace a view under its name that it did not generate, until repairViews', async () => {
    await migrate(db, [users], db.schema)
    await db.exec(
      `create or replace view \`${db.schema}\`.users_live as select id from \`${db.schema}\`.users where email like '%@acme.com'`,
    )
    await db.query(`delete from \`${db.schema}\`.\`_migrations\``)
    const bumped = users
    await expect(migrate(db, [bumped], db.schema)).rejects.toThrow(/did not generate/)
    const lines: string[] = []
    const logger: Logger = { debug() {}, info() {}, warn: (m) => lines.push(m), error() {} }
    await migrate(db, [bumped], db.schema, logger, { repairViews: true })
    expect(lines).toContain('replacing a view that is not the generated shape')
    const r = await db.query<{ view_definition: string }>(
      `select view_definition as view_definition from information_schema.views where table_schema = ? and table_name = 'users_live'`,
      [db.schema],
    )
    expect(isGeneratedDefinition(r.rows[0]!.view_definition, db.schema, 'users')).toBe(true)
  })

  it('refuses a table standing where its view would go', async () => {
    await db.exec(`create database \`${db.schema}\``)
    await db.exec(`create table \`${db.schema}\`.users_live (id int primary key)`)
    await expect(migrate(db, [users], db.schema)).rejects.toThrow(/is a table/)
  })
})
