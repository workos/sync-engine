import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { migrate, fingerprint, DDL_VERSION } from '../src/schema/migrate.js'
import {
  buildSchemaSql,
  safeIdentifier,
  columnExpression,
  columnType,
  createTableSql,
  indexSpecs,
  generatedColumnSql,
  jsonPathArgs,
} from '../src/schema/ddl.js'
import { liveViewName, liveViewSql, isGeneratedDefinition } from '../src/schema/views.js'
import { STREAMS, streamByName, type Logger } from '@workos/sync-engine'

const users = streamByName('users')!

describe('DDL generation', () => {
  it('projects every typed column out of _raw as a DEFAULT, never writing them directly', () => {
    const sql = createTableSql(users, 'workos')
    expect(sql).toContain('`_raw`       String')
    for (const col of users.columns) expect(sql).toMatch(new RegExp(`\`${col.name}\` [^\\n]* DEFAULT `))
    expect(sql).not.toContain('MATERIALIZED')
  })

  it('versions mirror tables by the source timestamp and keeps append logs plain', () => {
    expect(createTableSql(users, 'workos')).toContain(
      'ENGINE = ReplacingMergeTree(`_updated_at`) ORDER BY (`_env`, `id`)',
    )
    expect(createTableSql(users, 'workos')).toContain('`_deleted_at`')
    const events = createTableSql(streamByName('events')!, 'workos')
    expect(events).toContain('ENGINE = MergeTree ORDER BY (`_env`, `id`)')
    expect(events).not.toContain('`_deleted_at`')
    const memberships = createTableSql(streamByName('directory_group_memberships')!, 'workos')
    expect(memberships).toContain('ORDER BY (`_env`, `directory_group_id`, `directory_user_id`)')
  })

  it('keeps keys non-nullable and everything else nullable', () => {
    expect(generatedColumnSql({ name: 'id', type: 'text', path: 'id' }, users)).toBe(
      "`id` String DEFAULT JSONExtractString(`_raw`, 'id')",
    )
    expect(generatedColumnSql({ name: 'email', type: 'text', path: 'email' }, users)).toBe(
      "`email` Nullable(String) DEFAULT JSONExtract(`_raw`, 'email', 'Nullable(String)')",
    )
    expect(columnType({ name: 'x', type: 'timestamptz', path: 'x' }, false)).toBe("Nullable(DateTime64(3, 'UTC'))")
    expect(columnType({ name: 'x', type: 'numeric', path: 'x' }, false)).toBe('Nullable(Decimal(38, 10))')
  })

  it('walks nested paths one argument per step, quoting them', () => {
    expect(jsonPathArgs(['role', 'slug'])).toBe("'role', 'slug'")
    expect(jsonPathArgs(`it's`)).toBe(`'it\\'s'`)
    expect(columnExpression({ name: 'x', type: 'jsonb', path: ['a', 'b'] }, false)).toBe(
      "if(JSONHas(`_raw`, 'a', 'b'), JSONExtractRaw(`_raw`, 'a', 'b'), NULL)",
    )
    expect(columnExpression({ name: 'x', type: 'timestamptz', path: 'created_at' }, false)).toBe(
      "parseDateTime64BestEffortOrNull(JSONExtractString(`_raw`, 'created_at'), 3, 'UTC')",
    )
  })

  it('turns indexed columns into skipping indexes of the right kind', () => {
    expect(indexSpecs(users)).toContainEqual({
      name: 'users_email_idx',
      column: 'email',
      type: 'bloom_filter',
      granularity: 4,
    })
    expect(indexSpecs(streamByName('events')!)).toContainEqual({
      name: 'events_created_at_idx',
      column: 'created_at',
      type: 'minmax',
      granularity: 1,
    })
    expect(indexSpecs(users).map((i) => i.column)).toContain('_deleted_at')
    expect(indexSpecs(streamByName('events')!).map((i) => i.column)).not.toContain('_deleted_at')
  })

  it('declares no foreign keys between mirror tables', () => {
    const all = buildSchemaSql(STREAMS, 'workos').join('\n')
    expect(all.toLowerCase()).not.toContain('foreign key')
    for (const sql of buildSchemaSql(STREAMS, 'workos')) expect(sql).toMatch(/IF NOT EXISTS|OR REPLACE/)
  })

  it('shortens identifiers longer than the shared limit', () => {
    const long = 'a'.repeat(80)
    expect(safeIdentifier(long)).toHaveLength(63)
    expect(safeIdentifier(long)).not.toBe(safeIdentifier(long + 'b'))
    expect(safeIdentifier('users')).toBe('users')
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
        expect(sql).toEqual([
          `CREATE OR REPLACE VIEW \`workos\`.\`${s.name}_live\` AS SELECT * FROM \`workos\`.\`${s.name}\` FINAL WHERE \`_deleted_at\` IS NULL`,
        ])
      }
    }
    expect(liveViewName({ ...users, name: 'x'.repeat(70) })).toHaveLength(63)
  })

  it('recognises its own view definition by shape', () => {
    const ours = 'SELECT * FROM workos.users FINAL WHERE _deleted_at IS NULL'
    expect(isGeneratedDefinition(ours, 'workos', 'users')).toBe(true)
    expect(
      isGeneratedDefinition('SELECT * FROM `workos`.`users` FINAL WHERE `_deleted_at` IS NULL', 'workos', 'users'),
    ).toBe(true)
    expect(isGeneratedDefinition(ours.replace('IS NULL', 'IS NOT NULL'), 'workos', 'users')).toBe(false)
    expect(isGeneratedDefinition(ours, 'workos', 'organizations')).toBe(false)
  })
})

describeDb('migrations against real ClickHouse', () => {
  let db: TestDb

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  const insertUser = (raw: Record<string, unknown>) =>
    db.insert(`\`${db.schema}\`.users`, [
      { _env: 'prod', _raw: JSON.stringify(raw), _updated_at: '2026-01-01 00:00:00.000' },
    ])
  const tables = async () =>
    (
      await db.query<{ name: string }>(`SELECT name FROM system.tables WHERE database = {s:String} ORDER BY name`, {
        s: db.schema,
      })
    ).map((r) => r.name)

  it('creates every stream table with working projected columns, and records them', async () => {
    await migrate(db, STREAMS, db.schema)
    const names = await tables()
    for (const s of STREAMS) expect(names).toContain(s.name)
    for (const t of ['_sync', '_migrations', '_rejected', '_columns']) expect(names).toContain(t)
    const recorded = await db.query<{ n: string }>(`SELECT toString(count()) AS n FROM \`${db.schema}\`.\`_columns\``)
    expect(Number(recorded[0]!.n)).toBe(STREAMS.reduce((n, s) => n + s.columns.length, 0))
  })

  it('is a no-op on the second run', async () => {
    const first = await migrate(db, STREAMS, db.schema)
    const second = await migrate(db, STREAMS, db.schema)
    expect(first.applied).toBe(true)
    expect(second.applied).toBe(false)
    expect(second.statements).toBe(0)
  })

  it('creates the database when it is missing and leaves it alone when it is not', async () => {
    await db.command(`CREATE DATABASE \`${db.schema}\``)
    await db.command(`CREATE TABLE \`${db.schema}\`.keep (id Int32) ENGINE = MergeTree ORDER BY id`)
    await migrate(db, [users], db.schema)
    expect(await tables()).toContain('keep')
  })

  it('adds a newly projected column, computed for the rows already there, and the view picks it up', async () => {
    await migrate(db, [users], db.schema)
    await insertUser({ id: 'user_1', email: 'a@b.com', nickname: 'ada' })
    const extended = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text' as const, path: 'nickname' }],
    }
    const r = await migrate(db, [extended], db.schema)
    expect(r.applied).toBe(true)
    expect(r.drift).toEqual([])
    const row = await db.query<{ nickname: string }>(`SELECT nickname FROM \`${db.schema}\`.users_live`)
    expect(row[0]?.nickname).toBe('ada')
  })

  it('adds a missing index on a table that already exists', async () => {
    await migrate(db, [users], db.schema)
    await db.command(`ALTER TABLE \`${db.schema}\`.users DROP INDEX users_email_idx`)
    await db.command(
      `ALTER TABLE \`${db.schema}\`.\`_migrations\` DELETE WHERE 1`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )
    const r = await migrate(db, [users], db.schema)
    expect(r.applied).toBe(true)
    const idx = await db.query<{ name: string }>(
      `SELECT name FROM system.data_skipping_indices WHERE database = {s:String} AND table = 'users'`,
      { s: db.schema },
    )
    expect(idx.map((i) => i.name)).toContain('users_email_idx')
  })

  it('parses the ISO-8601 formats WorkOS emits, and nulls anything else', async () => {
    await migrate(db, [users], db.schema)
    const cases: [string | null, string | null][] = [
      ['2026-01-01T00:00:00.000Z', '2026-01-01 00:00:00.000'],
      ['2026-08-06T17:44:03.481Z', '2026-08-06 17:44:03.481'],
      ['2026-01-01T00:00:00Z', '2026-01-01 00:00:00.000'],
      ['2026-01-01T00:00:00.123456789Z', '2026-01-01 00:00:00.123'],
      ['2026-01-01T05:00:00+02:00', '2026-01-01 03:00:00.000'],
      ['2024-02-29T00:00:00Z', '2024-02-29 00:00:00.000'],
      ['not-a-timestamp', null],
      [null, null],
      ['2026-02-30T12:00:00Z', null],
      ['2026-13-01T00:00:00Z', null],
      // Best-effort parsing reads 24:00 as the next midnight, where Postgres refuses it.
      ['2026-01-01T24:00:00Z', '2026-01-02 00:00:00.000'],
    ]
    for (const [input, expected] of cases) {
      const id = `user_${String(input)}`
      await insertUser({ id, created_at: input })
      const r = await db.query<{ created_at: string | null }>(
        `SELECT created_at FROM \`${db.schema}\`.users FINAL WHERE id = {id:String}`,
        { id },
      )
      expect([input, r[0]?.created_at ?? null]).toEqual([input, expected])
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
      `SELECT id, toString(n) AS n, d, flag, metadata FROM \`${db.schema}\`.users FINAL ORDER BY id`,
    )
    expect(String(rows[0]!['n'])).toBe('9007199254740993')
    expect(Number(rows[0]!['d'])).toBe(12.5)
    expect(rows[0]!['flag']).toBe(false)
    expect(JSON.parse(rows[0]!['metadata'] as string)).toEqual({ a: 1 })
    expect(rows[1]).toMatchObject({ n: null, d: null, flag: null, metadata: null })
  })

  it('serialises nothing: two concurrent migrations both finish with one schema', async () => {
    const results = await Promise.all([migrate(db, STREAMS, db.schema), migrate(db, STREAMS, db.schema)])
    expect(results.map((r) => r.applied)).toContain(true)
    expect((await migrate(db, STREAMS, db.schema)).applied).toBe(false)
  })
})

describeDb('live views against real ClickHouse', () => {
  let db: TestDb

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  const views = async () =>
    (
      await db.query<{ name: string }>(
        `SELECT name FROM system.tables WHERE database = {s:String} AND engine = 'View' ORDER BY name`,
        { s: db.schema },
      )
    ).map((r) => r.name)
  const forget = () =>
    db.command(
      `ALTER TABLE \`${db.schema}\`.\`_migrations\` DELETE WHERE 1`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )

  it('creates a view for every mirror stream and none for the append-only logs', async () => {
    await migrate(db, STREAMS, db.schema)
    const names = await views()
    for (const s of STREAMS) {
      if (s.mode === 'append') expect(names).not.toContain(`${s.name}_live`)
      else expect(names).toContain(`${s.name}_live`)
    }
  })

  it('collapses versions and excludes tombstoned rows, and nothing else', async () => {
    await migrate(db, [users], db.schema)
    await db.insert(`\`${db.schema}\`.users`, [
      { _env: 'prod', _raw: JSON.stringify({ id: 'live', email: 'old@x' }), _updated_at: '2026-01-01 00:00:00.000' },
      { _env: 'prod', _raw: JSON.stringify({ id: 'live', email: 'new@x' }), _updated_at: '2026-01-02 00:00:00.000' },
      {
        _env: 'prod',
        _raw: JSON.stringify({ id: 'gone' }),
        _updated_at: '2026-01-01 00:00:00.000',
        _deleted_at: '2026-01-03 00:00:00.000',
      },
    ])
    const live = await db.query<{ id: string; email: string }>(`SELECT id, email FROM \`${db.schema}\`.users_live`)
    expect(live).toEqual([{ id: 'live', email: 'new@x' }])
  })

  it('re-creates a view that was dropped once the schema is applied again', async () => {
    await migrate(db, [users], db.schema)
    await db.command(`DROP VIEW \`${db.schema}\`.users_live`)
    await forget()
    await migrate(db, [users], db.schema)
    expect(await views()).toContain('users_live')
  })

  it('refuses to replace a view under its name that it did not generate, until repairViews', async () => {
    await migrate(db, [users], db.schema)
    await db.command(
      `CREATE OR REPLACE VIEW \`${db.schema}\`.users_live AS SELECT id FROM \`${db.schema}\`.users WHERE email LIKE '%@acme.com'`,
    )
    await forget()
    await expect(migrate(db, [users], db.schema)).rejects.toThrow(/did not generate/)
    const lines: string[] = []
    const logger: Logger = { debug() {}, info() {}, warn: (m) => lines.push(m), error() {} }
    await migrate(db, [users], db.schema, logger, { repairViews: true })
    expect(lines).toContain('replacing a view that is not the generated shape')
    const r = await db.query<{ as_select: string }>(
      `SELECT as_select FROM system.tables WHERE database = {s:String} AND name = 'users_live'`,
      {
        s: db.schema,
      },
    )
    expect(isGeneratedDefinition(r[0]!.as_select, db.schema, 'users')).toBe(true)
  })

  it('refuses a table standing where its view would go', async () => {
    await db.command(`CREATE DATABASE \`${db.schema}\``)
    await db.command(`CREATE TABLE \`${db.schema}\`.users_live (id Int32) ENGINE = MergeTree ORDER BY id`)
    await expect(migrate(db, [users], db.schema)).rejects.toThrow(/is a table/)
  })
})
