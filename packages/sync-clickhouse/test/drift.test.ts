/**
 * Projected-column drift, against a real ClickHouse. Detection compares the
 * DDL the engine recorded in `_columns` with what the catalog would emit
 * now, and the live type with the catalog's; only a real server can show
 * that a repair redefines the column for the rows already there.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createTestDb, describeDb, type TestDb } from './helpers/clickhouse.js'
import { migrate, fingerprint } from '../src/schema/migrate.js'
import { detectColumnDrift, unrepairableReason } from '../src/schema/drift.js'
import { main } from '../src/cli.js'
import { STREAMS, streamByName, type ColumnDef, type Logger, type StreamDef } from '@workos/sync-engine'

const users = streamByName('users')!

/** The stream with one column swapped out, the way a catalog release would change it. */
const withColumn = (stream: StreamDef, col: ColumnDef): StreamDef => ({
  ...stream,
  columns: stream.columns.map((c) => (c.name === col.name ? col : c)),
})

/** `email` projected from a different key: the classic path change. */
const usersMovedEmail = withColumn(users, { name: 'email', type: 'text', path: 'primary_email', index: true })

const capture = () => {
  const lines: { level: string; msg: string; fields?: Record<string, unknown> }[] = []
  const at = (level: string) => (msg: string, fields?: Record<string, unknown>) => {
    lines.push({ level, msg, fields })
  }
  const logger: Logger = { debug: at('debug'), info: at('info'), warn: at('warn'), error: at('error') }
  return { logger, lines }
}

describe('unrepairable reasons', () => {
  it('names why a column is left alone', () => {
    const base = { stream: 'users', column: 'x', expected: 'e', actual: 'a', key: false }
    expect(unrepairableReason({ ...base, kind: 'orphan' })).toMatch(/no longer names/)
    expect(unrepairableReason({ ...base, kind: 'not_generated' })).toMatch(/not a DEFAULT/)
    expect(unrepairableReason({ ...base, kind: 'expression', key: true })).toMatch(/key columns/)
    expect(unrepairableReason({ ...base, kind: 'expression' })).toBeNull()
    expect(unrepairableReason({ ...base, kind: 'type' })).toBeNull()
  })
})

describeDb('column drift detection', () => {
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
  const recorded = async (streams: StreamDef[]) =>
    (
      await db.query(`SELECT 1 FROM \`${db.schema}\`.\`_migrations\` WHERE fingerprint = {fp:String}`, {
        fp: fingerprint(streams, db.schema),
      })
    ).length > 0
  const columnExists = async (column: string) =>
    (
      await db.query(
        `SELECT 1 FROM system.columns WHERE database = {s:String} AND table = 'users' AND name = {c:String}`,
        {
          s: db.schema,
          c: column,
        },
      )
    ).length > 0

  it('reports nothing on a fresh schema built from the full catalog', async () => {
    await migrate(db, STREAMS, db.schema)
    expect(await detectColumnDrift(db, STREAMS, db.schema)).toEqual([])
  })

  it('reports a changed path as expression drift, with both clauses', async () => {
    await migrate(db, [users], db.schema)
    const r = await migrate(db, [usersMovedEmail], db.schema)
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ stream: 'users', column: 'email', kind: 'expression', key: false })
    expect(r.drift[0]!.expected).toContain("'primary_email'")
    expect(r.drift[0]!.actual).toContain("'email'")
  })

  it('reports a changed type as type drift', async () => {
    await migrate(db, [users], db.schema)
    const changed = withColumn(users, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], db.schema)
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'first_name', kind: 'type', actual: 'Nullable(String)' })
    expect(r.drift[0]!.expected).toContain('`first_name` Nullable(Bool)')
  })

  it('withholds the fingerprint while drift is unrepaired, and records it once repaired', async () => {
    await migrate(db, [users], db.schema)
    await migrate(db, [usersMovedEmail], db.schema)
    expect(await recorded([usersMovedEmail])).toBe(false)
    const again = await migrate(db, [usersMovedEmail], db.schema)
    expect(again.applied).toBe(true)
    expect(again.drift).toHaveLength(1)
    const fixed = await migrate(db, [usersMovedEmail], db.schema, undefined, { repair: true })
    expect(fixed.drift).toEqual([])
    expect(fixed.repaired).toHaveLength(1)
    expect(await recorded([usersMovedEmail])).toBe(true)
    expect((await migrate(db, [usersMovedEmail], db.schema)).applied).toBe(false)
  })

  it('repairs non-key drift in place and recomputes rows already present', async () => {
    await migrate(db, [users], db.schema)
    await insertUser({ id: 'user_1', email: 'old@example.com', primary_email: 'new@example.com', first_name: true })
    const changed = withColumn(usersMovedEmail, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], db.schema, undefined, { repair: true })
    expect(r.repaired.map((d) => [d.column, d.kind])).toEqual([
      ['email', 'expression'],
      ['first_name', 'type'],
    ])
    expect(r.drift).toEqual([])
    const row = await db.query<{ email: string; first_name: boolean }>(
      `SELECT email, first_name FROM \`${db.schema}\`.users FINAL`,
    )
    expect(row[0]).toEqual({ email: 'new@example.com', first_name: true })
    // The skipping index on the column survives a redefinition.
    const idx = await db.query<{ name: string }>(
      `SELECT name FROM system.data_skipping_indices WHERE database = {s:String} AND table = 'users'`,
      { s: db.schema },
    )
    expect(idx.map((i) => i.name)).toContain('users_email_idx')
    expect(await detectColumnDrift(db, [changed], db.schema)).toEqual([])
    expect(await recorded([changed])).toBe(true)
  })

  it('refuses to repair a key column', async () => {
    await migrate(db, [users], db.schema)
    const rekeyed = withColumn(users, { name: 'id', type: 'text', path: 'uid' })
    const { logger, lines } = capture()
    const r = await migrate(db, [rekeyed], db.schema, logger, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'id', kind: 'expression', key: true })
    expect(await recorded([rekeyed])).toBe(false)
    const logged = lines.find((l) => l.level === 'error' && l.msg.startsWith('column drift'))
    expect(String(logged?.fields?.['reason'])).toMatch(/key columns are never rebuilt/)
  })

  it('reports an orphan and leaves it alone, even when repairing', async () => {
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], db.schema)
    const { logger, lines } = capture()
    const r = await migrate(db, [users], db.schema, logger, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({
      stream: 'users',
      column: 'nickname',
      kind: 'orphan',
      expected: null,
      key: false,
    })
    expect(await columnExists('nickname')).toBe(true)
    expect(lines.some((l) => l.level === 'warn' && l.msg === 'orphan projected column')).toBe(true)
    expect(await recorded([users])).toBe(true)
  })

  it('forgets a record whose column was dropped by hand', async () => {
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], db.schema)
    await db.command(`ALTER TABLE \`${db.schema}\`.users DROP COLUMN nickname`)
    expect(await detectColumnDrift(db, [users], db.schema)).toEqual([])
    const left = await db.query(`SELECT 1 FROM \`${db.schema}\`.\`_columns\` WHERE column_name = 'nickname'`)
    expect(left).toEqual([])
  })

  it('reports a column of yours that is not a DEFAULT expression, and never touches it', async () => {
    await migrate(db, [{ ...users, columns: users.columns.filter((c) => c.name !== 'locale') }], db.schema)
    await db.command(`ALTER TABLE \`${db.schema}\`.users ADD COLUMN locale String`)
    const r = await migrate(db, [users], db.schema, undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'locale', kind: 'not_generated', actual: 'String' })
    expect(await recorded([users])).toBe(false)
  })

  it('reports a live type changed by hand, whatever the record says', async () => {
    await migrate(db, [users], db.schema)
    await db.command(
      `ALTER TABLE \`${db.schema}\`.users MODIFY COLUMN locale String DEFAULT JSONExtractString(\`_raw\`, 'locale')`,
    )
    const drift = await detectColumnDrift(db, [users], db.schema)
    expect(drift).toHaveLength(1)
    expect(drift[0]).toMatchObject({ column: 'locale', kind: 'type', actual: 'String' })
    await db.command(
      `ALTER TABLE \`${db.schema}\`.\`_migrations\` DELETE WHERE 1`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )
    const r = await migrate(db, [users], db.schema, undefined, { repair: true })
    expect(r.repaired.map((d) => [d.column, d.kind])).toEqual([['locale', 'type']])
    expect(await detectColumnDrift(db, [users], db.schema)).toEqual([])
    expect(await recorded([users])).toBe(true)
  })

  it('forgets earlier fingerprints before a repair runs any DDL', async () => {
    await migrate(db, [users], db.schema)
    await migrate(db, [usersMovedEmail], db.schema, undefined, { repair: true })
    expect(await recorded([users])).toBe(false)
    expect(await recorded([usersMovedEmail])).toBe(true)
    // Rolled back to the old catalog: the live column is still the new one.
    const r = await migrate(db, [users], db.schema)
    expect(r.applied).toBe(true)
    expect(r.drift.map((d) => [d.column, d.kind])).toEqual([['email', 'expression']])
  })

  it('keeps earlier fingerprints when a change is only additive', async () => {
    await migrate(db, [users], db.schema)
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], db.schema)
    expect(await recorded([users])).toBe(true)
    expect(await recorded([extended])).toBe(true)
  })
})

describeDb('init via the CLI', () => {
  let db: TestDb
  let out: string[]

  beforeEach(async () => {
    db = await createTestDb()
    out = []
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
    process.env['WORKOS_API_KEY'] = 'sk_test_1'
    process.env['WORKOS_SYNC_SCHEMA'] = db.schema
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    delete process.env['WORKOS_API_KEY']
    delete process.env['WORKOS_SYNC_SCHEMA']
    await db.close()
  })

  it('exits 2 on drift, repairs with --repair, then is clean', async () => {
    expect(await main(['init', '--streams', 'users'], { db })).toBe(0)
    expect(out.join('')).toContain('schema ready')
    // A release moved a column: simulate by rewriting the record.
    await db.command(
      `ALTER TABLE \`${db.schema}\`.\`_columns\` UPDATE ddl = 'something else' WHERE column_name = 'email'`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )
    await db.command(
      `ALTER TABLE \`${db.schema}\`.\`_migrations\` DELETE WHERE 1`,
      {},
      { async_insert: 0, keeper_map_strict_mode: 0 },
    )

    out = []
    expect(await main(['init', '--streams', 'users'], { db })).toBe(2)
    const report = JSON.parse(out.join('')) as { drift: { column: string; kind: string }[]; repaired: unknown[] }
    expect(report.drift.map((d) => [d.column, d.kind])).toEqual([['email', 'expression']])

    out = []
    expect(await main(['init', '--streams', 'users', '--repair'], { db })).toBe(0)
    const fixed = JSON.parse(out[0]!) as { drift: unknown[]; repaired: { column: string }[] }
    expect(fixed.repaired.map((d) => d.column)).toEqual(['email'])
    expect(out.join('')).toContain('schema ready')

    out = []
    expect(await main(['init', '--streams', 'users'], { db })).toBe(0)
    expect(out.join('')).toBe('schema ready\n')
  })
})
