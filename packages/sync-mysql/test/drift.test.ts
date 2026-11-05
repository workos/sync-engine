/**
 * Generated-column drift, against a real MySQL. Detection compares the DDL
 * the engine recorded in `_columns` with what the catalog would emit now,
 * and only a real server can show that a repair rebuilds the column from
 * `_raw` and puts the engine's index back.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
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
    expect(unrepairableReason({ ...base, kind: 'not_generated' })).toMatch(/not generated/)
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
    db.query(`insert into \`${db.schema}\`.users (\`_env\`, \`_raw\`, \`_updated_at\`) values ('prod', ?, now(6))`, [
      JSON.stringify(raw),
    ])
  const recorded = async (streams: StreamDef[]) =>
    (
      await db.query(`select 1 from \`${db.schema}\`.\`_migrations\` where fingerprint = ?`, [
        fingerprint(streams, db.schema),
      ])
    ).rows.length > 0
  const columnExists = async (column: string) =>
    (
      await db.query(
        `select 1 from information_schema.columns where table_schema = ? and table_name = 'users' and column_name = ?`,
        [db.schema, column],
      )
    ).rows.length > 0

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
    expect(r.drift[0]!.expected).toContain(`'$."primary_email"'`)
    expect(r.drift[0]!.actual).toContain(`'$."email"'`)
  })

  it('reports a changed type as type drift', async () => {
    await migrate(db, [users], db.schema)
    const changed = withColumn(users, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], db.schema)
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'first_name', kind: 'type' })
    expect(r.drift[0]!.expected).toContain('`first_name` boolean')
    expect(r.drift[0]!.actual).toBe('text')
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

    const settled = await migrate(db, [usersMovedEmail], db.schema)
    expect(settled.applied).toBe(false)
  })

  it('repairs non-key drift from _raw and recomputes rows already present', async () => {
    await migrate(db, [users], db.schema)
    await insertUser({ id: 'user_1', email: 'old@example.com', primary_email: 'new@example.com', first_name: true })

    const changed = withColumn(usersMovedEmail, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], db.schema, undefined, { repair: true })
    expect(r.repaired.map((d) => [d.column, d.kind])).toEqual([
      ['email', 'expression'],
      ['first_name', 'type'],
    ])
    expect(r.drift).toEqual([])

    const row = await db.query<{ email: string; first_name: number }>(
      `select email, first_name from \`${db.schema}\`.users`,
    )
    expect(row.rows[0]).toEqual({ email: 'new@example.com', first_name: 1 })

    // The index went with the dropped column and came back with the new one.
    const idx = await db.query<{ index_name: string }>(
      `select distinct index_name as index_name from information_schema.statistics where table_schema = ? and table_name = 'users'`,
      [db.schema],
    )
    expect(idx.rows.map((i) => i.index_name)).toContain('users_email_idx')
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
    expect(lines.some((l) => l.level === 'warn' && l.msg === 'orphan generated column')).toBe(true)
    // An orphan is harmless, so it does not hold the schema hostage.
    expect(await recorded([users])).toBe(true)
  })

  it('forgets a record whose column was dropped by hand', async () => {
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], db.schema)
    await db.exec(`alter table \`${db.schema}\`.users drop column nickname`)
    expect(await detectColumnDrift(db, [users], db.schema)).toEqual([])
    const left = await db.query(`select 1 from \`${db.schema}\`.\`_columns\` where column_name = 'nickname'`)
    expect(left.rows).toEqual([])
  })

  it('refuses to repair a column that carries an index of yours', async () => {
    await migrate(db, [users], db.schema)
    await db.exec(`alter table \`${db.schema}\`.users add index my_first_name_idx (first_name(50))`)
    const changed = withColumn(users, { name: 'first_name', type: 'text', path: 'given_name' })
    const { logger, lines } = capture()
    const r = await migrate(db, [changed], db.schema, logger, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    const logged = lines.find((l) => l.level === 'error' && l.msg.startsWith('column drift'))
    expect(String(logged?.fields?.['reason'])).toMatch(/my_first_name_idx/)
    expect(await recorded([changed])).toBe(false)
  })

  it('reports a column of yours that is not generated, and never touches it', async () => {
    await migrate(db, [{ ...users, columns: users.columns.filter((c) => c.name !== 'locale') }], db.schema)
    await db.exec(`alter table \`${db.schema}\`.users add column locale varchar(8) default 'en'`)
    const r = await migrate(db, [users], db.schema, undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'locale', kind: 'not_generated', actual: 'varchar(8)' })
    expect(await recorded([users])).toBe(false)
  })

  it('records a generated column it finds unrecorded when the type is right, and reports it when not', async () => {
    await migrate(db, [users], db.schema)
    await db.query(`delete from \`${db.schema}\`.\`_columns\` where column_name in ('email', 'first_name')`)
    await db.exec(`alter table \`${db.schema}\`.users drop column first_name`)
    await db.exec(
      `alter table \`${db.schema}\`.users add column first_name bigint generated always as (json_value(\`_raw\`, '$.n' returning signed null on error)) stored`,
    )
    const drift = await detectColumnDrift(db, [users], db.schema)
    expect(drift.map((d) => [d.column, d.kind])).toEqual([['first_name', 'type']])
    const back = await db.query(`select 1 from \`${db.schema}\`.\`_columns\` where column_name = 'email'`)
    expect(back.rows).toHaveLength(1)
  })

  it('reports a live type changed by hand, whatever the record says', async () => {
    await migrate(db, [users], db.schema)
    // The record still describes the column the engine created.
    await db.exec(
      `alter table \`${db.schema}\`.users modify column locale varchar(8) generated always as (json_value(\`_raw\`, '$."locale"' returning char(8) null on error)) stored`,
    )
    const drift = await detectColumnDrift(db, [users], db.schema)
    expect(drift).toHaveLength(1)
    expect(drift[0]).toMatchObject({ column: 'locale', kind: 'type', actual: 'varchar(8)' })
    // And a repair puts the catalog's type back.
    await db.query(`delete from \`${db.schema}\`.\`_migrations\``)
    const r = await migrate(db, [users], db.schema, undefined, { repair: true })
    expect(r.repaired.map((d) => [d.column, d.kind])).toEqual([['locale', 'type']])
    expect(await detectColumnDrift(db, [users], db.schema)).toEqual([])
    expect(await recorded([users])).toBe(true)
  })

  it('forgets earlier fingerprints before a repair runs any DDL, even one that then fails', async () => {
    // MySQL commits each DDL statement on its own, so a repair that dies
    // halfway has already changed the table. The older fingerprints must be
    // gone before the first statement, not after the last.
    await migrate(db, [users], db.schema)
    await db.exec(`alter table \`${db.schema}\`.users add index my_first_name_idx (first_name(50))`)
    const changed = withColumn(users, { name: 'first_name', type: 'text', path: 'given_name' })
    const r = await migrate(db, [changed], db.schema, undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(await recorded([users])).toBe(false)
    expect(await recorded([changed])).toBe(false)
  })

  it('forgets earlier fingerprints when a repair changes columns, so a rollback re-checks', async () => {
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
    await db.query(`update \`${db.schema}\`.\`_columns\` set ddl = 'something else' where column_name = 'email'`)
    await db.query(`delete from \`${db.schema}\`.\`_migrations\``)

    out = []
    expect(await main(['init', '--streams', 'users'], { db })).toBe(2)
    const report = JSON.parse(out.join('')) as { drift: { column: string; kind: string }[]; repaired: unknown[] }
    expect(report.drift.map((d) => [d.column, d.kind])).toEqual([['email', 'expression']])
    expect(report.repaired).toEqual([])

    out = []
    expect(await main(['init', '--streams', 'users', '--repair'], { db })).toBe(0)
    const fixed = JSON.parse(out[0]!) as { drift: unknown[]; repaired: { column: string }[] }
    expect(fixed.repaired.map((d) => d.column)).toEqual(['email'])
    expect(fixed.drift).toEqual([])
    expect(out.join('')).toContain('schema ready')

    out = []
    expect(await main(['init', '--streams', 'users'], { db })).toBe(0)
    expect(out.join('')).toBe('schema ready\n')
  })
})
