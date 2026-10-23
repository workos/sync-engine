/**
 * Generated-column drift, against real Postgres (PGlite). The whole point of
 * the feature is that `pg_get_expr` output on a temp table matches the live
 * table for every column the catalog can produce, and only a real server can
 * vouch for that.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createTestDb } from './helpers/pg.js'
import { migrate, fingerprint } from '../src/schema/migrate.js'
import { detectColumnDrift } from '../src/schema/drift.js'
import { main } from '../src/cli.js'
import { STREAMS, streamByName, type ColumnDef, type Logger, type StreamDef } from '@workos/sync-engine'
import type { SqlClient } from '../src/db/client.js'

const users = streamByName('users')!

/** The stream with one column swapped out, the way a catalog release would change it. */
const withColumn = (stream: StreamDef, col: ColumnDef): StreamDef => ({
  ...stream,
  columns: stream.columns.map((c) => (c.name === col.name ? col : c)),
})

/** `email` projected from a different key: the classic path change. */
const usersMovedEmail = withColumn(users, { name: 'email', type: 'text', path: 'primary_email', index: true })

const insertUser = (db: SqlClient, raw: Record<string, unknown>) =>
  db.query(`insert into workos.users ("_env","_raw","_updated_at") values ('prod', $1::jsonb, now())`, [
    JSON.stringify(raw),
  ])

const recorded = async (db: SqlClient, streams: StreamDef[]) =>
  (await db.query(`select 1 from workos._migrations where fingerprint = $1`, [fingerprint(streams, 'workos')])).rows
    .length > 0

const columnExists = async (db: SqlClient, column: string) =>
  (
    await db.query(
      `select 1 from information_schema.columns where table_schema = 'workos' and table_name = 'users' and column_name = $1`,
      [column],
    )
  ).rows.length > 0

const capture = () => {
  const lines: { level: string; msg: string; fields?: Record<string, unknown> }[] = []
  const at = (level: string) => (msg: string, fields?: Record<string, unknown>) => {
    lines.push({ level, msg, fields })
  }
  const logger: Logger = { debug: at('debug'), info: at('info'), warn: at('warn'), error: at('error') }
  return { logger, lines }
}

describe('column drift detection', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  it('reports nothing on a fresh schema built from the full catalog', async () => {
    // Every column type and path shape the catalog can produce must round-trip
    // through pg_get_expr identically on the temp table and the live table,
    // or operators would be chasing phantom drift on every deploy.
    await migrate(db, STREAMS, 'workos')
    expect(await detectColumnDrift(db, STREAMS, 'workos')).toEqual([])
  })

  it('reports a changed path as expression drift, with both expressions', async () => {
    await migrate(db, [users], 'workos')
    const r = await migrate(db, [usersMovedEmail], 'workos')

    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    const d = r.drift[0]!
    expect(d).toMatchObject({
      stream: 'users',
      column: 'email',
      kind: 'expression',
      expected: `(_raw ->> 'primary_email'::text)`,
      actual: `(_raw ->> 'email'::text)`,
      key: false,
    })
    // The DDL is the exact repair: drop, re-add from the catalog, and put the index back.
    expect(d.ddl[0]).toBe(`alter table "workos"."users" drop column "email";`)
    expect(d.ddl[1]).toContain(`add column "email" text generated always as (_raw->>'primary_email') stored`)
    expect(d.ddl[2]).toContain(`create index if not exists "users_email_idx"`)
  })

  it('reports a changed type as type drift', async () => {
    await migrate(db, [users], 'workos')
    const changed = withColumn(users, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], 'workos')
    expect(r.drift).toHaveLength(1)
    expect(r.drift[0]).toMatchObject({ column: 'first_name', kind: 'type', expected: 'boolean', actual: 'text' })
  })

  it('withholds the fingerprint while drift is unrepaired, and records it once repaired', async () => {
    await migrate(db, [users], 'workos')
    await migrate(db, [usersMovedEmail], 'workos')
    expect(await recorded(db, [usersMovedEmail])).toBe(false)

    // Every init re-detects until it is fixed: nothing is silently forgotten.
    const again = await migrate(db, [usersMovedEmail], 'workos')
    expect(again.applied).toBe(true)
    expect(again.drift).toHaveLength(1)

    const fixed = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(fixed.drift).toEqual([])
    expect(fixed.repaired).toHaveLength(1)
    expect(await recorded(db, [usersMovedEmail])).toBe(true)

    const settled = await migrate(db, [usersMovedEmail], 'workos')
    expect(settled.applied).toBe(false)
  })

  it('repairs non-key drift from _raw and recomputes rows already present', async () => {
    await migrate(db, [users], 'workos')
    await insertUser(db, { id: 'user_1', email: 'old@example.com', primary_email: 'new@example.com', first_name: 'true' })

    const changed = withColumn(usersMovedEmail, { name: 'first_name', type: 'boolean', path: 'first_name' })
    const r = await migrate(db, [changed], 'workos', undefined, { repair: true })
    expect(r.repaired.map((d) => [d.column, d.kind])).toEqual([
      ['email', 'expression'],
      ['first_name', 'type'],
    ])
    expect(r.drift).toEqual([])

    // STORED recomputed the new projection for the existing row: that is the backfill.
    const row = await db.query<{ email: string; first_name: boolean }>(`select email, first_name from workos.users`)
    expect(row.rows[0]).toEqual({ email: 'new@example.com', first_name: true })

    // The index went with the dropped column and came back with the new one.
    const idx = await db.query<{ indexname: string }>(
      `select indexname from pg_indexes where schemaname = 'workos' and tablename = 'users'`,
    )
    expect(idx.rows.map((i) => i.indexname)).toContain('users_email_idx')

    expect(await detectColumnDrift(db, [changed], 'workos')).toEqual([])
    expect(await recorded(db, [changed])).toBe(true)
  })

  it('refuses to repair a key column and reports the DDL instead', async () => {
    await migrate(db, [users], 'workos')
    const rekeyed = withColumn(users, { name: 'id', type: 'text', path: 'uid' })
    const r = await migrate(db, [rekeyed], 'workos', undefined, { repair: true })

    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    const d = r.drift[0]!
    expect(d).toMatchObject({ column: 'id', kind: 'expression', key: true })
    // Dropping the column drops the primary key, so the DDL puts it back; an
    // operator runs that in a window, the engine never does.
    expect(d.ddl[d.ddl.length - 1]).toBe(`alter table "workos"."users" add primary key ("_env", "id");`)
    expect(await recorded(db, [rekeyed])).toBe(false)

    // Untouched: the live column still projects the old path.
    const still = await detectColumnDrift(db, [rekeyed], 'workos')
    expect(still[0]?.actual).toBe(`(_raw ->> 'id'::text)`)
  })

  it('reports an orphan and leaves it alone, even when repairing', async () => {
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], 'workos')

    // The column is gone from the catalog, as if an extraColumns entry were removed.
    const r = await migrate(db, [users], 'workos', undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toEqual([
      {
        stream: 'users',
        column: 'nickname',
        kind: 'orphan',
        expected: null,
        actual: `(_raw ->> 'nickname'::text)`,
        key: false,
        ddl: [`alter table "workos"."users" drop column "nickname";`],
      },
    ])
    expect(await columnExists(db, 'nickname')).toBe(true)
    // An orphan is harmless, so it does not hold the schema hostage: the
    // fingerprint is recorded and the next init is a single SELECT.
    expect(await recorded(db, [users])).toBe(true)
  })

  it('does not cascade through a view that depends on the column', async () => {
    await migrate(db, [users], 'workos')
    await db.exec(`create view workos.users_live as select id, email from workos.users`)

    const r = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(await recorded(db, [usersMovedEmail])).toBe(false)

    // The view survived and the column is as it was: the failed transaction changed nothing.
    const view = await db.query(`select 1 from pg_views where schemaname = 'workos' and viewname = 'users_live'`)
    expect(view.rows).toHaveLength(1)
    expect((await detectColumnDrift(db, [usersMovedEmail], 'workos'))[0]?.actual).toBe(`(_raw ->> 'email'::text)`)
  })

  it('logs every drifted column at error with its repair DDL, and orphans at warn', async () => {
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [extended], 'workos')
    const { logger, lines } = capture()
    await migrate(db, [usersMovedEmail], 'workos', logger)

    const errors = lines.filter((l) => l.level === 'error' && l.fields?.['column'] === 'email')
    expect(errors).toHaveLength(1)
    expect(errors[0]?.fields?.['ddl']).toEqual(expect.arrayContaining([expect.stringContaining('drop column "email"')]))
    expect(errors[0]?.fields?.['reason']).toContain('init --repair')

    const warns = lines.filter((l) => l.level === 'warn')
    expect(warns).toHaveLength(1)
    expect(warns[0]?.fields).toMatchObject({ column: 'nickname' })
  })
})

describe('init via the CLI', () => {
  let db: SqlClient
  let out: string[]
  const env = { ...process.env }

  beforeEach(async () => {
    db = await createTestDb()
    out = []
    process.env['WORKOS_API_KEY'] = 'sk_test_unused'
    process.env['LOG_LEVEL'] = 'silent'
    delete process.env['DATABASE_URL']
    vi.spyOn(process.stdout, 'write').mockImplementation((chunk) => {
      out.push(String(chunk))
      return true
    })
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    process.env = { ...env }
    await db.close()
  })

  it('exits 2 on drift, repairs with --repair, then is clean', async () => {
    // A database set up by a catalog that projected email from another key.
    const older = STREAMS.map((s) => (s.name === 'users' ? usersMovedEmail : s))
    await migrate(db, older, 'workos')

    expect(await main(['init'], { db })).toBe(2)
    const report = JSON.parse(out.join('').split('\n').filter(Boolean).join('\n')) as {
      drift: { column: string; kind: string }[]
      repaired: unknown[]
    }
    expect(report.drift).toEqual([expect.objectContaining({ column: 'email', kind: 'expression' })])
    expect(report.repaired).toEqual([])
    expect(out.join('')).not.toContain('schema ready')

    out = []
    expect(await main(['init', '--repair'], { db })).toBe(0)
    expect(out.join('')).toContain('"repaired"')
    expect(out.join('')).toContain('schema ready')

    out = []
    expect(await main(['init'], { db })).toBe(0)
    expect(out.join('')).toBe('schema ready\n')
  })
})
