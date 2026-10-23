/**
 * Generated-column drift, against real Postgres (PGlite). The whole point of
 * the feature is that `pg_get_expr` output on a temp table matches the live
 * table for every column the catalog can produce, and only a real server can
 * vouch for that.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { createTestDb } from './helpers/pg.js'
import { migrate, fingerprint } from '../src/schema/migrate.js'
import { detectColumnDrift, repairColumnDrift } from '../src/schema/drift.js'
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
        catalog: null,
        key: false,
        dependents: [],
        ddl: [`alter table "workos"."users" drop column "nickname";`],
      },
    ])
    expect(await columnExists(db, 'nickname')).toBe(true)
    // An orphan is harmless, so it does not hold the schema hostage: the
    // fingerprint is recorded and the next init is a single SELECT.
    expect(await recorded(db, [users])).toBe(true)
  })

  it('forgets earlier fingerprints when a repair changes columns, so a rollback re-checks', async () => {
    // A is recorded, B is repaired and recorded. Roll back to A: the live
    // column is B's, and the fingerprint for A must not vouch for it.
    await migrate(db, [users], 'workos')
    await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(await recorded(db, [usersMovedEmail])).toBe(true)
    expect(await recorded(db, [users])).toBe(false)

    const back = await migrate(db, [users], 'workos')
    expect(back.applied).toBe(true)
    expect(back.drift).toEqual([
      expect.objectContaining({
        column: 'email',
        kind: 'expression',
        expected: `(_raw ->> 'email'::text)`,
        actual: `(_raw ->> 'primary_email'::text)`,
      }),
    ])
    expect(await recorded(db, [users])).toBe(false)

    // Repairing on the way back restores A and, in turn, forgets B.
    const restored = await migrate(db, [users], 'workos', undefined, { repair: true })
    expect(restored.repaired).toHaveLength(1)
    expect(await recorded(db, [users])).toBe(true)
    expect(await recorded(db, [usersMovedEmail])).toBe(false)
  })

  it('keeps earlier fingerprints when a change is only additive', async () => {
    // Nothing was rebuilt, so A still describes the live columns: rolling
    // back to it stays a single SELECT, and a rolling deploy does not ping-pong.
    const extended: StreamDef = {
      ...users,
      columns: [...users.columns, { name: 'nickname', type: 'text', path: 'nickname' }],
    }
    await migrate(db, [users], 'workos')
    await migrate(db, [extended], 'workos', undefined, { repair: true })
    expect(await recorded(db, [users])).toBe(true)
    expect(await recorded(db, [extended])).toBe(true)
  })

  it('treats paths that differ only by whitespace inside the key as drift', async () => {
    // `'a b'` and `'a  b'` are different JSON keys. A comparison that collapsed
    // whitespace would call them equal and record the fingerprint over a stale column.
    const oneSpace = withColumn(users, { name: 'email', type: 'text', path: 'a b' })
    const twoSpaces = withColumn(users, { name: 'email', type: 'text', path: 'a  b' })
    await migrate(db, [oneSpace], 'workos')

    const r = await migrate(db, [twoSpaces], 'workos', undefined, { repair: true })
    expect(r.repaired).toHaveLength(1)
    expect(r.repaired[0]).toMatchObject({
      column: 'email',
      kind: 'expression',
      expected: `(_raw ->> 'a  b'::text)`,
      actual: `(_raw ->> 'a b'::text)`,
    })
    expect(await detectColumnDrift(db, [twoSpaces], 'workos')).toEqual([])
  })

  it('refuses to repair a column that carries an index or constraint of yours', async () => {
    await migrate(db, [users], 'workos')
    await db.exec(`alter table workos.users add constraint users_email_unique unique (email)`)
    await db.exec(`create index users_email_lower on workos.users (lower(email))`)

    const r = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    const d = r.drift[0]!
    // Listed so the operator knows what to put back; the catalog's own
    // `users_email_idx` is not, because repair recreates it.
    expect(d.dependents).toEqual([
      expect.stringContaining('users_email_unique'),
      expect.stringContaining('users_email_lower'),
    ])
    expect(d.dependents.join(' ')).not.toContain('users_email_idx')
    expect(await recorded(db, [usersMovedEmail])).toBe(false)

    // Nothing was dropped: the constraint and the index are still there.
    const cons = await db.query(`select 1 from pg_constraint where conname = 'users_email_unique'`)
    expect(cons.rows).toHaveLength(1)
    const idx = await db.query(`select 1 from pg_indexes where indexname = 'users_email_lower'`)
    expect(idx.rows).toHaveLength(1)

    const { logger, lines } = capture()
    await migrate(db, [usersMovedEmail], 'workos', logger)
    const err = lines.find((l) => l.level === 'error' && l.fields?.['column'] === 'email')
    expect(err?.fields?.['reason']).toContain('users_email_unique')
  })

  it('does not rebuild a column another process already rebuilt', async () => {
    await migrate(db, [users], 'workos')
    // Two processes detect the same drift before either repairs it.
    const seenByFirst = await detectColumnDrift(db, [usersMovedEmail], 'workos')
    const seenBySecond = await detectColumnDrift(db, [usersMovedEmail], 'workos')
    expect(seenByFirst).toHaveLength(1)

    const first = await repairColumnDrift(db, seenByFirst, 'workos')
    expect(first.repaired).toHaveLength(1)
    const attnum = async () =>
      (
        await db.query<{ attnum: number }>(
          `select attnum from pg_attribute where attrelid = 'workos.users'::regclass and attname = 'email'`,
        )
      ).rows[0]!.attnum
    const after = await attnum()

    // The second arrives with its stale report, takes the lock, sees the
    // column already matches, and leaves it: a re-add would have moved attnum.
    const { logger, lines } = capture()
    const second = await repairColumnDrift(db, seenBySecond, 'workos', logger)
    expect(second.repaired).toHaveLength(1)
    expect(second.failed).toEqual([])
    expect(await attnum()).toBe(after)
    expect(lines.map((l) => l.msg)).toContain('column already rebuilt by another process')
  })

  it('rebuilds when another process changed the type but reads a different path', async () => {
    // Two releases both turn first_name into a boolean, from different keys.
    // Each sees `type` drift against the original text column. After the
    // first rebuilds, the type alone looks right to the second; the
    // expression does not, and recording a fingerprint over it would hide the
    // wrong path behind "schema up to date".
    await migrate(db, [users], 'workos')
    const fromA = withColumn(users, { name: 'first_name', type: 'boolean', path: 'flag_a' })
    const fromB = withColumn(users, { name: 'first_name', type: 'boolean', path: 'flag_b' })
    const seenByA = await detectColumnDrift(db, [fromA], 'workos')
    const seenByB = await detectColumnDrift(db, [fromB], 'workos')
    expect(seenByB[0]).toMatchObject({ column: 'first_name', kind: 'type', expected: 'boolean' })

    await repairColumnDrift(db, seenByA, 'workos')
    const { logger, lines } = capture()
    const r = await repairColumnDrift(db, seenByB, 'workos', logger)
    expect(r.repaired).toHaveLength(1)
    expect(lines.map((l) => l.msg)).toEqual(['column rebuilt from _raw'])
    expect(await detectColumnDrift(db, [fromB], 'workos')).toEqual([])
  })

  it('does not cascade through a view that depends on the column', async () => {
    await migrate(db, [users], 'workos')
    // Somebody's own view, next to the generated `users_live`.
    await db.exec(`create view workos.users_by_email as select id, email from workos.users`)

    const r = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(r.repaired).toEqual([])
    expect(r.drift).toHaveLength(1)
    expect(await recorded(db, [usersMovedEmail])).toBe(false)

    // Both views survived and the column is as it was: the failed transaction changed nothing.
    const views = await db.query<{ viewname: string }>(
      `select viewname from pg_views where schemaname = 'workos' and viewname in ('users_live', 'users_by_email') order by 1`,
    )
    expect(views.rows.map((v) => v.viewname)).toEqual(['users_by_email', 'users_live'])
    expect((await detectColumnDrift(db, [usersMovedEmail], 'workos'))[0]?.actual).toBe(`(_raw ->> 'email'::text)`)
  })

  it('sets the generated _live view aside for a repair and restores it with its grants', async () => {
    await migrate(db, [users], 'workos')
    await db.exec(`
      create role reader;
      grant usage on schema workos to reader;
      grant select on workos.users_live to reader;
      insert into workos.users ("_env", "_raw", "_updated_at")
        values ('prod', '{"id":"user_1","email":"old@x","primary_email":"new@x"}', now());
    `)

    const r = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(r.repaired.map((d) => d.column)).toEqual(['email'])
    expect(r.drift).toEqual([])
    expect(await recorded(db, [usersMovedEmail])).toBe(true)

    // The view is back, reads through the rebuilt column, and the reader kept access.
    await db.exec(`set role reader`)
    try {
      const rows = await db.query<{ email: string }>(`select email from workos.users_live`)
      expect(rows.rows).toEqual([{ email: 'new@x' }])
    } finally {
      await db.exec(`reset role`)
    }
    const comment = await db.query<{ c: string | null }>(
      `select obj_description(to_regclass('workos.users_live'), 'pg_class') as c`,
    )
    expect(comment.rows[0]?.c).toContain('@workos/sync-postgres')
  })

  it('carries column-level grants on the _live view through a repair', async () => {
    await migrate(db, [users], 'workos')
    // A reader allowed two columns and nothing else, which lives in
    // pg_attribute.attacl rather than the view's own ACL.
    await db.exec(`
      create role narrow;
      grant usage on schema workos to narrow;
      grant select (id, email) on workos.users_live to narrow;
      insert into workos.users ("_env", "_raw", "_updated_at")
        values ('prod', '{"id":"user_1","email":"old@x","primary_email":"new@x"}', now());
    `)

    const r = await migrate(db, [usersMovedEmail], 'workos', undefined, { repair: true })
    expect(r.repaired.map((d) => d.column)).toEqual(['email'])

    await db.exec(`set role narrow`)
    try {
      const rows = await db.query<{ id: string; email: string }>(`select id, email from workos.users_live`)
      expect(rows.rows).toEqual([{ id: 'user_1', email: 'new@x' }])
      await expect(db.query(`select "_raw" from workos.users_live`)).rejects.toThrow(/permission denied/)
    } finally {
      await db.exec(`reset role`)
    }
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
