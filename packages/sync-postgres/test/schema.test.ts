import { createHash } from 'node:crypto'
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb } from './helpers/pg.js'
import { migrate, fingerprint } from '../src/schema/migrate.js'
import { buildSchemaSql, safeIdentifier, columnExpression, createTableSql } from '../src/schema/ddl.js'
import { liveViewName, liveViewSql } from '../src/schema/views.js'
import { STREAMS, streamByName } from '@workos/sync-engine'
import type { Logger, StreamDef } from '@workos/sync-engine'
import type { SqlClient } from '../src/db/client.js'

/**
 * The fingerprint formula from before `DDL_VERSION` existed, verbatim. A
 * database migrated by that release must still pick up the live views, and
 * keeping the old formula here means the test keeps checking that even after
 * the formula moves on again.
 */
function legacyFingerprint(streams: StreamDef[], schema: string): string {
  const canonical = JSON.stringify({
    schema,
    streams: streams
      .map((s) => ({
        n: s.name,
        k: s.key,
        m: s.mode,
        c: s.columns.map((c) => [c.name, c.type, c.path, c.index ?? false]),
      }))
      .sort((a, b) => a.n.localeCompare(b.n)),
  })
  return createHash('sha256').update(canonical).digest('hex').slice(0, 16)
}

describe('DDL generation', () => {
  it('projects every typed column out of _raw, never writing them directly', () => {
    const users = streamByName('users')!
    const sql = createTableSql(users, 'workos')
    expect(sql).toContain('"_raw"       jsonb not null')
    for (const col of users.columns) {
      expect(sql).toContain(`"${col.name}"`)
      expect(sql).toMatch(new RegExp(`"${col.name}"[^,]*generated always as`))
    }
  })

  it('uses the immutable ISO-8601 parser for timestamps, not a direct cast', () => {
    // A plain ::timestamptz cast is rejected by Postgres in a generated column
    // because it depends on DateStyle/TimeZone.
    const expr = columnExpression({ name: 'created_at', type: 'timestamptz', path: 'created_at' }, 'workos')
    expect(expr).toContain('parse_iso8601')
    expect(expr).not.toContain('::timestamptz')
  })

  it('walks nested paths for columns like role.slug', () => {
    const expr = columnExpression({ name: 'role_slug', type: 'text', path: ['role', 'slug'] }, 'workos')
    expect(expr).toBe(`(_raw->'role'->>'slug')`)
  })

  it('declares no foreign keys between mirror tables', () => {
    // Events routinely arrive with children before parents; an FK would turn
    // that ordering artifact into a hard write failure.
    const all = buildSchemaSql(STREAMS, 'workos').join('\n')
    expect(all.toLowerCase()).not.toContain('references')
    expect(all.toLowerCase()).not.toContain('foreign key')
  })

  it('truncates identifiers longer than Postgres allows', () => {
    const long = 'a'.repeat(80)
    const safe = safeIdentifier(long)
    expect(Buffer.byteLength(safe)).toBeLessThanOrEqual(63)
    expect(safeIdentifier(long)).toBe(safe)
    expect(safeIdentifier('short')).toBe('short')
  })

  it('changes the fingerprint when the catalog changes', () => {
    const a = fingerprint(STREAMS, 'workos')
    const modified = STREAMS.map((s) =>
      s.name === 'users' ? { ...s, columns: [...s.columns, { name: 'x', type: 'text' as const, path: 'x' }] } : s,
    )
    expect(fingerprint(modified, 'workos')).not.toBe(a)
    expect(fingerprint(STREAMS, 'other_schema')).not.toBe(a)
    expect(fingerprint(STREAMS, 'workos')).toBe(a)
  })

  it('changes the fingerprint when only the generated DDL changes', () => {
    // Otherwise a database whose catalog did not change would never run the
    // view DDL on upgrade.
    expect(fingerprint(STREAMS, 'workos')).not.toBe(legacyFingerprint(STREAMS, 'workos'))
  })

  it('generates a live view for every stream that has tombstones, and no other', () => {
    for (const s of STREAMS) {
      const sql = liveViewSql(s, 'workos').join('\n')
      if (s.mode === 'append') {
        expect(sql).toBe('')
      } else {
        expect(sql).toContain(`create or replace view "workos"."${s.name}_live" as select * from "workos"."${s.name}"`)
        expect(sql).toContain(`where "_deleted_at" is null`)
        expect(sql).toContain(`comment on view "workos"."${s.name}_live"`)
      }
    }
  })

  it('emits each live view after the column DDL for its table', () => {
    // `select *` is expanded when the view is created, so a view created
    // before `add column` would not carry the new column.
    const all = buildSchemaSql(STREAMS, 'workos')
    const lastAlter = all.map((s) => s.startsWith('alter table "workos"."users" ')).lastIndexOf(true)
    const view = all.findIndex((s) => s.startsWith('create or replace view "workos"."users_live"'))
    expect(lastAlter).toBeGreaterThan(-1)
    expect(view).toBeGreaterThan(lastAlter)
  })

  it('truncates a live view name the same way as a table name', () => {
    const long = { ...streamByName('users')!, name: 'a'.repeat(80) }
    expect(Buffer.byteLength(liveViewName(long))).toBeLessThanOrEqual(63)
    expect(liveViewName(streamByName('users')!)).toBe('users_live')
  })
})

describe('migrations against real Postgres', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  it('creates every stream table with working generated columns', async () => {
    await migrate(db, STREAMS, 'workos')
    const tables = await db.query<{ table_name: string }>(
      `select table_name from information_schema.tables where table_schema = 'workos' order by table_name`,
    )
    const names = tables.rows.map((r) => r.table_name)
    for (const s of STREAMS) expect(names).toContain(s.name)
    expect(names).toContain('_sync_state')
    expect(names).toContain('_migrations')
  })

  it('is a no-op on the second run', async () => {
    const first = await migrate(db, STREAMS, 'workos')
    const second = await migrate(db, STREAMS, 'workos')
    expect(first.applied).toBe(true)
    expect(second.applied).toBe(false)
    expect(second.statements).toBe(0)
  })

  it('adds a newly projected column without a backfill', async () => {
    const users = streamByName('users')!
    await migrate(db, [users], 'workos')
    await db.query(
      `insert into workos.users ("_env","_raw","_updated_at") values ('prod', $1::jsonb, now())`,
      [JSON.stringify({ id: 'user_1', email: 'a@b.com', nickname: 'ada' })],
    )
    // Promote an existing raw field to a typed column.
    const extended = { ...users, columns: [...users.columns, { name: 'nickname', type: 'text' as const, path: 'nickname' }] }
    await migrate(db, [extended], 'workos')
    const r = await db.query<{ nickname: string }>(`select nickname from workos.users`)
    // STORED computes it from data already present — no rewrite required.
    expect(r.rows[0]?.nickname).toBe('ada')
  })

  it('parses the ISO-8601 formats WorkOS emits, and nulls anything else', async () => {
    await migrate(db, [streamByName('users')!], 'workos')
    const cases: [string, string | null][] = [
      ['2026-01-01T00:00:00.000Z', '2026-01-01T00:00:00.000Z'],
      ['2026-08-06T17:44:03.481Z', '2026-08-06T17:44:03.481Z'],
      ['2026-01-01T00:00:00Z', '2026-01-01T00:00:00.000Z'],
      ['2026-01-01T05:00:00+02:00', '2026-01-01T03:00:00.000Z'],
      ['not-a-timestamp', null],
    ]
    for (const [input, expected] of cases) {
      await db.query(
        `insert into workos.users ("_env","_raw","_updated_at") values ('prod', $1::jsonb, now())`,
        [JSON.stringify({ id: `user_${input}`, created_at: input })],
      )
      const r = await db.query<{ created_at: Date | null }>(
        `select created_at from workos.users where id = $1`,
        [`user_${input}`],
      )
      const got = r.rows[0]?.created_at
      expect(got ? new Date(got).toISOString() : null).toBe(expected)
    }
  })

  it('indexes the columns marked for it', async () => {
    await migrate(db, [streamByName('users')!], 'workos')
    const idx = await db.query<{ indexname: string }>(
      `select indexname from pg_indexes where schemaname = 'workos' and tablename = 'users'`,
    )
    const names = idx.rows.map((r) => r.indexname)
    expect(names).toContain('users_email_idx')
    expect(names).toContain('users_external_id_idx')
    expect(names).toContain('users_synced_at_idx')
  })
})

describe('live views against real Postgres', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
  })
  afterEach(async () => {
    await db.close()
  })

  const viewNames = async () => {
    const r = await db.query<{ table_name: string }>(
      `select table_name from information_schema.views where table_schema = 'workos' order by table_name`,
    )
    return r.rows.map((x) => x.table_name)
  }

  // `information_schema.columns` covers views too, in declared order.
  const columnsOf = async (relation: string) => {
    const r = await db.query<{ column_name: string; data_type: string }>(
      `select column_name, data_type from information_schema.columns
       where table_schema = 'workos' and table_name = $1 order by ordinal_position`,
      [relation],
    )
    return r.rows.map((x) => `${x.column_name}:${x.data_type}`)
  }

  it('creates a view for every mirror stream and none for the append-only logs', async () => {
    await migrate(db, STREAMS, 'workos')
    const views = await viewNames()
    for (const s of STREAMS) {
      if (s.mode === 'append') expect(views).not.toContain(`${s.name}_live`)
      else expect(views).toContain(`${s.name}_live`)
    }
    expect(views).not.toContain('events_live')
    expect(views).not.toContain('auth_attempts_live')
  })

  it('exposes exactly the columns of the table it covers', async () => {
    await migrate(db, STREAMS, 'workos')
    for (const s of STREAMS) {
      if (s.mode === 'append') continue
      expect(await columnsOf(`${s.name}_live`)).toEqual(await columnsOf(s.name))
    }
  })

  it('excludes tombstoned rows and nothing else', async () => {
    await migrate(db, [streamByName('users')!], 'workos')
    await db.query(
      `insert into workos.users ("_env","_raw","_updated_at","_deleted_at")
       values ('prod', '{"id":"user_live"}', now(), null),
              ('prod', '{"id":"user_gone"}', now(), now())`,
    )
    const live = await db.query<{ id: string }>(`select id from workos.users_live`)
    expect(live.rows.map((r) => r.id)).toEqual(['user_live'])
    const all = await db.query<{ n: string }>(`select count(*)::text as n from workos.users`)
    expect(all.rows[0]!.n).toBe('2')
  })

  it('says what it is in the view comment', async () => {
    await migrate(db, [streamByName('users')!], 'workos')
    const r = await db.query<{ c: string }>(
      `select obj_description('workos.users_live'::regclass, 'pg_class') as c`,
    )
    expect(r.rows[0]!.c).toContain('@workos/sync-postgres')
    expect(r.rows[0]!.c).toContain('Do not edit')
  })

  it('picks up a column promoted after the view was first created', async () => {
    const users = streamByName('users')!
    await migrate(db, [users], 'workos')
    expect(await columnsOf('users_live')).not.toContain('nickname:text')

    const extended = { ...users, columns: [...users.columns, { name: 'nickname', type: 'text' as const, path: 'nickname' }] }
    await migrate(db, [extended], 'workos')
    expect(await columnsOf('users_live')).toEqual(await columnsOf('users'))
    expect(await columnsOf('users_live')).toContain('nickname:text')
  })

  it('is idempotent when the DDL is applied twice', async () => {
    await migrate(db, STREAMS, 'workos')
    const before = await viewNames()
    // Force the DDL to run again rather than short-circuit on the fingerprint.
    await db.exec(`delete from workos."_migrations"`)
    const second = await migrate(db, STREAMS, 'workos')
    expect(second.applied).toBe(true)
    expect(await viewNames()).toEqual(before)
  })

  it('re-creates a view that was dropped once the schema is applied again', async () => {
    await migrate(db, STREAMS, 'workos')
    await db.exec(`drop view workos.users_live; drop view workos.organizations_live;`)
    await db.exec(`delete from workos."_migrations"`)
    expect(await viewNames()).not.toContain('users_live')

    const r = await migrate(db, STREAMS, 'workos')
    expect(r.applied).toBe(true)
    expect(await viewNames()).toContain('users_live')
    expect(await viewNames()).toContain('organizations_live')
  })

  it('reaches a database that was migrated before live views existed', async () => {
    await migrate(db, STREAMS, 'workos')
    // Rewind to what the previous release left behind: the tables, no views,
    // and a _migrations row stamped with the fingerprint it computed.
    for (const s of STREAMS) if (s.mode !== 'append') await db.exec(`drop view workos.${s.name}_live`)
    await db.exec(`delete from workos."_migrations"`)
    await db.query(`insert into workos."_migrations" (fingerprint) values ($1)`, [legacyFingerprint(STREAMS, 'workos')])

    const r = await migrate(db, STREAMS, 'workos')
    expect(r.applied).toBe(true)
    expect(await viewNames()).toContain('users_live')
  })

  it('drops and re-creates a view whose shape cannot be replaced in place', async () => {
    const users = streamByName('users')!
    await migrate(db, [users], 'workos')
    // Someone (or an older definition) left a view under our name whose column
    // list does not match. `create or replace view` refuses to rename or
    // reorder columns, so this is the case the fallback exists for.
    await db.exec(`drop view workos.users_live`)
    await db.exec(`create view workos.users_live as select email, id from workos.users`)
    await db.exec(`delete from workos."_migrations"`)

    const warnings: Record<string, unknown>[] = []
    const logger: Logger = {
      debug() {},
      info() {},
      warn(_msg, fields) {
        warnings.push(fields ?? {})
      },
      error() {},
    }
    await migrate(db, [users], 'workos', logger)

    expect(await columnsOf('users_live')).toEqual(await columnsOf('users'))
    expect(warnings).toHaveLength(1)
    expect(warnings[0]).toMatchObject({ view: '"workos"."users_live"' })
    expect(String(warnings[0]!['reason'])).toMatch(/view column/)
  })
})
