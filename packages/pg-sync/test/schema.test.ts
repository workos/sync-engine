import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb } from './helpers/pg.js'
import { migrate, fingerprint } from '../src/schema/migrate.js'
import { buildSchemaSql, safeIdentifier, columnExpression, createTableSql } from '../src/schema/ddl.js'
import { STREAMS, streamByName } from '@workos/sync'
import type { SqlClient } from '../src/db/client.js'

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
