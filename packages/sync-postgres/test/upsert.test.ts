import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb } from './helpers/pg.js'
import { migrate } from '../src/schema/migrate.js'
import { upsertRows, deleteRows, relinkRows, dedupeByKey, keyValues } from '../src/db/upsert.js'
import { streamByName, STREAMS } from '@workos/sync-engine'
import type { SqlClient } from '../src/db/client.js'

const users = streamByName('users')!
const opts = { schema: 'workos' }

const userPayload = (over: Record<string, unknown> = {}) => ({
  object: 'user',
  id: 'user_1',
  email: 'ada@example.com',
  first_name: 'Ada',
  created_at: '2026-01-01T00:00:00.000Z',
  updated_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describe('key extraction', () => {
  it('reads slug-keyed streams that have no id in their event payload', () => {
    // role.* events carry only a slug — no id at all.
    const roles = streamByName('roles')!
    expect(keyValues(roles, { slug: 'admin', permissions: [] })).toEqual(['admin'])
    expect(keyValues(roles, { permissions: [] })).toBeUndefined()
  })

  it('reads composite keys', () => {
    const orgRoles = streamByName('organization_roles')!
    expect(keyValues(orgRoles, { organization_id: 'org_1', slug: 'admin' })).toEqual(['org_1', 'admin'])
  })

  it('collapses duplicate keys within a batch, keeping the newest', () => {
    // Postgres refuses to let one INSERT ... ON CONFLICT touch a row twice.
    const { rows, dropped } = dedupeByKey(users, [
      { data: userPayload({ email: 'old@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
      { data: userPayload({ email: 'new@example.com' }), updatedAt: '2026-01-02T00:00:00.000Z' },
      { data: { email: 'no-id@example.com' }, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(rows).toHaveLength(1)
    expect(rows[0]!.data['email']).toBe('new@example.com')
    expect(dropped).toBe(1)
  })
})

describe('write path', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
    await migrate(db, STREAMS, 'workos')
  })
  afterEach(async () => {
    await db.close()
  })

  const readUser = async (id = 'user_1') => {
    const r = await db.query<Record<string, unknown>>(
      `select * from workos.users where "_env" = 'prod' and id = $1`,
      [id],
    )
    return r.rows[0]
  }

  it('inserts and reports created vs updated', async () => {
    const first = await upsertRows(db, opts, users, 'prod', [
      { data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' },
    ], 'replace')
    expect(first).toMatchObject({ created: 1, updated: 0 })

    const second = await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'changed@example.com' }), updatedAt: '2026-01-02T00:00:00.000Z' },
    ], 'replace')
    expect(second).toMatchObject({ created: 0, updated: 1 })
    expect((await readUser())!['email']).toBe('changed@example.com')
  })

  it('rejects a write that is older than the stored row', async () => {
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'current@example.com' }), updatedAt: '2026-01-05T00:00:00.000Z' },
    ], 'replace')

    // An out-of-order delivery arriving late must not win.
    const stale = await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'stale@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
    ], 'replace')

    expect(stale).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    expect((await readUser())!['email']).toBe('current@example.com')
  })

  it('accepts a write with an identical timestamp', async () => {
    // The gate is >= rather than >. Two events can share a timestamp, and with
    // a strict comparison the second is dropped forever — which is how the
    // Stripe engine kept showing trialing subscriptions after they converted.
    const ts = '2026-01-05T00:00:00.000Z'
    await upsertRows(db, opts, users, 'prod', [{ data: userPayload({ email: 'first@example.com' }), updatedAt: ts }], 'replace')
    const same = await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'second@example.com' }), updatedAt: ts },
    ], 'replace')

    expect(same.updated).toBe(1)
    expect((await readUser())!['email']).toBe('second@example.com')
  })

  it('skips a write that would change nothing', async () => {
    await upsertRows(db, opts, users, 'prod', [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }], 'replace')
    const again = await upsertRows(db, opts, users, 'prod', [
      { data: userPayload(), updatedAt: '2026-01-02T00:00:00.000Z' },
    ], 'replace')
    // Backfill and reconcile re-fetch mostly-unchanged rows; rewriting them all
    // would bloat WAL and churn autovacuum for nothing.
    expect(again).toMatchObject({ created: 0, updated: 0, skipped: 1 })
  })

  it('merges event payloads instead of nulling the fields events omit', async () => {
    // The whole reason `merge` exists: user.* events do not carry external_id,
    // metadata, last_sign_in_at or locale.
    await upsertRows(db, opts, users, 'prod', [
      {
        data: userPayload({ external_id: 'ext_42', metadata: { plan: 'enterprise' }, locale: 'fr' }),
        updatedAt: '2026-01-01T00:00:00.000Z',
      },
    ], 'replace')

    await upsertRows(db, opts, users, 'prod', [
      {
        // Exactly what a user.updated event contains.
        data: {
          object: 'user',
          id: 'user_1',
          email: 'renamed@example.com',
          first_name: 'Ada',
          created_at: '2026-01-01T00:00:00.000Z',
          updated_at: '2026-01-09T00:00:00.000Z',
        },
        updatedAt: '2026-01-09T00:00:00.000Z',
      },
    ], 'merge')

    const row = await readUser()
    expect(row!['email']).toBe('renamed@example.com')
    expect(row!['external_id']).toBe('ext_42')
    expect(row!['metadata']).toEqual({ plan: 'enterprise' })
    expect(row!['locale']).toBe('fr')
  })

  it('replaces the payload when the row came from the REST API', async () => {
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ locale: 'fr' }), updatedAt: '2026-01-01T00:00:00.000Z' },
    ], 'replace')
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ updated_at: '2026-02-01T00:00:00.000Z' }), updatedAt: '2026-02-01T00:00:00.000Z' },
    ], 'replace')
    // The REST object is authoritative and complete: a field removed upstream
    // must disappear here too.
    expect((await readUser())!['locale']).toBeNull()
  })

  it('tombstones on delete and keeps the last known payload', async () => {
    await upsertRows(db, opts, users, 'prod', [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }], 'replace')
    const stats = await deleteRows(db, opts, users, 'prod', [
      { data: { id: 'user_1' }, updatedAt: '2026-01-10T00:00:00.000Z' },
    ])
    expect(stats.deleted).toBe(1)
    const row = await readUser()
    expect(row!['_deleted_at']).not.toBeNull()
    // The payload survives — it is the only record of what was deleted.
    expect(row!['email']).toBe('ada@example.com')
  })

  it('records a delete for an object it never saw created', async () => {
    // A missed create followed by a delete still has to leave a tombstone.
    const stats = await deleteRows(db, opts, users, 'prod', [
      { data: { id: 'user_ghost', email: 'ghost@example.com' }, updatedAt: '2026-01-10T00:00:00.000Z' },
    ])
    expect(stats.deleted).toBe(1)
    expect((await readUser('user_ghost'))!['_deleted_at']).not.toBeNull()
  })

  it('does not resurrect a deleted row when a late update arrives', async () => {
    await upsertRows(db, opts, users, 'prod', [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }], 'replace')
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_1' }, updatedAt: '2026-01-10T00:00:00.000Z' }])
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'zombie@example.com' }), updatedAt: '2026-01-20T00:00:00.000Z' },
    ], 'merge')

    const row = await readUser()
    // Deletion is terminal: ids are ULIDs and are never reused upstream.
    expect(row!['_deleted_at']).not.toBeNull()
  })

  it('keeps environments isolated under one schema', async () => {
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ email: 'prod@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
    ], 'replace')
    await upsertRows(db, opts, users, 'staging', [
      { data: userPayload({ email: 'staging@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
    ], 'replace')

    const r = await db.query<{ _env: string; email: string }>(
      `select "_env", email from workos.users order by "_env"`,
    )
    expect(r.rows).toEqual([
      { _env: 'prod', email: 'prod@example.com' },
      { _env: 'staging', email: 'staging@example.com' },
    ])
  })

  it('treats a re-added membership as live again', async () => {
    const memberships = streamByName('group_memberships')!
    const pair = { group_id: 'group_1', organization_membership_id: 'om_1' }
    await relinkRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-01T00:00:00.000Z' }])
    await deleteRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-02T00:00:00.000Z' }])
    await relinkRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-03T00:00:00.000Z' }])

    const r = await db.query<{ _deleted_at: string | null }>(
      `select "_deleted_at" from workos.group_memberships where group_id = 'group_1'`,
    )
    // Unlike an object delete, a membership can legitimately come back.
    expect(r.rows[0]!._deleted_at).toBeNull()
  })

  it('never updates an append-only row', async () => {
    const events = streamByName('events')!
    const e = { id: 'event_1', event: 'user.created', data: { id: 'user_1' }, created_at: '2026-01-01T00:00:00.000Z' }
    await upsertRows(db, opts, events, 'prod', [{ data: e, updatedAt: e.created_at }], 'replace')
    const dup = await upsertRows(db, opts, events, 'prod', [
      { data: { ...e, event: 'tampered' }, updatedAt: e.created_at },
    ], 'replace')

    expect(dup.created).toBe(0)
    const r = await db.query<{ event: string }>(`select event from workos.events`)
    expect(r.rows).toHaveLength(1)
    expect(r.rows[0]!.event).toBe('user.created')
  })

  it('writes large batches in chunks', async () => {
    const rows = Array.from({ length: 1200 }, (_, i) => ({
      data: userPayload({ id: `user_${i}`, email: `u${i}@example.com` }),
      updatedAt: '2026-01-01T00:00:00.000Z',
    }))
    const stats = await upsertRows(db, { schema: 'workos', batchSize: 250 }, users, 'prod', rows, 'replace')
    expect(stats.created).toBe(1200)
    const count = await db.query<{ n: string }>(`select count(*)::text as n from workos.users`)
    expect(count.rows[0]!.n).toBe('1200')
  })
})

describe('freshness reporting', () => {
  let db: SqlClient

  beforeEach(async () => {
    db = await createTestDb()
    await migrate(db, STREAMS, 'workos')
  })
  afterEach(async () => {
    await db.close()
  })

  it('does not report the age of old objects as sync lag', async () => {
    // Found by running against a real environment: backfilling objects last
    // modified months ago reported a lag of months, while the sync was
    // perfectly current. Lag must measure the delay between an upstream change
    // and the write, so it is only meaningful for recently-changed rows.
    const { PostgresDestination } = await import('../src/destination.js')
    const destination = new PostgresDestination({ db, schema: 'workos' })

    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ id: 'user_old' }), updatedAt: '2024-01-01T00:00:00.000Z' },
    ], 'replace')

    const stale = await destination.freshness(users, 'prod', 24)
    expect(stale.rows).toBe(1)
    expect(stale.lagSampleSize).toBe(0)
    expect(stale.lagSeconds).toBeNull()

    // A row that changed upstream moments ago does produce a lag figure.
    await upsertRows(db, opts, users, 'prod', [
      { data: userPayload({ id: 'user_fresh' }), updatedAt: new Date(Date.now() - 2000).toISOString() },
    ], 'replace')

    const fresh = await destination.freshness(users, 'prod', 24)
    expect(fresh.lagSampleSize).toBe(1)
    expect(fresh.lagSeconds).not.toBeNull()
    expect(fresh.lagSeconds!).toBeLessThan(60)
  })
})
