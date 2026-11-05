import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { createTestDb, describeDb, type TestDb } from './helpers/mysql.js'
import { migrate } from '../src/schema/migrate.js'
import {
  upsertRows,
  deleteRows,
  relinkRows,
  unlinkRows,
  dedupeByKey,
  wavesByKey,
  keyValues,
  parseUpdatedAt,
  jsonEqual,
} from '../src/db/upsert.js'
import { isTransientMysqlError } from '../src/db/errors.js'
import { streamByName } from '@workos/sync-engine'

const users = streamByName('users')!

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
    const roles = streamByName('roles')!
    expect(keyValues(roles, { slug: 'admin', permissions: [] })).toEqual(['admin'])
    expect(keyValues(roles, { permissions: [] })).toBeUndefined()
  })

  it('reads composite keys', () => {
    const orgRoles = streamByName('organization_roles')!
    expect(keyValues(orgRoles, { organization_id: 'org_1', slug: 'admin' })).toEqual(['org_1', 'admin'])
  })

  it('collapses duplicate keys within a batch, keeping the newest', () => {
    const { rows, dropped } = dedupeByKey(users, [
      { data: userPayload({ email: 'old@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
      { data: userPayload({ email: 'new@example.com' }), updatedAt: '2026-01-02T00:00:00.000Z' },
      { data: { email: 'no-id@example.com' }, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(rows).toHaveLength(1)
    expect(rows[0]!.data['email']).toBe('new@example.com')
    expect(dropped).toBe(1)
  })

  it('splits repeated keys into ordered waves and leaves distinct keys in one', () => {
    const first = { data: userPayload({ external_id: 'ext_1' }), updatedAt: '2026-01-01T00:00:00.000Z' }
    const other = { data: userPayload({ id: 'user_2' }), updatedAt: '2026-01-01T00:00:00.000Z' }
    const second = { data: { id: 'user_1', first_name: 'Grace' }, updatedAt: '2026-01-02T00:00:00.000Z' }
    const { waves, dropped } = wavesByKey(users, [
      first,
      other,
      second,
      { data: { email: 'no-id@example.com' }, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(waves).toEqual([[first, other], [second]])
    expect(waves[0]![0]).toBe(first)
    expect(dropped).toBe(1)
  })
})

describe('timestamps and equality', () => {
  it('parses an ISO-8601 updatedAt and refuses anything else as a deterministic error', () => {
    expect(parseUpdatedAt('2026-01-01T00:00:00.000Z').toISOString()).toBe('2026-01-01T00:00:00.000Z')
    let err: unknown
    try {
      parseUpdatedAt('maybe')
    } catch (e) {
      err = e
    }
    expect(err).toBeInstanceOf(Error)
    expect((err as { code: string }).code).toBe('ER_INVALID_TIMESTAMP')
    // The quarantine must see it as a verdict on the row, not a bad moment.
    expect(isTransientMysqlError(err)).toBe(false)
  })

  it('compares JSON structurally, ignoring key order', () => {
    expect(jsonEqual({ a: 1, b: [1, { c: null }] }, { b: [1, { c: null }], a: 1 })).toBe(true)
    expect(jsonEqual({ a: 1 }, { a: 1, b: undefined })).toBe(false)
    expect(jsonEqual({ a: [1, 2] }, { a: [2, 1] })).toBe(false)
    expect(jsonEqual(null, {})).toBe(false)
  })
})

const SOME_STREAMS = ['users', 'roles', 'group_memberships', 'events'].map((n) => streamByName(n)!)

describeDb('write path', () => {
  let db: TestDb
  let opts: { schema: string; batchSize?: number }

  beforeEach(async () => {
    db = await createTestDb()
    opts = { schema: db.schema }
    await migrate(db, SOME_STREAMS, db.schema)
  })
  afterEach(async () => {
    await db.close()
  })

  const readUser = async (id = 'user_1') => {
    const r = await db.query<Record<string, unknown>>(
      `select * from \`${db.schema}\`.users where \`_env\` = 'prod' and id = ?`,
      [id],
    )
    return r.rows[0]
  }

  it('inserts and reports created vs updated', async () => {
    const first = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    expect(first).toMatchObject({ created: 1, updated: 0 })

    const second = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'changed@example.com' }), updatedAt: '2026-01-02T00:00:00.000Z' }],
      'replace',
    )
    expect(second).toMatchObject({ created: 0, updated: 1 })
    expect((await readUser())!['email']).toBe('changed@example.com')
  })

  it('rejects a write that is older than the stored row', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'current@example.com' }), updatedAt: '2026-01-05T00:00:00.000Z' }],
      'replace',
    )
    const stale = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'stale@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    expect(stale).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    expect((await readUser())!['email']).toBe('current@example.com')
  })

  it('accepts a write with an identical timestamp', async () => {
    const ts = '2026-01-05T00:00:00.000Z'
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'first@example.com' }), updatedAt: ts }],
      'replace',
    )
    const same = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'second@example.com' }), updatedAt: ts }],
      'replace',
    )
    expect(same.updated).toBe(1)
    expect((await readUser())!['email']).toBe('second@example.com')
  })

  it('skips a write with the same payload and timestamp, leaving _synced_at alone', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    const before = (await readUser())!['_synced_at'] as Date
    const again = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    expect(again).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    expect((await readUser())!['_synced_at']).toEqual(before)
  })

  it('keeps the fields of a full snapshot under a later subset in one merge batch', async () => {
    const stats = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [
        {
          data: userPayload({ external_id: 'ext_1', metadata: { plan: 'pro' } }),
          updatedAt: '2026-01-01T00:00:00.000Z',
        },
        {
          data: { object: 'user', id: 'user_1', email: 'ada@example.com', first_name: 'Grace' },
          updatedAt: '2026-01-02T00:00:00.000Z',
        },
      ],
      'merge',
    )
    expect(stats).toMatchObject({ created: 1, updated: 1, skipped: 0 })
    const row = (await readUser())!
    expect(row['first_name']).toBe('Grace')
    expect(row['external_id']).toBe('ext_1')
    expect(row['metadata']).toEqual({ plan: 'pro' })
  })

  it('gates every row of a merge batch against the stored row, not only against the batch', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ metadata: { plan: 'current' } }), updatedAt: '2026-01-02T00:00:00.000Z' }],
      'replace',
    )
    const stats = await upsertRows(
      db,
      opts,
      users,
      'prod',
      [
        { data: { object: 'user', id: 'user_1', metadata: { plan: 'old' } }, updatedAt: '2026-01-01T00:00:00.000Z' },
        { data: { object: 'user', id: 'user_1', first_name: 'Grace' }, updatedAt: '2026-01-03T00:00:00.000Z' },
      ],
      'merge',
    )
    expect(stats).toMatchObject({ created: 0, updated: 1, skipped: 1 })
    const row = (await readUser())!
    expect(row['first_name']).toBe('Grace')
    expect(row['metadata']).toEqual({ plan: 'current' })
    expect((row['_updated_at'] as Date).toISOString()).toBe('2026-01-03T00:00:00.000Z')
  })

  it.each(['merge', 'replace'] as const)('advances the timestamp for unchanged payloads in %s mode', async (mode) => {
    const roles = streamByName('roles')!
    const data = { slug: 'admin', permissions: ['read'] }
    await upsertRows(db, opts, roles, 'prod', [{ data, updatedAt: '2026-01-01T00:00:00.000Z' }], mode)
    const newer = await upsertRows(db, opts, roles, 'prod', [{ data, updatedAt: '2026-01-03T00:00:00.000Z' }], mode)
    const stale = await upsertRows(
      db,
      opts,
      roles,
      'prod',
      [{ data: { ...data, permissions: ['write'] }, updatedAt: '2026-01-02T00:00:00.000Z' }],
      mode,
    )
    expect(newer).toMatchObject({ created: 0, updated: 1, skipped: 0 })
    expect(stale).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    const result = await db.query(`select permissions, \`_updated_at\` from \`${db.schema}\`.roles`)
    expect(result.rows).toEqual([{ permissions: ['read'], _updated_at: new Date('2026-01-03T00:00:00.000Z') }])
  })

  it('merges shallowly: a nested object in the event replaces the stored one', async () => {
    // JSON_MERGE_PATCH would keep `plan` here. The contract says shallow, so
    // the merge happens in Node and `metadata` is whatever the event carried.
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ metadata: { plan: 'pro', seats: 5 } }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: { object: 'user', id: 'user_1', metadata: { seats: 6 } }, updatedAt: '2026-01-02T00:00:00.000Z' }],
      'merge',
    )
    expect((await readUser())!['metadata']).toEqual({ seats: 6 })
  })

  it('merges event payloads instead of nulling the fields events omit', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [
        {
          data: userPayload({ external_id: 'ext_42', metadata: { plan: 'enterprise' }, locale: 'fr' }),
          updatedAt: '2026-01-01T00:00:00.000Z',
        },
      ],
      'replace',
    )
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [
        {
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
      ],
      'merge',
    )
    const row = await readUser()
    expect(row!['email']).toBe('renamed@example.com')
    expect(row!['external_id']).toBe('ext_42')
    expect(row!['metadata']).toEqual({ plan: 'enterprise' })
    expect(row!['locale']).toBe('fr')
  })

  it('replaces the payload when the row came from the REST API', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ locale: 'fr' }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ updated_at: '2026-02-01T00:00:00.000Z' }), updatedAt: '2026-02-01T00:00:00.000Z' }],
      'replace',
    )
    expect((await readUser())!['locale']).toBeNull()
  })

  it('projects a JSON null as SQL NULL, not the text "null"', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ first_name: null, email_verified: null }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    const row = (await readUser())!
    expect(row['first_name']).toBeNull()
    expect(row['email_verified']).toBeNull()
  })

  it('projects booleans and lands a value that cannot be cast as NULL, keeping it in _raw', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [
        { data: userPayload({ id: 'user_t', email_verified: true }), updatedAt: '2026-01-01T00:00:00.000Z' },
        { data: userPayload({ id: 'user_f', email_verified: false }), updatedAt: '2026-01-01T00:00:00.000Z' },
        { data: userPayload({ id: 'user_m', email_verified: 'maybe' }), updatedAt: '2026-01-01T00:00:00.000Z' },
      ],
      'replace',
    )
    expect((await readUser('user_t'))!['email_verified']).toBe(1)
    expect((await readUser('user_f'))!['email_verified']).toBe(0)
    const m = (await readUser('user_m'))!
    expect(m['email_verified']).toBeNull()
    expect((m['_raw'] as Record<string, unknown>)['email_verified']).toBe('maybe')
  })

  it('refuses a row whose key does not fit the key column, rather than storing it under NULL', async () => {
    await expect(
      upsertRows(
        db,
        opts,
        users,
        'prod',
        [{ data: userPayload({ id: 'x'.repeat(200) }), updatedAt: '2026-01-01T00:00:00.000Z' }],
        'replace',
      ),
    ).rejects.toMatchObject({ code: 'ER_BAD_NULL_ERROR' })
  })

  it('tombstones on delete and keeps the last known payload', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    const stats = await deleteRows(db, opts, users, 'prod', [
      { data: { id: 'user_1' }, updatedAt: '2026-01-10T00:00:00.000Z' },
    ])
    expect(stats.deleted).toBe(1)
    const row = await readUser()
    expect(row!['_deleted_at']).not.toBeNull()
    expect(row!['email']).toBe('ada@example.com')
    // A delete is not a staleness-gated write: the row's own time stays.
    expect((row!['_updated_at'] as Date).toISOString()).toBe('2026-01-01T00:00:00.000Z')
  })

  it('records a delete for an object it never saw created', async () => {
    const stats = await deleteRows(db, opts, users, 'prod', [
      { data: { id: 'user_ghost', email: 'ghost@example.com' }, updatedAt: '2026-01-10T00:00:00.000Z' },
    ])
    expect(stats.deleted).toBe(1)
    expect((await readUser('user_ghost'))!['_deleted_at']).not.toBeNull()
  })

  it('does not resurrect a deleted row when a late update arrives', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_1' }, updatedAt: '2026-01-10T00:00:00.000Z' }])
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'zombie@example.com' }), updatedAt: '2026-01-20T00:00:00.000Z' }],
      'merge',
    )
    const row = await readUser()
    expect(row!['_deleted_at']).not.toBeNull()
    expect(row!['email']).toBe('zombie@example.com')
  })

  it('keeps the latest removal when a row is deleted twice', async () => {
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_1' }, updatedAt: '2026-01-10T00:00:00.000Z' }])
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_1' }, updatedAt: '2026-01-05T00:00:00.000Z' }])
    expect(((await readUser())!['_deleted_at'] as Date).toISOString()).toBe('2026-01-10T00:00:00.000Z')
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_1' }, updatedAt: '2026-01-12T00:00:00.000Z' }])
    expect(((await readUser())!['_deleted_at'] as Date).toISOString()).toBe('2026-01-12T00:00:00.000Z')
  })

  it('keeps environments isolated under one schema', async () => {
    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ email: 'prod@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    await upsertRows(
      db,
      opts,
      users,
      'staging',
      [{ data: userPayload({ email: 'staging@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' }],
      'replace',
    )
    const r = await db.query<{ _env: string; email: string }>(
      `select \`_env\`, email from \`${db.schema}\`.users order by \`_env\``,
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
    await unlinkRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-02T00:00:00.000Z' }])
    const back = await relinkRows(db, opts, memberships, 'prod', [
      { data: pair, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(back).toMatchObject({ created: 0, updated: 1, skipped: 0 })
    const r = await db.query<{ _deleted_at: Date | null }>(
      `select \`_deleted_at\` from \`${db.schema}\`.group_memberships where group_id = 'group_1'`,
    )
    expect(r.rows[0]!._deleted_at).toBeNull()
  })

  it('ignores a re-add older than the removal, even when the row itself is older', async () => {
    const memberships = streamByName('group_memberships')!
    const pair = { group_id: 'group_1', organization_membership_id: 'om_1' }
    await relinkRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-01T00:00:00.000Z' }])
    // A reconcile repair tombstones without moving `_updated_at`.
    await deleteRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-05T00:00:00.000Z' }])
    // A link replayed from quarantine, dated between the two: newer than the
    // row, older than the removal. It must not resurrect the membership.
    const stale = await relinkRows(db, opts, memberships, 'prod', [
      { data: pair, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(stale).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    const r = await db.query<{ _deleted_at: Date | null }>(
      `select \`_deleted_at\` from \`${db.schema}\`.group_memberships where group_id = 'group_1'`,
    )
    expect(r.rows[0]!._deleted_at).not.toBeNull()
  })

  it('ignores a membership removal older than the row, unlike an object delete', async () => {
    const memberships = streamByName('group_memberships')!
    const pair = { group_id: 'group_1', organization_membership_id: 'om_1' }
    const deletedAt = async () => {
      const r = await db.query<{ _deleted_at: Date | null }>(
        `select \`_deleted_at\` from \`${db.schema}\`.group_memberships where group_id = 'group_1'`,
      )
      return r.rows[0]!._deleted_at
    }
    await relinkRows(db, opts, memberships, 'prod', [{ data: pair, updatedAt: '2026-01-03T00:00:00.000Z' }])
    const stale = await unlinkRows(db, opts, memberships, 'prod', [
      { data: pair, updatedAt: '2026-01-02T00:00:00.000Z' },
    ])
    expect(stale).toMatchObject({ deleted: 0, skipped: 1 })
    expect(await deletedAt()).toBeNull()
    const fresh = await unlinkRows(db, opts, memberships, 'prod', [
      { data: pair, updatedAt: '2026-01-04T00:00:00.000Z' },
    ])
    expect(fresh).toMatchObject({ deleted: 1, skipped: 0 })
    expect(await deletedAt()).not.toBeNull()
  })

  it('never updates an append-only row', async () => {
    const events = streamByName('events')!
    const e = { id: 'event_1', event: 'user.created', data: { id: 'user_1' }, created_at: '2026-01-01T00:00:00.000Z' }
    const first = await upsertRows(db, opts, events, 'prod', [{ data: e, updatedAt: e.created_at }], 'replace')
    expect(first).toMatchObject({ created: 1, skipped: 0 })
    const dup = await upsertRows(
      db,
      opts,
      events,
      'prod',
      [{ data: { ...e, event: 'tampered' }, updatedAt: e.created_at }],
      'replace',
    )
    expect(dup).toMatchObject({ created: 0, skipped: 1 })
    const r = await db.query<{ event: string }>(`select event from \`${db.schema}\`.events`)
    expect(r.rows).toHaveLength(1)
    expect(r.rows[0]!.event).toBe('user.created')
  })

  it('writes large batches in chunks', async () => {
    const rows = Array.from({ length: 1200 }, (_, i) => ({
      data: userPayload({ id: `user_${i}`, email: `u${i}@example.com` }),
      updatedAt: '2026-01-01T00:00:00.000Z',
    }))
    const stats = await upsertRows(db, { schema: db.schema, batchSize: 250 }, users, 'prod', rows, 'replace')
    expect(stats.created).toBe(1200)
    const count = await db.query<{ n: number }>(`select count(*) as n from \`${db.schema}\`.users`)
    expect(Number(count.rows[0]!.n)).toBe(1200)
  })

  it('stores a payload whose string values look like SQL or JSON paths verbatim', async () => {
    const data = userPayload({ first_name: 'O\'Brien `$.x` \\ "quoted"', metadata: { '$.weird key': 'v' } })
    await upsertRows(db, opts, users, 'prod', [{ data, updatedAt: '2026-01-01T00:00:00.000Z' }], 'replace')
    const row = (await readUser())!
    expect(row['first_name']).toBe('O\'Brien `$.x` \\ "quoted"')
    expect(row['metadata']).toEqual({ '$.weird key': 'v' })
  })
})

describeDb('freshness reporting', () => {
  let db: TestDb

  beforeEach(async () => {
    db = await createTestDb()
    await migrate(db, SOME_STREAMS, db.schema)
  })
  afterEach(async () => {
    await db.close()
  })

  it('does not report the age of old objects as sync lag', async () => {
    const { MysqlDestination } = await import('../src/destination.js')
    const destination = new MysqlDestination({ db, schema: db.schema })
    const opts = { schema: db.schema }

    const empty = await destination.freshness(users, 'prod', 24)
    expect(empty).toEqual({
      stream: 'users',
      rows: 0,
      deleted: 0,
      lastSyncedAt: null,
      stalenessSeconds: null,
      lagSeconds: null,
      lagSampleSize: 0,
    })

    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ id: 'user_old' }), updatedAt: '2024-01-01T00:00:00.000Z' }],
      'replace',
    )
    const stale = await destination.freshness(users, 'prod', 24)
    expect(stale.rows).toBe(1)
    expect(stale.lagSampleSize).toBe(0)
    expect(stale.lagSeconds).toBeNull()
    expect(stale.stalenessSeconds!).toBeGreaterThan(365 * 86400)
    expect(Date.parse(stale.lastSyncedAt!)).toBeGreaterThan(Date.now() - 60_000)

    await upsertRows(
      db,
      opts,
      users,
      'prod',
      [{ data: userPayload({ id: 'user_fresh' }), updatedAt: new Date(Date.now() - 2000).toISOString() }],
      'replace',
    )
    await deleteRows(db, opts, users, 'prod', [{ data: { id: 'user_old' }, updatedAt: new Date().toISOString() }])

    const fresh = await destination.freshness(users, 'prod', 24)
    expect(fresh.rows).toBe(2)
    expect(fresh.deleted).toBe(1)
    expect(fresh.lagSampleSize).toBe(1)
    expect(fresh.lagSeconds).not.toBeNull()
    expect(fresh.lagSeconds!).toBeLessThan(60)
    expect(fresh.lagSeconds!).toBeGreaterThan(0)
  })
})
