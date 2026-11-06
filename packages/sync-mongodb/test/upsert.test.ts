import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName, STREAMS, keyValues } from '@workos/sync-engine'
import type { Collection } from 'mongodb'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { migrate } from '../src/schema/migrate.js'
import { Names } from '../src/schema/names.js'
import { upsertRows, deleteRows, relinkRows, unlinkRows, keyFilter, type MirrorDoc } from '../src/db/upsert.js'
import { InvalidTimestampError } from '../src/db/timestamps.js'

const users = streamByName('users')!
const events = streamByName('events')!
const memberships = streamByName('directory_group_memberships')!
const names = new Names()
const opts = {}

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
  it('reads composite keys, and names a row by every one of them', () => {
    const orgRoles = streamByName('organization_roles')!
    expect(keyValues(orgRoles, { organization_id: 'org_1', slug: 'admin' })).toEqual(['org_1', 'admin'])
    expect(keyFilter(orgRoles, 'prod', { organization_id: 'org_1', slug: 'admin' })).toEqual({
      _env: 'prod',
      '_raw.organization_id': { $eq: 'org_1' },
      '_raw.slug': { $eq: 'admin' },
    })
    // Nested paths are addressed through _raw too.
    expect(keyFilter(users, 'prod', { id: 'user_1' })).toEqual({ _env: 'prod', '_raw.id': { $eq: 'user_1' } })
  })

  it('refuses an object where a key should be, and never lets one into a filter as an operator', () => {
    expect(keyValues(users, { id: { $ne: null } })).toBeUndefined()
    expect(keyValues(users, { id: ['user_1'] })).toBeUndefined()
    // Were a row to reach the filter anyway, the object is a value to equal, not a query.
    expect(keyFilter(users, 'prod', { id: { $ne: null } })).toEqual({ _env: 'prod', '_raw.id': { $eq: { $ne: null } } })
  })
})

describe('write path', () => {
  let t: TestDb
  let coll: Collection<MirrorDoc>

  beforeEach(async () => {
    t = await createTestDb()
    await migrate(t.db, STREAMS, names)
    coll = t.db.collection<MirrorDoc>(names.mirror(users))
  })
  afterEach(async () => {
    await t.close()
  })

  const row = (data: Record<string, unknown>, updatedAt: string) => ({ data, updatedAt })
  const readUser = async (id = 'user_1') => coll.findOne({ _env: 'prod', '_raw.id': id })

  it('judges duplicate rows by the strict parser the write uses, and refuses a bad timestamp before discarding any', async () => {
    await upsertRows(coll, opts, users, 'prod', [row(userPayload(), '2026-01-15T00:00:00.000Z')], 'replace')
    // Zone-less at ten and zoned at noon: noon is newer, whatever the process's zone.
    await deleteRows(coll, opts, users, 'prod', [
      row({ id: 'user_1' }, '2026-01-15T12:00:00Z'),
      row({ id: 'user_1' }, '2026-01-15T10:00:00'),
    ])
    const doc = await coll.findOne({ '_raw.id': 'user_1' })
    expect(doc?._deleted_at?.toISOString()).toBe('2026-01-15T12:00:00.000Z')
    await expect(
      deleteRows(coll, opts, users, 'prod', [
        row({ id: 'user_1' }, '2026-01-16T00:00:00Z'),
        row({ id: 'user_1' }, 'yesterday'),
      ]),
    ).rejects.toMatchObject({ code: 'INVALID_TIMESTAMP' })
  })

  it('inserts and reports created vs updated', async () => {
    const first = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload(), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    expect(first).toMatchObject({ created: 1, updated: 0, skipped: 0 })

    const second = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ email: 'changed@example.com' }), '2026-01-02T00:00:00.000Z')],
      'replace',
    )
    expect(second).toMatchObject({ created: 0, updated: 1, skipped: 0 })
    const doc = (await readUser())!
    expect(doc._raw['email']).toBe('changed@example.com')
    expect(doc._updated_at.toISOString()).toBe('2026-01-02T00:00:00.000Z')
    expect(doc._synced_at.getTime()).toBeGreaterThan(Date.now() - 60_000)
    expect(doc._env).toBe('prod')
  })

  it('rejects a write that is older than the stored row', async () => {
    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ email: 'current@example.com' }), '2026-01-05T00:00:00.000Z')],
      'replace',
    )
    // An out-of-order delivery arriving late must not win.
    const stale = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ email: 'stale@example.com' }), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    expect(stale).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    expect((await readUser())!._raw['email']).toBe('current@example.com')
  })

  it('accepts a write with an identical timestamp', async () => {
    // The gate is >= rather than >. Two events can share a timestamp, and with
    // a strict comparison the second is dropped forever.
    const ts = '2026-01-05T00:00:00.000Z'
    await upsertRows(coll, opts, users, 'prod', [row(userPayload({ email: 'first@example.com' }), ts)], 'replace')
    const same = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ email: 'second@example.com' }), ts)],
      'replace',
    )
    expect(same.updated).toBe(1)
    expect((await readUser())!._raw['email']).toBe('second@example.com')
  })

  it('skips a write with the same payload and timestamp', async () => {
    await upsertRows(coll, opts, users, 'prod', [row(userPayload(), '2026-01-01T00:00:00.000Z')], 'replace')
    const before = (await readUser())!._synced_at
    const again = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload(), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    // Rewriting an identical observation would move _synced_at for nothing.
    expect(again).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    expect((await readUser())!._synced_at).toEqual(before)
  })

  it('keeps the fields of a full snapshot under a later subset in one merge batch', async () => {
    // A hydrated create and the update event that followed it share a poll.
    const stats = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [
        row(userPayload({ external_id: 'ext_1', metadata: { plan: 'pro' } }), '2026-01-01T00:00:00.000Z'),
        row(
          { object: 'user', id: 'user_1', email: 'ada@example.com', first_name: 'Grace' },
          '2026-01-02T00:00:00.000Z',
        ),
      ],
      'merge',
    )
    expect(stats).toMatchObject({ created: 1, updated: 1, skipped: 0 })
    const doc = (await readUser())!
    expect(doc._raw).toMatchObject({ first_name: 'Grace', external_id: 'ext_1', metadata: { plan: 'pro' } })
    expect(doc._updated_at.toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })

  it('merges shallowly: a nested object is replaced whole, a null is kept, a payload is never read as an expression', async () => {
    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ metadata: { plan: 'pro', region: 'eu' }, locale: 'en' }), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    const merged = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [
        row(
          {
            id: 'user_1',
            // `$100` would be a field path in an expression; it must land as text.
            first_name: '$100 plan',
            // A key the event omits inside metadata is gone, as `jsonb ||` would make it.
            metadata: { plan: 'free' },
            locale: null,
            // Directory payloads carry attribute names with dots and dollars.
            raw_attributes: { 'urn:ietf:params:scim:schemas:core:2.0:User': { 'a.b': 1, $ref: 'x' } },
          },
          '2026-01-02T00:00:00.000Z',
        ),
      ],
      'merge',
    )
    expect(merged).toMatchObject({ updated: 1 })
    const doc = (await readUser())!
    expect(doc._raw).toMatchObject({
      email: 'ada@example.com',
      first_name: '$100 plan',
      metadata: { plan: 'free' },
      locale: null,
      raw_attributes: { 'urn:ietf:params:scim:schemas:core:2.0:User': { 'a.b': 1, $ref: 'x' } },
    })
    expect((doc._raw['metadata'] as Record<string, unknown>)['region']).toBeUndefined()
  })

  it('replaces whole when told to, dropping what the payload omits', async () => {
    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ external_id: 'ext_1' }), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row({ id: 'user_1', email: 'x@example.com' }, '2026-01-02T00:00:00.000Z')],
      'replace',
    )
    expect((await readUser())!._raw).toEqual({ id: 'user_1', email: 'x@example.com' })
  })

  it('tombstones terminally: a later update refreshes the payload but never resurrects the row', async () => {
    await upsertRows(coll, opts, users, 'prod', [row(userPayload(), '2026-01-01T00:00:00.000Z')], 'replace')
    const deleted = await deleteRows(coll, opts, users, 'prod', [row({ id: 'user_1' }, '2026-01-02T00:00:00.000Z')])
    expect(deleted).toMatchObject({ deleted: 1 })
    let doc = (await readUser())!
    expect(doc._deleted_at?.toISOString()).toBe('2026-01-02T00:00:00.000Z')
    expect(doc._raw['email']).toBe('ada@example.com')

    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row({ id: 'user_1', email: 'late@example.com' }, '2026-01-03T00:00:00.000Z')],
      'merge',
    )
    doc = (await readUser())!
    expect(doc._raw['email']).toBe('late@example.com')
    expect(doc._deleted_at?.toISOString()).toBe('2026-01-02T00:00:00.000Z')

    // A delete for a row never seen inserts the tombstone, and an older
    // delete never moves a newer tombstone back.
    expect(
      await deleteRows(coll, opts, users, 'prod', [row({ id: 'user_9' }, '2026-01-01T00:00:00.000Z')]),
    ).toMatchObject({ deleted: 1 })
    expect((await readUser('user_9'))!._deleted_at?.toISOString()).toBe('2026-01-01T00:00:00.000Z')
    await deleteRows(coll, opts, users, 'prod', [row({ id: 'user_1' }, '2026-01-01T12:00:00.000Z')])
    expect((await readUser())!._deleted_at?.toISOString()).toBe('2026-01-02T00:00:00.000Z')
  })

  it('applies membership links and unlinks by time, in both directions', async () => {
    const mcoll = t.db.collection<MirrorDoc>(names.mirror(memberships))
    const pair = { directory_group_id: 'dg_1', directory_user_id: 'du_1', directory_id: 'dir_1' }
    const at = (day: number) => `2026-01-${String(day).padStart(2, '0')}T00:00:00.000Z`
    const read = () =>
      mcoll.findOne({ _env: 'prod', '_raw.directory_group_id': 'dg_1', '_raw.directory_user_id': 'du_1' })

    // Added, then removed.
    expect(await relinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(1))])).toMatchObject({ created: 1 })
    expect(await unlinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(3))])).toMatchObject({ deleted: 1 })
    expect((await read())!._deleted_at?.toISOString()).toBe(at(3))

    // A re-add older than the removal is a replayed stale event: a no-op.
    expect(await relinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(2))])).toMatchObject({
      skipped: 1,
      updated: 0,
    })
    expect((await read())!._deleted_at?.toISOString()).toBe(at(3))

    // A re-add at least as new as the removal clears the tombstone.
    expect(await relinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(3))])).toMatchObject({ updated: 1 })
    expect((await read())!._deleted_at).toBeNull()

    // A removal older than the re-add must not hide a membership active now.
    expect(await unlinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(2))])).toMatchObject({
      skipped: 1,
      deleted: 0,
    })
    expect((await read())!._deleted_at).toBeNull()

    // A removal that really is newer lands, and a reconcile tombstone (not gated)
    // is compared against by the next re-add.
    expect(await unlinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(5))])).toMatchObject({ deleted: 1 })
    await deleteRows(mcoll, opts, memberships, 'prod', [row(pair, at(7))])
    expect((await read())!._deleted_at?.toISOString()).toBe(at(7))
    expect(await relinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(6))])).toMatchObject({ skipped: 1 })
    expect(await relinkRows(mcoll, opts, memberships, 'prod', [row(pair, at(7))])).toMatchObject({ updated: 1 })
    expect((await read())!._deleted_at).toBeNull()

    // An unlink for a membership never seen inserts it tombstoned.
    const other = { ...pair, directory_user_id: 'du_2' }
    expect(await unlinkRows(mcoll, opts, memberships, 'prod', [row(other, at(1))])).toMatchObject({ deleted: 1 })
    expect((await mcoll.findOne({ '_raw.directory_user_id': 'du_2' }))!._deleted_at?.toISOString()).toBe(at(1))
  })

  it('treats an append-only stream as an immutable log', async () => {
    const ecoll = t.db.collection<MirrorDoc>(names.mirror(events))
    const event = {
      id: 'event_1',
      event: 'user.created',
      data: { id: 'user_1', name: '$x' },
      created_at: '2026-01-01T00:00:00.000Z',
    }
    expect(await upsertRows(ecoll, opts, events, 'prod', [row(event, event.created_at)], 'replace')).toMatchObject({
      created: 1,
    })
    // Re-delivery, even with a changed payload or a newer timestamp, changes nothing.
    expect(
      await upsertRows(
        ecoll,
        opts,
        events,
        'prod',
        [row({ ...event, data: { id: 'other' } }, '2026-01-02T00:00:00.000Z')],
        'replace',
      ),
    ).toMatchObject({ created: 0, updated: 0, skipped: 1 })
    const doc = (await ecoll.findOne({ '_raw.id': 'event_1' }))!
    expect(doc._raw).toEqual(event)
    expect(doc._deleted_at).toBeUndefined()
  })

  it('keeps environments apart under the same key', async () => {
    await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row(userPayload({ email: 'prod@example.com' }), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    await upsertRows(
      coll,
      opts,
      users,
      'staging',
      [row(userPayload({ email: 'staging@example.com' }), '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    expect(await coll.countDocuments()).toBe(2)
    expect((await coll.findOne({ _env: 'staging' }))!._raw['email']).toBe('staging@example.com')
  })

  it('leaves every other row alone when a key is an object', async () => {
    await upsertRows(coll, opts, users, 'prod', [row(userPayload(), '2026-01-01T00:00:00.000Z')], 'replace')
    const malformed = [row(userPayload({ id: { $ne: null } }), '2026-02-01T00:00:00.000Z')]
    expect(await upsertRows(coll, opts, users, 'prod', malformed, 'replace')).toMatchObject({ skipped: 1 })
    expect(await deleteRows(coll, opts, users, 'prod', malformed)).toMatchObject({ skipped: 1 })
    expect(await coll.countDocuments()).toBe(1)
    const stored = (await readUser())!
    expect(stored._raw['updated_at']).toBe('2026-01-01T00:00:00.000Z')
    expect(stored._deleted_at ?? null).toBeNull()
    // And the filter itself, given such a row, matches nothing rather than everything.
    expect(await coll.countDocuments(keyFilter(users, 'prod', { id: { $ne: null } }))).toBe(0)
  })

  it('stores an environment label that starts with "$" as text', async () => {
    const ecoll = t.db.collection<MirrorDoc>(names.mirror(events))
    await upsertRows(coll, opts, users, '$staging', [row(userPayload(), '2026-01-01T00:00:00.000Z')], 'merge')
    await upsertRows(ecoll, opts, events, '$staging', [row({ id: 'event_1' }, '2026-01-01T00:00:00.000Z')], 'replace')
    expect((await coll.findOne({ '_raw.id': 'user_1' }))!._env).toBe('$staging')
    expect((await ecoll.findOne({ '_raw.id': 'event_1' }))!._env).toBe('$staging')
  })

  it('counts rows it cannot key as skipped, and refuses a timestamp that is not an instant', async () => {
    const stats = await upsertRows(
      coll,
      opts,
      users,
      'prod',
      [row({ email: 'no-id@example.com' }, '2026-01-01T00:00:00.000Z')],
      'replace',
    )
    expect(stats).toMatchObject({ created: 0, skipped: 1 })

    await expect(
      upsertRows(coll, opts, users, 'prod', [row(userPayload(), '2026-02-30T00:00:00.000Z')], 'replace'),
    ).rejects.toThrow(InvalidTimestampError)
    expect(await coll.countDocuments()).toBe(0)
  })

  it('writes in batches of the size asked for', async () => {
    const rows = Array.from({ length: 7 }, (_, i) => row(userPayload({ id: `user_${i}` }), '2026-01-01T00:00:00.000Z'))
    expect(await upsertRows(coll, { batchSize: 3 }, users, 'prod', rows, 'replace')).toMatchObject({ created: 7 })
    expect(await coll.countDocuments({ _env: 'prod' })).toBe(7)
  })
})
