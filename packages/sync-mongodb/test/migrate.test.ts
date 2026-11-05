/**
 * Schema setup: collections, indexes and views derived from the catalog, with
 * a fingerprint so an unchanged catalog costs one query.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { streamByName, STREAMS, selectStreams, type ColumnDef, type TableDef } from '@workos/sync-engine'
import { createTestDb, type TestDb } from './helpers/mongo.js'
import { captureLogger } from './helpers/faulty.js'
import { migrate, fingerprint, indexSpecs, indexName, DDL_VERSION } from '../src/schema/migrate.js'
import { ensureLiveView, LIVE_PIPELINE } from '../src/schema/views.js'
import { Names } from '../src/schema/names.js'
import { MongoDestination } from '../src/destination.js'

const names = new Names()
const users = streamByName('users')!

const withColumns = (stream: TableDef, extra: ColumnDef[]): TableDef => ({
  ...stream,
  columns: [...stream.columns, ...extra],
})

describe('fingerprint', () => {
  it('changes with the catalog, the prefix and the DDL version, and not with stream order', () => {
    const a = fingerprint(STREAMS, 'workos_')
    expect(a).toMatch(/^[0-9a-f]{16}$/)
    expect(fingerprint([...STREAMS].reverse(), 'workos_')).toBe(a)
    expect(fingerprint(STREAMS, 'mirror_')).not.toBe(a)
    expect(fingerprint(selectStreams(['users']), 'workos_')).not.toBe(a)
    expect(
      fingerprint(
        STREAMS.map((s) =>
          s.name === 'users'
            ? withColumns(s, [{ name: 'region', type: 'text', path: ['metadata', 'region'], index: true }])
            : s,
        ),
        'workos_',
      ),
    ).not.toBe(a)
    expect(DDL_VERSION).toBeGreaterThanOrEqual(1)
  })
})

describe('indexSpecs', () => {
  it('derives the key, the indexed columns and the engine’s own scans, under the default names', () => {
    const specs = indexSpecs(users)
    expect(specs[0]).toEqual({ key: { _env: 1, '_raw.id': 1 }, unique: true })
    expect(specs.map((s) => indexName(s.key))).toEqual([
      '_env_1__raw.id_1',
      '_env_1__raw.email_1',
      '_env_1__raw.external_id_1',
      '_synced_at_1',
      '_deleted_at_1',
    ])
    expect(specs.at(-1)).toEqual({
      key: { _deleted_at: 1 },
      partialFilterExpression: { _deleted_at: { $type: 'date' } },
    })
    // Composite keys and nested paths.
    const roles = streamByName('organization_roles')!
    expect(indexSpecs(roles)[0]).toEqual({ key: { _env: 1, '_raw.organization_id': 1, '_raw.slug': 1 }, unique: true })
    // Append-only: no tombstone index. An indexed key column is not indexed twice.
    const events = streamByName('events')!
    expect(indexSpecs(events).some((s) => '_deleted_at' in s.key)).toBe(false)
    expect(
      indexSpecs(withColumns(users, [{ name: 'id', type: 'text', path: 'id', index: true }])).filter(
        (s) => indexName(s.key) === '_env_1__raw.id_1',
      ),
    ).toHaveLength(1)
  })
})

describe('migrate', () => {
  let t: TestDb

  beforeEach(async () => {
    t = await createTestDb()
  })
  afterEach(async () => {
    await t.close()
  })

  // `system.views` appears once the first view exists; it is the server's, not ours.
  const collections = async () =>
    (await t.db.listCollections().toArray())
      .filter((c) => !c.name.startsWith('system.'))
      .map((c) => [c.name, c.type] as const)
      .sort()
  const indexNames = async (coll: string) =>
    (await t.db.collection(coll).listIndexes().toArray()).map((i) => i.name as string).sort()

  it('creates collections, indexes, views and bookkeeping once, then is a no-op', async () => {
    const streams = selectStreams(['users', 'events'])
    const first = await migrate(t.db, streams, names)
    // users: key, email, external_id, _synced_at, _deleted_at; events: key, event, object_id, created_at, _synced_at.
    expect(first).toMatchObject({
      applied: true,
      collections: 2,
      indexes: 5 + 5,
      views: ['workos_users_live'],
      orphans: [],
    })

    expect(await collections()).toEqual([
      ['workos__migrations', 'collection'],
      ['workos__rejected', 'collection'],
      ['workos__sync', 'collection'],
      ['workos_events', 'collection'],
      ['workos_users', 'collection'],
      ['workos_users_live', 'view'],
    ])
    expect(await indexNames('workos_users')).toEqual([
      '_deleted_at_1',
      '_env_1__raw.email_1',
      '_env_1__raw.external_id_1',
      '_env_1__raw.id_1',
      '_id_',
      '_synced_at_1',
    ])
    expect(await indexNames('workos__sync')).toEqual(['_id_', 'sync_id_1_env_1'])
    expect(await indexNames('workos__rejected')).toEqual(['_id_', 'sync_id_1_env_1_event_id_1'])
    const unique = (await t.db.collection('workos_users').listIndexes().toArray()).find(
      (i) => i.name === '_env_1__raw.id_1',
    )
    expect(unique?.unique).toBe(true)

    const second = await migrate(t.db, streams, names)
    expect(second).toMatchObject({ applied: false, fingerprint: first.fingerprint })
    expect(await t.db.collection('workos__migrations').countDocuments()).toBe(1)
  })

  it('adds an index for a column promoted later and reports one the catalog dropped, without removing it', async () => {
    await migrate(t.db, [users], names)
    const promoted = withColumns(users, [{ name: 'region', type: 'text', path: ['metadata', 'region'], index: true }])
    const { logger, lines } = captureLogger()
    const r = await migrate(t.db, [promoted], names, logger)
    expect(r.applied).toBe(true)
    expect(await indexNames('workos_users')).toContain('_env_1__raw.metadata.region_1')
    expect(r.orphans).toEqual([])

    // The column moves to another path: the old index lingers and is named, never dropped.
    const moved = withColumns(users, [{ name: 'region', type: 'text', path: ['metadata', 'zone'], index: true }])
    const back = await migrate(t.db, [moved], names, logger)
    expect(back.applied).toBe(true)
    expect(back.orphans).toEqual([
      {
        collection: 'workos_users',
        index: '_env_1__raw.metadata.region_1',
        key: { _env: 1, '_raw.metadata.region': 1 },
      },
    ])
    expect(await indexNames('workos_users')).toContain('_env_1__raw.metadata.region_1')
    expect(await indexNames('workos_users')).toContain('_env_1__raw.metadata.zone_1')
    expect(lines.find((l) => l.level === 'warn' && l.msg === 'orphan index')?.fields).toMatchObject({
      index: '_env_1__raw.metadata.region_1',
    })
    // Every catalog seen was recorded; detection only runs when one is new.
    expect(await t.db.collection('workos__migrations').countDocuments()).toBe(3)
    expect((await migrate(t.db, [users], names, logger)).applied).toBe(false)
  })

  it('tolerates two inits racing', async () => {
    const results = await Promise.all([
      migrate(t.db, [users], names),
      migrate(t.db, [users], names),
      migrate(t.db, [users], names),
    ])
    expect(results.every((r) => r.fingerprint === results[0]!.fingerprint)).toBe(true)
    expect(await t.db.collection('workos__migrations').countDocuments()).toBe(1)
    expect(await collections()).toContainEqual(['workos_users_live', 'view'])
  })

  it('keeps prefixes apart in one database', async () => {
    await migrate(t.db, [users], new Names('a_'))
    await migrate(t.db, [users], new Names('b_'))
    const all = (await collections()).map(([n]) => n)
    expect(all).toContain('a_users')
    expect(all).toContain('b_users')
    expect(all).toContain('a__sync')
    expect(all).toContain('b__sync')
  })

  describe('live views', () => {
    it('hide tombstones and nothing else', async () => {
      const dest = new MongoDestination({ client: t.client, database: t.name })
      await dest.init([users])
      const at = '2026-01-01T00:00:00.000Z'
      await dest.upsert(
        users,
        'prod',
        [
          { data: { id: 'user_1', email: 'stays@example.com' }, updatedAt: at },
          { data: { id: 'user_2', email: 'gone@example.com' }, updatedAt: at },
        ],
        'replace',
      )
      await dest.delete(users, 'prod', [{ data: { id: 'user_2' }, updatedAt: at }])
      const live = t.db.collection('workos_users_live')
      expect(await live.countDocuments()).toBe(1)
      expect((await live.findOne({}))!['_raw']).toMatchObject({ email: 'stays@example.com' })
      expect(await t.db.collection('workos_users').countDocuments()).toBe(2)
      // A re-add surfaces again, since the pipeline matches null as well as missing.
      const memberships = streamByName('directory_group_memberships')!
      await dest.init([users, memberships])
      const pair = { directory_group_id: 'g', directory_user_id: 'u' }
      await dest.unlink(memberships, 'prod', [{ data: pair, updatedAt: at }])
      expect(await t.db.collection('workos_directory_group_memberships_live').countDocuments()).toBe(0)
      await dest.relink(memberships, 'prod', [{ data: pair, updatedAt: '2026-01-02T00:00:00.000Z' }])
      expect(await t.db.collection('workos_directory_group_memberships_live').countDocuments()).toBe(1)
    })

    it('are redefined by a repair even though the fingerprint is already recorded', async () => {
      const dest = new MongoDestination({ client: t.client, database: t.name })
      await dest.init([users])
      expect(await migrate(t.db, [users], names)).toMatchObject({ applied: false })

      // Somebody changed the pipeline by hand after init recorded its fingerprint.
      await t.db.command({ collMod: 'workos_users_live', viewOn: 'workos_users', pipeline: [] })
      // Same streams, same prefix: the fingerprint matches, and a repair must not trust it.
      const r = await migrate(t.db, [users], names, undefined, { repairViews: true })
      expect(r).toMatchObject({ applied: true, views: ['workos_users_live'] })
      const info = (await t.db.listCollections({ name: 'workos_users_live' }, { nameOnly: false }).toArray())[0]!
      expect(info.options?.['pipeline']).toEqual(LIVE_PIPELINE)
      // The fingerprint was recorded once; the repair did not duplicate it.
      expect(await t.db.collection(names.migrations).countDocuments()).toBe(1)
      expect(await migrate(t.db, [users], names)).toMatchObject({ applied: false })
    })

    it('are kept when already right, redefined only on request, and never clobber a stranger', async () => {
      expect(await ensureLiveView(t.db, names, users)).toBe('created')
      expect(await ensureLiveView(t.db, names, users)).toBe('kept')

      // Somebody changed the pipeline by hand.
      await t.db.command({
        collMod: 'workos_users_live',
        viewOn: 'workos_users',
        pipeline: [{ $match: { _env: 'prod' } }],
      })
      await expect(ensureLiveView(t.db, names, users)).rejects.toThrow(/init --repair/)
      await expect(migrate(t.db, [users], names)).rejects.toThrow(/init --repair/)
      const { logger, lines } = captureLogger()
      expect(await ensureLiveView(t.db, names, users, logger, { repairViews: true })).toBe('rebuilt')
      expect(lines.some((l) => l.msg === 'live view redefined to the generated pipeline')).toBe(true)
      const info = (await t.db.listCollections({ name: 'workos_users_live' }, { nameOnly: false }).toArray())[0]!
      expect(info.options?.['pipeline']).toEqual(LIVE_PIPELINE)

      // A view over something else, or a plain collection, is not ours to touch.
      await t.db.dropCollection('workos_users_live')
      await t.db.createCollection('workos_users_live', { viewOn: 'somebody_elses', pipeline: [] })
      await expect(ensureLiveView(t.db, names, users, undefined, { repairViews: true })).rejects.toThrow(
        /not the generated view/,
      )
      await t.db.dropCollection('workos_users_live')
      await t.db.createCollection('workos_users_live')
      await expect(ensureLiveView(t.db, names, users, undefined, { repairViews: true })).rejects.toThrow(
        /not the generated view/,
      )
    })
  })
})
