import { randomUUID } from 'node:crypto'
import { readFileSync } from 'node:fs'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { LeaseHeldError, STREAMS, SyncEngine, WorkOSClient, silentLogger } from '@workos/sync-engine'
import { FakeWorkOS } from '../../sync-engine/test/helpers/fake-workos.js'
import { connect, type SqlClient } from '../src/db/client.js'
import { PostgresDestination } from '../src/destination.js'

// Run unchanged against local Postgres, Neon, or Supabase. Only our randomly named
// schema is changed; there are no real WorkOS calls or privileged server operations.
describe.skipIf(!process.env.TEST_DATABASE_URL)('Postgres provider compatibility', () => {
  let db: SqlClient
  let schema: string
  let fake: FakeWorkOS
  let destination: PostgresDestination
  let engine: SyncEngine

  function makeEngine() {
    return new SyncEngine({
      destination,
      environments: [{ name: 'test', apiKey: 'sk_fake' }],
      streams: ['users', 'organizations', 'organization_memberships', 'auth_attempts'],
      logger: silentLogger,
      lagWindowMs: 0,
    }, {
      clientFor: () => new WorkOSClient({ apiKey: 'sk_fake', fetchImpl: fake.fetchImpl }),
    })
  }

  beforeEach(async () => {
    schema = `compat_test_${randomUUID().replaceAll('-', '')}`
    db = await connect(process.env.TEST_DATABASE_URL!, {
      sslCa: process.env.TEST_DATABASE_SSL_CA_FILE
        ? readFileSync(process.env.TEST_DATABASE_SSL_CA_FILE, 'utf8') : undefined,
    })
    fake = new FakeWorkOS()
    destination = new PostgresDestination({ db, schema })
    engine = makeEngine()
  })

  afterEach(async () => {
    if (!db) return
    try {
      await db.exec(`drop schema if exists "${schema}" cascade`)
    } finally {
      await db.close()
    }
  })

  it('creates every stream and initializes the same schema twice', async () => {
    await destination.init(STREAMS)
    expect(destination.lastMigration?.drift).toEqual([])
    await destination.init(STREAMS)
    expect(destination.lastMigration?.applied).toBe(false)
  })

  it('backfills joins, tails updates and auth events, resumes its cursor, and tombstones deletes', async () => {
    const org = fake.createOrganization({ name: 'Compatibility Air' })
    const user = fake.createUser({ email: 'before@example.test' })
    const userId = user['id'] as string
    fake.createMembership(userId, org['id'] as string, 'admin')
    await engine.backfill()
    const joined = await db.query(`select u.email, o.name, m.role_slug
      from "${schema}".users_live u
      join "${schema}".organization_memberships_live m on m.user_id = u.id and m._env = u._env
      join "${schema}".organizations_live o on o.id = m.organization_id and o._env = m._env`)
    expect(joined.rows).toEqual([{ email: 'before@example.test', name: 'Compatibility Air', role_slug: 'admin' }])
    await engine.tick()

    fake.updateUser(userId, { email: 'after@example.test' })
    fake.emitAuthAttempt(userId, false)
    await engine.tick()
    expect((await db.query(`select email from "${schema}".users_live`)).rows).toEqual([{ email: 'after@example.test' }])
    expect((await db.query(`select status from "${schema}".auth_attempts`)).rows).toEqual([{ status: 'failed' }])
    const cursor = (await engine.status())['test']!.cursor
    expect(cursor).not.toBeNull()

    const restarted = makeEngine()
    expect((await restarted.status())['test']!.cursor).toBe(cursor)
    expect((await restarted.tick()).processed).toBe(0)
    fake.deleteUser(userId)
    await restarted.tick()
    expect((await db.query(`select id from "${schema}".users_live`)).rows).toEqual([])
    expect((await db.query(`select id from "${schema}".users where _deleted_at is not null`)).rows).toEqual([{ id: userId }])
    expect((await db.query(`select count(*)::int as n from "${schema}".events`)).rows[0]).toEqual({ n: 6 })
  })

  it('reconciles a missed delete using temporary tables and enforces the writer lease', async () => {
    const user = fake.createUser()
    await engine.backfill()
    await engine.tick()
    fake.users.delete(user['id'] as string) // Simulate a missed event.
    await engine.reconcile({ repair: true, streams: ['users'] })
    expect((await db.query(`select id from "${schema}".users_live`)).rows).toEqual([])
    expect((await db.query(`select count(*)::int as n from "${schema}".users where _deleted_at is not null`)).rows[0]).toEqual({ n: 1 })

    expect((await destination.acquireLease('test', 'another-writer', 60_000)).acquired).toBe(true)
    await expect(engine.tick()).rejects.toBeInstanceOf(LeaseHeldError)
    await destination.releaseLease('test', 'another-writer')
    await engine.tick()
    expect(await destination.getLease('test')).toBeNull()
  })
})
