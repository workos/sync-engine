/**
 * End-to-end walkthrough of the WorkOS sync engine.
 *
 *   pnpm start
 *
 * It runs against a stand-in WorkOS API over real HTTP and an embedded
 * Postgres, so it needs no credentials, no Docker and no network. Everything
 * else — the schema, the write path, the cursor, the reconciler — is the code
 * that runs in production.
 *
 * The script walks the full lifecycle:
 *
 *   1. create the schema
 *   2. backfill existing state from the REST list endpoints
 *   3. tail the Events API and apply live changes
 *   4. answer the questions that motivated the mirror, in SQL
 *   5. detect drift the event stream cannot see, and repair it
 */
import { PGlite } from '@electric-sql/pglite'
import { SyncEngine, WorkOSClient, PostgresDestination, type SqlClient } from '@workos/sync-postgres'
import { FakeWorkOS } from '../../packages/sync-engine/test/helpers/fake-workos.js'

const heading = (n: number, title: string) =>
  console.log(`\n\x1b[1m${n}. ${title}\x1b[0m\n${'─'.repeat(60)}`)

const table = (rows: Record<string, unknown>[]) => {
  if (rows.length === 0) return console.log('  (no rows)')
  console.table(rows)
}

async function main() {
  // ---------------------------------------------------------------- setup
  const pg = new PGlite()
  await pg.waitReady
  const db: SqlClient = {
    async query(sql, params) {
      const r = await pg.query(sql, params as never[])
      return { rows: r.rows as never[] }
    },
    async exec(sql) {
      await pg.exec(sql)
    },
    async transaction(fn) {
      await pg.exec('begin')
      try {
        const out = await fn(this)
        await pg.exec('commit')
        return out
      } catch (e) {
        await pg.exec('rollback')
        throw e
      }
    },
    async close() {
      await pg.close()
    },
  }

  const fake = new FakeWorkOS()
  const { server, url } = await fake.listen()
  console.log(`Stand-in WorkOS API listening on ${url}`)
  console.log('Embedded Postgres ready')

  // Seed the state that already exists before the sync engine is installed.
  // This is the data a webhook-only integration could never recover.
  const acme = fake.createOrganization({
    name: 'Acme Corp',
    domains: [{ object: 'organization_domain', id: 'org_domain_acme', domain: 'acme.com', state: 'verified' }],
  })
  // Initech has a verified domain but never finished setting up SCIM — the
  // gap the "verified domain, no directory" query below is meant to surface.
  const initech = fake.createOrganization({
    name: 'Initech',
    domains: [{ object: 'organization_domain', id: 'org_domain_initech', domain: 'initech.com', state: 'verified' }],
  })
  const ada = fake.createUser({ email: 'ada@acme.com', first_name: 'Ada', last_name: 'Lovelace' })
  const grace = fake.createUser({ email: 'grace@acme.com', first_name: 'Grace', last_name: 'Hopper' })
  const alan = fake.createUser({ email: 'alan@initech.com', first_name: 'Alan', last_name: 'Turing' })
  fake.createMembership(ada['id'] as string, acme['id'] as string, 'admin')
  fake.createMembership(grace['id'] as string, acme['id'] as string, 'member')
  fake.createMembership(alan['id'] as string, initech['id'] as string, 'admin')
  const directory = fake.createDirectory(acme['id'] as string)
  fake.createDirectoryUser(directory['id'] as string, acme['id'] as string)

  const engine = new SyncEngine({
    destination: new PostgresDestination({ db, schema: 'workos' }),
    environments: [{ name: 'production', apiKey: 'sk_test_example', apiUrl: url }],
    streams: [
      'users',
      'organizations',
      'organization_memberships',
      'organization_domains',
      'directories',
      'directory_users',
      'events',
      'auth_attempts',
    ],
    // Hold nothing back: the fake's clock is deterministic.
    lagWindowMs: 0,
    hydrate: { users: 'on_create' },
    logger: { debug: () => {}, info: () => {}, warn: () => {}, error: (m, f) => console.error(m, f) },
  }, {
    clientFor: (env) => new WorkOSClient({ apiKey: env.apiKey, apiUrl: url, pageSize: 100 }),
  })

  // ------------------------------------------------------------ 1. schema
  heading(1, 'Create the schema')
  await engine.init()
  const tables = await db.query<{ table_name: string }>(
    `select table_name from information_schema.tables
     where table_schema = 'workos' and table_name not like '\\_%' order by table_name`,
  )
  console.log('  tables:', tables.rows.map((r) => r.table_name).join(', '))

  const cols = await db.query<{ column_name: string; is_generated: string }>(
    `select column_name, is_generated from information_schema.columns
     where table_schema = 'workos' and table_name = 'users' order by ordinal_position`,
  )
  console.log('\n  workos.users — only _raw is ever written; the rest are projected from it:')
  table(
    cols.rows.map((c) => ({
      column: c.column_name,
      generated: c.is_generated === 'ALWAYS' ? 'yes' : '—',
    })),
  )

  // ---------------------------------------------------------- 2. backfill
  heading(2, 'Backfill what already exists')
  const results = await engine.backfill()
  table(
    results
      .filter((r) => r.stats.created + r.stats.updated > 0 || r.skippedReason)
      .map((r) => ({
        stream: r.stream,
        rows: r.stats.created + r.stats.updated,
        note: r.skippedReason ?? '',
      })),
  )

  // -------------------------------------------------------------- 3. tail
  heading(3, 'Apply live changes from the Events API')

  // Things happen upstream while the engine is running.
  fake.updateUser(grace['id'] as string, { last_name: 'Hopper-Murray' })
  const newHire = fake.createUser({ email: 'katherine@acme.com', first_name: 'Katherine', last_name: 'Johnson' })
  fake.createMembership(newHire['id'] as string, acme['id'] as string, 'member')
  fake.emitAuthAttempt(ada['id'] as string, true)
  fake.emitAuthAttempt(grace['id'] as string, false)
  fake.emitAuthAttempt(grace['id'] as string, false)
  fake.deleteUser(alan['id'] as string)
  // An event type this build has never heard of. It must not break the stream.
  fake.emitUnknown()

  const tick = await engine.tick()
  console.log(`  processed ${tick.processed} events`)
  console.log(`  writes:`, tick.stats)

  const archived = await db.query<{ event: string; n: string }>(
    `select event, count(*)::text as n from workos.events group by event order by event`,
  )
  console.log('\n  every event archived, including the unrecognised one:')
  table(archived.rows.map((r) => ({ event: r.event, count: r.n })))

  // ------------------------------------------------------------- 4. query
  heading(4, 'Ask questions that need a join')

  console.log('\n  Members per organization, with their roles:')
  table(
    (
      await db.query(
        // The _live views are the tables minus tombstones, so no _deleted_at filter.
        `select o.name as organization, u.email, m.role_slug as role
         from workos.organization_memberships_live m
         join workos.users_live u on u.id = m.user_id and u."_env" = m."_env"
         join workos.organizations_live o on o.id = m.organization_id and o."_env" = m."_env"
         order by o.name, u.email`,
      )
    ).rows,
  )

  console.log('\n  Failed sign-ins by user — this history has no REST endpoint at all:')
  table(
    (
      await db.query(
        `select coalesce(u.email, a.email) as email,
                count(*) filter (where a.status = 'failed')::int as failed,
                count(*) filter (where a.status = 'succeeded')::int as succeeded
         from workos.auth_attempts a
         left join workos.users u on u.id = a.user_id and u."_env" = a."_env"
         group by 1 order by failed desc, email`,
      )
    ).rows,
  )

  console.log('\n  Organizations with a verified domain but no SCIM directory:')
  table(
    (
      await db.query(
        `select o.name as organization, d.domain
         from workos.organizations o
         join workos.organization_domains d
           on d.organization_id = o.id and d."_env" = o."_env" and d.state = 'verified'
         left join workos.directories dir
           on dir.organization_id = o.id and dir."_env" = o."_env" and dir."_deleted_at" is null
         where o."_deleted_at" is null and dir.id is null
         order by o.name`,
      )
    ).rows,
  )

  // Join mirrored identity data to a table the application owns.
  await db.exec(`create table app_usage (user_id text primary key, sessions int, last_seen timestamptz)`)
  await db.query(`insert into app_usage values ($1, 42, now()), ($2, 0, now() - interval '45 days')`, [
    ada['id'],
    grace['id'],
  ])

  console.log('\n  Identity data joined to product usage — the reason the mirror exists:')
  table(
    (
      await db.query(
        `select u.email, u.metadata->>'plan' as plan, coalesce(a.sessions, 0) as sessions,
                case when a.last_seen < now() - interval '30 days' then 'dormant' else 'active' end as state
         from workos.users u
         left join app_usage a on a.user_id = u.id
         where u."_deleted_at" is null and a.user_id is not null
         order by u.email`,
      )
    ).rows,
  )

  console.log('\n  Deleted users are tombstoned, not dropped — you can still see who left:')
  table(
    (
      await db.query(
        `select email, "_deleted_at" is not null as deleted from workos.users
         where "_deleted_at" is not null`,
      )
    ).rows,
  )

  // --------------------------------------------------------- 5. reconcile
  heading(5, 'Detect and repair drift')

  // Simulate the two failures the event stream cannot show you: an object
  // deleted while the engine was not watching, and one created in the same gap.
  fake.users.delete(grace['id'] as string)
  const missed = fake.createUser({ email: 'missed@acme.com', first_name: 'Missed', last_name: 'Entirely' })
  fake.events = [] // the events for both are gone

  const before = await engine.reconcile()
  const usersDiff = before.diffs['production']!.find((d) => d.stream === 'users')!
  console.log(`  in the mirror but not upstream (missed deletes): ${usersDiff.localOnly.length}`)
  console.log(`  upstream but not in the mirror (missed creates): ${usersDiff.remoteOnly.length}`)
  console.log('  a missed delete is invisible to any re-list: only a set difference finds it')

  await engine.reconcile({ repair: true })
  const after = await engine.reconcile()
  const fixed = after.diffs['production']!.find((d) => d.stream === 'users')!
  console.log(`\n  after repair — localOnly: ${fixed.localOnly.length}, remoteOnly: ${fixed.remoteOnly.length}`)
  console.log(`  ${missed['email']} is now present:`,
    (await db.query(`select 1 from workos.users where email = 'missed@acme.com' and "_deleted_at" is null`)).rows.length === 1)

  // ------------------------------------------------------------- 6. status
  heading(6, 'Operational view')
  const status = await engine.status()
  const prod = status['production']!
  console.log(`  cursor: ${prod.cursor}`)
  console.log(`  cursor age: ${prod.cursorAgeDays === null ? 'n/a' : prod.cursorAgeDays.toFixed(4)} days (alarm at 75, data gone at 90)`)
  table(
    prod.streams
      .filter((s) => s.rows > 0)
      .map((s) => ({
        stream: s.stream,
        rows: s.rows,
        deleted: s.deleted,
        'lag (s)': s.lagSeconds === null ? '—' : s.lagSeconds.toFixed(2),
      })),
  )

  console.log('\n\x1b[32mDone.\x1b[0m The mirror is a read model: eventually consistent, usually within seconds.')
  console.log('Read from it for joins, lists, dashboards and agents. Send writes to the WorkOS API.\n')

  await engine.close()
  await db.close()
  server.close()
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
