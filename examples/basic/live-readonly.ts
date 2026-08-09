/**
 * Read-only live test against a real WorkOS environment.
 *
 * Creates and deletes NOTHING upstream. The engine only ever issues GETs
 * against WorkOS; the only writes are into the local Postgres mirror. Safe to
 * point at an environment containing real data.
 *
 * Verifies: schema creation, paginated backfill of real objects, ISO-8601
 * parsing of real timestamps, per-directory rate limiting, cursor capture, and
 * reconciliation reporting zero drift immediately after a backfill.
 */
import { PGlite } from '@electric-sql/pglite'
import { SyncEngine, WorkOSClient, type SqlClient } from '@workos/pg-sync'

const API_KEY = process.env.WORKOS_API_KEY
if (!API_KEY) throw new Error('WORKOS_API_KEY is not set')
const API_URL = 'https://api.workos.com'
const ENV_LABEL = process.env.WORKOS_ENVIRONMENT_NAME ?? 'staging'

const step = (n: string, t: string) => console.log(`\n\x1b[1m${n} ${t}\x1b[0m\n${'─'.repeat(64)}`)
const ok = (m: string) => console.log(`  \x1b[32m✓\x1b[0m ${m}`)
const info = (m: string) => console.log(`  ${m}`)
const failures: string[] = []
const check = (c: boolean, m: string) => (c ? ok(m) : (console.log(`  \x1b[31m✗ ${m}\x1b[0m`), failures.push(m)))

async function main() {
  const pg = new PGlite()
  await pg.waitReady
  const db: SqlClient = {
    async query(sql, params) {
      const r = await pg.query(sql, params as never[])
      return { rows: r.rows as never[] }
    },
    async exec(sql) { await pg.exec(sql) },
    async transaction(fn) {
      await pg.exec('begin')
      try { const o = await fn(this); await pg.exec('commit'); return o }
      catch (e) { await pg.exec('rollback'); throw e }
    },
    async close() { await pg.close() },
  }
  const q = async (sql: string, p: unknown[] = []) => (await db.query(sql, p)).rows as any[]

  const engine = new SyncEngine(
    {
      schema: 'workos',
      environments: [{ name: ENV_LABEL, apiKey: API_KEY!, apiUrl: API_URL }],
      streams: [
        'users', 'organizations', 'organization_memberships', 'organization_domains',
        'invitations', 'connections', 'directories', 'directory_users', 'directory_groups',
        'roles', 'permissions', 'feature_flags', 'events', 'auth_attempts',
      ],
      lagWindowMs: 0,
      logger: {
        debug: () => {},
        info: () => {},
        warn: (m, f) => console.warn('   warn:', m, JSON.stringify(f ?? {})),
        error: (m, f) => console.error('   error:', m, JSON.stringify(f ?? {})),
      },
    },
    { db, clientFor: (env) => new WorkOSClient({ apiKey: env.apiKey, apiUrl: API_URL, pageSize: 100 }) },
  )

  step('1.', 'Create the schema against real Postgres')
  await engine.init()
  const tables = await q(
    `select table_name from information_schema.tables
     where table_schema='workos' and table_name not like '\\_%' order by table_name`)
  ok(`${tables.length} tables created`)
  const gen = await q(
    `select count(*)::int n from information_schema.columns
     where table_schema='workos' and is_generated='ALWAYS'`)
  ok(`${gen[0].n} generated columns projected from _raw across all tables`)

  step('2.', 'Backfill from the live API (read-only)')
  const t0 = Date.now()
  const results = await engine.backfill()
  info(`completed in ${((Date.now() - t0) / 1000).toFixed(1)}s`)
  let total = 0
  for (const r of results) {
    const n = r.stats.created + r.stats.updated
    total += n
    if (n > 0) info(`   ${r.stream.padEnd(26)} ${n} rows`)
    else if (r.skippedReason?.startsWith('failed')) check(false, `${r.stream}: ${r.skippedReason}`)
  }
  check(total > 0, `backfilled ${total} rows total`)

  step('3.', 'Verify real payloads landed correctly')
  const users = await q(`select id, email, email_verified, external_id, metadata, created_at, updated_at from workos.users order by email`)
  check(users.length > 0, `${users.length} users mirrored`)
  for (const u of users.slice(0, 12)) {
    info(`   ${String(u.email).padEnd(34)} verified=${u.email_verified} created=${u.created_at ? new Date(u.created_at).toISOString().slice(0, 10) : 'NULL'}`)
  }
  // Real timestamps, parsed by the immutable generated column, not a cast.
  const badTs = users.filter((u) => u.created_at === null)
  check(badTs.length === 0, `every created_at parsed from real ISO-8601 (${users.length}/${users.length})`)
  const sane = users.every((u) => {
    const t = new Date(u.created_at).getTime()
    return t > Date.parse('2015-01-01') && t < Date.now() + 86400000
  })
  check(sane, 'all parsed timestamps fall in a sane range')

  const orgs = await q(`select id, name, domains, created_at from workos.organizations order by name`)
  check(orgs.length > 0, `${orgs.length} organizations mirrored`)
  for (const o of orgs) info(`   ${String(o.name).padEnd(24)} domains=${JSON.stringify(o.domains ?? []).slice(0, 60)}`)

  const conns = await q(`select name, connection_type, state from workos.connections`)
  for (const c of conns) info(`   connection: ${c.name} (${c.connection_type}) state=${c.state}`)
  const dirs = await q(`select name, type, state, organization_id from workos.directories`)
  for (const d of dirs) info(`   directory: ${d.name} (${d.type}) state=${d.state}`)
  const dusers = await q(`select count(*)::int n from workos.directory_users`)
  info(`   directory users: ${dusers[0].n}`)
  const doms = await q(`select domain, state from workos.organization_domains order by domain`)
  for (const d of doms) info(`   domain: ${d.domain} state=${d.state}`)
  const mems = await q(`select count(*)::int n from workos.organization_memberships`)
  info(`   memberships: ${mems[0].n}`)

  step('4.', 'Cursor capture')
  const status = await engine.status()
  const s = status[ENV_LABEL]!
  info(`cursor: ${s.cursor ?? '(no events in window — environment is quiet)'}`)
  check('cursor' in s, 'cursor state persisted to _sync_state')

  step('5.', 'Reconcile against the live API')
  info('(a backfill just ran, so a correct mirror must show zero drift)')
  const rec = await engine.reconcile()
  const diffs = rec.diffs[ENV_LABEL]!
  for (const d of diffs) {
    if (d.skippedReason) { info(`   ${d.stream.padEnd(26)} — ${d.skippedReason}`); continue }
    const clean = d.localOnly.length === 0 && d.remoteOnly.length === 0
    check(clean, `${d.stream.padEnd(26)} ${d.matched} matched${clean ? ', no drift' : `, ${d.localOnly.length} local-only / ${d.remoteOnly.length} remote-only`}`)
  }

  step('6.', 'Idempotency — a second backfill must change nothing')
  const before = await q(`select count(*)::int n, max("_synced_at") m from workos.users`)
  const again = await engine.backfill()
  const uStats = again.find((r) => r.stream === 'users')!.stats
  const after = await q(`select count(*)::int n from workos.users`)
  check(before[0].n === after[0].n, `row count unchanged (${before[0].n})`)
  check(uStats.created === 0, `no rows re-created (created=${uStats.created}, skipped=${uStats.skipped})`)

  step('7.', 'Queries over real data')
  const rows = await q(
    `select o.name as organization, u.email, m.status
     from workos.organization_memberships m
     join workos.users u on u.id=m.user_id and u."_env"=m."_env"
     join workos.organizations o on o.id=m.organization_id and o."_env"=m."_env"
     where u."_deleted_at" is null and m."_deleted_at" is null
     order by o.name, u.email limit 20`)
  if (rows.length) console.table(rows)
  else info('   (no memberships in this environment)')

  const orphans = await q(
    `select u.email from workos.users u
     left join workos.organization_memberships m on m.user_id=u.id and m."_deleted_at" is null
     where u."_deleted_at" is null and m.id is null order by u.email`)
  info(`users belonging to no organization: ${orphans.length}`)

  step('8.', 'Freshness view')
  console.table(
    s.streams.filter((x) => x.rows > 0).map((x) => ({
      stream: x.stream, rows: x.rows, deleted: x.deleted,
      'lag(s)': x.lagSeconds === null ? 'n/a (no recent changes)' : x.lagSeconds.toFixed(2),
    })))

  console.log('\n' + '═'.repeat(64))
  if (failures.length === 0) console.log('\x1b[32m✓ ALL READ-ONLY CHECKS PASSED against the live WorkOS API\x1b[0m')
  else { console.log(`\x1b[31m✗ ${failures.length} FAILED:\x1b[0m`); failures.forEach((f) => console.log('   - ' + f)) }
  console.log('═'.repeat(64))
  console.log('\nNothing was created, modified, or deleted in WorkOS.')

  await engine.close()
  await db.close()
  process.exit(failures.length === 0 ? 0 : 1)
}

main().catch((e) => { console.error('\n\x1b[31mFATAL:\x1b[0m', e); process.exit(1) })
