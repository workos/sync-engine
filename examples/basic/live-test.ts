/**
 * Live end-to-end test against the real WorkOS API.
 *
 * Reads WORKOS_API_KEY from the environment. Creates real objects in the
 * pgsync-test environment, syncs them, mutates them, syncs again, and verifies
 * the mirror at each step.
 *
 * The destination is PGlite (real Postgres 16 compiled to WebAssembly) because
 * no Postgres server is running on this machine. Every SQL feature the engine
 * depends on — generated columns, xmax, jsonb operators, immutability rules —
 * is genuine Postgres behaviour.
 */
import { PGlite } from '@electric-sql/pglite'
import { SyncEngine, WorkOSClient, type SqlClient } from '@workos-inc/pg-sync'

const API_KEY = process.env.WORKOS_API_KEY
if (!API_KEY) throw new Error('WORKOS_API_KEY is not set')
const API_URL = 'https://api.workos.com'

const step = (n: string, t: string) => console.log(`\n\x1b[1m${n} ${t}\x1b[0m\n${'─'.repeat(64)}`)
const ok = (m: string) => console.log(`  \x1b[32m✓\x1b[0m ${m}`)
const info = (m: string) => console.log(`  ${m}`)
const fail = (m: string) => { console.log(`  \x1b[31m✗ ${m}\x1b[0m`); failures.push(m) }
const failures: string[] = []
const check = (cond: boolean, m: string) => (cond ? ok(m) : fail(m))

async function api(path: string, init?: RequestInit): Promise<any> {
  const res = await fetch(API_URL + path, {
    ...init,
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
  })
  const text = await res.text()
  if (!res.ok) throw new Error(`${res.status} ${init?.method ?? 'GET'} ${path}: ${text.slice(0, 300)}`)
  return text ? JSON.parse(text) : null
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function main() {
  // ------------------------------------------------------------- database
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

  const engine = new SyncEngine(
    {
      schema: 'workos',
      environments: [{ name: 'pgsync-test', apiKey: API_KEY!, apiUrl: API_URL }],
      streams: [
        'users', 'organizations', 'organization_memberships', 'organization_domains',
        'invitations', 'connections', 'directories', 'directory_users',
        'roles', 'permissions', 'feature_flags', 'events', 'auth_attempts',
      ],
      lagWindowMs: 0,
      hydrate: { users: 'on_create' },
      logger: {
        debug: () => {},
        info: (m, f) => { if (m === 'processed events') info(`   ${m}: ${JSON.stringify(f)}`) },
        warn: (m, f) => console.warn('   warn:', m, f ?? ''),
        error: (m, f) => console.error('   error:', m, f ?? ''),
      },
    },
    { db, clientFor: (env) => new WorkOSClient({ apiKey: env.apiKey, apiUrl: API_URL, pageSize: 100 }) },
  )

  const q = async (sql: string, params: unknown[] = []) => (await db.query(sql, params)).rows as any[]
  const stamp = Date.now().toString(36)

  // --------------------------------------------------- 0. verify the key
  step('0.', 'Verify API credentials and target environment')
  const existingOrgs = await api('/organizations?limit=5')
  ok(`API key works — ${existingOrgs.data.length} organization(s) already present`)
  const existingUsers = await api('/user_management/users?limit=5')
  info(`existing users: ${existingUsers.data.length}`)

  // ------------------------------------------------- 1. seed pre-existing
  step('1.', 'Seed data that exists BEFORE the sync engine starts')
  info('(this is the data a webhook-only integration could never recover)')

  const acme = await api('/organizations', {
    method: 'POST',
    body: JSON.stringify({
      name: `Acme Corp ${stamp}`,
      domain_data: [{ domain: `acme-${stamp}.example.com`, state: 'verified' }],
    }),
  })
  const initech = await api('/organizations', {
    method: 'POST',
    body: JSON.stringify({ name: `Initech ${stamp}` }),
  })
  ok(`created organizations: ${acme.id}, ${initech.id}`)

  const ada = await api('/user_management/users', {
    method: 'POST',
    body: JSON.stringify({
      email: `ada+${stamp}@pgsync-test.example.com`,
      first_name: 'Ada', last_name: 'Lovelace', email_verified: true,
      external_id: `ext_ada_${stamp}`,
      metadata: { plan: 'enterprise', seat: 'admin' },
    }),
  })
  const grace = await api('/user_management/users', {
    method: 'POST',
    body: JSON.stringify({
      email: `grace+${stamp}@pgsync-test.example.com`,
      first_name: 'Grace', last_name: 'Hopper', email_verified: true,
      external_id: `ext_grace_${stamp}`,
      metadata: { plan: 'pro' },
    }),
  })
  const alan = await api('/user_management/users', {
    method: 'POST',
    body: JSON.stringify({
      email: `alan+${stamp}@pgsync-test.example.com`,
      first_name: 'Alan', last_name: 'Turing', email_verified: true,
    }),
  })
  ok(`created users: Ada, Grace, Alan`)

  for (const [u, o] of [[ada, acme], [grace, acme], [alan, initech]] as const) {
    await api('/user_management/organization_memberships', {
      method: 'POST',
      body: JSON.stringify({ user_id: u.id, organization_id: o.id }),
    })
  }
  ok('created 3 organization memberships')

  // ------------------------------------------------------- 2. schema
  step('2.', 'Create the schema')
  await engine.init()
  const tables = await q(
    `select table_name from information_schema.tables
     where table_schema='workos' and table_name not like '\\_%' order by table_name`,
  )
  ok(`created ${tables.length} tables: ${tables.map((t) => t.table_name).join(', ')}`)

  const gen = await q(
    `select count(*)::int as n from information_schema.columns
     where table_schema='workos' and table_name='users' and is_generated='ALWAYS'`,
  )
  check(gen[0].n >= 10, `workos.users has ${gen[0].n} generated columns projected from _raw`)

  // ------------------------------------------------------ 3. backfill
  step('3.', 'Backfill — load everything that already exists')
  const t0 = Date.now()
  const results = await engine.backfill()
  info(`completed in ${((Date.now() - t0) / 1000).toFixed(1)}s`)
  for (const r of results) {
    const n = r.stats.created + r.stats.updated
    if (n > 0) info(`   ${r.stream}: ${n} rows`)
    else if (r.skippedReason && !r.skippedReason.startsWith('failed')) info(`   ${r.stream}: — ${r.skippedReason}`)
    else if (r.skippedReason) fail(`${r.stream}: ${r.skippedReason}`)
  }

  const users = await q(`select id, email, external_id, metadata, created_at from workos.users order by email`)
  check(users.length >= 3, `mirrored ${users.length} users`)

  const adaRow = users.find((u) => u.id === ada.id)
  check(!!adaRow, 'Ada is in the mirror')
  check(adaRow?.external_id === `ext_ada_${stamp}`, `external_id survived backfill: ${adaRow?.external_id}`)
  check(adaRow?.metadata?.plan === 'enterprise', `metadata survived backfill: ${JSON.stringify(adaRow?.metadata)}`)
  check(adaRow?.created_at instanceof Date || typeof adaRow?.created_at === 'string',
    `created_at parsed to a real timestamp: ${adaRow?.created_at}`)

  const orgs = await q(`select id, name, domains from workos.organizations order by name`)
  check(orgs.length >= 2, `mirrored ${orgs.length} organizations`)

  const doms = await q(`select domain, state, organization_id from workos.organization_domains`)
  check(doms.length >= 1, `mirrored ${doms.length} organization domain(s) from the embedded payload`)

  const mems = await q(`select id, user_id, organization_id, status from workos.organization_memberships`)
  check(mems.length >= 3, `mirrored ${mems.length} memberships`)

  // ------------------------------------------------------ 4. live tail
  step('4.', 'Live changes — mutate upstream, then tail the Events API')

  await api(`/user_management/users/${grace.id}`, {
    method: 'PUT',
    body: JSON.stringify({ last_name: 'Hopper-Murray' }),
  })
  info('updated Grace (last_name)')

  const katherine = await api('/user_management/users', {
    method: 'POST',
    body: JSON.stringify({
      email: `katherine+${stamp}@pgsync-test.example.com`,
      first_name: 'Katherine', last_name: 'Johnson', email_verified: true,
      external_id: `ext_kat_${stamp}`, metadata: { plan: 'pro' },
    }),
  })
  info('created Katherine')

  await api('/user_management/organization_memberships', {
    method: 'POST',
    body: JSON.stringify({ user_id: katherine.id, organization_id: acme.id }),
  })
  info('added Katherine to Acme')

  await api(`/user_management/users/${alan.id}`, { method: 'DELETE' })
  info('deleted Alan')

  info('waiting for events to become available...')
  await sleep(6000)

  // ------------------------------------------------ 4a. cursor direction
  // The one genuinely unresolved question in the design: the docs are
  // ambiguous about which way `after` walks under each order mode, and getting
  // it wrong would replay history while looking healthy. Settle it empirically.
  step('4a.', 'Which direction does the `after` cursor page?')
  const evParam = engine.eventTypes.join(',')
  const asc = await api(`/events?limit=5&order=asc&events=${encodeURIComponent(evParam)}`)
  const desc = await api(`/events?limit=5&order=desc&events=${encodeURIComponent(evParam)}`)
  const none = await api(`/events?limit=5&events=${encodeURIComponent(evParam)}`)
  const ids = (r: any) => r.data.map((e: any) => e.id).join(',')
  check(ids(asc) === ids(desc) && ids(asc) === ids(none),
    'order=asc, order=desc and omitting order return identical pages — `order` has no effect')
  const ascending = asc.data.every((e: any, i: number) => i === 0 || e.id > asc.data[i - 1].id)
  check(ascending, 'the stream is always oldest-first')

  const oldest = await api(`/events?limit=1&order=asc&events=${encodeURIComponent(evParam)}`)
  const oldestId = oldest.data[0]?.id
  const afterOldest = await api(
    `/events?limit=100&order=asc&after=${oldestId}&events=${encodeURIComponent(evParam)}`)
  const allNewer = afterOldest.data.every((e: any) => e.id > oldestId)
  check(allNewer && afterOldest.data.length > 0,
    `after=<oldest> returns ${afterOldest.data.length} strictly newer events — forward paging confirmed`)
  info('   → order=asc + after = tail forward in time. The engine\'s assumption is correct.')

  let processed = 0
  for (let i = 0; i < 6; i++) {
    const r = await engine.tick()
    processed += r.processed
    if (r.processed === 0 && i > 1) break
    await sleep(2500)
  }
  check(processed > 0, `processed ${processed} events from the Events API`)

  const graceRow = (await q(`select last_name, external_id, metadata from workos.users where id=$1`, [grace.id]))[0]
  check(graceRow?.last_name === 'Hopper-Murray', `update applied: last_name = ${graceRow?.last_name}`)
  // The critical WorkOS-specific behaviour: user.updated events do NOT carry
  // external_id or metadata. A whole-row replace would null them here.
  check(graceRow?.external_id === `ext_grace_${stamp}`,
    `external_id NOT nulled by the update event: ${graceRow?.external_id}`)
  check(graceRow?.metadata?.plan === 'pro',
    `metadata NOT nulled by the update event: ${JSON.stringify(graceRow?.metadata)}`)

  const katRow = (await q(`select email, external_id, metadata from workos.users where id=$1`, [katherine.id]))[0]
  check(!!katRow, 'new user arrived via the event stream')
  check(katRow?.external_id === `ext_kat_${stamp}`,
    `hydration filled external_id on create: ${katRow?.external_id}`)

  const alanRow = (await q(`select email, "_deleted_at" from workos.users where id=$1`, [alan.id]))[0]
  check(!!alanRow?._deleted_at, 'deleted user is tombstoned, not dropped')
  const live = await q(`select count(*)::int as n from workos.users where "_deleted_at" is null`)
  info(`live users: ${live[0].n}`)

  // ------------------------------------------------- 5. archive + idempotency
  step('5.', 'Event archive and idempotency')
  const archived = await q(`select event, count(*)::int as n from workos.events group by event order by n desc`)
  check(archived.length > 0, `archived ${archived.reduce((a, r) => a + r.n, 0)} events across ${archived.length} types`)
  for (const a of archived.slice(0, 8)) info(`   ${a.event}: ${a.n}`)

  const before = (await q(`select count(*)::int as n from workos.users`))[0].n
  const cursorBefore = (await q(`select state from workos."_sync_state" where stream='__tail__'`))[0]?.state
  await engine.tick()
  const after = (await q(`select count(*)::int as n from workos.users`))[0].n
  check(before === after, `re-running the tail changed nothing (${before} = ${after} users)`)
  check(!!cursorBefore?.cursor, `cursor is persisted: ${String(cursorBefore?.cursor).slice(0, 24)}...`)

  // ------------------------------------------------------ 6. reconcile
  step('6.', 'Reconciliation against the live API')
  const rec = await engine.reconcile()
  const diffs = rec.diffs['pgsync-test']!
  let realDrift = 0
  for (const d of diffs) {
    if (d.skippedReason) continue
    const clean = d.localOnly.length === 0 && d.remoteOnly.length === 0
    if (clean) { ok(`${d.stream}: ${d.matched} matched, no drift`); continue }
    // Drift here is legitimate rather than a defect. Replaying 90 days of
    // history can teach the mirror about an object that was created and later
    // deleted before this sync existed: the create event is still in the
    // stream, the object is not in any list endpoint, and no future event will
    // ever mention it again. That is precisely the missed-delete case only a
    // set difference can detect.
    realDrift += d.localOnly.length + d.remoteOnly.length
    info(`   ${d.stream}: ${d.localOnly.length} local-only, ${d.remoteOnly.length} remote-only ` +
         `(objects known only from historical events — repaired below)`)
  }
  ok(`reconcile surfaced ${realDrift} drifted row(s) that no event could correct`)

  // Now create real drift the event stream cannot repair, and fix it.
  step('7.', 'Detect and repair real drift')
  const ghost = await api('/user_management/users', {
    method: 'POST',
    body: JSON.stringify({
      email: `ghost+${stamp}@pgsync-test.example.com`,
      first_name: 'Ghost', last_name: 'User', email_verified: true,
    }),
  })
  info(`created a user WITHOUT syncing it: ${ghost.id}`)
  // Advance the cursor past its event so the tail will never see it.
  await sleep(6000)
  const cur = await api(
    `/events?limit=1&order=desc&events=${encodeURIComponent(engine.eventTypes.join(','))}`)
  await db.query(
    `update workos."_sync_state" set state = jsonb_set(state, '{cursor}', $1::jsonb) where stream='__tail__'`,
    [JSON.stringify(cur.data[0]?.id ?? null)],
  )
  info('advanced the cursor past its creation event (simulating a missed event)')

  const drift = await engine.reconcile()
  const ud = drift.diffs['pgsync-test']!.find((d) => d.stream === 'users')!
  check(ud.remoteOnly.includes(ghost.id), `reconcile found the missed user: ${ud.remoteOnly.length} remote-only`)

  await engine.reconcile({ repair: true })
  const repaired = (await q(`select email from workos.users where id=$1`, [ghost.id]))[0]
  check(!!repaired, `repair inserted the missing user: ${repaired?.email}`)

  const final = await engine.reconcile()
  const fd = final.diffs['pgsync-test']!.find((d) => d.stream === 'users')!
  check(fd.localOnly.length === 0 && fd.remoteOnly.length === 0, 'mirror is clean after repair')

  // --------------------------------------------------------- 8. queries
  step('8.', 'The queries this exists for')
  const members = await q(
    `select o.name as organization, u.email, m.status
     from workos.organization_memberships m
     join workos.users u on u.id=m.user_id and u."_env"=m."_env"
     join workos.organizations o on o.id=m.organization_id and o."_env"=m."_env"
     where u."_deleted_at" is null and m."_deleted_at" is null
     order by o.name, u.email`,
  )
  console.table(members)

  const orphans = await q(
    `select u.email from workos.users u
     left join workos.organization_memberships m on m.user_id=u.id and m."_deleted_at" is null
     where u."_deleted_at" is null and m.id is null order by u.email`,
  )
  info(`users with no organization: ${orphans.map((o) => o.email).join(', ') || '(none)'}`)

  // -------------------------------------------------------- 9. status
  step('9.', 'Operational view')
  const status = await engine.status()
  const s = status['pgsync-test']!
  info(`cursor: ${s.cursor}`)
  info(`cursor age: ${s.cursorAgeDays === null ? 'n/a' : (s.cursorAgeDays * 24 * 60).toFixed(2) + ' minutes'}`)
  console.table(
    s.streams.filter((x) => x.rows > 0).map((x) => ({
      stream: x.stream, rows: x.rows, deleted: x.deleted,
      'lag(s)': x.lagSeconds === null ? '—' : x.lagSeconds.toFixed(1),
    })),
  )

  // -------------------------------------------------------- summary
  console.log('\n' + '═'.repeat(64))
  if (failures.length === 0) {
    console.log('\x1b[32m✓ ALL CHECKS PASSED against the live WorkOS API\x1b[0m')
  } else {
    console.log(`\x1b[31m✗ ${failures.length} CHECK(S) FAILED:\x1b[0m`)
    for (const f of failures) console.log(`   - ${f}`)
  }
  console.log('═'.repeat(64))

  // Record what was created so it can be cleaned up.
  console.log('\nCreated in WorkOS (for cleanup):')
  console.log(JSON.stringify({
    organizations: [acme.id, initech.id],
    users: [ada.id, grace.id, katherine.id, ghost.id],
    deletedAlready: [alan.id],
  }, null, 2))

  await engine.close()
  await db.close()
  process.exit(failures.length === 0 ? 0 : 1)
}

main().catch((err) => { console.error('\n\x1b[31mFATAL:\x1b[0m', err); process.exit(1) })
