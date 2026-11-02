import { randomUUID } from 'node:crypto'
import { Pool } from 'pg'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { streamByName } from '@workos/sync-engine'
import { fromPgPool, type SqlClient } from '../src/db/client.js'
import { migrate } from '../src/schema/migrate.js'
import { quoteIdent } from '../src/schema/ddl.js'
import { detectColumnDrift } from '../src/schema/drift.js'
import { GENERATED_BY } from '../src/schema/views.js'

function pauseAfterSchema(client: SqlClient, created: () => void, resume: Promise<void>): SqlClient {
  return {
    ...client,
    async exec(sql) {
      await client.exec(sql)
      if (sql.startsWith('create schema')) {
        created()
        await resume
      }
    },
    transaction: (fn) => client.transaction((tx) => fn(pauseAfterSchema(tx, created, resume))),
  }
}

describe.skipIf(!process.env.TEST_DATABASE_URL)('schema initialization on PostgreSQL', () => {
  const users = streamByName('users')!
  const changed = {
    ...users,
    columns: users.columns.map((column) => (column.name === 'email' ? { ...column, path: 'primary_email' } : column)),
  }
  let pools: Pool[]
  let db: SqlClient
  let other: SqlClient
  let observer: SqlClient
  let otherPid: number
  let schema: string

  beforeEach(async () => {
    schema = `migrate_test_${randomUUID().replaceAll('-', '')}`
    pools = Array.from(
      { length: 3 },
      () =>
        new Pool({
          connectionString: process.env.TEST_DATABASE_URL,
          max: 1,
          statement_timeout: 10_000,
        }),
    )
    db = fromPgPool(pools[0]!)
    other = fromPgPool(pools[1]!)
    observer = fromPgPool(pools[2]!)
    otherPid = (await other.query<{ pid: number }>('select pg_backend_pid() as pid')).rows[0]!.pid
  })

  afterEach(async () => {
    try {
      await observer.exec(`drop schema if exists "${schema}" cascade`)
    } finally {
      await Promise.all(pools.map((pool) => pool.end()))
    }
  })

  it('waits for an in-progress first migration before creating any catalog objects', async () => {
    let created!: () => void
    let resume!: () => void
    const ready = new Promise<void>((resolve) => {
      created = resolve
    })
    const paused = new Promise<void>((resolve) => {
      resume = resolve
    })
    const first = db.transaction((tx) => migrate(pauseAfterSchema(tx, created, paused), [users], schema))
    let second: ReturnType<typeof migrate> | undefined
    try {
      await Promise.race([ready, first])
      second = migrate(other, [users], schema)
      await expect
        .poll(
          async () => {
            const result = await observer.query<{ wait_event_type: string }>(
              'select wait_event_type from pg_stat_activity where pid = $1',
              [otherPid],
            )
            return result.rows[0]?.wait_event_type
          },
          { timeout: 5_000, interval: 10 },
        )
        .toBe('Lock')
      resume()
      const results = await Promise.all([first, second])
      expect(results.map((result) => result.applied)).toEqual([true, false])
      expect((await other.query(`select count(*)::int as n from "${schema}"._migrations`)).rows).toEqual([{ n: 1 }])
      expect((await other.query(`select * from "${schema}".users_live`)).rows).toEqual([])
    } finally {
      resume()
      await Promise.allSettled(second ? [first, second] : [first])
    }
  })

  it('allows concurrent warm startup with single-connection pools', async () => {
    await migrate(db, [users], schema)
    const results = await Promise.all([migrate(db, [users], schema), migrate(other, [users], schema)])
    expect(results.map((result) => result.applied)).toEqual([false, false])
  })

  it('rolls back failed bootstrap and releases the lock for another initializer', async () => {
    await expect(migrate(db, [{ ...users, key: ['missing_column'] }], schema)).rejects.toThrow(/does not exist/)
    expect((await observer.query('select to_regnamespace($1) as name', [schema])).rows).toEqual([{ name: null }])
    expect((await migrate(other, [users], schema)).applied).toBe(true)
  })

  it('recovers from a refused view replacement without losing grants', async () => {
    await migrate(db, [users], schema)
    const columns = await db.query<{ column_name: string }>(
      `select column_name from information_schema.columns
       where table_schema = $1 and table_name = 'users' order by ordinal_position desc`,
      [schema],
    )
    await db.exec(`
      drop view "${schema}".users_live;
      create view "${schema}".users_live as
        select ${columns.rows.map((column) => quoteIdent(column.column_name)).join(', ')} from "${schema}".users;
      comment on view "${schema}".users_live is '${GENERATED_BY}: older definition';
      grant select on "${schema}".users_live to public;
      delete from "${schema}"._migrations;
    `)
    expect((await migrate(other, [users], schema, undefined, { repairViews: true })).applied).toBe(true)
    const grants = await observer.query(
      `select privilege_type from information_schema.table_privileges
       where table_schema = $1 and table_name = 'users_live' and grantee = 'PUBLIC'`,
      [schema],
    )
    expect(grants.rows).toEqual([{ privilege_type: 'SELECT' }])
  })

  it('can check column drift again after repairing it in the migration transaction', async () => {
    await migrate(db, [users], schema)
    await db.query(`insert into "${schema}".users (_env, _raw, _updated_at)
      values ('prod', '{"id":"user_1","email":"old@example.test","primary_email":"new@example.test"}', now())`)
    const result = await other.transaction(async (tx) => {
      const repaired = await migrate(tx, [changed], schema, undefined, { repair: true })
      expect(await detectColumnDrift(tx, [changed], schema)).toEqual([])
      return repaired
    })
    expect(result.repaired.map((column) => column.column)).toEqual(['email'])
    expect(result.drift).toEqual([])
    expect((await observer.query(`select email from "${schema}".users_live`)).rows).toEqual([
      { email: 'new@example.test' },
    ])
    expect((await migrate(db, [changed], schema)).applied).toBe(false)
  })

  it('reports a refused column repair without aborting the migration transaction', async () => {
    await migrate(db, [users], schema)
    await db.exec(`create view "${schema}".custom_emails as select email from "${schema}".users`)
    const result = await migrate(other, [changed], schema, undefined, { repair: true })
    expect(result.repaired).toEqual([])
    expect(result.drift).toHaveLength(1)
    expect((await observer.query(`select * from "${schema}".custom_emails`)).rows).toEqual([])
    expect((await observer.query(`select * from "${schema}".users_live`)).rows).toEqual([])
    expect((await migrate(db, [users], schema)).applied).toBe(false)
  })
})
