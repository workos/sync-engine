import { describe, it, expect } from 'vitest'
import { poolConfig, fromPool, APPLICATION_NAME, SESSION_SQL, type PoolLike } from '../src/db/client.js'

describe('poolConfig', () => {
  it('hands the connection string to mysql2 with the driver reading and writing UTC', () => {
    const cfg = poolConfig('mysql://u:p@h:3307/db')
    expect(cfg).toMatchObject({
      uri: 'mysql://u:p@h:3307/db',
      timezone: 'Z',
      multipleStatements: false,
      supportBigNumbers: true,
    })
    expect(cfg['ssl']).toBeUndefined()
  })

  it('introduces its connections by name', () => {
    expect(poolConfig('mysql://h/db')['connectAttributes']).toEqual({ program_name: APPLICATION_NAME })
    expect(APPLICATION_NAME).toBe('workos-sync-engine')
  })

  it('requires TLS and verifies against the CA when one is given', () => {
    expect(poolConfig('mysql://h/db', { sslCa: 'pem' })['ssl']).toEqual({ ca: 'pem', rejectUnauthorized: true })
  })

  it('puts every new session in UTC', () => {
    expect(SESSION_SQL[0]).toBe(`set time_zone = '+00:00'`)
  })
})

describe('fromPool', () => {
  /** A pool that records what it was asked, answering rows for selects and a header otherwise. */
  function fakePool() {
    const log: string[] = []
    const answer = async (sql: string): Promise<[unknown, unknown]> => {
      log.push(sql)
      if (sql.startsWith('select')) return [[{ n: 1 }], []]
      return [{ affectedRows: 2 }, undefined]
    }
    const conn = { query: answer, release: () => log.push('release') }
    const pool: PoolLike = {
      query: answer,
      getConnection: async () => conn,
      end: async () => {
        log.push('end')
      },
    }
    return { pool, log }
  }

  it('reports rows for a select and affected rows for a write', async () => {
    const { pool } = fakePool()
    const db = fromPool(pool)
    expect(await db.query('select 1')).toEqual({ rows: [{ n: 1 }], affectedRows: 0 })
    expect(await db.query('update t set x = 1')).toEqual({ rows: [], affectedRows: 2 })
  })

  it('pins a connection for a transaction, commits on return and rolls back on a throw', async () => {
    const { pool, log } = fakePool()
    const db = fromPool(pool)
    await db.transaction(async (tx) => {
      await tx.query('update t set x = 1')
    })
    expect(log).toEqual(['begin', 'update t set x = 1', 'commit', 'release'])
    log.length = 0
    await expect(
      db.transaction(async () => {
        throw new Error('boom')
      }),
    ).rejects.toThrow('boom')
    expect(log).toEqual(['begin', 'rollback', 'release'])
  })

  it('nests a transaction under a savepoint on the same connection', async () => {
    const { pool, log } = fakePool()
    const db = fromPool(pool)
    await db.transaction(async (tx) => {
      await tx.transaction(async (inner) => {
        await inner.query('update t set x = 2')
      })
      await expect(
        tx.transaction(async () => {
          throw new Error('inner')
        }),
      ).rejects.toThrow('inner')
    })
    expect(log).toEqual([
      'begin',
      'savepoint workos_sync_tx_1',
      'update t set x = 2',
      'release savepoint workos_sync_tx_1',
      'savepoint workos_sync_tx_1',
      'rollback to savepoint workos_sync_tx_1',
      'release savepoint workos_sync_tx_1',
      'commit',
      'release',
    ])
  })

  it('pins a connection without a transaction for session-scoped work', async () => {
    const { pool, log } = fakePool()
    const db = fromPool(pool)
    await db.pinned(async (conn) => {
      await conn.exec('create temporary table t (x int)')
      await conn.pinned(async (same) => same.exec('drop temporary table t'))
    })
    expect(log).toEqual(['create temporary table t (x int)', 'drop temporary table t', 'release'])
    await db.close()
    expect(log[log.length - 1]).toBe('end')
  })
})
