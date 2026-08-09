/**
 * A minimal Postgres interface so the engine can run against `pg` in
 * production and PGlite in tests without a driver-specific code path.
 */
export interface QueryResult<T = Record<string, unknown>> {
  rows: T[]
}

export interface SqlClient {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>
  /** Multi-statement DDL. */
  exec(sql: string): Promise<void>
  transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T>
  close(): Promise<void>
}

/** Wrap a node-postgres Pool. */
export function fromPgPool(pool: {
  query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }>
  connect: () => Promise<{
    query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }>
    release: () => void
  }>
  end: () => Promise<void>
}): SqlClient {
  const wrapConn = (conn: {
    query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }>
  }): SqlClient => ({
    async query<T>(sql: string, params?: unknown[]) {
      const r = await conn.query(sql, params)
      return { rows: r.rows as T[] }
    },
    async exec(sql: string) {
      await conn.query(sql)
    },
    async transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T> {
      // Already inside a transaction — reuse the connection.
      return fn(wrapConn(conn))
    },
    async close() {
      /* no-op for a borrowed connection */
    },
  })

  return {
    async query<T>(sql: string, params?: unknown[]) {
      const r = await pool.query(sql, params)
      return { rows: r.rows as T[] }
    },
    async exec(sql: string) {
      await pool.query(sql)
    },
    async transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T> {
      const conn = await pool.connect()
      try {
        await conn.query('begin')
        const result = await fn(wrapConn(conn))
        await conn.query('commit')
        return result
      } catch (err) {
        try {
          await conn.query('rollback')
        } catch {
          /* the connection is already broken; the original error matters more */
        }
        throw err
      } finally {
        conn.release()
      }
    },
    async close() {
      await pool.end()
    },
  }
}

export async function connect(databaseUrl: string): Promise<SqlClient> {
  const { default: pg } = await import('pg')
  const pool = new pg.Pool({ connectionString: databaseUrl })
  return fromPgPool(pool as never)
}
