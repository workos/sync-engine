/**
 * A minimal Postgres interface so the engine can run against `pg` in
 * production and PGlite in tests without a driver-specific code path.
 */
import type { Logger } from '@workos/sync-engine'
import { parse as parseConnectionString } from 'pg-connection-string'

/**
 * How the engine's connections introduce themselves to the server, so they
 * can be told apart from the application's own in `pg_stat_activity`, a
 * provider's connection dashboard and `log_line_prefix`. Handed to `pg` as
 * `fallback_application_name`, which the driver uses only when neither the
 * connection string nor `PGAPPNAME` names one: the operator's choice always
 * wins. Kept a constant rather than carrying a version so that filters
 * written against it do not break on release.
 */
export const APPLICATION_NAME = 'workos-sync-engine'

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
  const wrapConn = (conn: { query: (sql: string, params?: unknown[]) => Promise<{ rows: unknown[] }> }): SqlClient => ({
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

export interface ConnectOptions {
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to.
   * TLS is then required and the certificate verified against this CA and
   * the host name, whatever the connection string says short of
   * `sslmode=no-verify`; `sslmode=disable` is refused as a contradiction.
   */
  sslCa?: string
  /**
   * Where a connection the server closed while it sat idle in the pool is
   * reported, at warn level. The pool has already discarded that connection
   * and opens a fresh one for the next query, so this is a notice and not a
   * failure. Without a logger the notice is dropped; the pool recovers the
   * same way.
   */
  logger?: Logger
}

/**
 * What `new pg.Pool(...)` is given for a connection string.
 *
 * Without a CA the string is handed over as is, so TLS behaves exactly as
 * `pg` defines it for the `sslmode` and `PGSSLMODE` in play. With one, the
 * obvious `{ connectionString, ssl: { ca } }` does not work: `pg` parses the
 * string after merging the options and lets whatever it finds there win, so
 * any `sslmode=` in the URL replaces the `ssl` object, CA and all, and
 * `sslnegotiation=direct` replaces it with `true`. Parsing the string here,
 * with the parser `pg` itself uses, and handing over the result instead
 * keeps every other setting as `pg` would have read it and rewrites only
 * `ssl`: the CA is added to whatever TLS settings the URL asked for, so
 * `sslmode=no-verify` still turns verification off and a `sslrootcert=` path
 * is overridden.
 *
 * Either way the pool is told to fall back to {@link APPLICATION_NAME}. Beside a
 * `connectionString` that is safe: what `pg` parses out of the string lands
 * on top of the other options, so an `application_name=` in the URL is
 * still read, and it takes precedence over the fallback inside the driver.
 */
export function poolConfig(databaseUrl: string, options: ConnectOptions = {}): Record<string, unknown> {
  if (options.sslCa === undefined) {
    return { connectionString: databaseUrl, fallback_application_name: APPLICATION_NAME }
  }
  const parsed = parseConnectionString(databaseUrl)
  return { ...parsed, fallback_application_name: APPLICATION_NAME, ssl: sslWithCa(parsed.ssl, options.sslCa) }
}

function sslWithCa(ssl: unknown, ca: string): Record<string, unknown> {
  if (ssl === false) {
    throw new Error(
      'a CA certificate was given but the connection string turns TLS off (sslmode=disable or ssl=0): drop one or the other',
    )
  }
  // `ssl=no-verify` arrives as the bare string; `pg` would turn it into this.
  if (ssl === 'no-verify') return { rejectUnauthorized: false, ca }
  if (ssl !== null && typeof ssl === 'object') return { ...ssl, ca }
  return { ca }
}

export async function connect(databaseUrl: string, options: ConnectOptions = {}): Promise<SqlClient> {
  const { default: pg } = await import('pg')
  const pool = new pg.Pool(poolConfig(databaseUrl, options) as never)
  // A connection sitting idle in the pool is still a live socket, and the
  // server can close it at any time: a restart, a failover, a proxy's idle
  // timeout, a network partition. `pg` reports that by emitting `error` on
  // the pool, after it has already removed the connection, so the next query
  // simply opens a new one. An `error` event with no listener is fatal in
  // Node, though, which would turn every such disconnect into a crash of the
  // whole process, `listen` included. Listening is what keeps it a log line.
  pool.on('error', (err) => {
    options.logger?.warn('idle database connection closed by the server; the pool will reconnect', {
      error: String(err),
    })
  })
  return fromPgPool(pool as never)
}
