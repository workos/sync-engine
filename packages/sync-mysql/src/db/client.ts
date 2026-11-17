/**
 * A minimal MySQL interface, so the rest of the package reads like its
 * Postgres sibling: `query`, `exec`, a pinned `transaction`, `close`.
 *
 * Two things every connection is told on arrival, because the engine's
 * arithmetic depends on them:
 *
 * - `time_zone = '+00:00'`. The mirror's timestamps are `DATETIME(6)`, which
 *   MySQL stores without a zone. `_updated_at` comes from the source as UTC;
 *   `_synced_at` and the lease expiry come from `NOW(6)`, which is in the
 *   session zone. With the session in UTC they compare like for like, and
 *   `lag = _synced_at - _updated_at` means what it says whatever the server's
 *   own `time_zone` is set to.
 * - `transaction_isolation = 'REPEATABLE-READ'`, the InnoDB default made
 *   explicit. The write path reads the stored row with `SELECT ... FOR
 *   UPDATE`, decides in Node, then writes; a missing row is covered by a gap
 *   lock under this level, so a competing writer cannot slip a newer row in
 *   between the read and the write. See `db/upsert.ts`.
 */
import { isIP } from 'node:net'
import type { Logger } from '@workos/sync-engine'

/**
 * How the engine's connections introduce themselves, as the `program_name`
 * connection attribute (`performance_schema.session_connect_attrs`), the
 * MySQL counterpart of Postgres's `application_name`. Kept a constant rather
 * than carrying a version so that filters written against it survive a
 * release.
 */
export const APPLICATION_NAME = 'workos-sync-engine'

export interface QueryResult<T = Record<string, unknown>> {
  rows: T[]
  /**
   * `affectedRows` as mysql2 reports it, with the driver's default
   * `FOUND_ROWS` flag: an `UPDATE` counts matched rows, not changed ones,
   * and `INSERT IGNORE` counts only the rows it inserted.
   */
  affectedRows: number
}

export interface SqlClient {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>
  /** One DDL statement. MySQL DDL commits implicitly, so there is nothing to roll back. */
  exec(sql: string): Promise<void>
  /**
   * Run `fn` on one pinned connection inside `BEGIN ... COMMIT`, rolled back
   * on a throw. A call made inside a transaction reuses the connection under
   * a savepoint; one made on a pinned connection that has no transaction
   * open begins and commits its own.
   */
  transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T>
  /**
   * Run `fn` on one pinned connection with no transaction around it, for
   * work that needs session state (`GET_LOCK`, temporary tables) next to
   * DDL, which would commit a transaction anyway.
   */
  pinned<T>(fn: (conn: SqlClient) => Promise<T>): Promise<T>
  close(): Promise<void>
}

/** The slice of a mysql2 promise pool this package uses, so nothing here types against the driver. */
export interface PoolLike {
  query(sql: string, values?: unknown[]): Promise<[unknown, unknown]>
  getConnection(): Promise<ConnectionLike>
  end(): Promise<void>
}

export interface ConnectionLike {
  query(sql: string, values?: unknown[]): Promise<[unknown, unknown]>
  release(): void
}

interface ResultHeader {
  affectedRows?: number
}

function toResult<T>(res: [unknown, unknown]): QueryResult<T> {
  const [first] = res
  if (Array.isArray(first)) return { rows: first as T[], affectedRows: 0 }
  return { rows: [], affectedRows: (first as ResultHeader | null)?.affectedRows ?? 0 }
}

/**
 * `fn` inside `BEGIN ... COMMIT` on `conn`, rolled back on a throw. The
 * transaction the pool starts and the one a pinned connection starts for
 * itself are this same thing.
 */
async function inTransaction<T>(conn: ConnectionLike, fn: (tx: SqlClient) => Promise<T>): Promise<T> {
  try {
    await conn.query('begin')
    const result = await fn(wrapConnection(conn, 1))
    await conn.query('commit')
    return result
  } catch (err) {
    try {
      await conn.query('rollback')
    } catch {
      /* the connection is already broken; the original error matters more */
    }
    throw err
  }
}

/**
 * Wrap one borrowed connection. `depth` is how many transactions are open on
 * it: none for a connection that is only pinned, one inside `BEGIN`, more
 * under savepoints. The outermost owns `BEGIN`/`COMMIT` and inner ones get a
 * savepoint, which is how `setStateIfHeld` can roll back a refused
 * checkpoint without ending the caller's transaction. A pinned connection
 * with nothing open begins its own: a savepoint outside a transaction would
 * be released by the next autocommit and protect nothing, and it is what
 * lets `diffIdsInSql` keep its session for a whole listing while holding a
 * transaction only for the moment it needs one.
 */
function wrapConnection(conn: ConnectionLike, depth: number): SqlClient {
  const run = async (sql: string, params?: unknown[]) => toResult(await conn.query(sql, params))
  return {
    async query<T>(sql: string, params?: unknown[]) {
      return run(sql, params) as Promise<QueryResult<T>>
    },
    async exec(sql: string) {
      await conn.query(sql)
    },
    async transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T> {
      if (depth === 0) return inTransaction(conn, fn)
      const name = `workos_sync_tx_${depth}`
      await conn.query(`savepoint ${name}`)
      try {
        const out = await fn(wrapConnection(conn, depth + 1))
        await conn.query(`release savepoint ${name}`)
        return out
      } catch (err) {
        try {
          await conn.query(`rollback to savepoint ${name}`)
          await conn.query(`release savepoint ${name}`)
        } catch {
          // The outer transaction still rolls back; the original error matters more.
        }
        throw err
      }
    },
    async pinned<T>(fn: (c: SqlClient) => Promise<T>): Promise<T> {
      // Already pinned.
      return fn(wrapConnection(conn, depth))
    },
    async close() {
      /* no-op for a borrowed connection */
    },
  }
}

/** Wrap a mysql2 promise pool (`mysql.createPool(...)`). */
export function fromPool(pool: PoolLike): SqlClient {
  return {
    async query<T>(sql: string, params?: unknown[]) {
      return toResult<T>(await pool.query(sql, params))
    },
    async exec(sql: string) {
      await pool.query(sql)
    },
    async transaction<T>(fn: (tx: SqlClient) => Promise<T>): Promise<T> {
      const conn = await pool.getConnection()
      try {
        return await inTransaction(conn, fn)
      } finally {
        conn.release()
      }
    },
    async pinned<T>(fn: (c: SqlClient) => Promise<T>): Promise<T> {
      const conn = await pool.getConnection()
      try {
        return await fn(wrapConnection(conn, 0))
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
   * the DNS host name in the connection string. IP-address URLs are refused
   * because mysql2 does not verify their identity correctly.
   */
  sslCa?: string
  /** Where a connection the server closed while idle is reported, at warn level. */
  logger?: Logger
}

/**
 * Session settings applied to every connection the pool opens, before any
 * query the engine runs on it. The module header says why each is here.
 */
export const SESSION_SQL = [`set time_zone = '+00:00'`, `set session transaction_isolation = 'REPEATABLE-READ'`]

/**
 * `databaseUrl` without its path. Every statement the engine runs qualifies
 * its tables with the configured schema, so no default database is needed;
 * and one named in the URL would be selected during the handshake, where a
 * database that `init` has yet to create fails the connection outright.
 */
export function withoutDatabase(databaseUrl: string): string {
  const url = new URL(databaseUrl)
  url.pathname = ''
  return url.toString()
}

/**
 * What `mysql.createPool(...)` is given. `timezone: 'Z'` makes the driver
 * read and write `DATETIME` as UTC, matching the session zone set above, so
 * a `Date` round-trips unchanged.
 */
export function poolConfig(databaseUrl: string, options: ConnectOptions = {}): Record<string, unknown> {
  const config: Record<string, unknown> = {
    uri: withoutDatabase(databaseUrl),
    timezone: 'Z',
    connectAttributes: { program_name: APPLICATION_NAME },
    // A BIGINT beyond 2^53 comes back as a string rather than a rounded
    // number; everything that fits stays a number.
    supportBigNumbers: true,
    // Multi-statement strings are never sent; every DDL statement goes alone.
    multipleStatements: false,
  }
  const ssl = sslConfig(databaseUrl, options)
  if (ssl !== undefined) config['ssl'] = ssl
  return config
}

/**
 * mysql2 3.24.6 omits both `host` and `servername` from its TLS options for
 * an IP literal. Node then verifies `localhost`: a matching IP SAN fails,
 * while a certificate for localhost passes for any IP. Until the driver
 * verifies the actual address, refuse these URLs before opening a socket.
 * A DNS hostname is verified normally; an explicit verification opt-out
 * remains the caller's choice. Match mysql2's decoding of IPv6 URL hosts.
 */
function requireTlsHostname(databaseUrl: string, ssl: Record<string, unknown>): void {
  if (ssl['rejectUnauthorized'] === false || !ssl['verifyIdentity']) return
  const hostname = new URL(databaseUrl).hostname
  const host = decodeURIComponent(hostname.startsWith('[') ? hostname.slice(1, -1) : hostname)
  if (isIP(host)) {
    throw new Error(
      'verified MySQL TLS requires a DNS hostname: mysql2 cannot verify IP-address hosts correctly; ' +
        "use the server certificate's DNS hostname in databaseUrl",
    )
  }
}

/**
 * The TLS settings to hand mysql2 beside the connection string, or
 * undefined to leave what the string itself says alone.
 *
 * mysql2 verifies the certificate chain when `rejectUnauthorized` is on but
 * checks the host name only when `verifyIdentity` is on as well, and that
 * one is off by default. A chain check alone accepts any certificate the CA
 * ever issued, for any host; with the operating system's bundle as the CA,
 * that is every publicly trusted certificate there is. So both ways of
 * asking for verified TLS get the host name check added:
 *
 * - `sslCa` (`WORKOS_SYNC_MYSQL_SSL_CA`): always.
 * - an `ssl={...}` object in the connection string, which mysql2 reads
 *   itself: unless it turns verification off (`"rejectUnauthorized":false`)
 *   or already says what it wants (`"verifyIdentity":false`, for a tunnel
 *   whose host name is not the server's).
 *
 * A value given here overrides the one in the string, and `sslCa` wins when
 * both are present. An `ssl` parameter that is not an object (a named
 * mysql2 profile) is the driver's business and is left alone.
 */
export function sslConfig(databaseUrl: string, options: ConnectOptions = {}): Record<string, unknown> | undefined {
  if (options.sslCa !== undefined) {
    const ssl = { ca: options.sslCa, rejectUnauthorized: true, verifyIdentity: true }
    requireTlsHostname(databaseUrl, ssl)
    return ssl
  }
  const raw = new URL(databaseUrl).searchParams.get('ssl')
  if (raw === null) return undefined
  let parsed: unknown
  try {
    parsed = JSON.parse(raw)
  } catch {
    return undefined
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return undefined
  const fromUrl = parsed as Record<string, unknown>
  if (fromUrl['rejectUnauthorized'] === false || fromUrl['verifyIdentity'] !== undefined) {
    requireTlsHostname(databaseUrl, fromUrl)
    return undefined
  }
  const ssl = { ...fromUrl, verifyIdentity: true }
  requireTlsHostname(databaseUrl, ssl)
  return ssl
}

export async function connect(databaseUrl: string, options: ConnectOptions = {}): Promise<SqlClient> {
  const { default: mysql } = await import('mysql2/promise')
  const pool = mysql.createPool(poolConfig(databaseUrl, options) as never)
  // mysql2 hands a fresh connection to this listener before it serves any
  // query from the pool, and a connection runs its queries in order, so the
  // session statements queued here come first on it.
  // The listener is handed the callback-style connection, so the statements
  // take a callback: an error with nobody to receive it would be emitted on
  // the query and, unlistened, would crash the process.
  const events = pool as unknown as {
    on(
      event: 'connection',
      listener: (conn: { query: (sql: string, cb: (err: unknown) => void) => unknown }) => void,
    ): void
  }
  events.on('connection', (conn) => {
    for (const sql of SESSION_SQL) {
      conn.query(sql, (err) => {
        if (err) {
          options.logger?.warn('could not apply session settings to a new connection', { sql, error: String(err) })
        }
      })
    }
  })
  return fromPool(pool as unknown as PoolLike)
}
