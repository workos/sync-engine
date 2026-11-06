/**
 * A minimal ClickHouse interface over the official HTTP client, so the rest
 * of the package reads like its SQL siblings: `query`, `command`, `insert`,
 * `close`. Values travel as query parameters (`{name:Type}`), never spliced
 * into SQL; rows travel as JSONEachRow.
 *
 * Every connection carries a few settings the engine's arithmetic depends on:
 *
 * - `async_insert = 1` with `wait_for_async_insert = 1`. The tail writes a
 *   small batch per stream every few seconds, and a MergeTree part per batch
 *   would outrun the merges on a busy mirror. Async inserts let the server
 *   batch them, and waiting for the flush keeps the engine's promise that a
 *   write is durable when the call returns. KeeperMap statements turn it off
 *   again: a batched insert collapses duplicate keys silently where a strict
 *   one must refuse them.
 * - `mutations_sync = 2`. Lightweight deletes and KeeperMap updates return
 *   once every replica has them, so a redaction or a cursor is visible to
 *   the next query.
 * - `select_sequential_consistency = 1`. On a replicated deployment a read
 *   may otherwise land on a replica the last insert has not reached, and the
 *   write path reads before it writes.
 * - `date_time_input_format = 'best_effort'`. Timestamps are still sent in
 *   the plain `YYYY-MM-DD hh:mm:ss.fff` form, so this only widens what the
 *   server accepts; it never changes how a value is read.
 */
import type { Logger } from '@workos/sync-engine'
import { createClient, type ClickHouseClient, type ClickHouseSettings } from '@clickhouse/client'

/**
 * How the engine's connections introduce themselves: the `http_user_agent`
 * the server records in `system.query_log`, the ClickHouse counterpart of
 * Postgres's `application_name`. Kept a constant rather than carrying a
 * version so that filters written against it survive a release.
 */
export const APPLICATION_NAME = 'workos-sync-engine'

/** Settings every statement runs with unless it says otherwise. */
export const DEFAULT_SETTINGS: ClickHouseSettings = {
  async_insert: 1,
  wait_for_async_insert: 1,
  // 64-bit settings travel as strings in the client's types.
  mutations_sync: '2',
  select_sequential_consistency: '1',
  date_time_input_format: 'best_effort',
  keeper_map_strict_mode: 1,
}

/** What a KeeperMap statement adds: strict mode is already on, async inserts must be off. */
export const KEEPER_SETTINGS: ClickHouseSettings = { async_insert: 0 }

export type Params = Record<string, unknown>

export interface ChClient {
  /** The rows of a `SELECT`, parsed from JSONEachRow. */
  query<T = Record<string, unknown>>(sql: string, params?: Params, settings?: ClickHouseSettings): Promise<T[]>
  /** A statement with no result: DDL, `ALTER`, `DELETE`. */
  command(sql: string, params?: Params, settings?: ClickHouseSettings): Promise<void>
  /** Rows into `table` (fully qualified) as JSONEachRow. */
  insert(table: string, rows: Record<string, unknown>[], settings?: ClickHouseSettings): Promise<void>
  close(): Promise<void>
}

/** Wrap a pre-built `@clickhouse/client` instance. */
export function fromClient(client: ClickHouseClient): ChClient {
  return {
    async query<T>(sql: string, params?: Params, settings?: ClickHouseSettings) {
      const rs = await client.query({
        query: sql,
        query_params: params,
        format: 'JSONEachRow',
        clickhouse_settings: settings,
      })
      return rs.json<T>()
    },
    async command(sql: string, params?: Params, settings?: ClickHouseSettings) {
      await client.command({ query: sql, query_params: params, clickhouse_settings: settings })
    },
    async insert(table: string, rows: Record<string, unknown>[], settings?: ClickHouseSettings) {
      if (rows.length === 0) return
      await client.insert({ table, values: rows, format: 'JSONEachRow', clickhouse_settings: settings })
    },
    async close() {
      await client.close()
    },
  }
}

export interface ConnectOptions {
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to.
   * TLS is then required and the certificate verified against this CA and
   * the host name.
   */
  sslCa?: string
  /** Reserved for connection notices; the HTTP client has no idle sockets to lose. */
  logger?: Logger
}

/**
 * What `createClient(...)` is given for a connection string. The URL's user
 * info becomes the credentials and its path is dropped: every statement the
 * engine runs qualifies its tables with the configured schema, and a path
 * would name a default database that `init` may not have created yet.
 */
export function clientConfig(databaseUrl: string, options: ConnectOptions = {}): Record<string, unknown> {
  const url = new URL(databaseUrl)
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`unsupported ClickHouse URL scheme ${url.protocol}: use http:// or https:// (the HTTP interface)`)
  }
  const config: Record<string, unknown> = {
    url: `${url.protocol}//${url.host}`,
    application: APPLICATION_NAME,
    clickhouse_settings: DEFAULT_SETTINGS,
    request_timeout: 120_000,
  }
  if (url.username) config['username'] = decodeURIComponent(url.username)
  if (url.password) config['password'] = decodeURIComponent(url.password)
  if (options.sslCa !== undefined) {
    if (url.protocol !== 'https:') {
      throw new Error('a CA certificate was given but the connection string is http://, which has no TLS: use https://')
    }
    config['tls'] = { ca_cert: Buffer.from(options.sslCa) }
  }
  return config
}

export async function connect(databaseUrl: string, options: ConnectOptions = {}): Promise<ChClient> {
  return fromClient(createClient(clientConfig(databaseUrl, options) as never))
}

/** `YYYY-MM-DD hh:mm:ss.fff` in UTC, the form every DateTime64(3) value is sent and received in. */
export function toCh(d: Date): string {
  return d.toISOString().replace('T', ' ').replace('Z', '')
}

/** A DateTime64 as the server returns it in JSONEachRow, read as UTC. */
export function fromCh(value: string): Date {
  return new Date(value.replace(' ', 'T') + 'Z')
}

/** ISO-8601 with millisecond precision, from a value the server returned. */
export function isoOf(value: string): string {
  return fromCh(value).toISOString()
}
