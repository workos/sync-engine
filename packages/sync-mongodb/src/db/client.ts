/**
 * The connection.
 *
 * One `MongoClient` is a pool, so the destination holds one for its life and
 * every method draws on it. The application may hand in its own instead
 * (`fromClient`), in which case the destination never closes it.
 */
import { MongoClient, type MongoClientOptions } from 'mongodb'
import type { Logger } from '@workos/sync-engine'

/**
 * How the engine's connections introduce themselves to the server, so they
 * can be told apart from the application's own in `currentOp`, the server
 * log and a provider's connection dashboard. Set only when the connection
 * string names no `appName` of its own: the operator's choice always wins.
 * Kept a constant rather than carrying a version so that filters written
 * against it do not break on release.
 */
export const APPLICATION_NAME = 'workos-sync-engine'

export interface ConnectOptions {
  /**
   * PEM-encoded CA certificate(s) the server's certificate must chain to.
   * TLS is then required, whatever the connection string says short of
   * `tls=false`, which is refused as a contradiction.
   */
  sslCa?: string
  logger?: Logger
}

/** The database the connection string names, if any. */
export function databaseFromUrl(url: string): string | undefined {
  const m = /^mongodb(?:\+srv)?:\/\/(?:[^@/?]*@)?[^/?]*(?:\/([^?]*))?/i.exec(url)
  const db = m?.[1]
  return db ? decodeURIComponent(db) : undefined
}

/** What `new MongoClient(url, ...)` is given. */
export function clientOptions(url: string, options: ConnectOptions = {}): MongoClientOptions {
  const out: MongoClientOptions = {}
  if (!/[?&]appName=/i.test(url)) out.appName = APPLICATION_NAME
  if (options.sslCa !== undefined) {
    if (/[?&](?:tls|ssl)=false(?:&|$)/i.test(url)) {
      throw new Error(
        'a CA certificate was given but the connection string turns TLS off (tls=false or ssl=false): drop one or the other',
      )
    }
    out.tls = true
    out.ca = options.sslCa
  }
  return out
}

export async function connect(url: string, options: ConnectOptions = {}): Promise<MongoClient> {
  const client = new MongoClient(url, clientOptions(url, options))
  await client.connect()
  return client
}

/** A pre-built client, e.g. the application's own. Not closed by the destination. */
export function fromClient(client: MongoClient): MongoClient {
  return client
}
