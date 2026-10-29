import { describe, it, expect } from 'vitest'
import { PostgresDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'
import { TEST_CA_PEM } from './helpers/ca.js'

/** Enough of a client for the constructor, which never queries. */
const stub: SqlClient = {
  query: async () => ({ rows: [] }),
  exec: async () => {},
  transaction: async (fn) => fn(stub),
  close: async () => {},
}

describe('PostgresDestination options', () => {
  it('needs a connection string or a client', () => {
    expect(() => new PostgresDestination({})).toThrow(/databaseUrl or db is required/)
  })

  it('takes a CA alongside a connection string', () => {
    // Constructing does not connect, so no server is needed to accept the option.
    expect(
      () => new PostgresDestination({ databaseUrl: 'postgres://db.example.internal/mirror', sslCa: TEST_CA_PEM }),
    ).not.toThrow()
  })

  it('refuses a CA with a pre-built client, which carries its own TLS settings', () => {
    expect(() => new PostgresDestination({ db: stub, sslCa: TEST_CA_PEM })).toThrow(/sslCa applies to databaseUrl/)
    // With both given, the client is what gets used and `init()` never
    // connects, so accepting the CA here would mean silently dropping it.
    expect(
      () =>
        new PostgresDestination({
          db: stub,
          databaseUrl: 'postgres://db.example.internal/mirror',
          sslCa: TEST_CA_PEM,
        }),
    ).toThrow(/sslCa applies to databaseUrl/)
  })
})
