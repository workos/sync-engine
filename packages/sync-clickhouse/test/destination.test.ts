import { describe, it, expect } from 'vitest'
import { ClickHouseDestination } from '../src/destination.js'
import type { ChClient } from '../src/db/client.js'

const db = {} as ChClient

describe('ClickHouseDestination options', () => {
  it('needs a connection string or a client', () => {
    expect(() => new ClickHouseDestination({})).toThrow(/databaseUrl or db is required/)
    expect(new ClickHouseDestination({ db }).schema).toBe('workos')
    expect(new ClickHouseDestination({ databaseUrl: 'https://u:p@host:8443', schema: 'mirror' }).schema).toBe('mirror')
  })

  it('takes a CA alongside an https connection string', () => {
    expect(() => new ClickHouseDestination({ databaseUrl: 'https://host:8443', sslCa: 'pem' })).not.toThrow()
  })

  it('refuses a CA with a pre-built client, which carries its own TLS settings', () => {
    expect(() => new ClickHouseDestination({ db, sslCa: 'pem' })).toThrow(/sslCa applies to databaseUrl/)
  })

  it('refuses a schema name that is not a plain identifier', () => {
    expect(() => new ClickHouseDestination({ db, schema: '' })).toThrow(/plain identifier/)
    expect(() => new ClickHouseDestination({ db, schema: 'with space' })).toThrow(/plain identifier/)
    expect(() => new ClickHouseDestination({ db, schema: 'x'.repeat(64) })).toThrow(/plain identifier/)
  })

  it('refuses to be used before init', () => {
    const dest = new ClickHouseDestination({ databaseUrl: 'http://localhost:8123' })
    expect(() => dest.getLease('prod')).toThrow(/call init\(\) first/)
  })
})
