import { describe, it, expect } from 'vitest'
import { MysqlDestination } from '../src/destination.js'
import type { SqlClient } from '../src/db/client.js'

const db = {} as SqlClient

describe('MysqlDestination options', () => {
  it('needs a connection string or a client', () => {
    expect(() => new MysqlDestination({})).toThrow(/databaseUrl or db is required/)
    expect(new MysqlDestination({ db }).schema).toBe('workos')
    expect(new MysqlDestination({ databaseUrl: 'mysql://root@localhost/app', schema: 'mirror' }).schema).toBe('mirror')
  })

  it('takes a CA alongside a connection string', () => {
    expect(() => new MysqlDestination({ databaseUrl: 'mysql://localhost/app', sslCa: 'pem' })).not.toThrow()
  })

  it('refuses a CA with a pre-built client, which carries its own TLS settings', () => {
    expect(() => new MysqlDestination({ db, sslCa: 'pem' })).toThrow(/sslCa applies to databaseUrl/)
    expect(() => new MysqlDestination({ db, databaseUrl: 'mysql://localhost/app', sslCa: 'pem' })).toThrow(
      /sslCa applies to databaseUrl/,
    )
  })

  it('refuses a schema name MySQL would', () => {
    expect(() => new MysqlDestination({ db, schema: '' })).toThrow(/between 1 and 64/)
    expect(() => new MysqlDestination({ db, schema: 'x'.repeat(65) })).toThrow(/between 1 and 64/)
  })

  it('refuses to be used before init', () => {
    const dest = new MysqlDestination({ databaseUrl: 'mysql://localhost/app' })
    expect(() => dest.getLease('prod')).toThrow(/call init\(\) first/)
  })
})
