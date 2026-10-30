import { describe, it, expect, vi } from 'vitest'
import type { EventEmitter } from 'node:events'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { connect, poolConfig } from '../src/db/client.js'
import { TEST_CA_PEM } from './helpers/ca.js'

// Every pool `connect` builds, so a test can play the server and close an
// idle connection out from under it. The real `pg.Pool` is an EventEmitter
// that emits `error` for that, and the emitter is all that matters here.
const pools = vi.hoisted(() => [] as EventEmitter[])
vi.mock('pg', async () => {
  const { EventEmitter } = await import('node:events')
  class Pool extends EventEmitter {
    constructor(readonly config: unknown) {
      super()
      pools.push(this)
    }
    async query() {
      return { rows: [] }
    }
    async connect() {
      return { query: async () => ({ rows: [] }), release() {} }
    }
    async end() {}
  }
  return { default: { Pool } }
})

const CA = TEST_CA_PEM
const URL = 'postgres://app:s3cret@db.example.internal:6543/mirror?application_name=sync'

describe('poolConfig', () => {
  it('hands the connection string to pg untouched without a CA', () => {
    expect(poolConfig(URL)).toEqual({ connectionString: URL })
    expect(poolConfig(`${URL}&sslmode=disable`)).toEqual({ connectionString: `${URL}&sslmode=disable` })
  })

  it('requires TLS and verifies against the CA when the URL says nothing about it', () => {
    expect(poolConfig(URL, { sslCa: CA }).ssl).toEqual({ ca: CA })
  })

  it('keeps every other setting the URL carries', () => {
    const config = poolConfig(URL, { sslCa: CA })
    expect(config).toMatchObject({
      host: 'db.example.internal',
      port: '6543',
      user: 'app',
      password: 's3cret',
      database: 'mirror',
      application_name: 'sync',
    })
    // The string itself must not go along: pg would parse it last and let it win.
    expect(config).not.toHaveProperty('connectionString')
  })

  it('adds the CA to the TLS settings the URL asked for', () => {
    expect(poolConfig(`${URL}&sslmode=verify-full`, { sslCa: CA }).ssl).toEqual({ ca: CA })
    expect(poolConfig(`${URL}&ssl=true`, { sslCa: CA }).ssl).toEqual({ ca: CA })
    // Turning verification off remains the operator's call, in either spelling.
    expect(poolConfig(`${URL}&sslmode=no-verify`, { sslCa: CA }).ssl).toEqual({ rejectUnauthorized: false, ca: CA })
    expect(poolConfig(`${URL}&ssl=no-verify`, { sslCa: CA }).ssl).toEqual({ rejectUnauthorized: false, ca: CA })
  })

  it('wins over a sslrootcert path in the URL', () => {
    const dir = mkdtempSync(join(tmpdir(), 'sync-postgres-ca-'))
    const file = join(dir, 'other.crt')
    writeFileSync(file, '-----BEGIN CERTIFICATE-----\nother\n-----END CERTIFICATE-----\n')
    expect(poolConfig(`${URL}&sslrootcert=${file}`, { sslCa: CA }).ssl).toEqual({ ca: CA })
  })

  it('survives sslnegotiation=direct, which pg would otherwise read as ssl: true', () => {
    const config = poolConfig(`${URL}&sslnegotiation=direct`, { sslCa: CA })
    expect(config.ssl).toEqual({ ca: CA })
    expect(config.sslnegotiation).toBe('direct')
  })

  it('refuses a CA together with a URL that turns TLS off', () => {
    expect(() => poolConfig(`${URL}&sslmode=disable`, { sslCa: CA })).toThrow(/turns TLS off/)
    expect(() => poolConfig(`${URL}&ssl=0`, { sslCa: CA })).toThrow(/turns TLS off/)
  })
})

describe('connect', () => {
  const logger = () => ({ debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn() })

  it('survives the server closing an idle connection, and says so', async () => {
    const log = logger()
    await connect(URL, { logger: log })
    const pool = pools.at(-1)!
    // With nothing listening, `emit('error')` throws: that is how Node reports
    // an unhandled error event, and what would have taken the process down.
    expect(() => pool.emit('error', new Error('terminating connection due to administrator command'))).not.toThrow()
    expect(log.warn).toHaveBeenCalledWith('idle database connection closed by the server; the pool will reconnect', {
      error: 'Error: terminating connection due to administrator command',
    })
    expect(log.error).not.toHaveBeenCalled()
  })

  it('survives it without a logger too', async () => {
    await connect(URL)
    const pool = pools.at(-1)!
    expect(() => pool.emit('error', new Error('Connection terminated unexpectedly'))).not.toThrow()
  })
})
