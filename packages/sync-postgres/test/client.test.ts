import { describe, it, expect } from 'vitest'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { poolConfig } from '../src/db/client.js'
import { TEST_CA_PEM } from './helpers/ca.js'

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
