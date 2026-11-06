import { describe, it, expect } from 'vitest'
import {
  clientConfig,
  fromClient,
  connect,
  toCh,
  fromCh,
  isoOf,
  APPLICATION_NAME,
  DEFAULT_SETTINGS,
} from '../src/db/client.js'
import { describeDb, TEST_CLICKHOUSE_URL } from './helpers/clickhouse.js'

describe('clientConfig', () => {
  it('splits the URL into origin and credentials, and drops the path', () => {
    const cfg = clientConfig('https://me:p%40ss@host.example:8443/some_db?x=1')
    expect(cfg).toMatchObject({
      url: 'https://host.example:8443',
      username: 'me',
      password: 'p@ss',
      application: APPLICATION_NAME,
    })
    expect(cfg['tls']).toBeUndefined()
    expect(cfg['clickhouse_settings']).toBe(DEFAULT_SETTINGS)
  })

  it('leaves credentials out when the URL has none', () => {
    const cfg = clientConfig('http://127.0.0.1:8123')
    expect(cfg['username']).toBeUndefined()
    expect(cfg['password']).toBeUndefined()
  })

  it('verifies against a CA over https only', () => {
    const cfg = clientConfig('https://host:8443', { sslCa: 'pem' })
    expect((cfg['tls'] as { ca_cert: Buffer }).ca_cert.toString()).toBe('pem')
    expect(() => clientConfig('http://host:8123', { sslCa: 'pem' })).toThrow(/https/)
  })

  it('refuses a URL that is not the HTTP interface', () => {
    expect(() => clientConfig('clickhouse://host:9000')).toThrow(/http:\/\/ or https:\/\//)
    expect(() => clientConfig('not a url')).toThrow()
  })

  it('writes and reads DateTime64 in the one plain form', () => {
    expect(toCh(new Date('2026-01-02T03:04:05.678Z'))).toBe('2026-01-02 03:04:05.678')
    expect(fromCh('2026-01-02 03:04:05.678').toISOString()).toBe('2026-01-02T03:04:05.678Z')
    expect(isoOf('2026-01-02 03:04:05.678123')).toBe('2026-01-02T03:04:05.678Z')
  })

  it('sends no request for an empty insert', async () => {
    let inserted = 0
    const client = fromClient({ insert: async () => void inserted++ } as never)
    await client.insert('t', [])
    expect(inserted).toBe(0)
  })
})

describeDb('connect', () => {
  it('connects, runs with the engine settings, and tolerates a path in the URL', async () => {
    const url = new URL(TEST_CLICKHOUSE_URL!)
    url.pathname = '/does_not_exist_' + Date.now()
    const db = await connect(url.toString())
    try {
      expect(await db.query<{ one: number }>('SELECT 1 AS one')).toEqual([{ one: 1 }])
      const settings = await db.query<{ name: string; value: string }>(
        `SELECT name, value FROM system.settings WHERE name IN ('async_insert', 'wait_for_async_insert', 'mutations_sync', 'keeper_map_strict_mode') ORDER BY name`,
      )
      expect(settings).toEqual([
        { name: 'async_insert', value: '1' },
        { name: 'keeper_map_strict_mode', value: '1' },
        { name: 'mutations_sync', value: '2' },
        { name: 'wait_for_async_insert', value: '1' },
      ])
      expect(await db.query<{ ua: string }>(`SELECT http_user_agent AS ua FROM system.query_log LIMIT 0`)).toEqual([])
    } finally {
      await db.close()
    }
  })
})
