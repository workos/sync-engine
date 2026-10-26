import { describe, it, expect } from 'vitest'
import { streamByName } from '../src/catalog.js'
import { encodeKey, keyPayload, remoteKey } from '../src/sync/diff.js'

const roles = streamByName('organization_roles')!

describe('reconciliation keys', () => {
  it('leaves scalar ids unchanged, even when they look like JSON', () => {
    const users = streamByName('users')!
    const data = { id: '["user_1"]' }
    expect(remoteKey(users, data)).toBe(data.id)
    expect(keyPayload(users, data.id)).toEqual(data)
  })

  it('encodes every component without delimiter collisions', () => {
    const data = { organization_id: 'org,1', slug: 'admin,"read"' }
    const key = encodeKey([data.organization_id, data.slug])
    expect(remoteKey(roles, data)).toBe(key)
    expect(keyPayload(roles, key)).toEqual(data)
    expect(encodeKey(['org,1', 'admin'])).not.toBe(encodeKey(['org', '1,admin']))
  })

  it.each([
    { organization_id: 'org_1' },
    { slug: 'admin' },
    { organization_id: 'org_1', slug: null },
    { organization_id: 'org_1', slug: '' },
  ])('skips an incomplete key: %j', (data) => {
    expect(remoteKey(roles, data)).toBeUndefined()
  })

  it('reads and reconstructs keys at their catalog paths', () => {
    const nested = {
      ...roles,
      columns: roles.columns.map((c) => roles.key.includes(c.name)
        ? { ...c, path: ['identity', c.name] }
        : c),
    }
    const data = { identity: { organization_id: 'org_1', slug: 'admin' } }
    const key = remoteKey(nested, data)!
    expect(key).toBe('["org_1","admin"]')
    expect(keyPayload(nested, key)).toEqual(data)
  })

  it.each(['not-json', 'null', '["org_1"]', '["org_1",null]', '["org_1",""]'])(
    'refuses a malformed deletion key: %s', (key) => {
      expect(() => keyPayload(roles, key)).toThrow()
    },
  )
})
