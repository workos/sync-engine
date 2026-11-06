import { describe, it, expect } from 'vitest'
import { MongoClient } from 'mongodb'
import { MongoDestination } from '../src/destination.js'
import { clientOptions, databaseFromUrl, APPLICATION_NAME } from '../src/db/client.js'
import { Names, fieldPath, validatePrefix } from '../src/schema/names.js'
import { TEST_CA_PEM } from './helpers/ca.js'

/** Enough of a client for the constructor, which never connects. */
const stub = new MongoClient('mongodb://db.example.internal/app')

describe('MongoDestination options', () => {
  it('needs a connection string or a client', () => {
    expect(() => new MongoDestination({})).toThrow(/url or client is required/)
  })

  it('takes a CA alongside a connection string', () => {
    // Constructing does not connect, so no server is needed to accept the option.
    expect(
      () => new MongoDestination({ url: 'mongodb://db.example.internal/mirror', sslCa: TEST_CA_PEM }),
    ).not.toThrow()
  })

  it('refuses a CA with a pre-built client, which carries its own TLS settings', () => {
    expect(() => new MongoDestination({ client: stub, sslCa: TEST_CA_PEM })).toThrow(/sslCa applies to url/)
    // With both given, the client is what gets used and `init()` never
    // connects, so accepting the CA here would mean silently dropping it.
    expect(
      () => new MongoDestination({ client: stub, url: 'mongodb://db.example.internal/mirror', sslCa: TEST_CA_PEM }),
    ).toThrow(/sslCa applies to url/)
  })

  it('reads the database off the connection string unless told otherwise', () => {
    expect(new MongoDestination({ url: 'mongodb://db.example.internal/app?retryWrites=true' }).databaseName).toBe('app')
    expect(new MongoDestination({ url: 'mongodb://db.example.internal/app', database: 'mirror' }).databaseName).toBe(
      'mirror',
    )
    // Nothing named: decided at init, where the client's default applies.
    expect(new MongoDestination({ url: 'mongodb://db.example.internal' }).databaseName).toBeUndefined()
  })

  it('validates the prefix up front', () => {
    expect(() => new MongoDestination({ url: 'mongodb://x/app', prefix: 'work os' })).toThrow(
      /invalid collection prefix/,
    )
    expect(() => new MongoDestination({ url: 'mongodb://x/app', prefix: '$w' })).toThrow(/invalid collection prefix/)
    expect(() => new MongoDestination({ url: 'mongodb://x/app', prefix: '1w' })).toThrow(/invalid collection prefix/)
    expect(new MongoDestination({ url: 'mongodb://x/app', prefix: 'mirror_' }).prefix).toBe('mirror_')
    expect(new MongoDestination({ url: 'mongodb://x/app' }).prefix).toBe('workos_')
  })
})

describe('databaseFromUrl', () => {
  it('reads the path of plain, SRV, multi-host and credentialed strings', () => {
    expect(databaseFromUrl('mongodb://localhost:27017/app')).toBe('app')
    expect(databaseFromUrl('mongodb+srv://cluster0.example.net/app?retryWrites=true&w=majority')).toBe('app')
    expect(databaseFromUrl('mongodb://a:27017,b:27017,c:27017/app?replicaSet=rs0')).toBe('app')
    expect(databaseFromUrl('mongodb://user:p%40ss@localhost/my%20app')).toBe('my app')
  })

  it('says when there is none', () => {
    expect(databaseFromUrl('mongodb://localhost:27017')).toBeUndefined()
    expect(databaseFromUrl('mongodb://localhost:27017/')).toBeUndefined()
    expect(databaseFromUrl('mongodb://localhost:27017/?appName=x')).toBeUndefined()
  })
})

describe('clientOptions', () => {
  it('introduces the connection as the engine unless the string names an appName', () => {
    expect(clientOptions('mongodb://localhost/app')).toEqual({ appName: APPLICATION_NAME })
    expect(clientOptions('mongodb://localhost/app?appName=mine')).toEqual({})
    expect(clientOptions('mongodb://localhost/app?w=majority&appname=mine')).toEqual({})
  })

  it('requires TLS when a CA is given, and refuses a string that turns TLS off', () => {
    expect(clientOptions('mongodb://localhost/app', { sslCa: TEST_CA_PEM })).toMatchObject({
      tls: true,
      ca: TEST_CA_PEM,
    })
    expect(() => clientOptions('mongodb://localhost/app?tls=false', { sslCa: TEST_CA_PEM })).toThrow(/turns TLS off/)
    expect(() => clientOptions('mongodb://localhost/app?ssl=false&w=1', { sslCa: TEST_CA_PEM })).toThrow(
      /turns TLS off/,
    )
    expect(() => clientOptions('mongodb://localhost/app?tls=true', { sslCa: TEST_CA_PEM })).not.toThrow()
  })
})

describe('Names', () => {
  it('derives every collection from the prefix', () => {
    const n = new Names('workos_')
    expect(n.mirror('users')).toBe('workos_users')
    expect(n.liveView('users')).toBe('workos_users_live')
    expect(n.sync).toBe('workos__sync')
    expect(n.rejected).toBe('workos__rejected')
    expect(n.migrations).toBe('workos__migrations')
    expect(n.staging('local', 'abc')).toBe('workos__reconcile_local_abc')
    expect(validatePrefix('ok_1')).toBe('ok_1')
  })

  it('addresses payload fields under _raw and refuses what dotted paths cannot say', () => {
    expect(fieldPath('id')).toBe('_raw.id')
    expect(fieldPath(['role', 'slug'])).toBe('_raw.role.slug')
    expect(() => fieldPath(['a.b'])).toThrow(/cannot address/)
    expect(() => fieldPath(['$a'])).toThrow(/cannot address/)
    expect(() => fieldPath([''])).toThrow(/cannot address/)
  })
})
