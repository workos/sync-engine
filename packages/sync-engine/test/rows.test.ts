import { describe, it, expect } from 'vitest'
import { streamByName } from '../src/catalog.js'
import {
  canonicalJson,
  chunk,
  dedupeByKey,
  isoInstant,
  jsonEqual,
  keyValues,
  parseUpdatedAt,
  readPath,
  wavesByKey,
} from '../src/rows.js'

const users = streamByName('users')!

const userPayload = (over: Record<string, unknown> = {}) => ({
  object: 'user',
  id: 'user_1',
  email: 'ada@example.com',
  first_name: 'Ada',
  created_at: '2026-01-01T00:00:00.000Z',
  updated_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describe('key extraction', () => {
  it('walks a path into nested objects and stops at anything that is not one', () => {
    expect(readPath({ role: { slug: 'admin' } }, ['role', 'slug'])).toBe('admin')
    expect(readPath({ role: 'admin' }, ['role', 'slug'])).toBeUndefined()
    expect(readPath({ id: 'x' }, 'id')).toBe('x')
  })

  it('reads slug-keyed streams that have no id in their event payload', () => {
    // role.* events carry only a slug — no id at all.
    const roles = streamByName('roles')!
    expect(keyValues(roles, { slug: 'admin', permissions: [] })).toEqual(['admin'])
    expect(keyValues(roles, { permissions: [] })).toBeUndefined()
  })

  it('reads composite keys', () => {
    const orgRoles = streamByName('organization_roles')!
    expect(keyValues(orgRoles, { organization_id: 'org_1', slug: 'admin' })).toEqual(['org_1', 'admin'])
  })

  it('refuses an object where a key should be: a malformed row, not a key', () => {
    expect(keyValues(users, { id: { $ne: null } })).toBeUndefined()
    expect(keyValues(users, { id: ['user_1'] })).toBeUndefined()
    expect(keyValues(users, { id: '' })).toBeUndefined()
    expect(keyValues(users, { id: 7 })).toEqual(['7'])
  })

  it('collapses duplicate keys within a batch, keeping the newest', () => {
    const { rows, dropped } = dedupeByKey(users, [
      { data: userPayload({ email: 'old@example.com' }), updatedAt: '2026-01-01T00:00:00.000Z' },
      { data: userPayload({ email: 'new@example.com' }), updatedAt: '2026-01-02T00:00:00.000Z' },
      { data: { email: 'no-id@example.com' }, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(rows).toHaveLength(1)
    expect(rows[0]!.data['email']).toBe('new@example.com')
    expect(dropped).toBe(1)
  })

  it('judges the newest row by its instant, not by the text of its timestamp', () => {
    // "01:00:00+01:00" sorts after "00:30:00Z" as text and is half an hour earlier as a time.
    const { rows } = dedupeByKey(users, [
      { data: userPayload({ email: 'older@example.com' }), updatedAt: '2026-01-01T01:00:00+01:00' },
      { data: userPayload({ email: 'newer@example.com' }), updatedAt: '2026-01-01T00:30:00Z' },
    ])
    expect(rows.map((r) => r.data['email'])).toEqual(['newer@example.com'])
    // A database may keep six fractional digits, so two rows a microsecond apart stay ordered.
    const fine = dedupeByKey(users, [
      { data: userPayload({ email: 'newer@example.com' }), updatedAt: '2026-01-01T00:00:00.000900Z' },
      { data: userPayload({ email: 'older@example.com' }), updatedAt: '2026-01-01T00:00:00.000100Z' },
    ])
    expect(fine.rows.map((r) => r.data['email'])).toEqual(['newer@example.com'])
    // What is not a date is ordered as text, and the database refuses it on the write.
    const junk = dedupeByKey(users, [
      { data: userPayload({ email: 'a@example.com' }), updatedAt: 'later' },
      { data: userPayload({ email: 'b@example.com' }), updatedAt: 'yesterday' },
    ])
    expect(junk.rows.map((r) => r.data['email'])).toEqual(['b@example.com'])
  })

  it('reads a timestamp without a zone as UTC, the way every destination stores it', () => {
    expect(isoInstant('2026-01-15T10:00:00')).toBe(Date.parse('2026-01-15T10:00:00Z'))
    expect(isoInstant('2026-01-15 10:00:00.5')).toBe(Date.parse('2026-01-15T10:00:00.5Z'))
    expect(isoInstant('2026-01-15T10:00:00+01:00')).toBe(Date.parse('2026-01-15T09:00:00Z'))
    expect(isoInstant('later')).toBeNaN()
    expect(parseUpdatedAt('2026-01-15T10:00:00').toISOString()).toBe('2026-01-15T10:00:00.000Z')
    // Zone-less at ten and zoned at noon: noon is newer, whatever the process's zone.
    const { rows } = dedupeByKey(users, [
      { data: userPayload({ email: 'ten@example.com' }), updatedAt: '2026-01-15T10:00:00' },
      { data: userPayload({ email: 'noon@example.com' }), updatedAt: '2026-01-15T12:00:00Z' },
    ])
    expect(rows.map((r) => r.data['email'])).toEqual(['noon@example.com'])
  })

  it("judges by a destination's own parser when given one, and fails as it would", () => {
    const strict = (iso: string) => {
      if (!/Z$/.test(iso)) throw Object.assign(new Error(`invalid timestamp ${iso}`), { code: 'INVALID_TIMESTAMP' })
      return Date.parse(iso)
    }
    const { rows } = dedupeByKey(
      users,
      [
        { data: userPayload({ email: 'older@example.com' }), updatedAt: '2026-01-01T00:00:00Z' },
        { data: userPayload({ email: 'newer@example.com' }), updatedAt: '2026-01-02T00:00:00Z' },
      ],
      strict,
    )
    expect(rows.map((r) => r.data['email'])).toEqual(['newer@example.com'])
    expect(() => dedupeByKey(users, [{ data: userPayload(), updatedAt: '2026-01-01T00:00:00+01:00' }], strict)).toThrow(
      /invalid timestamp/,
    )
  })

  it('splits repeated keys into ordered waves and leaves distinct keys in one', () => {
    const first = { data: userPayload({ external_id: 'ext_1' }), updatedAt: '2026-01-01T00:00:00.000Z' }
    const other = { data: userPayload({ id: 'user_2' }), updatedAt: '2026-01-01T00:00:00.000Z' }
    const second = { data: { id: 'user_1', first_name: 'Grace' }, updatedAt: '2026-01-02T00:00:00.000Z' }
    const { waves, dropped } = wavesByKey(users, [
      first,
      other,
      second,
      { data: { email: 'no-id@example.com' }, updatedAt: '2026-01-03T00:00:00.000Z' },
    ])
    expect(waves).toEqual([[first, other], [second]])
    expect(waves[0]![0]).toBe(first)
    expect(dropped).toBe(1)
    expect(wavesByKey(users, [first, other]).waves).toEqual([[first, other]])
  })

  it('splits into batches of a positive whole size only', () => {
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]])
    expect(chunk([], 2)).toEqual([])
    for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(() => chunk([1], bad)).toThrow(/batchSize must be a positive integer/)
    }
  })
})

describe('timestamps and equality', () => {
  it('parses an ISO-8601 updatedAt and refuses anything else as a deterministic error', () => {
    expect(parseUpdatedAt('2026-01-01T00:00:00.000Z').toISOString()).toBe('2026-01-01T00:00:00.000Z')
    let err: unknown
    try {
      parseUpdatedAt('maybe')
    } catch (e) {
      err = e
    }
    expect(err).toBeInstanceOf(Error)
    expect((err as { code: string }).code).toBe('INVALID_TIMESTAMP')
    expect((err as Error).message).toMatch(/invalid updatedAt timestamp/)
  })

  it('compares JSON structurally, ignoring key order', () => {
    expect(jsonEqual({ a: 1, b: [1, { c: null }] }, { b: [1, { c: null }], a: 1 })).toBe(true)
    expect(jsonEqual({ a: 1 }, { a: 1, b: undefined })).toBe(false)
    expect(jsonEqual({ a: [1, 2] }, { a: [2, 1] })).toBe(false)
    expect(jsonEqual(null, {})).toBe(false)
  })

  it('serialises equal values equally whatever the key order', () => {
    expect(canonicalJson({ b: 1, a: { d: [3, { f: 1, e: 2 }], c: null } })).toBe(
      canonicalJson({ a: { c: null, d: [3, { e: 2, f: 1 }] }, b: 1 }),
    )
    expect(canonicalJson({ a: undefined, b: 1 })).toBe('{"b":1}')
    expect(canonicalJson([{ b: 1, a: 2 }])).toBe('[{"a":2,"b":1}]')
  })
})
