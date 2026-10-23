import { describe, it, expect } from 'vitest'
import { redactPayload, redactRows, RedactingDestination, redacting } from '../src/redact.js'
import { STREAMS, streamByName } from '../src/catalog.js'
import type { Destination, Row } from '../src/destination.js'
import { emptyStats } from '../src/types.js'

const invitations = streamByName('invitations')!
const users = streamByName('users')!

const TOKEN = 'Z1uX3H0uPl3aseD0ntM1rr0rMe'
const invite = (over: Record<string, unknown> = {}): Record<string, unknown> => ({
  object: 'invitation',
  id: 'invitation_1',
  email: 'ada@example.com',
  state: 'pending',
  organization_id: 'org_1',
  expires_at: '2026-02-01T00:00:00.000Z',
  token: TOKEN,
  accept_invitation_url: `https://auth.example.com/invite?invitation_token=${TOKEN}`,
  created_at: '2026-01-01T00:00:00.000Z',
  updated_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describe('redactPayload', () => {
  it('strips the invitation acceptance credential and keeps the rest', () => {
    const out = redactPayload(invitations, invite())
    expect(out['token']).toBeUndefined()
    expect(out['accept_invitation_url']).toBeUndefined()
    expect(JSON.stringify(out)).not.toContain(TOKEN)
    expect(out).toMatchObject({ id: 'invitation_1', email: 'ada@example.com', state: 'pending' })
  })

  it('never mutates the caller payload', () => {
    const payload = invite()
    redactPayload(invitations, payload)
    expect(payload['token']).toBe(TOKEN)
  })

  it('returns the same object when there is nothing to strip', () => {
    const user = { id: 'user_1', email: 'ada@example.com' }
    expect(redactPayload(users, user)).toBe(user)
    // An event payload for a redacted stream omits the fields already.
    const { token: _t, accept_invitation_url: _u, ...event } = invite()
    expect(redactPayload(invitations, event)).toBe(event)
  })

  it('strips the webhook endpoint signing secret', () => {
    const out = redactPayload(streamByName('webhook_endpoints')!, {
      id: 'we_1',
      endpoint_url: 'https://example.com/hook',
      secret: 'wh_secret_value',
    })
    expect(out['secret']).toBeUndefined()
    expect(out['endpoint_url']).toBe('https://example.com/hook')
  })

  it('follows nested paths and copies only the objects it touches', () => {
    const stream = { ...users, redact: [['credential', 'secret']] }
    const untouched = { keep: true }
    const data = { id: 'x', credential: { secret: 's', kind: 'k' }, other: untouched }
    const out = redactPayload(stream, data)
    expect(out['credential']).toEqual({ kind: 'k' })
    expect(out['other']).toBe(untouched)
    expect((data['credential'] as Record<string, unknown>)['secret']).toBe('s')
    // A path through a non-object is simply absent.
    expect(redactPayload(stream, { credential: 'opaque' })).toEqual({ credential: 'opaque' })
  })

  it('is declared by no stream on a path a column projects from', () => {
    for (const stream of STREAMS) {
      for (const path of stream.redact ?? []) {
        const parts = typeof path === 'string' ? [path] : path
        for (const col of stream.columns) {
          const colParts = typeof col.path === 'string' ? [col.path] : col.path
          const overlaps = parts.every((p, i) => colParts[i] === p)
          expect(overlaps, `${stream.name}.${col.name} projects from redacted ${parts.join('.')}`).toBe(false)
        }
      }
    }
  })
})

describe('RedactingDestination', () => {
  const recording = () => {
    const calls: { method: string; rows: Row[] }[] = []
    const stats = async (method: string, rows: Row[]) => {
      calls.push({ method, rows })
      return emptyStats()
    }
    const inner: Destination = {
      init: async () => {},
      close: async () => {},
      upsert: (_s, _e, rows) => stats('upsert', rows),
      delete: (_s, _e, rows) => stats('delete', rows),
      relink: (_s, _e, rows) => stats('relink', rows),
      liveIds: async () => ['invitation_1'],
      rawRows: async () => [invite()],
      freshness: async (s) => ({
        stream: s.name,
        rows: 1,
        deleted: 0,
        lastSyncedAt: null,
        stalenessSeconds: null,
        lagSeconds: null,
        lagSampleSize: 0,
      }),
      redact: async () => [],
      getState: async () => null,
      setState: async () => {},
      reject: async () => {},
      rejected: async () => [],
      rejectedCount: async () => 0,
      clearRejected: async () => 0,
    }
    return { inner, calls }
  }

  it('strips declared paths on every write and leaves reads alone', async () => {
    const { inner, calls } = recording()
    const dest = new RedactingDestination(inner)
    const rows: Row[] = [{ data: invite(), updatedAt: '2026-01-01T00:00:00.000Z' }]

    await dest.upsert(invitations, 'prod', rows, 'replace')
    await dest.upsert(invitations, 'prod', rows, 'merge')
    await dest.delete(invitations, 'prod', rows)
    await dest.relink(invitations, 'prod', rows)

    expect(calls.map((c) => c.method)).toEqual(['upsert', 'upsert', 'delete', 'relink'])
    for (const call of calls) {
      expect(JSON.stringify(call.rows)).not.toContain(TOKEN)
      expect(call.rows[0]!.data['email']).toBe('ada@example.com')
      expect(call.rows[0]!.updatedAt).toBe('2026-01-01T00:00:00.000Z')
    }
    // The caller's rows are untouched.
    expect(rows[0]!.data['token']).toBe(TOKEN)

    // Reads are not the engine's concern here: what is stored is already clean.
    expect(await dest.liveIds(invitations, 'prod')).toEqual(['invitation_1'])
    expect((await dest.freshness(invitations, 'prod', 24)).rows).toBe(1)
  })

  it('hands rows through unchanged for streams with nothing to redact', async () => {
    const { inner, calls } = recording()
    const rows: Row[] = [{ data: { id: 'user_1' }, updatedAt: 't' }]
    await new RedactingDestination(inner).upsert(users, 'prod', rows, 'replace')
    expect(calls[0]!.rows).toBe(rows)
    expect(redactRows(users, rows)).toBe(rows)
  })

  it('wraps a destination once', () => {
    const { inner } = recording()
    const once = redacting(inner)
    expect(once).toBeInstanceOf(RedactingDestination)
    expect(redacting(once)).toBe(once)
  })
})
