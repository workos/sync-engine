import { describe, it, expect } from 'vitest'
import { redactPayload, redactRows, redactEvent, RedactingDestination, redacting } from '../src/redact.js'
import { STREAMS, streamByName } from '../src/catalog.js'
import type { Destination, Row } from '../src/destination.js'
import { emptyStats, type WorkOSEvent } from '../src/types.js'

const invitations = streamByName('invitations')!
const users = streamByName('users')!
const organizations = streamByName('organizations')!
const organizationDomains = streamByName('organization_domains')!
const archive = streamByName('events')!

const DNS_TOKEN = 'm5Oztg3jdK4NJLgs8uIlIprMw'
const DNS_PREFIX = 'superapp-domain-verification-z3kjny'
const domain = (over: Record<string, unknown> = {}): Record<string, unknown> => ({
  object: 'organization_domain',
  id: 'org_domain_1',
  organization_id: 'org_1',
  domain: 'foo-corp.com',
  state: 'pending',
  verification_strategy: 'dns',
  verification_token: DNS_TOKEN,
  verification_prefix: DNS_PREFIX,
  created_at: '2026-01-01T00:00:00.000Z',
  updated_at: '2026-01-01T00:00:00.000Z',
  ...over,
})
const event = (type: string, data: Record<string, unknown>): WorkOSEvent => ({
  id: 'event_1',
  event: type,
  data,
  created_at: '2026-01-01T00:00:00.000Z',
})
/** An event as the archive stores it: the whole event is the row's payload. */
const archived = (e: WorkOSEvent): Row => ({ data: e as unknown as Record<string, unknown>, updatedAt: e.created_at })

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

  it('strips the DNS challenge from an organization domain', () => {
    const out = redactPayload(organizationDomains, domain())
    expect(out['verification_token']).toBeUndefined()
    expect(out['verification_prefix']).toBeUndefined()
    expect(out).toMatchObject({
      id: 'org_domain_1',
      domain: 'foo-corp.com',
      state: 'pending',
      verification_strategy: 'dns',
    })
  })

  it('traverses arrays, stripping the challenge from every embedded domain', () => {
    const clean = { object: 'organization_domain', id: 'org_domain_0', domain: 'clean.com', state: 'verified' }
    const data: Record<string, unknown> = {
      id: 'org_1',
      name: 'Foo',
      domains: [domain(), clean, domain({ id: 'org_domain_2' })],
    }
    const out = redactPayload(organizations, data)
    const domains = out['domains'] as Record<string, unknown>[]
    expect(JSON.stringify(out)).not.toContain(DNS_TOKEN)
    expect(JSON.stringify(out)).not.toContain(DNS_PREFIX)
    expect(domains.map((d) => d['id'])).toEqual(['org_domain_1', 'org_domain_0', 'org_domain_2'])
    expect(domains[0]).toMatchObject({ domain: 'foo-corp.com', state: 'pending' })
    // An element with nothing to strip keeps its identity; the caller's data is untouched.
    expect(domains[1]).toBe(clean)
    expect((data['domains'] as Record<string, unknown>[])[0]!['verification_token']).toBe(DNS_TOKEN)
    // Nothing to strip: the same object back. A non-array where one is expected is left alone.
    const none = { id: 'org_2', domains: [clean] }
    expect(redactPayload(organizations, none)).toBe(none)
    const odd = { id: 'org_3', domains: 'n/a' }
    expect(redactPayload(organizations, odd)).toBe(odd)
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

describe('redactEvent', () => {
  it('strips the stream paths from an event that is the object', () => {
    for (const type of ['organization_domain.created', 'organization_domain.verified', 'organization_domain.deleted']) {
      const e = event(type, domain())
      const out = redactEvent(e)
      expect(JSON.stringify(out)).not.toContain(DNS_TOKEN)
      expect(JSON.stringify(out)).not.toContain(DNS_PREFIX)
      expect(out.data).toMatchObject({ id: 'org_domain_1', state: 'pending' })
      expect(out.id).toBe(e.id)
      // The caller's event is untouched.
      expect(e.data['verification_token']).toBe(DNS_TOKEN)
    }
  })

  it('strips inside a wrapped event and keeps the siblings', () => {
    const e = event('organization_domain.verification_failed', {
      reason: 'domain_verification_period_expired',
      organization_domain: domain({ state: 'failed' }),
    })
    const out = redactEvent(e)
    expect(JSON.stringify(out)).not.toContain(DNS_TOKEN)
    expect(out.data['reason']).toBe('domain_verification_period_expired')
    expect(out.data['organization_domain']).toMatchObject({ id: 'org_domain_1', state: 'failed' })
    expect((e.data['organization_domain'] as Record<string, unknown>)['verification_token']).toBe(DNS_TOKEN)
  })

  it('strips the embedded domains from organization events', () => {
    const e = event('organization.updated', {
      id: 'org_1',
      name: 'Foo',
      domains: [domain(), domain({ id: 'org_domain_2' })],
    })
    const out = redactEvent(e)
    expect(JSON.stringify(out)).not.toContain(DNS_TOKEN)
    expect((out.data['domains'] as Record<string, unknown>[]).map((d) => d['id'])).toEqual([
      'org_domain_1',
      'org_domain_2',
    ])
  })

  it('returns the same event when there is nothing to strip', () => {
    const user = event('user.created', { id: 'user_1', email: 'ada@example.com' })
    expect(redactEvent(user)).toBe(user)
    // Already clean, as a payload WorkOS strips itself is.
    const { token: _t, accept_invitation_url: _u, ...invitation } = invite()
    const created = event('invitation.created', invitation)
    expect(redactEvent(created)).toBe(created)
    // Redaction follows the stream's declaration, not the key name: an event
    // no stream subscribes to is archived as it came.
    const unknown = event('quantum_widget.entangled', { id: 'qw_1', verification_token: 'spin' })
    expect(redactEvent(unknown)).toBe(unknown)
  })

  it('can find the object inside every wrapped event the catalog declares', () => {
    // A wrapped object is located by the reference `unwrap` returns.
    for (const stream of STREAMS) {
      if (!stream.unwrap) continue
      const plain = { id: 'x' }
      expect(stream.unwrap(plain), `${stream.name}.unwrap must return the payload itself when nothing wraps it`).toBe(
        plain,
      )
    }
    const inner = domain()
    expect(organizationDomains.unwrap!({ reason: 'r', organization_domain: inner })).toBe(inner)
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
      unlink: (_s, _e, rows) => stats('unlink', rows),
      liveIds: async () => ['invitation_1'],
      rawRows: async () => [invite()],
      diffIds: async () => ({ matched: 0, localOnly: [], remoteOnly: [] }),
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
      setStateIfHeld: async () => true,
      acquireLease: async (_e, holder) => ({ acquired: true, holder, expiresAt: null }),
      releaseLease: async () => {},
      getLease: async () => null,
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
    await dest.unlink(invitations, 'prod', rows)

    expect(calls.map((c) => c.method)).toEqual(['upsert', 'upsert', 'delete', 'relink', 'unlink'])
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

  it('strips declared paths from every remote page before it is staged', async () => {
    const { inner } = recording()
    const staged: Record<string, unknown>[][] = []
    inner.diffIds = async (_s, _e, remote) => {
      for await (const page of remote) staged.push(page)
      return { matched: 0, localOnly: [], remoteOnly: [] }
    }
    const pages = [[invite(), invite({ id: 'invitation_2' })], [invite({ id: 'invitation_3' })]]
    async function* remote() {
      for (const page of pages) yield page
    }

    await new RedactingDestination(inner).diffIds(invitations, 'prod', remote())

    expect(staged.map((page) => page.map((d) => d['id']))).toEqual([['invitation_1', 'invitation_2'], ['invitation_3']])
    expect(JSON.stringify(staged)).not.toContain(TOKEN)
    for (const data of staged.flat()) {
      expect(data['token']).toBeUndefined()
      expect(data['accept_invitation_url']).toBeUndefined()
      expect(data['email']).toBe('ada@example.com')
    }
    // The caller's pages are untouched.
    for (const data of pages.flat()) expect(data['token']).toBe(TOKEN)
  })

  it('hands the remote pages through as-is for streams with nothing to redact', async () => {
    const { inner } = recording()
    let received: unknown
    inner.diffIds = async (_s, _e, remote) => {
      received = remote
      return { matched: 0, localOnly: [], remoteOnly: [] }
    }
    async function* remote() {
      yield [{ id: 'user_1' }]
    }
    const pages = remote()

    await new RedactingDestination(inner).diffIds(users, 'prod', pages)

    expect(received).toBe(pages)
  })

  it('hands rows through unchanged for streams with nothing to redact', async () => {
    const { inner, calls } = recording()
    const rows: Row[] = [{ data: { id: 'user_1' }, updatedAt: 't' }]
    await new RedactingDestination(inner).upsert(users, 'prod', rows, 'replace')
    expect(calls[0]!.rows).toBe(rows)
    expect(redactRows(users, rows)).toBe(rows)
  })

  it('strips each archived event by the stream it belongs to', async () => {
    const { inner, calls } = recording()
    const dest = new RedactingDestination(inner)
    const rows: Row[] = [
      archived(event('organization_domain.created', domain())),
      archived(event('user.created', { id: 'user_1' })),
    ]

    await dest.upsert(archive, 'prod', rows, 'replace')

    const written = calls[0]!.rows
    expect(JSON.stringify(written)).not.toContain(DNS_TOKEN)
    expect((written[0]!.data as unknown as WorkOSEvent).data).toMatchObject({ id: 'org_domain_1' })
    // A row with nothing to strip is the same row; the caller's rows are untouched.
    expect(written[1]).toBe(rows[1]!)
    expect((rows[0]!.data as unknown as WorkOSEvent).data['verification_token']).toBe(DNS_TOKEN)
  })

  it('hands archive rows through as the same array when no event needs stripping', () => {
    const rows: Row[] = [archived(event('user.created', { id: 'user_1' }))]
    expect(redactRows(archive, rows)).toBe(rows)
  })

  it('strips quarantined events the same way', async () => {
    const { inner } = recording()
    let received: { event: WorkOSEvent; error: string }[] | undefined
    inner.reject = async (_e, rows) => {
      received = rows
    }
    const rows = [{ event: event('organization_domain.verified', domain()), error: 'boom' }]

    await new RedactingDestination(inner).reject('prod', rows)

    expect(JSON.stringify(received)).not.toContain(DNS_TOKEN)
    expect(received![0]!.error).toBe('boom')
    expect(received![0]!.event.data).toMatchObject({ id: 'org_domain_1', state: 'pending' })
    expect(rows[0]!.event.data['verification_token']).toBe(DNS_TOKEN)
  })

  it('wraps a destination once', () => {
    const { inner } = recording()
    const once = redacting(inner)
    expect(once).toBeInstanceOf(RedactingDestination)
    expect(redacting(once)).toBe(once)
  })
})
