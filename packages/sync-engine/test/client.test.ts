/**
 * What the client tells WorkOS about itself. The first User-Agent token is the
 * `name/version` shape every WorkOS SDK sends, which is what the API's poller
 * telemetry parses; the destination and the phase follow it so a log query can
 * tell a tail poll from a backfill or reconcile sweep.
 */
import { describe, it, expect } from 'vitest'
import { WorkOSClient, buildUserAgent } from '../src/workos/client.js'
import { VERSION } from '../src/version.js'
import { FakeWorkOS, resetIds } from './helpers/fake-workos.js'

const clientFor = (fake: FakeWorkOS, product?: { name: string; version: string }) =>
  new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl, product })

describe('User-Agent', () => {
  it('leads with the engine as an SDK-style name/version token', () => {
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+/)
    expect(buildUserAgent()).toBe(`workos-sync-engine/${VERSION}`)
  })

  it('names the destination and then the phase', () => {
    expect(buildUserAgent({ name: 'workos-sync-postgres', version: '1.2.3' }, 'backfill')).toBe(
      `workos-sync-engine/${VERSION} workos-sync-postgres/1.2.3 (phase=backfill)`,
    )
  })

  it('keeps a destination token header-safe', () => {
    const ua = (name: string, version: string) => buildUserAgent({ name, version })
    // Whitespace, slashes and parentheses would split or end the token.
    expect(ua('my dest (beta)', '1/0')).toBe(`workos-sync-engine/${VERSION} my-dest-beta/1-0`)
    // Control characters and non-ASCII make `fetch` refuse the header outright.
    expect(ua('dest\r\nX-Injected: 1', '1.0')).toBe(`workos-sync-engine/${VERSION} dest-X-Injected-1/1.0`)
    expect(ua('同步 engine', '1.0\u0000')).toBe(`workos-sync-engine/${VERSION} engine/1.0`)
    // A name with nothing left is dropped, not sent as an empty token.
    expect(ua('同步', '1.0')).toBe(`workos-sync-engine/${VERSION}`)
    expect(ua('', '1.0')).toBe(`workos-sync-engine/${VERSION}`)
    // A missing version leaves the bare name rather than a dangling slash.
    expect(ua('dest', '')).toBe(`workos-sync-engine/${VERSION} dest`)
  })

  it('never produces a header value fetch would refuse', () => {
    for (const name of ['同步', 'a\r\nb', '\u0000', ' lead', 'trail ', 'ok']) {
      const ua = buildUserAgent({ name, version: '1.0' })
      expect(() => new Headers({ 'User-Agent': ua })).not.toThrow()
      expect(ua).toMatch(/^[\x21-\x7e ]+$/)
    }
  })

  it('is sent on every request', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    fake.createOrganization()
    const client = clientFor(fake, { name: 'workos-sync-postgres', version: '1.2.3' })

    await client.list('/organizations')
    expect(fake.userAgents.at(-1)).toBe(`workos-sync-engine/${VERSION} workos-sync-postgres/1.2.3`)

    await client.withPhase('reconcile').list('/organizations')
    expect(fake.userAgents.at(-1)).toBe(
      `workos-sync-engine/${VERSION} workos-sync-postgres/1.2.3 (phase=reconcile)`,
    )
  })

  it('withPhase is a view of the same client, not a new one per phase', () => {
    const client = clientFor(new FakeWorkOS())
    const tail = client.withPhase('tail')
    expect(tail.phase).toBe('tail')
    expect(tail.withPhase('tail')).toBe(tail)
    expect(tail.withPhase('backfill')).not.toBe(tail)
    expect(client.phase).toBeUndefined()
  })

  it('shares rate buckets across phases', async () => {
    // A directory bucket holds four tokens and refills four a second. Five
    // requests spread over two phases of one client must wait on the shared
    // bucket for the fifth; with a bucket per phase they would not wait at all.
    resetIds()
    const fake = new FakeWorkOS()
    const org = fake.createOrganization()
    const dir = fake.createDirectory(org['id'] as string)
    const client = clientFor(fake)
    const phases = [client.withPhase('backfill'), client.withPhase('reconcile')]
    const key = `directory:${dir['id']}`

    const start = Date.now()
    for (let i = 0; i < 5; i++) {
      await phases[i % 2]!.list('/directory_users', { directory: dir['id'] as string }, key)
    }
    expect(Date.now() - start).toBeGreaterThanOrEqual(200)
  })
})
