/**
 * The engine half of reconciliation: `diffStream` paginates the list endpoint
 * and hands the pages to the destination. The database half is covered in
 * sync-postgres/test/reconcile.test.ts, and drift and repair end to end in
 * sync-postgres/test/engine.test.ts.
 */
import { describe, it, expect } from 'vitest'
import { diffStream } from '../src/sync/reconcile.js'
import { WorkOSClient } from '../src/workos/client.js'
import { streamByName } from '../src/catalog.js'
import type { Destination } from '../src/destination.js'
import { emptyStats } from '../src/types.js'
import { FakeWorkOS, resetIds } from './helpers/fake-workos.js'

const memberships = streamByName('organization_memberships')!

describe('diffStream', () => {
  it('reads the parent ids before the destination starts draining the pages', async () => {
    resetIds()
    const fake = new FakeWorkOS()
    const org = fake.createOrganization()
    const user = fake.createUser()
    fake.createMembership(user['id'] as string, org['id'] as string)
    const api = new WorkOSClient({
      apiKey: 'sk_test_1',
      apiUrl: 'http://fake.workos.local',
      fetchImpl: fake.fetchImpl,
    })

    // A destination drains the pages while holding one pinned connection, so
    // a `liveIds` that arrived in the middle of `diffIds` would be a read
    // through the pool, waiting for the very connection the diff is holding.
    const calls: string[] = []
    const destination: Destination = {
      init: async () => {},
      close: async () => {},
      upsert: async () => emptyStats(),
      delete: async () => emptyStats(),
      relink: async () => emptyStats(),
      liveIds: async (stream) => {
        calls.push(`liveIds:${stream.name}`)
        return [org['id'] as string]
      },
      rawRows: async () => [],
      diffIds: async (_s, _e, remote) => {
        calls.push('diffIds')
        const seen: Record<string, unknown>[] = []
        for await (const page of remote) {
          calls.push('page')
          seen.push(...page)
        }
        return { matched: seen.length, localOnly: [], remoteOnly: [] }
      },
      freshness: async (s) => ({
        stream: s.name,
        rows: 0,
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
    }

    const result = await diffStream(destination, api, 'prod', memberships)

    expect(calls.slice(0, 2)).toEqual(['liveIds:organizations', 'diffIds'])
    expect(calls.filter((c) => c.startsWith('liveIds'))).toHaveLength(1)
    expect(calls).toContain('page')
    expect(result).toEqual({
      stream: 'organization_memberships',
      matched: 1,
      localOnly: [],
      remoteOnly: [],
    })
  })
})
