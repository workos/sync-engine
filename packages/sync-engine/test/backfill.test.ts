/**
 * The engine half of a per-parent backfill: what it records between slices
 * and what a resumed slice lists. A destination is stubbed with an in-memory
 * state store so the checkpoints themselves can be read; the same behavior
 * against a real database is in sync-postgres/test/engine.test.ts.
 */
import { describe, it, expect, beforeEach } from 'vitest'
import { backfillStream, pageBudget } from '../src/sync/backfill.js'
import { StateStore, type BackfillState } from '../src/state.js'
import { WorkOSClient } from '../src/workos/client.js'
import { streamByName } from '../src/catalog.js'
import type { Destination } from '../src/destination.js'
import { emptyStats } from '../src/types.js'
import { FakeWorkOS, resetIds } from './helpers/fake-workos.js'

const memberships = streamByName('organization_memberships')!

describe('per-parent backfill progress', () => {
  let fake: FakeWorkOS
  let api: WorkOSClient
  let states: Map<string, unknown>
  let parents: string[]
  let destination: Destination
  let state: StateStore

  beforeEach(() => {
    resetIds()
    fake = new FakeWorkOS()
    api = new WorkOSClient({ apiKey: 'sk_test_1', apiUrl: 'http://fake.workos.local', fetchImpl: fake.fetchImpl })
    states = new Map()
    parents = []
    destination = {
      init: async () => {},
      close: async () => {},
      upsert: async (_s, _e, rows) => ({ ...emptyStats(), created: rows.length }),
      delete: async () => emptyStats(),
      relink: async () => emptyStats(),
      unlink: async () => emptyStats(),
      liveIds: async () => [...parents],
      rawRows: async () => [],
      diffIds: async () => ({ matched: 0, localOnly: [], remoteOnly: [] }),
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
      getState: async <T>(env: string, stream: string) => (states.get(`${env}:${stream}`) as T | undefined) ?? null,
      setState: async (env, stream, value) => {
        states.set(`${env}:${stream}`, value)
      },
      setStateIfHeld: async () => true,
      acquireLease: async (_e, holder) => ({ acquired: true, holder, expiresAt: null }),
      releaseLease: async () => {},
      getLease: async () => null,
      reject: async () => {},
      rejected: async () => [],
      rejectedCount: async () => 0,
      clearRejected: async () => 0,
    }
    state = new StateStore(destination)
  })

  /** An organization with `n` memberships, registered as a live parent. */
  const org = (n = 1) => {
    const o = fake.createOrganization()
    const id = o['id'] as string
    for (let i = 0; i < n; i++) fake.createMembership(fake.createUser()['id'] as string, id)
    parents.push(id)
    return id
  }
  const stored = () => states.get('prod:organization_memberships') as BackfillState
  /** The parents listed so far, in request order. */
  const listed = () =>
    fake.requestLog
      .filter((r) => r.startsWith('/user_management/organization_memberships'))
      .map((r) => new URL(r, 'http://fake.workos.local').searchParams.get('organization_id'))
  const run = (opts: { resume?: boolean; maxPages?: number } = {}) =>
    backfillStream(destination, api, state, memberships, {
      env: 'prod',
      resume: opts.resume,
      budget: pageBudget(opts.maxPages),
    })

  it('records the last parent finished, not every parent, and resumes after it', async () => {
    const [a, b, c, d] = [org(), org(), org(), org()]

    expect(await run({ maxPages: 2 })).toMatchObject({ pages: 2, complete: false })
    expect(listed()).toEqual([a, b])
    expect(stored()).toMatchObject({ status: 'running', doneThrough: b, count: 2 })
    expect(stored()).not.toHaveProperty('doneParents')
    expect(stored()).not.toHaveProperty('parent')

    expect(await run({ resume: true })).toMatchObject({ pages: 2, complete: true })
    expect(listed()).toEqual([a, b, c, d])
    expect(stored()).toMatchObject({ status: 'complete' })
  })

  it('checkpoints inside a parent with the watermark of the parents before it', async () => {
    const a = org()
    const b = org(150)

    // One page for a, then the first of b's two.
    expect(await run({ maxPages: 2 })).toMatchObject({ pages: 2, complete: false })
    expect(stored()).toMatchObject({ status: 'running', doneThrough: a, parent: b })
    expect(stored().after).toEqual(expect.any(String))

    // The resumed slice asks for b's second page and nothing else.
    const before = fake.requestLog.length
    expect(await run({ resume: true })).toMatchObject({ pages: 1, complete: true })
    const since = fake.requestLog.slice(before)
    expect(since).toHaveLength(1)
    expect(since[0]).toContain(`organization_id=${b}`)
    expect(since[0]).toContain('after=')
  })

  it('sorts the parents itself rather than trusting the order the destination returns', async () => {
    const [a, b, c, d] = [org(), org(), org(), org()]
    parents = [d, b, a, c]

    expect(await run({ maxPages: 2 })).toMatchObject({ pages: 2, complete: false })
    expect(listed()).toEqual([a, b])
    expect(stored()).toMatchObject({ doneThrough: b })
  })

  it('lists a parent created after a slice, since its id sorts past the watermark', async () => {
    const [a, b, c] = [org(), org(), org()]
    await run({ maxPages: 2 })
    expect(stored()).toMatchObject({ doneThrough: b })

    const late = org()
    expect(await run({ resume: true })).toMatchObject({ pages: 2, complete: true })
    expect(listed()).toEqual([a, b, c, late])
  })

  it('honors a checkpoint that recorded each finished parent by id, and carries those past the watermark', async () => {
    const [a, b, c, d] = [org(), org(), org(), org()]
    // What a release before the watermark left behind after finishing a and c.
    states.set('prod:organization_memberships', {
      kind: 'backfill',
      status: 'running',
      doneParents: [c, a],
      count: 2,
    } satisfies BackfillState)

    // b is the first parent not yet done; the one page budgeted goes to it.
    expect(await run({ resume: true, maxPages: 1 })).toMatchObject({ pages: 1, complete: false })
    expect(listed()).toEqual([b])
    // a is behind the watermark now and dropped; c is still ahead of it and kept.
    expect(stored()).toMatchObject({ doneThrough: b, doneParents: [c] })

    expect(await run({ resume: true })).toMatchObject({ pages: 1, complete: true })
    expect(listed()).toEqual([b, d])
  })
})
