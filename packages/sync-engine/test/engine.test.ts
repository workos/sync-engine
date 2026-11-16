/**
 * The listen loop's lifecycle against a fake destination: what `stop()` does
 * when it lands at an awkward moment. The loop's behaviour under real
 * writes is covered in the destination packages, where a database exists.
 */
import { describe, it, expect } from 'vitest'
import { SyncEngine, WorkOSClient, silentLogger, emptyStats, type Destination } from '../src/index.js'

/** A destination that does nothing, whose `init()` waits for the test to let it through. */
function fakeDestination(initGate: Promise<void>): Destination {
  const none = async () => emptyStats()
  return {
    init: async () => {
      await initGate
    },
    close: async () => {},
    upsert: none,
    delete: none,
    relink: none,
    unlink: none,
    liveIds: async () => [],
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
}

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

/** Resolves to `'ended'` when `p` settles first, `'still running'` otherwise. */
const outcome = (p: Promise<unknown>, ms: number) =>
  Promise.race([p.then(() => 'ended' as const), sleep(ms).then(() => 'still running' as const)])

function makeEngine(destination: Destination, polls: { n: number }) {
  return new SyncEngine(
    {
      destination,
      environments: [{ name: 'prod', apiKey: 'sk_test_1' }],
      streams: ['users'],
      logger: silentLogger,
      pollIntervalMs: 10,
    },
    {
      clientFor: () =>
        new WorkOSClient({
          apiKey: 'sk_test_1',
          fetchImpl: async () => {
            polls.n++
            return Response.json({ data: [], list_metadata: { after: null } })
          },
        }),
    },
  )
}

describe('listen() and stop()', () => {
  it('ends the loop when stop() arrives while init() is still running', async () => {
    const gate = deferred()
    const polls = { n: 0 }
    const engine = makeEngine(fakeDestination(gate.promise), polls)

    const listening = engine.listen()
    await sleep(20)
    // A SIGTERM during a slow schema apply. Before the fix this was reset
    // away as soon as init returned, and the loop polled forever.
    engine.stop()
    gate.resolve()

    expect(await outcome(listening, 2_000)).toBe('ended')
    expect(polls.n).toBe(0)
    expect(engine.health().state).toBe('stopped')
  })

  it('starts fresh on a later listen() after a stop', async () => {
    const polls = { n: 0 }
    const engine = makeEngine(fakeDestination(Promise.resolve()), polls)

    const first = engine.listen()
    engine.stop()
    expect(await outcome(first, 2_000)).toBe('ended')

    // The earlier stop must not stick: a new listen polls.
    const second = engine.listen()
    const deadline = Date.now() + 2_000
    while (polls.n === 0 && Date.now() < deadline) await sleep(5)
    expect(polls.n).toBeGreaterThan(0)
    engine.stop()
    expect(await outcome(second, 2_000)).toBe('ended')
  })
})

describe('status()', () => {
  const empty = {
    initialized: false,
    cursor: null,
    rangeStart: null,
    started: false,
    cursorAgeDays: null,
    lastPolledAt: null,
    pollAgeSeconds: null,
    lease: null,
    rejected: 0,
    oversized: [],
    streams: [],
  }

  it('creates nothing when the destination says the mirror is not there', async () => {
    let inits = 0
    const destination: Destination = {
      ...fakeDestination(Promise.resolve()),
      init: async () => {
        inits++
      },
      ready: async () => false,
    }
    // Before this, `status` against an empty database created every table
    // and view, then reported that nothing had started.
    expect(await makeEngine(destination, { n: 0 }).status()).toEqual({ prod: empty })
    expect(inits).toBe(0)
  })

  it('reads a mirror that is there without applying the schema', async () => {
    let inits = 0
    const destination: Destination = {
      ...fakeDestination(Promise.resolve()),
      init: async () => {
        inits++
      },
      ready: async () => true,
    }
    const status = await makeEngine(destination, { n: 0 }).status()
    expect(status['prod']).toMatchObject({ initialized: true, started: false })
    expect(status['prod']!.streams.map((s) => s.stream)).toContain('users')
    expect(inits).toBe(0)
  })

  it('applies the schema first for a destination that cannot say', async () => {
    let inits = 0
    const destination: Destination = {
      ...fakeDestination(Promise.resolve()),
      init: async () => {
        inits++
      },
    }
    expect((await makeEngine(destination, { n: 0 }).status())['prod']!.initialized).toBe(true)
    expect(inits).toBe(1)
  })
})

describe('reconcile()', () => {
  it('marks a stream whose diff threw as failed, and still reports the others', async () => {
    const destination: Destination = {
      ...fakeDestination(Promise.resolve()),
      diffIds: async () => {
        throw new Error('DDL command denied')
      },
    }
    const { diffs } = await makeEngine(destination, { n: 0 }).reconcile()
    const users = diffs['prod']!.find((d) => d.stream === 'users')!
    expect(users).toMatchObject({
      complete: false,
      failed: true,
      skippedReason: 'failed: Error: DDL command denied',
    })
    // An append-only stream has nothing to diff: complete, and not a failure.
    const events = diffs['prod']!.find((d) => d.stream === 'events')!
    expect(events.complete).toBe(true)
    expect(events.failed).toBeUndefined()
  })
})
