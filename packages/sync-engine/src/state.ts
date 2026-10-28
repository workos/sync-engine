/**
 * Cursor and run state.
 *
 * State lives in the destination, alongside the data, so a cursor checkpoint
 * and the rows it covers share a transaction domain. The invariant that
 * matters: a cursor is never advanced before the writes it describes are
 * durable. Getting that backwards loses data silently on a crash.
 */
import type { Destination } from './destination.js'

/** Live tail position: the last event id processed. */
export interface TailState {
  kind: 'tail'
  cursor: string | null
  /**
   * Bootstrap position, used until the first event yields a real cursor.
   *
   * `GET /events` always returns oldest-first — the `order` parameter has no
   * observable effect — so there is no cheap way to ask for "the id of the
   * newest event". Recording the wall clock at capture time and starting the
   * tail with `range_start` is the way to say "begin from here" without
   * replaying the whole retention window.
   */
  rangeStart?: string | null
  /** created_at of the last processed event. Drives the retention-cliff alarm. */
  lastEventAt?: string | null
  /**
   * Wall-clock time of the last poll that completed without error, whether
   * or not it returned events. This is the liveness signal: a quiet upstream
   * leaves `lastEventAt` old while the sync is perfectly healthy, so stall
   * detection has to look here instead.
   */
  lastPolledAt?: string | null
  updatedAt?: string
}

/** Backfill progress for one stream, resumable at page granularity. */
export interface BackfillState {
  kind: 'backfill'
  status: 'pending' | 'running' | 'complete'
  /** Pagination cursor within the current listing. */
  after?: string | null
  /** For N+1 streams: parents already fully listed. */
  doneParents?: string[]
  /**
   * For N+1 streams: the parent whose listing `after` belongs to, when a page
   * budget stopped the run inside a parent. Absent at a parent boundary.
   */
  parent?: string | null
  count?: number
  startedAt?: string
  completedAt?: string
}

export type StreamState = TailState | BackfillState | Record<string, unknown>

const TAIL_STREAM = '__tail__'

export interface TailUpdate {
  cursor: string | null
  lastEventAt?: string | null
  rangeStart?: string | null
  lastPolledAt?: string | null
}

function tailRecord(next: TailUpdate): TailState {
  return {
    kind: 'tail',
    cursor: next.cursor,
    // Once a real cursor exists the bootstrap position is redundant, and the
    // API rejects a request carrying both.
    rangeStart: next.cursor ? null : (next.rangeStart ?? null),
    lastEventAt: next.lastEventAt ?? null,
    lastPolledAt: next.lastPolledAt ?? null,
  }
}

export class StateStore {
  constructor(private destination: Destination) {}

  get<T = StreamState>(env: string, stream: string): Promise<T | null> {
    return this.destination.getState<T>(env, stream)
  }

  set(env: string, stream: string, state: StreamState): Promise<void> {
    return this.destination.setState(env, stream, state)
  }

  getTail(env: string): Promise<TailState | null> {
    return this.get<TailState>(env, TAIL_STREAM)
  }

  async setTail(env: string, next: TailUpdate): Promise<void> {
    await this.set(env, TAIL_STREAM, tailRecord(next))
  }

  /**
   * Save the tail position only while `holder` has the lease for `env` and
   * the stored position is still `expected`, in one atomic write. This is
   * how the tail checkpoints: see `Destination.setStateIfHeld`.
   */
  setTailIfHeld(env: string, next: TailUpdate, holder: string, expected: TailState | null): Promise<boolean> {
    return this.destination.setStateIfHeld(env, TAIL_STREAM, tailRecord(next), holder, expected)
  }

  /** Forget the tail position entirely; the next capture starts from now. */
  clearTail(env: string): Promise<void> {
    return this.set(env, TAIL_STREAM, { kind: 'tail', cursor: null, rangeStart: null, lastEventAt: null, lastPolledAt: null })
  }
}
