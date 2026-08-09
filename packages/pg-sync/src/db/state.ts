/**
 * Cursor and run state.
 *
 * State lives in the destination database, in the same schema as the data, so a
 * cursor checkpoint and the rows it covers share a transaction domain. The
 * invariant that matters: a cursor is never advanced before the writes it
 * describes are durable. Getting that backwards loses data silently on a crash.
 */
import type { SqlClient } from './client.js'
import { quoteIdent } from '../schema/ddl.js'

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
  /** created_at of the last processed event, for lag reporting. */
  lastEventAt?: string | null
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
  count?: number
  startedAt?: string
  completedAt?: string
}

export type StreamState = TailState | BackfillState | Record<string, unknown>

const TAIL_STREAM = '__tail__'

export class StateStore {
  constructor(
    private db: SqlClient,
    private schema: string,
    private syncId: string,
  ) {}

  private table(): string {
    return `${quoteIdent(this.schema)}."_sync_state"`
  }

  async get<T = StreamState>(env: string, stream: string): Promise<T | null> {
    const r = await this.db.query<{ state: T }>(
      `select state from ${this.table()} where sync_id = $1 and env = $2 and stream = $3`,
      [this.syncId, env, stream],
    )
    return r.rows[0]?.state ?? null
  }

  async set(env: string, stream: string, state: StreamState): Promise<void> {
    await this.db.query(
      `insert into ${this.table()} (sync_id, env, stream, state, updated_at)
       values ($1, $2, $3, $4::jsonb, now())
       on conflict (sync_id, env, stream) do update
         set state = excluded.state, updated_at = now()`,
      [this.syncId, env, stream, JSON.stringify(state)],
    )
  }

  getTail(env: string): Promise<TailState | null> {
    return this.get<TailState>(env, TAIL_STREAM)
  }

  async setTail(
    env: string,
    next: { cursor: string | null; lastEventAt?: string | null; rangeStart?: string | null },
  ): Promise<void> {
    await this.set(env, TAIL_STREAM, {
      kind: 'tail',
      cursor: next.cursor,
      // Once a real cursor exists the bootstrap position is redundant, and the
      // API rejects a request carrying both.
      rangeStart: next.cursor ? null : (next.rangeStart ?? null),
      lastEventAt: next.lastEventAt ?? null,
    })
  }

  async all(env: string): Promise<{ stream: string; state: StreamState; updatedAt: string }[]> {
    const r = await this.db.query<{ stream: string; state: StreamState; updated_at: string }>(
      `select stream, state, updated_at from ${this.table()} where sync_id = $1 and env = $2 order by stream`,
      [this.syncId, env],
    )
    return r.rows.map((row) => ({ stream: row.stream, state: row.state, updatedAt: row.updated_at }))
  }
}
