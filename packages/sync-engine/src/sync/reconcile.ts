/**
 * Reconciliation: proving the mirror matches, and repairing it when it doesn't.
 *
 * Every other mechanism in the engine reduces drift; none of them detect it.
 * The failure modes that matter are all silent — a missed delete leaves a row
 * that no future event will ever correct, a list endpoint's default filter
 * quietly excludes a category of object, a bad deploy writes nulls for an hour.
 * Sequin and comparable engines alike discovered their worst sync bugs through
 * reconciliation rather than error reports.
 *
 * Three tiers, cheapest first:
 *   1. freshness  — per-stream lag and liveness. Continuous.
 *   2. counts     — row counts against the API. Daily.
 *   3. id diff    — full set difference. Weekly, or after any incident.
 *
 * Only the id diff finds missed deletes: a deleted object is not returned by
 * any list endpoint, so no amount of re-listing will ever surface its absence.
 *
 * A page budget bounds one `reconcile()` call the way it bounds a backfill,
 * with one difference. The diff is a set difference against the whole
 * listing, so a listing cannot stop partway: every object on the pages a
 * budget cut off would read as a missed delete, and `repair` would tombstone
 * objects that exist upstream. The budget therefore decides whether the next
 * stream starts, never where a listing stops. A stream that starts runs to
 * its last page, a call overshoots by at most one stream's listing, and the
 * streams not started are left for a later call with `resume`.
 */
import type { Destination, FreshnessRow } from '../destination.js'
import type { PageBudget } from './backfill.js'
import { streamByName } from '../catalog.js'
import type { WorkOSClient } from '../workos/client.js'
import type { Logger, StreamDef } from '../types.js'
import { redacting } from '../redact.js'
import { keyPayload, remoteKey } from './diff.js'

export async function freshness(
  destination: Destination,
  env: string,
  streams: StreamDef[],
  lagWindowHours = 24,
): Promise<FreshnessRow[]> {
  const out: FreshnessRow[] = []
  for (const s of streams) out.push(await destination.freshness(s, env, lagWindowHours))
  return out
}

export interface DiffResult {
  stream: string
  matched: number
  /** In the mirror but not upstream: missed deletes. */
  localOnly: string[]
  /** Upstream but not in the mirror: missed creates or backfill gaps. */
  remoteOnly: string[]
  repaired?: { deleted: number; inserted: number }
  /** Pages this call fetched for the stream. */
  pages: number
  /**
   * True when the stream was diffed, or has nothing to diff. False when the
   * page budget was spent before it started, or it failed; a call with
   * `resume` comes back to it.
   */
  complete: boolean
  skippedReason?: string
}

/** Whether a stream has a listing to diff against. */
export function diffable(stream: StreamDef): boolean {
  return Boolean(stream.list) && stream.mode !== 'append'
}

/**
 * Full id-set difference for one stream.
 *
 * Note the asymmetry in what each direction means: `localOnly` rows are almost
 * always missed deletes, `remoteOnly` rows are missed creates. They have
 * different causes and different fixes, so they are reported separately rather
 * than as a single drift number.
 *
 * The difference itself is the destination's job (`Destination.diffIds`): the
 * remote pages are streamed to it and it diffs them against the live rows in
 * the database, so this function never holds more than one page. Holding the
 * whole stream here is what made a million-user directory a memory problem.
 */
export async function diffStream(
  destination: Destination,
  api: WorkOSClient,
  env: string,
  stream: StreamDef,
  opts: {
    repair?: boolean
    logger?: Logger
    budget?: PageBudget
    /** Receives the page count as pages arrive, so it survives a failure. */
    progress?: { pages: number }
  } = {},
): Promise<DiffResult> {
  destination = redacting(destination)
  api = api.withPhase('reconcile')
  if (!stream.list || stream.mode === 'append') {
    return {
      stream: stream.name,
      matched: 0,
      localOnly: [],
      remoteOnly: [],
      pages: 0,
      complete: true,
      skippedReason:
        stream.mode === 'append'
          ? 'append-only stream: nothing to reconcile'
          : 'no list endpoint upstream: drift cannot be detected for this stream',
    }
  }

  // The budget is consulted once, here. A stream that starts runs to its
  // last page, for the reason in the module comment.
  const budget = opts.budget
  if (budget !== undefined && budget.remaining <= 0) {
    return {
      stream: stream.name,
      matched: 0,
      localOnly: [],
      remoteOnly: [],
      pages: 0,
      complete: false,
      skippedReason: 'page budget spent before this stream — resume to continue',
    }
  }

  const list = stream.list

  const rateKey =
    list.rateKey === 'directory' ? 'directory' : list.rateKey === 'user_management' ? 'user_management' : 'global'

  // The parent ids are read here, before the diff starts, never from inside
  // the generator below. The destination drains the pages while holding one
  // pinned connection (its staging table is per session), so a read that
  // went through the pool from inside the generator would wait for a free
  // connection while the diff waited for the next page. With a pool of one
  // that is a deadlock, and the reconcile never finishes.
  //
  // The cost of reading them here, rather than after the destination has
  // captured its live set, is a window of a few statements in which the tail
  // can land a new parent and its first child: the child is then in the live
  // set, the parent is not in this list, and the child reports `localOnly`.
  // The window is milliseconds against a listing that takes minutes; closing
  // it needs a parent read on the diff's own connection, which the interface
  // does not offer.
  const parent = list.parent
  const parents = parent ? await destination.liveIds(streamByName(parent.stream)!, env) : []

  // Charged as each page arrives, before the destination stages it, the way
  // a backfill charges a page before its write. The count lives in the
  // caller's `progress` when one is given, so a stream that fails partway
  // can still report the pages it drew on the budget.
  const progress = opts.progress ?? { pages: 0 }
  const charge = () => {
    progress.pages++
    if (budget) budget.remaining--
  }

  /**
   * The remote side, one page at a time. The pagination — the per-parent
   * loop, the rate keys — is exactly what it was when the diff ran here in
   * memory; it is a generator so the destination can consume each page as it
   * arrives instead of this function collecting them all first. It talks to
   * the API only, for the reason above.
   */
  async function* remotePages(): AsyncGenerator<Record<string, unknown>[]> {
    if (parent) {
      for (const pid of parents) {
        const usePath = parent.from === 'path'
        const path = usePath ? list.path.replace(`{${parent.param}}`, pid) : list.path
        const params: Record<string, string> = { ...list.params }
        if (!usePath) params[parent.param] = pid
        const key = rateKey === 'directory' ? `directory:${pid}` : rateKey
        for await (const page of api.paginate(path, params, key, list.shape ?? 'list')) {
          charge()
          yield page.rows as Record<string, unknown>[]
        }
      }
    } else {
      for await (const page of api.paginate(list.path, list.params ?? {}, rateKey, list.shape ?? 'list')) {
        charge()
        yield page.rows as Record<string, unknown>[]
      }
    }
  }

  // A tombstoned row is not live, so one that still exists upstream reports
  // as `remoteOnly`, and repair upserts it without clearing the tombstone
  // (deletion is terminal). The in-memory diff behaved the same way; this
  // moves the diff, it does not decide that question.
  const diff = await destination.diffIds(stream, env, remotePages())
  const { matched, localOnly } = diff
  // A destination never returns an unkeyed payload; the filter is belt and braces.
  const remoteOnly = diff.remoteOnly.flatMap((data) => remoteKey(stream, data) ?? [])

  const result: DiffResult = {
    stream: stream.name,
    matched,
    localOnly,
    remoteOnly,
    pages: progress.pages,
    complete: true,
  }

  if (opts.repair && (localOnly.length > 0 || diff.remoteOnly.length > 0)) {
    const now = new Date().toISOString()
    let deleted = 0
    let inserted = 0
    if (localOnly.length > 0) {
      const s = await destination.delete(
        stream,
        env,
        localOnly.map((id) => ({ data: keyPayload(stream, id), updatedAt: now })),
      )
      deleted = s.deleted
    }
    if (diff.remoteOnly.length > 0) {
      const rows = diff.remoteOnly.map((data) => ({
        data,
        updatedAt:
          (typeof data['updated_at'] === 'string' && data['updated_at']) ||
          (typeof data['created_at'] === 'string' && data['created_at']) ||
          now,
      }))
      const s = await destination.upsert(stream, env, rows, 'replace')
      inserted = s.created + s.updated
    }
    result.repaired = { deleted, inserted }
    opts.logger?.info('repaired drift', { stream: stream.name, deleted, inserted })
  }

  return result
}
