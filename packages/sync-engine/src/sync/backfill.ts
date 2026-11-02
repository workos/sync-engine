/**
 * Backfill: initial and historical state from the REST list endpoints.
 *
 * Backfill is not optional. Events are retained for 90 days, so the stream
 * alone can never produce the objects that existed before it — the single most
 * common complaint about webhook-only "sync" implementations.
 *
 * Ordering with the tail is deliberately overlapping rather than exact. The
 * cursor is captured *before* the first page is fetched, so anything that
 * changes mid-backfill is replayed afterwards; the staleness gate and
 * idempotent upserts resolve the collision. Trying to make the two phases abut
 * precisely is how gaps get created.
 *
 * A page budget bounds one call. A cron job, an edge function or a workflow
 * activity cannot hold a process open for the whole of a large backfill, so
 * `budget` stops a stream once the pages are spent, with its checkpoint
 * exactly where it stopped, and `resume` continues from there on the next
 * call. The budget is shared by every stream and environment in one call: it
 * is the call that has to fit the host's time limit, not each stream. With
 * `resume`, a stream already marked complete is skipped, so repeated bounded
 * calls converge instead of re-listing what already finished.
 */
import type { Destination, Row } from '../destination.js'
import type { StateStore, BackfillState } from '../state.js'
import type { WorkOSClient } from '../workos/client.js'
import { streamByName } from '../catalog.js'
import type { Logger, StreamDef, WriteStats } from '../types.js'
import { addStats, emptyStats } from '../types.js'
import { redacting } from '../redact.js'

/**
 * Pages one `backfill()` call may still fetch. Mutable on purpose: one object
 * is threaded through every stream of the call so they all draw on it.
 */
export interface PageBudget {
  remaining: number
}

/** The budget for `maxPages`, or none. Refuses anything but a positive integer. */
export function pageBudget(maxPages: number | undefined): PageBudget | undefined {
  if (maxPages === undefined) return undefined
  if (!Number.isInteger(maxPages) || maxPages < 1) {
    throw new Error(`maxPages must be a positive integer, got ${String(maxPages)}`)
  }
  return { remaining: maxPages }
}

export interface BackfillOptions {
  env: string
  logger?: Logger
  /** Resume from stored state instead of restarting the stream. */
  resume?: boolean
  /** Stop once this is spent, leaving the checkpoint where it stopped. Unbounded when absent. */
  budget?: PageBudget
  /** Receives the page count as pages arrive, so it survives a failure. */
  progress?: { pages: number }
  /**
   * What the listing is for. A refresh is the same listing with the same
   * checkpoint, made for the fields event payloads omit; it is labelled as
   * such in the log and counted under the `reconcile` phase upstream.
   */
  purpose?: 'backfill' | 'refresh'
}

export interface BackfillStreamResult {
  stream: string
  stats: WriteStats
  pages: number
  /**
   * True when nothing is left to list for this stream: it finished, or it has
   * no list endpoint. False when the page budget stopped it or it failed; in
   * both cases its checkpoint is where it stopped and `resume` picks it up.
   */
  complete: boolean
  skippedReason?: string
}

export async function backfillStream(
  destination: Destination,
  api: WorkOSClient,
  state: StateStore,
  stream: StreamDef,
  opts: BackfillOptions,
): Promise<BackfillStreamResult> {
  const stats = emptyStats()
  const progress = opts.progress ?? { pages: 0 }
  const label = opts.purpose ?? 'backfill'
  destination = redacting(destination)
  api = api.withPhase(label === 'refresh' ? 'reconcile' : 'backfill')

  // Organization domains arrive embedded in the organization payload; there is
  // no standalone list endpoint for them. Exploding them costs no API pages,
  // so the budget does not apply and every call does it again: the
  // organizations they come from may have grown since the last one.
  if (stream.name === 'organization_domains') {
    return backfillEmbeddedDomains(destination, opts, stats)
  }

  if (!stream.list) {
    return {
      stream: stream.name,
      stats,
      pages: 0,
      complete: true,
      skippedReason: 'no list endpoint upstream — this stream is event-sourced only',
    }
  }

  const prior = opts.resume ? await state.get<BackfillState>(opts.env, stream.name) : null
  if (prior?.status === 'complete') {
    opts.logger?.info(`${label} already complete; skipping`, { stream: stream.name })
    return {
      stream: stream.name,
      stats,
      pages: 0,
      complete: true,
      skippedReason: 'already complete — run without resume to re-list',
    }
  }

  const budget = opts.budget
  const spent = () => budget !== undefined && budget.remaining <= 0
  if (spent()) {
    return {
      stream: stream.name,
      stats,
      pages: 0,
      complete: false,
      skippedReason: 'page budget spent before this stream — resume to continue',
    }
  }

  const doneParents = new Set(prior?.doneParents ?? [])

  const write = async (rows: Record<string, unknown>[]) => {
    // REST is the authoritative full object, so it replaces `_raw` rather than
    // merging into it.
    const s = await destination.upsert(
      stream,
      opts.env,
      rows.map((data) => ({
        data,
        updatedAt: typeof data['updated_at'] === 'string'
          ? (data['updated_at'] as string)
          : typeof data['created_at'] === 'string'
            ? (data['created_at'] as string)
            : new Date().toISOString(),
      })),
      'replace',
    )
    return s
  }

  const rateKey =
    stream.list.rateKey === 'directory'
      ? 'directory'
      : stream.list.rateKey === 'user_management'
        ? 'user_management'
        : 'global'

  /**
   * Charged as soon as a page is fetched, before its write: a page whose
   * write fails still cost a request, and a budget that counted only
   * successes would let a failing stream pull one extra page per call.
   */
  const charge = () => {
    if (budget) budget.remaining--
  }

  const paused = (at: Record<string, unknown>): BackfillStreamResult => {
    opts.logger?.info(`${label} paused: page budget spent`, { stream: stream.name, pages: progress.pages, ...at })
    return { stream: stream.name, stats, pages: progress.pages, complete: false }
  }

  if (stream.list.parent) {
    const parentDef = streamByName(stream.list.parent.stream)
    const parents = parentDef ? await destination.liveIds(parentDef, opts.env) : []
    opts.logger?.info(`${label} starting (per-parent)`, {
      stream: stream.name,
      parents: parents.length,
    })
    for (const pid of parents) {
      if (doneParents.has(pid)) continue
      // The previous parent's checkpoint is already written, so stopping here
      // leaves nothing to redo.
      if (spent()) return paused({ parent: pid })
      const usePath = stream.list.parent.from === 'path'
      const path = usePath
        ? stream.list.path.replace(`{${stream.list.parent.param}}`, pid)
        : stream.list.path
      const params: Record<string, string> = { ...(stream.list.params ?? {}) }
      if (!usePath) params[stream.list.parent.param] = pid
      // Per-directory rate limits do not aggregate, so bucket per parent.
      const key = rateKey === 'directory' ? `directory:${pid}` : rateKey
      // A checkpoint taken inside this parent names it, so its listing picks
      // up at that page rather than from the parent's first one.
      const startAfter = prior?.parent === pid ? (prior.after ?? null) : null

      for await (const page of api.paginate(path, params, key, stream.list.shape ?? 'list', startAfter)) {
        progress.pages++
        charge()
        Object.assign(stats, addStats(stats, await write(page.rows)))
        // A last page is completion whatever the budget says, and the parent
        // boundary below records it. Every other page is checkpointed inside
        // the parent — after the write, never before it — so a crash or a
        // spent budget resumes at this page rather than the parent's first.
        if (page.after === null) continue
        await state.set(opts.env, stream.name, {
          kind: 'backfill',
          status: 'running',
          doneParents: [...doneParents],
          parent: pid,
          after: page.after,
          count: stats.created + stats.updated,
        } satisfies BackfillState)
        if (spent()) return paused({ parent: pid, after: page.after })
      }
      doneParents.add(pid)
      await state.set(opts.env, stream.name, {
        kind: 'backfill',
        status: 'running',
        doneParents: [...doneParents],
        count: stats.created + stats.updated,
      } satisfies BackfillState)
    }
  } else {
    opts.logger?.info(`${label} starting`, { stream: stream.name })
    for await (const page of api.paginate(
      stream.list.path,
      stream.list.params ?? {},
      rateKey,
      stream.list.shape ?? 'list',
      prior?.after ?? null,
    )) {
      progress.pages++
      charge()
      Object.assign(stats, addStats(stats, await write(page.rows)))
      // Checkpoint after the write, never before it.
      await state.set(opts.env, stream.name, {
        kind: 'backfill',
        status: 'running',
        after: page.after,
        count: stats.created + stats.updated,
      } satisfies BackfillState)
      // A last page is completion whatever the budget says: nothing is left
      // that a later call would have to fetch.
      if (page.after !== null && spent()) return paused({ after: page.after })
    }
  }

  await state.set(opts.env, stream.name, {
    kind: 'backfill',
    status: 'complete',
    count: stats.created + stats.updated,
    completedAt: new Date().toISOString(),
  } satisfies BackfillState)

  opts.logger?.info(`${label} complete`, { stream: stream.name, pages: progress.pages, ...stats })
  return { stream: stream.name, stats, pages: progress.pages, complete: true }
}

/**
 * Organization domains have events but no list endpoint of their own. They are
 * carried inline on the organization object, so we explode them out of the
 * rows the organizations backfill already wrote.
 */
async function backfillEmbeddedDomains(
  destination: Destination,
  opts: BackfillOptions,
  stats: WriteStats,
): Promise<BackfillStreamResult> {
  const def = streamByName('organization_domains')!
  const orgs = await destination.rawRows(streamByName('organizations')!, opts.env)
  const rows: Row[] = []
  for (const org of orgs) {
    const domains = Array.isArray(org['domains']) ? org['domains'] : []
    for (const d of domains as Record<string, unknown>[]) {
      if (!d || typeof d !== 'object' || !d['id']) continue
      rows.push({
        data: { organization_id: org['id'], ...d },
        updatedAt:
          (typeof d['updated_at'] === 'string' && d['updated_at']) ||
          (typeof d['created_at'] === 'string' && d['created_at']) ||
          new Date().toISOString(),
      })
    }
  }
  const s = await destination.upsert(def, opts.env, rows, 'replace')
  opts.logger?.info('backfill complete (from embedded organization payloads)', {
    stream: 'organization_domains',
    ...s,
  })
  return { stream: 'organization_domains', stats: addStats(stats, s), pages: 0, complete: true }
}
