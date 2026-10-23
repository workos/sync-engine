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
 */
import type { Destination, FreshnessRow } from '../destination.js'
import { streamByName } from '../catalog.js'
import type { WorkOSClient } from '../workos/client.js'
import type { Logger, StreamDef } from '../types.js'
import { redacting } from '../redact.js'
import { remoteKey } from './diff.js'

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
  skippedReason?: string
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
  opts: { repair?: boolean; logger?: Logger } = {},
): Promise<DiffResult> {
  destination = redacting(destination)
  if (!stream.list || stream.mode === 'append') {
    return {
      stream: stream.name,
      matched: 0,
      localOnly: [],
      remoteOnly: [],
      skippedReason: stream.mode === 'append'
        ? 'append-only stream: nothing to reconcile'
        : 'no list endpoint upstream: drift cannot be detected for this stream',
    }
  }

  const list = stream.list
  const keyCol = stream.key[0]!

  const rateKey =
    list.rateKey === 'directory'
      ? 'directory'
      : list.rateKey === 'user_management'
        ? 'user_management'
        : 'global'

  /**
   * The remote side, one page at a time. The pagination — the per-parent
   * loop, the rate keys — is exactly what it was when the diff ran here in
   * memory; it is a generator so the destination can consume each page as it
   * arrives instead of this function collecting them all first.
   */
  async function* remotePages(): AsyncGenerator<Record<string, unknown>[]> {
    if (list.parent) {
      const parentDef = streamByName(list.parent.stream)!
      const parents = await destination.liveIds(parentDef, env)
      for (const pid of parents) {
        const usePath = list.parent.from === 'path'
        const path = usePath ? list.path.replace(`{${list.parent.param}}`, pid) : list.path
        const params: Record<string, string> = { ...(list.params ?? {}) }
        if (!usePath) params[list.parent.param] = pid
        const key = rateKey === 'directory' ? `directory:${pid}` : rateKey
        for await (const page of api.paginate(path, params, key, list.shape ?? 'list')) {
          yield page.rows as Record<string, unknown>[]
        }
      }
    } else {
      for await (const page of api.paginate(list.path, list.params ?? {}, rateKey, list.shape ?? 'list')) {
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

  const result: DiffResult = { stream: stream.name, matched, localOnly, remoteOnly }

  if (opts.repair && (localOnly.length > 0 || diff.remoteOnly.length > 0)) {
    const now = new Date().toISOString()
    let deleted = 0
    let inserted = 0
    if (localOnly.length > 0) {
      const s = await destination.delete(
        stream,
        env,
        localOnly.map((id) => ({ data: { [keyCol]: id }, updatedAt: now })),
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

/**
 * Refresh every row from the list endpoints. This is also how fields that
 * event payloads omit get corrected, so it doubles as the answer to partial
 * event payloads.
 */
export async function refreshStream(
  destination: Destination,
  api: WorkOSClient,
  env: string,
  stream: StreamDef,
): Promise<{ stream: string; refreshed: number }> {
  if (!stream.list) return { stream: stream.name, refreshed: 0 }
  destination = redacting(destination)
  let refreshed = 0
  const rateKey = stream.list.rateKey === 'user_management' ? 'user_management' : 'global'
  for await (const page of api.paginate(
    stream.list.path,
    stream.list.params ?? {},
    rateKey,
    stream.list.shape ?? 'list',
  )) {
    const rows = page.rows.map((data) => ({
      data: data as Record<string, unknown>,
      updatedAt:
        (typeof (data as Record<string, unknown>)['updated_at'] === 'string' &&
          ((data as Record<string, unknown>)['updated_at'] as string)) ||
        new Date().toISOString(),
    }))
    const s = await destination.upsert(stream, env, rows, 'replace')
    refreshed += s.created + s.updated
  }
  return { stream: stream.name, refreshed }
}
