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
 */
import type { SqlClient } from '../db/client.js'
import { upsertRows } from '../db/upsert.js'
import type { StateStore, BackfillState } from '../db/state.js'
import type { WorkOSClient } from '../workos/client.js'
import { streamByName } from '../catalog.js'
import type { Logger, StreamDef, WriteStats } from '../types.js'
import { addStats, emptyStats } from '../types.js'

export interface BackfillOptions {
  schema: string
  env: string
  batchSize?: number
  logger?: Logger
  /** Resume from stored state instead of restarting the stream. */
  resume?: boolean
}

export interface BackfillStreamResult {
  stream: string
  stats: WriteStats
  pages: number
  skippedReason?: string
}

/** Rows of a parent stream, for endpoints that can only be listed per parent. */
async function parentIds(
  db: SqlClient,
  schema: string,
  env: string,
  parentStream: string,
): Promise<string[]> {
  const def = streamByName(parentStream)
  if (!def) return []
  const idCol = def.key[0] ?? 'id'
  const r = await db.query<{ id: string }>(
    `select "${idCol}" as id from "${schema}"."${parentStream}"
     where "_env" = $1 and "_deleted_at" is null order by "${idCol}"`,
    [env],
  )
  return r.rows.map((row) => row.id)
}

export async function backfillStream(
  db: SqlClient,
  api: WorkOSClient,
  state: StateStore,
  stream: StreamDef,
  opts: BackfillOptions,
): Promise<BackfillStreamResult> {
  const stats = emptyStats()
  let pages = 0

  // Organization domains arrive embedded in the organization payload; there is
  // no standalone list endpoint for them.
  if (stream.name === 'organization_domains') {
    return backfillEmbeddedDomains(db, opts, stats)
  }

  if (!stream.list) {
    return {
      stream: stream.name,
      stats,
      pages: 0,
      skippedReason: 'no list endpoint upstream — this stream is event-sourced only',
    }
  }

  const prior = opts.resume ? await state.get<BackfillState>(opts.env, stream.name) : null
  const doneParents = new Set(prior?.doneParents ?? [])

  const write = async (rows: Record<string, unknown>[]) => {
    // REST is the authoritative full object, so it replaces `_raw` rather than
    // merging into it.
    const s = await upsertRows(
      db,
      { schema: opts.schema, batchSize: opts.batchSize },
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

  if (stream.list.parent) {
    const parents = await parentIds(db, opts.schema, opts.env, stream.list.parent.stream)
    opts.logger?.info('backfill starting (per-parent)', {
      stream: stream.name,
      parents: parents.length,
    })
    for (const pid of parents) {
      if (doneParents.has(pid)) continue
      const usePath = stream.list.parent.from === 'path'
      const path = usePath
        ? stream.list.path.replace(`{${stream.list.parent.param}}`, pid)
        : stream.list.path
      const params: Record<string, string> = { ...(stream.list.params ?? {}) }
      if (!usePath) params[stream.list.parent.param] = pid
      // Per-directory rate limits do not aggregate, so bucket per parent.
      const key = rateKey === 'directory' ? `directory:${pid}` : rateKey

      for await (const page of api.paginate(path, params, key, stream.list.shape ?? 'list')) {
        pages++
        Object.assign(stats, addStats(stats, await write(page.rows)))
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
    opts.logger?.info('backfill starting', { stream: stream.name })
    for await (const page of api.paginate(
      stream.list.path,
      stream.list.params ?? {},
      rateKey,
      stream.list.shape ?? 'list',
      opts.resume ? (prior?.after ?? null) : null,
    )) {
      pages++
      Object.assign(stats, addStats(stats, await write(page.rows)))
      // Checkpoint after the write, never before it.
      await state.set(opts.env, stream.name, {
        kind: 'backfill',
        status: 'running',
        after: page.after,
        count: stats.created + stats.updated,
      } satisfies BackfillState)
    }
  }

  await state.set(opts.env, stream.name, {
    kind: 'backfill',
    status: 'complete',
    count: stats.created + stats.updated,
    completedAt: new Date().toISOString(),
  } satisfies BackfillState)

  opts.logger?.info('backfill complete', { stream: stream.name, pages, ...stats })
  return { stream: stream.name, stats, pages }
}

/**
 * Organization domains have events but no list endpoint of their own. They are
 * carried inline on the organization object, so we explode them out of the
 * rows the organizations backfill already wrote.
 */
async function backfillEmbeddedDomains(
  db: SqlClient,
  opts: BackfillOptions,
  stats: WriteStats,
): Promise<BackfillStreamResult> {
  const def = streamByName('organization_domains')!
  const r = await db.query<{ domains: unknown; org_id: string }>(
    `select "_raw"->'domains' as domains, "id" as org_id
     from "${opts.schema}"."organizations"
     where "_env" = $1 and "_raw" ? 'domains'`,
    [opts.env],
  )
  const rows: { data: Record<string, unknown>; updatedAt: string }[] = []
  for (const row of r.rows) {
    const domains = Array.isArray(row.domains) ? row.domains : []
    for (const d of domains as Record<string, unknown>[]) {
      if (!d || typeof d !== 'object' || !d['id']) continue
      rows.push({
        data: { organization_id: row.org_id, ...d },
        updatedAt:
          (typeof d['updated_at'] === 'string' && d['updated_at']) ||
          (typeof d['created_at'] === 'string' && d['created_at']) ||
          new Date().toISOString(),
      })
    }
  }
  const s = await upsertRows(
    db,
    { schema: opts.schema, batchSize: opts.batchSize },
    def,
    opts.env,
    rows,
    'replace',
  )
  opts.logger?.info('backfill complete (from embedded organization payloads)', {
    stream: 'organization_domains',
    ...s,
  })
  return { stream: 'organization_domains', stats: addStats(stats, s), pages: 0 }
}
