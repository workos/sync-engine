/**
 * Reconciliation: proving the mirror matches, and repairing it when it doesn't.
 *
 * Every other mechanism in the engine reduces drift; none of them detect it.
 * The failure modes that matter are all silent — a missed delete leaves a row
 * that no future event will ever correct, a list endpoint's default filter
 * quietly excludes a category of object, a bad deploy writes nulls for an hour.
 * Both Sequin and Stripe's engine discovered their worst sync bugs through
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
import type { SqlClient } from '../db/client.js'
import { upsertRows, deleteRows } from '../db/upsert.js'
import { streamByName } from '../catalog.js'
import type { WorkOSClient } from '../workos/client.js'
import type { Logger, StreamDef } from '../types.js'

export interface FreshnessRow {
  stream: string
  rows: number
  deleted: number
  lastSyncedAt: string | null
  /** Seconds since the most recent upstream change landed. */
  stalenessSeconds: number | null
  /**
   * Seconds between an object changing upstream and the mirror writing it,
   * averaged over objects that changed within `lagWindowHours`.
   *
   * Deliberately restricted to recently-changed rows. Measured across every
   * row it would report the age of the objects rather than the delay of the
   * sync: a freshly backfilled mirror of year-old objects would show a lag of
   * months while being perfectly current. `null` means nothing changed
   * upstream in the window, so there is no delay to measure.
   */
  lagSeconds: number | null
  /** Rows the lag average is computed from. Zero means lagSeconds is null. */
  lagSampleSize: number
}

export async function freshness(
  db: SqlClient,
  schema: string,
  env: string,
  streams: StreamDef[],
  lagWindowHours = 24,
): Promise<FreshnessRow[]> {
  const out: FreshnessRow[] = []
  for (const s of streams) {
    const hasDeleted = s.mode !== 'append'
    const r = await db.query<{
      rows: string
      deleted: string | null
      last_synced_at: string | null
      staleness: string | null
      lag: string | null
      lag_n: string
    }>(
      `select count(*)::text as rows,
              ${hasDeleted ? `count(*) filter (where "_deleted_at" is not null)::text` : `'0'`} as deleted,
              max("_synced_at")::text as last_synced_at,
              extract(epoch from (now() - max("_updated_at")))::text as staleness,
              avg(extract(epoch from ("_synced_at" - "_updated_at")))
                filter (where "_updated_at" > now() - ($2 || ' hours')::interval)::text as lag,
              count(*) filter (where "_updated_at" > now() - ($2 || ' hours')::interval)::text as lag_n
       from "${schema}"."${s.name}" where "_env" = $1`,
      [env, String(lagWindowHours)],
    )
    const row = r.rows[0]
    const lagN = Number(row?.lag_n ?? 0)
    out.push({
      stream: s.name,
      rows: Number(row?.rows ?? 0),
      deleted: Number(row?.deleted ?? 0),
      lastSyncedAt: row?.last_synced_at ?? null,
      stalenessSeconds: row?.staleness ? Number(row.staleness) : null,
      lagSeconds: lagN > 0 && row?.lag ? Number(row.lag) : null,
      lagSampleSize: lagN,
    })
  }
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

async function localIds(
  db: SqlClient,
  schema: string,
  env: string,
  stream: StreamDef,
): Promise<Set<string>> {
  const keyCol = stream.key[0]!
  const r = await db.query<{ k: string }>(
    `select "${keyCol}" as k from "${schema}"."${stream.name}"
     where "_env" = $1 and "_deleted_at" is null`,
    [env],
  )
  return new Set(r.rows.map((row) => row.k))
}

/**
 * Full id-set difference for one stream.
 *
 * Note the asymmetry in what each direction means: `localOnly` rows are almost
 * always missed deletes, `remoteOnly` rows are missed creates. They have
 * different causes and different fixes, so they are reported separately rather
 * than as a single drift number.
 */
export async function diffStream(
  db: SqlClient,
  api: WorkOSClient,
  schema: string,
  env: string,
  stream: StreamDef,
  opts: { repair?: boolean; logger?: Logger } = {},
): Promise<DiffResult> {
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

  const local = await localIds(db, schema, env, stream)
  const remote = new Map<string, Record<string, unknown>>()
  const keyCol = stream.key[0]!

  const rateKey =
    stream.list.rateKey === 'directory'
      ? 'directory'
      : stream.list.rateKey === 'user_management'
        ? 'user_management'
        : 'global'

  if (stream.list.parent) {
    const parentDef = streamByName(stream.list.parent.stream)!
    const parents = await db.query<{ id: string }>(
      `select "${parentDef.key[0]}" as id from "${schema}"."${parentDef.name}"
       where "_env" = $1 and "_deleted_at" is null`,
      [env],
    )
    for (const p of parents.rows) {
      const usePath = stream.list.parent.from === 'path'
      const path = usePath
        ? stream.list.path.replace(`{${stream.list.parent.param}}`, p.id)
        : stream.list.path
      const params: Record<string, string> = { ...(stream.list.params ?? {}) }
      if (!usePath) params[stream.list.parent.param] = p.id
      const key = rateKey === 'directory' ? `directory:${p.id}` : rateKey
      for await (const page of api.paginate(path, params, key, stream.list.shape ?? 'list')) {
        for (const row of page.rows) {
          const id = (row as Record<string, unknown>)[keyCol]
          if (typeof id === 'string') remote.set(id, row as Record<string, unknown>)
        }
      }
    }
  } else {
    for await (const page of api.paginate(
      stream.list.path,
      stream.list.params ?? {},
      rateKey,
      stream.list.shape ?? 'list',
    )) {
      for (const row of page.rows) {
        const id = (row as Record<string, unknown>)[keyCol]
        if (typeof id === 'string') remote.set(id, row as Record<string, unknown>)
      }
    }
  }

  const localOnly = [...local].filter((id) => !remote.has(id))
  const remoteOnly = [...remote.keys()].filter((id) => !local.has(id))
  const matched = local.size - localOnly.length

  const result: DiffResult = { stream: stream.name, matched, localOnly, remoteOnly }

  if (opts.repair && (localOnly.length > 0 || remoteOnly.length > 0)) {
    const now = new Date().toISOString()
    let deleted = 0
    let inserted = 0
    if (localOnly.length > 0) {
      const s = await deleteRows(
        db,
        { schema },
        stream,
        env,
        localOnly.map((id) => ({ data: { [keyCol]: id }, updatedAt: now })),
      )
      deleted = s.deleted
    }
    if (remoteOnly.length > 0) {
      const rows = remoteOnly.map((id) => {
        const data = remote.get(id)!
        return {
          data,
          updatedAt:
            (typeof data['updated_at'] === 'string' && data['updated_at']) ||
            (typeof data['created_at'] === 'string' && data['created_at']) ||
            now,
        }
      })
      const s = await upsertRows(db, { schema }, stream, env, rows, 'replace')
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
  db: SqlClient,
  api: WorkOSClient,
  schema: string,
  env: string,
  stream: StreamDef,
): Promise<{ stream: string; refreshed: number }> {
  if (!stream.list) return { stream: stream.name, refreshed: 0 }
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
    const s = await upsertRows(db, { schema }, stream, env, rows, 'replace')
    refreshed += s.created + s.updated
  }
  return { stream: stream.name, refreshed }
}
