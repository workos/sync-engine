/**
 * The reconcile set difference, computed in Postgres.
 *
 * The engine's `diffStream` once held every live local id in a `Set` and every
 * remote payload in a `Map`, so the memory a reconcile needed grew with the
 * size of the stream rather than the size of the drift: a directory with a
 * million users turned a weekly health check into an out-of-memory. Fivetran
 * and Airbyte stage the remote side into a table and diff in SQL, and so does
 * this.
 *
 * The remote pages are written into a temp table as they arrive, then three
 * queries produce the answer: a join for `matched`, an anti-join from the
 * mirror for `localOnly`, an anti-join from the staging table for
 * `remoteOnly`. Node holds one page of remote rows at a time, plus the drift.
 */
import type { IdDiff, StreamDef } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { chunk, readPath, type WriteOptions } from './upsert.js'
import { quoteIdent, safeIdentifier } from '../schema/ddl.js'

/**
 * Always qualified with `pg_temp`, so neither the lookups nor the drop below
 * can resolve to a real table of the same name somewhere on the search path.
 */
const STAGING = 'pg_temp."_reconcile_remote"'

export interface SqlDiff extends IdDiff {
  /** Remote rows with no value at the key path. Counted rather than thrown. */
  skipped: number
}

export async function diffIdsInSql(
  db: SqlClient,
  opts: WriteOptions,
  stream: StreamDef,
  env: string,
  remote: AsyncIterable<Record<string, unknown>[]>,
): Promise<SqlDiff> {
  // Temp tables are per session, and a pool hands each plain `query` to
  // whichever connection is free, so the staging table would vanish between
  // statements. `transaction` pins one connection for the whole callback.
  return db.transaction(async (tx) => {
    const table = `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`
    // The first key column, exactly as `liveIds` and the engine use it.
    const keyName = stream.key[0]!
    const key = quoteIdent(safeIdentifier(keyName))
    const keyPath = stream.columns.find((c) => c.name === keyName)?.path ?? keyName
    // Append-only tables have no tombstone column; every row is live.
    const live =
      stream.mode === 'append' ? `l."_env" = $1` : `l."_env" = $1 and l."_deleted_at" is null`

    // A call nested inside an outer transaction would find the previous
    // staging table still there, because ON COMMIT DROP waits for that outer
    // commit. Dropping first makes the function re-entrant.
    await tx.exec(`drop table if exists ${STAGING}`)
    await tx.exec(
      `create temp table "_reconcile_remote" (id text primary key, raw jsonb not null) on commit drop`,
    )

    let skipped = 0
    for await (const page of remote) {
      const keyed: { id: string; data: Record<string, unknown> }[] = []
      for (const data of page) {
        const v = readPath(data, keyPath)
        if (v == null || v === '') {
          skipped++
          continue
        }
        keyed.push({ id: String(v), data })
      }
      for (const batch of chunk(keyed, opts.batchSize ?? 500)) {
        const params: unknown[] = []
        const tuples = batch.map((r) => {
          const i = params.length
          params.push(r.id, JSON.stringify(r.data))
          return `($${i + 1}, $${i + 2}::jsonb)`
        })
        // An id the API returns twice (a page boundary moving under a
        // concurrent write) keeps its first copy. Nothing is lost that the
        // next reconcile would not find again.
        await tx.query(
          `insert into ${STAGING} (id, raw) values ${tuples.join(', ')} on conflict (id) do nothing`,
          params,
        )
      }
    }

    // Fresh statistics, so the anti-joins are planned against the real size
    // of the staging table rather than the planner's guess for a new one.
    await tx.exec(`analyze ${STAGING}`)

    // Composite-key streams diff on their first key column alone, as the
    // engine always has, so several live rows can share one id and must be
    // counted once. Single-key streams have a unique key per environment, and
    // skip the aggregate.
    const distinct = stream.key.length > 1 ? 'distinct ' : ''
    const matched = await tx.query<{ n: string }>(
      `select count(${distinct}l.${key})::text as n
       from ${table} l join ${STAGING} r on r.id = l.${key}
       where ${live}`,
      [env],
    )
    const localOnly = await tx.query<{ id: string }>(
      `select ${distinct}l.${key} as id from ${table} l
       where ${live}
         and not exists (select 1 from ${STAGING} r where r.id = l.${key})
       order by id`,
      [env],
    )
    const remoteOnly = await tx.query<{ raw: Record<string, unknown> }>(
      `select r.raw from ${STAGING} r
       where not exists (select 1 from ${table} l where ${live} and l.${key} = r.id)
       order by r.id`,
      [env],
    )

    return {
      matched: Number(matched.rows[0]?.n ?? 0),
      localOnly: localOnly.rows.map((r) => r.id),
      remoteOnly: remoteOnly.rows.map((r) => r.raw),
      skipped,
    }
  })
}
