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
 * The live ids are copied into one temp table before the first page is
 * requested, the remote pages are written into another as they arrive, then
 * three queries produce the answer: a join for `matched`, an anti-join from
 * the local table for `localOnly`, an anti-join from the remote table for
 * `remoteOnly`. Node holds one page of remote rows at a time, plus the drift.
 */
import { encodeKey, type IdDiff, type TableDef } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { chunk, keyValues, type WriteOptions } from './upsert.js'
import { quoteIdent, safeIdentifier } from '../schema/ddl.js'

/**
 * Both always qualified with `pg_temp`, so neither the lookups nor the drops
 * below can resolve to a real table of the same name somewhere on the search
 * path.
 */
const STAGING = 'pg_temp."_reconcile_remote"'
const LOCAL = 'pg_temp."_reconcile_local"'

export interface SqlDiff extends IdDiff {
  /** Remote rows with no value at the key path. Counted rather than thrown. */
  skipped: number
}

export async function diffIdsInSql(
  db: SqlClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  remote: AsyncIterable<Record<string, unknown>[]>,
): Promise<SqlDiff> {
  // Temp tables are per session, and a pool hands each plain `query` to
  // whichever connection is free, so the staging table would vanish between
  // statements. `transaction` pins one connection for the whole callback.
  return db.transaction(async (tx) => {
    const table = `${quoteIdent(opts.schema)}.${quoteIdent(safeIdentifier(stream.name))}`
    const keys = stream.key.map((name) => `l.${quoteIdent(safeIdentifier(name))}::text`).join(', ')
    // Append-only tables have no tombstone column; every row is live.
    const live = stream.mode === 'append' ? `l."_env" = $1` : `l."_env" = $1 and l."_deleted_at" is null`

    // A call nested inside an outer transaction would find the previous
    // tables still there, because ON COMMIT DROP waits for that outer
    // commit. Dropping first makes the function re-entrant.
    await tx.exec(`drop table if exists ${LOCAL}`)
    await tx.exec(`drop table if exists ${STAGING}`)
    await tx.exec(`create temp table "_reconcile_local" (id text[] primary key) on commit drop`)
    await tx.exec(`create temp table "_reconcile_remote" (id text[] primary key, raw jsonb not null) on commit drop`)

    // The live ids are frozen before the first page is requested, not read
    // once the pages are in. Listing a large stream takes minutes under the
    // API's rate limits and the tail keeps writing throughout: a row it
    // inserts after the listing has passed that object's position would be
    // live in the mirror and absent from the pages, and `--repair` would
    // tombstone an object that exists upstream. Deletion is terminal, so no
    // later event would undo that. Captured here, such a row is simply not a
    // candidate, which is how the in-memory diff behaved when it read
    // `liveIds` first.
    await tx.query(`insert into ${LOCAL} (id) select array[${keys}] from ${table} l where ${live}`, [env])

    let skipped = 0
    for await (const page of remote) {
      const keyed: { id: string[]; data: Record<string, unknown> }[] = []
      for (const data of page) {
        const id = keyValues(stream, data)
        if (!id) {
          skipped++
          continue
        }
        keyed.push({ id, data })
      }
      for (const batch of chunk(keyed, opts.batchSize ?? 500)) {
        const params: unknown[] = []
        const tuples = batch.map((r) => {
          const i = params.length
          params.push(r.id, JSON.stringify(r.data))
          return `($${i + 1}::text[], $${i + 2}::jsonb)`
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
    // of each table rather than the planner's guess for a new one.
    await tx.exec(`analyze ${LOCAL}`)
    await tx.exec(`analyze ${STAGING}`)

    const matched = await tx.query<{ n: string }>(
      `select count(*)::text as n from ${LOCAL} l join ${STAGING} r on r.id = l.id`,
    )
    const localOnly = await tx.query<{ id: string[] }>(
      `select l.id from ${LOCAL} l
       where not exists (select 1 from ${STAGING} r where r.id = l.id)
       order by l.id`,
    )
    const remoteOnly = await tx.query<{ raw: Record<string, unknown> }>(
      `select r.raw from ${STAGING} r
       where not exists (select 1 from ${LOCAL} l where l.id = r.id)
       order by r.id`,
    )

    return {
      matched: Number(matched.rows[0]?.n ?? 0),
      localOnly: localOnly.rows.map((r) => encodeKey(r.id)),
      remoteOnly: remoteOnly.rows.map((r) => r.raw),
      skipped,
    }
  })
}
