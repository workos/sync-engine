/**
 * The reconcile set difference, computed in ClickHouse.
 *
 * The live keys are copied into one staging table by the server, in a
 * single `INSERT ... SELECT`, before the first page is requested; the
 * remote pages are written into another as they arrive; then three queries
 * produce the answer: a count for `matched`, an anti-join from the local
 * side for `localOnly`, an anti-join from the remote side for `remoteOnly`.
 * Node holds one page of remote rows at a time, plus the drift; the
 * Postgres destination explains why that bound matters.
 *
 * The staging tables are ordinary `MergeTree` tables with a random suffix,
 * not temporary ones: a temporary table lives in one HTTP session on one
 * replica, and on a replicated deployment the next request may land
 * elsewhere. They are dropped when the diff ends, however it ends.
 */
import { encodeKey, type IdDiff, type TableDef } from '@workos/sync-engine'
import { randomBytes } from 'node:crypto'
import type { ChClient } from './client.js'
import { chunk, keyExpression, keyValues, tableRef, KEY_SEPARATOR, type WriteOptions } from './upsert.js'
import { quoteIdent, ENV } from '../schema/ddl.js'

export interface ChDiff extends IdDiff {
  /** Remote rows with no value at the key path. Counted rather than thrown. */
  skipped: number
}

const DEFAULT_BATCH_SIZE = 500

export async function diffIdsInClickHouse(
  db: ChClient,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  remote: AsyncIterable<Record<string, unknown>[]>,
): Promise<ChDiff> {
  const token = randomBytes(6).toString('hex')
  const s = quoteIdent(opts.schema)
  const LOCAL = `${s}.\`_reconcile_local_${token}\``
  const STAGING = `${s}.\`_reconcile_remote_${token}\``
  const decode = (k: string) => encodeKey(stream.key.length === 1 ? [k] : k.split(KEY_SEPARATOR))

  const drop = async () => {
    await db.command(`DROP TABLE IF EXISTS ${LOCAL}`)
    await db.command(`DROP TABLE IF EXISTS ${STAGING}`)
  }
  try {
    await db.command(`CREATE TABLE ${LOCAL} (k String) ENGINE = MergeTree ORDER BY k`)
    await db.command(`CREATE TABLE ${STAGING} (k String, seq UInt64, raw String) ENGINE = MergeTree ORDER BY (k, seq)`)

    // The live keys are frozen before the first page is requested: a row the
    // tail inserts after the listing has passed that object's position is
    // simply not a candidate, so `--repair` can never tombstone an object
    // that exists upstream. One statement, one snapshot.
    const live = stream.mode === 'append' ? '' : ` AND \`_deleted_at\` IS NULL`
    const final = stream.mode === 'append' ? '' : ' FINAL'
    await db.command(
      `INSERT INTO ${LOCAL} (k) SELECT ${keyExpression(stream)} FROM ${tableRef(opts.schema, stream)}${final}
       WHERE ${quoteIdent(ENV)} = {env:String}${live}`,
      { env },
    )

    let skipped = 0
    let seq = 0
    for await (const page of remote) {
      const keyed: { k: string; seq: number; raw: string }[] = []
      for (const data of page) {
        const id = keyValues(stream, data)
        if (!id) {
          skipped++
          continue
        }
        keyed.push({ k: id.join(KEY_SEPARATOR), seq: seq++, raw: JSON.stringify(data) })
      }
      for (const batch of chunk(keyed, opts.batchSize ?? DEFAULT_BATCH_SIZE)) await db.insert(STAGING, batch)
    }

    // An id the API returns twice (a page boundary moving under a concurrent
    // write) keeps its first copy, by `seq`. Nothing is lost that the next
    // reconcile would not find again.
    const [matched, localOnly, remoteOnly] = await Promise.all([
      db.query<{ n: string }>(
        `SELECT toString(count(DISTINCT k)) AS n FROM ${STAGING} WHERE k IN (SELECT k FROM ${LOCAL})`,
      ),
      db.query<{ k: string }>(`SELECT k FROM ${LOCAL} WHERE k NOT IN (SELECT k FROM ${STAGING}) ORDER BY k`),
      db.query<{ raw: string }>(
        `SELECT argMin(raw, seq) AS raw FROM ${STAGING} WHERE k NOT IN (SELECT k FROM ${LOCAL}) GROUP BY k ORDER BY k`,
      ),
    ])
    return {
      matched: Number(matched[0]?.n ?? 0),
      localOnly: localOnly.map((r) => decode(r.k)),
      remoteOnly: remoteOnly.map((r) => JSON.parse(r.raw) as Record<string, unknown>),
      skipped,
    }
  } finally {
    await drop()
  }
}
