/**
 * The reconcile set difference, computed in MySQL.
 *
 * The live ids are copied into one temporary table before the first page
 * is requested, the remote pages are written into another as they arrive,
 * then three queries produce the answer: a join for `matched`, an anti-join
 * from the local table for `localOnly`, an anti-join from the remote table
 * for `remoteOnly`. Node holds one page of remote rows at a time, plus the
 * drift; the Postgres destination explains why that bound matters.
 *
 * Temporary tables are per session, and a pool hands each plain `query` to
 * whichever connection is free, so the whole thing runs inside
 * `transaction`, which pins one connection. Key components are staged as
 * separate columns rather than one encoded string: MySQL has no array type,
 * and encoding in SQL would have to reproduce `encodeKey` byte for byte.
 */
import { encodeKey, type IdDiff, type TableDef, chunk, keyValues } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import type { WriteOptions } from './upsert.js'
import { quoteIdent, safeIdentifier, KEY_LENGTH } from '../schema/ddl.js'

/** Live keys read per statement while capturing the local side. */
const CAPTURE_PAGE = 5000

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
  return db.transaction(async (tx) => {
    const s = quoteIdent(opts.schema)
    // Qualified with the schema, so the session needs no default database.
    const LOCAL = `${s}.\`_reconcile_local\``
    const STAGING = `${s}.\`_reconcile_remote\``
    const table = `${s}.${quoteIdent(safeIdentifier(stream.name))}`
    const kcols = stream.key.map((_, i) => `k${i}`)
    const keyList = kcols.join(', ')
    const keyDefs = kcols.map((k) => `${k} varchar(${KEY_LENGTH}) not null`).join(', ')
    const sourceKeys = stream.key.map((name) => `l.${quoteIdent(safeIdentifier(name))}`).join(', ')
    const joinOn = (a: string, b: string) => kcols.map((k) => `${b}.${k} = ${a}.${k}`).join(' and ')
    const orderBy = (alias: string) => kcols.map((k) => `${alias}.${k}`).join(', ')
    // Append-only tables have no tombstone column; every row is live.
    const live = stream.mode === 'append' ? `l.\`_env\` = ?` : `l.\`_env\` = ? and l.\`_deleted_at\` is null`

    const drop = async () => {
      await tx.exec(`drop temporary table if exists ${LOCAL}`)
      await tx.exec(`drop temporary table if exists ${STAGING}`)
    }
    // A call nested inside an outer transaction, or one that failed before
    // its cleanup, would find the previous tables still there. Dropping
    // first makes the function re-entrant.
    await drop()
    try {
      const options = 'character set utf8mb4 collate utf8mb4_bin'
      await tx.exec(`create temporary table ${LOCAL} (${keyDefs}, primary key (${keyList})) ${options}`)
      await tx.exec(
        `create temporary table ${STAGING} (${keyDefs}, raw json not null, primary key (${keyList})) ${options}`,
      )

      // The live ids are frozen before the first page is requested: a row the
      // tail inserts after the listing has passed that object's position is
      // simply not a candidate, so `--repair` can never tombstone an object
      // that exists upstream. They are read with plain `SELECT`s, a page at
      // a time by key, rather than one `INSERT ... SELECT`: under REPEATABLE
      // READ that statement takes shared next-key locks on every source row,
      // and the tail would block behind the listing for as long as it runs.
      // A plain consistent read takes no locks, and every page reads the
      // transaction's first snapshot, so the set is still one instant's.
      let after: string[] | null = null
      for (;;) {
        const params: unknown[] = [env]
        let bound = ''
        if (after) {
          params.push(...after)
          bound =
            kcols.length === 1
              ? ` and ${sourceKeys} > ?`
              : ` and (${sourceKeys}) > (${kcols.map(() => '?').join(', ')})`
        }
        const page = await tx.query<Record<string, string>>(
          `select ${stream.key.map((name, i) => `l.${quoteIdent(safeIdentifier(name))} as k${i}`).join(', ')}
           from ${table} l where ${live}${bound}
           order by ${sourceKeys} limit ${CAPTURE_PAGE}`,
          params,
        )
        if (page.rows.length === 0) break
        const values: unknown[] = []
        const tuples = page.rows.map((r) => {
          values.push(...kcols.map((k) => r[k]))
          return `(${kcols.map(() => '?').join(', ')})`
        })
        await tx.query(`insert ignore into ${LOCAL} (${keyList}) values ${tuples.join(', ')}`, values)
        if (page.rows.length < CAPTURE_PAGE) break
        const last = page.rows[page.rows.length - 1]!
        after = kcols.map((k) => String(last[k]))
      }

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
            params.push(...r.id, JSON.stringify(r.data))
            return `(${kcols.map(() => '?').join(', ')}, ?)`
          })
          // An id the API returns twice (a page boundary moving under a
          // concurrent write) keeps its first copy. Nothing is lost that the
          // next reconcile would not find again.
          await tx.query(`insert ignore into ${STAGING} (${keyList}, raw) values ${tuples.join(', ')}`, params)
        }
      }

      const matched = await tx.query<{ n: number }>(
        `select count(*) as n from ${LOCAL} l join ${STAGING} r on ${joinOn('l', 'r')}`,
      )
      const localOnly = await tx.query<Record<string, string>>(
        `select ${orderBy('l')} from ${LOCAL} l
         left join ${STAGING} r on ${joinOn('l', 'r')}
         where r.k0 is null
         order by ${orderBy('l')}`,
      )
      const remoteOnly = await tx.query<{ raw: Record<string, unknown> }>(
        `select r.raw from ${STAGING} r
         left join ${LOCAL} l on ${joinOn('r', 'l')}
         where l.k0 is null
         order by ${orderBy('r')}`,
      )

      return {
        matched: Number(matched.rows[0]?.n ?? 0),
        localOnly: localOnly.rows.map((r) => encodeKey(kcols.map((k) => String(r[k])))),
        remoteOnly: remoteOnly.rows.map((r) => r.raw),
        skipped,
      }
    } finally {
      await drop()
    }
  })
}
