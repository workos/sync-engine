/**
 * The single-writer lease, as a row.
 *
 * A lease is a row in `_sync_lease` keyed by `(sync_id, env)`, with a holder
 * and an expiry. Taking it is one `INSERT ... ON DUPLICATE KEY UPDATE` whose
 * assignments only move the row when it is ours or has expired; the primary
 * key makes two concurrent attempts serialise, so exactly one sees itself as
 * the holder afterwards.
 *
 * MySQL has no `WHERE` on that update, so the condition lives in each
 * assignment, and their order matters: MySQL evaluates them left to right
 * and a later one sees the earlier one's new value. `acquired_at` is decided
 * first, while `holder` and `expires_at` are still the stored ones; then
 * `holder`, from the stored expiry; then `expires_at`, from the holder just
 * decided. Reordered, a takeover would refresh the expiry and then find the
 * lease unexpired, and keep the old holder.
 *
 * Why a row and not `GET_LOCK`: a named lock lives on one connection, which
 * means pinning a pool connection for the life of the process and losing
 * the lock silently if that connection is recycled. A row with an expiry
 * survives pool churn, is visible in `status`, and times out on its own
 * when the holder dies. Expiry is judged by the database clock, never the
 * client's, so two hosts with skewed clocks still agree on who holds it.
 */
import type { Lease, LeaseResult } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { quoteIdent } from '../schema/ddl.js'

/**
 * ISO-8601 in UTC with millisecond precision, built in SQL so the result is
 * the same whether the driver hands back a `Date` or text. The session is in
 * UTC (see `db/client.ts`), so no conversion is involved.
 */
export const iso = (expr: string) =>
  `concat(date_format(${expr}, '%Y-%m-%dT%H:%i:%s.'), lpad(floor(microsecond(${expr}) / 1000), 3, '0'), 'Z')`

interface LeaseRow {
  holder: string
  expires_at: string
}

export async function acquireLease(
  db: SqlClient,
  schema: string,
  syncId: string,
  env: string,
  holder: string,
  ttlMs: number,
): Promise<LeaseResult> {
  const table = `${quoteIdent(schema)}.\`_sync_lease\``
  // The target table cannot be aliased, so the stored row is referred to by
  // its unqualified name next to the `new` row alias; a bare column name
  // would be ambiguous between the two.
  await db.query(
    `insert into ${table} (sync_id, env, holder, acquired_at, expires_at)
     values (?, ?, ?, now(6), timestampadd(microsecond, ?, now(6))) as new
     on duplicate key update
       acquired_at = if(\`_sync_lease\`.holder = new.holder, \`_sync_lease\`.acquired_at, if(\`_sync_lease\`.expires_at <= now(6), new.acquired_at, \`_sync_lease\`.acquired_at)),
       holder = if(\`_sync_lease\`.holder = new.holder or \`_sync_lease\`.expires_at <= now(6), new.holder, \`_sync_lease\`.holder),
       expires_at = if(\`_sync_lease\`.holder = new.holder, new.expires_at, \`_sync_lease\`.expires_at)`,
    [syncId, env, holder, Math.max(0, Math.floor(ttlMs)) * 1000],
  )
  // Whoever the row names now is the holder: us if the lease was free,
  // expired or already ours, somebody else otherwise. A refused attempt reads
  // the same row so the caller can say who to wait for.
  const r = await db.query<LeaseRow>(
    `select holder, ${iso('expires_at')} as expires_at from ${table} where sync_id = ? and env = ?`,
    [syncId, env],
  )
  const row = r.rows[0]
  if (row && row.holder === holder) return { acquired: true, holder: row.holder, expiresAt: row.expires_at }
  const current = await getLease(db, schema, syncId, env)
  return { acquired: false, holder: current?.holder ?? null, expiresAt: current?.expiresAt ?? null }
}

export async function releaseLease(
  db: SqlClient,
  schema: string,
  syncId: string,
  env: string,
  holder: string,
): Promise<void> {
  // Deleting rather than expiring keeps `status` honest: a released lease
  // reads as free, not as a stale row with a past date.
  await db.query(`delete from ${quoteIdent(schema)}.\`_sync_lease\` where sync_id = ? and env = ? and holder = ?`, [
    syncId,
    env,
    holder,
  ])
}

/**
 * Write provisionally before locking the lease: even an invisible competing
 * insert must finish before this writer can block takeover. Validate
 * ownership and expiry after all waits, rolling back a refused write to its
 * savepoint. The savepoint also protects callers already inside a
 * transaction.
 *
 * `state = cast(? as json)` compares JSON values, which MySQL does without
 * regard to key order, as Postgres compares jsonb.
 */
export async function setStateIfHeld(
  db: SqlClient,
  schema: string,
  syncId: string,
  env: string,
  stream: string,
  state: unknown,
  holder: string,
  expected: unknown,
): Promise<boolean> {
  const s = quoteIdent(schema)
  return db.transaction(async (tx) => {
    await tx.exec('savepoint workos_sync_checkpoint')
    const written =
      expected == null
        ? // `INSERT IGNORE` inserts nothing, and counts nothing, when the row exists.
          await tx.query(
            `insert ignore into ${s}.\`_sync_state\` (sync_id, env, stream, state, updated_at)
             values (?, ?, ?, cast(? as json), now(6))`,
            [syncId, env, stream, JSON.stringify(state)],
          )
        : await tx.query(
            `update ${s}.\`_sync_state\` set state = cast(? as json), updated_at = now(6)
             where sync_id = ? and env = ? and stream = ? and state = cast(? as json)`,
            [JSON.stringify(state), syncId, env, stream, JSON.stringify(expected)],
          )
    let held = false
    if (written.affectedRows > 0) {
      const lease = await tx.query(
        `select 1 as held from ${s}.\`_sync_lease\`
         where sync_id = ? and env = ? and holder = ? and expires_at > now(6)
         for update`,
        [syncId, env, holder],
      )
      held = lease.rows.length > 0
    }
    if (!held) await tx.exec('rollback to savepoint workos_sync_checkpoint')
    await tx.exec('release savepoint workos_sync_checkpoint')
    return held
  })
}

export async function getLease(db: SqlClient, schema: string, syncId: string, env: string): Promise<Lease | null> {
  const r = await db.query<LeaseRow>(
    `select holder, ${iso('expires_at')} as expires_at
     from ${quoteIdent(schema)}.\`_sync_lease\`
     where sync_id = ? and env = ? and expires_at > now(6)`,
    [syncId, env],
  )
  const row = r.rows[0]
  return row ? { holder: row.holder, expiresAt: row.expires_at } : null
}
