/**
 * The single-writer lease, as a row.
 *
 * A lease is a row in `_sync_lease` keyed by `(sync_id, env)`, with a holder
 * and an expiry. Taking it is one `INSERT ... ON CONFLICT DO UPDATE` whose
 * `WHERE` only lets the update through when the row is ours or has expired;
 * the primary key makes two concurrent attempts serialise, so exactly one
 * sees its row come back.
 *
 * Why a row and not `pg_advisory_lock`: a session-level advisory lock lives
 * on one connection, which means pinning a pool connection for the life of
 * the process and losing the lock silently if that connection is recycled.
 * It also cannot be exercised for contention in PGlite, which has a single
 * session. A row with an expiry survives pool churn, is visible in `status`,
 * and times out on its own when the holder dies.
 *
 * Expiry is judged by the database clock, never the client's, so
 * two hosts with skewed clocks still agree on who holds it.
 */
import type { Lease, LeaseResult } from '@workos/sync-engine'
import type { SqlClient } from './client.js'
import { quoteIdent } from '../schema/ddl.js'

/**
 * ISO-8601 in UTC, built in SQL so the result is the same whether the driver
 * hands back a `Date` (pg, PGlite) or text.
 */
const iso = (col: string) => `to_char(${col} at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`

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
  const table = `${quoteIdent(schema)}."_sync_lease"`
  const r = await db.query<LeaseRow>(
    `insert into ${table} as t (sync_id, env, holder, acquired_at, expires_at)
     values ($1, $2, $3, now(), now() + ($4::bigint * interval '1 millisecond'))
     on conflict (sync_id, env) do update
       set holder = excluded.holder,
           expires_at = excluded.expires_at,
           acquired_at = case when t.holder = excluded.holder then t.acquired_at else excluded.acquired_at end
       where t.holder = excluded.holder or t.expires_at <= now()
     returning holder, ${iso('expires_at')} as expires_at`,
    [syncId, env, holder, Math.max(0, Math.floor(ttlMs))],
  )
  const row = r.rows[0]
  if (row) return { acquired: true, holder: row.holder, expiresAt: row.expires_at }
  // Refused. Read who has it so the caller can say so; it may already be
  // gone again by now, in which case the next attempt simply succeeds.
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
  await db.query(
    `delete from ${quoteIdent(schema)}."_sync_lease"
     where sync_id = $1 and env = $2 and holder = $3`,
    [syncId, env, holder],
  )
}

/**
 * Write provisionally before locking the lease: even an invisible competing
 * insert must finish before this writer can block takeover. Validate ownership
 * and expiry after all waits, rolling back a refused write to its savepoint.
 * The savepoint also protects callers already inside a transaction.
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
  const params = [syncId, env, stream, JSON.stringify(state)]
  const sql =
    expected == null
      ? `insert into ${s}."_sync_state" (sync_id, env, stream, state, updated_at)
       values ($1, $2, $3, $4::jsonb, now())
       on conflict (sync_id, env, stream) do nothing returning 1`
      : `update ${s}."_sync_state" set state = $4::jsonb, updated_at = now()
       where sync_id = $1 and env = $2 and stream = $3 and state = $5::jsonb
       returning 1`
  if (expected != null) params.push(JSON.stringify(expected))
  return db.transaction(async (tx) => {
    await tx.exec('savepoint workos_sync_checkpoint')
    const written = await tx.query(sql, params)
    let held = false
    if (written.rows.length > 0) {
      const lease = await tx.query(
        `with held_lease as materialized (
           select expires_at from ${s}."_sync_lease"
           where sync_id = $1 and env = $2 and holder = $3 for update
         )
         select 1 from held_lease where expires_at > clock_timestamp()`,
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
     from ${quoteIdent(schema)}."_sync_lease"
     where sync_id = $1 and env = $2 and expires_at > now()`,
    [syncId, env],
  )
  const row = r.rows[0]
  return row ? { holder: row.holder, expiresAt: row.expires_at } : null
}
