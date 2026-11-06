/**
 * The single-writer lease and the cursors, as one KeeperMap row.
 *
 * KeeperMap is a key-value table held in ClickHouse Keeper: linearizable
 * writes, and under `keeper_map_strict_mode` an `INSERT` of an existing key
 * is refused and an `ALTER ... UPDATE` only lands if the row did not change
 * between its read and its write. That is a compare-and-swap, and it is the
 * only one ClickHouse offers; a MergeTree row has no such thing. So the
 * lease and every cursor for an environment share one row in `_sync`, keyed
 * by `(sync_id, env)`:
 *
 *     k = sync_id ‹SEP› env
 *     holder, acquired_at, expires_at          the lease; holder '' when free
 *     states = {"__tail__": "<canonical json>", "users": "...", ...}
 *
 * Taking the lease is an `UPDATE` whose `WHERE` names the row as it must be
 * for the takeover to be allowed — ours, free, or expired by the server's
 * clock (`now64`, never the client's) — so either it lands or nothing
 * happens. A checkpoint is an `UPDATE` whose `WHERE` names the holder, the
 * unexpired lease and the whole `states` value as it was read: if anyone
 * moved a cursor or took the lease in between, the value differs and the
 * write is refused by the database. Each write is followed by a read, since
 * a mutation reports no row count; strict mode's own race verdict ("Bad
 * version") is retried, because the re-read then judges the new state.
 *
 * Cursor state is stored as canonical JSON — keys sorted, recursively — so
 * that equal cursors compare equal as text, as they do as jsonb.
 */
import type { Lease, LeaseResult } from '@workos/sync-engine'
import { canonicalJson } from '@workos/sync-engine'
import { isoOf, KEEPER_SETTINGS, type ChClient } from './client.js'
import { isKeeperConflict } from './errors.js'
import { quoteIdent } from '../schema/ddl.js'

const SEP = '\u0001'
/**
 * How often a read-then-swap is retried when the row changed in between.
 * Contention is rare in use — one tail per environment, one backfill at a
 * time — but each retry is cheap, and a lost race must never be reported as
 * a failure to a caller who did nothing wrong.
 */
const ATTEMPTS = 16

/** A short, jittered pause between attempts, so contenders do not retry in lockstep. */
const backoff = () => new Promise((r) => setTimeout(r, 5 + Math.random() * 20))

export interface SyncRow {
  holder: string
  acquired_at: string
  expires_at: string
  states: string
  /** The server's clock at the moment of the read, for judging expiry. */
  now: string
}

export const syncKey = (syncId: string, env: string) => `${syncId}${SEP}${env}`

const table = (schema: string) => `${quoteIdent(schema)}.\`_sync\``

export async function readSync(db: ChClient, schema: string, syncId: string, env: string): Promise<SyncRow | null> {
  const rows = await db.query<SyncRow>(
    `SELECT holder, acquired_at, expires_at, states, now64(3, 'UTC') AS now FROM ${table(schema)} WHERE k = {k:String}`,
    { k: syncKey(syncId, env) },
  )
  return rows[0] ?? null
}

/** Insert the row for `(syncId, env)` if it does not exist. False when it already did. */
async function insertSync(db: ChClient, schema: string, syncId: string, env: string, states: string): Promise<boolean> {
  try {
    await db.command(
      `INSERT INTO ${table(schema)} (k, holder, acquired_at, expires_at, states)
       VALUES ({k:String}, '', toDateTime64(0, 3, 'UTC'), toDateTime64(0, 3, 'UTC'), {states:String})`,
      { k: syncKey(syncId, env), states },
      KEEPER_SETTINGS,
    )
    return true
  } catch (err) {
    if (isKeeperConflict(err)) return false
    throw err
  }
}

const parseStates = (row: SyncRow | null): Record<string, string> =>
  row ? (JSON.parse(row.states) as Record<string, string>) : {}

const isHeld = (row: SyncRow | null, holder: string) =>
  row !== null && row.holder === holder && row.expires_at > row.now

export async function acquireLease(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  holder: string,
  ttlMs: number,
): Promise<LeaseResult> {
  const k = syncKey(syncId, env)
  const ttl = Math.max(0, Math.floor(ttlMs))
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    const row = await readSync(db, schema, syncId, env)
    if (!row) {
      await insertSync(db, schema, syncId, env, '{}')
      continue
    }
    if (attempt > 0) await backoff()
    try {
      // Free when it is ours, unheld, or expired by the server's clock;
      // anything else leaves the row exactly as it was.
      await db.command(
        `ALTER TABLE ${table(schema)} UPDATE
           acquired_at = if(holder = {holder:String}, acquired_at, now64(3, 'UTC')),
           expires_at = now64(3, 'UTC') + toIntervalMillisecond({ttl:UInt64}),
           holder = {holder:String}
         WHERE k = {k:String} AND (holder = {holder:String} OR holder = '' OR expires_at <= now64(3, 'UTC'))`,
        { k, holder, ttl },
        KEEPER_SETTINGS,
      )
    } catch (err) {
      // Somebody else wrote the row between the read and the write: look again.
      if (!isKeeperConflict(err)) throw err
      continue
    }
    const after = await readSync(db, schema, syncId, env)
    if (after && after.holder === holder) return { acquired: true, holder, expiresAt: isoOf(after.expires_at) }
    const current = after && after.holder !== '' && after.expires_at > after.now ? after : null
    return { acquired: false, holder: current?.holder ?? null, expiresAt: current ? isoOf(current.expires_at) : null }
  }
  throw new Error(`could not take the lease for ${env}: the row kept changing under ${ATTEMPTS} attempts`)
}

export async function releaseLease(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  holder: string,
): Promise<void> {
  // Clearing the holder rather than expiring it keeps `status` honest: a
  // released lease reads as free, not as a stale holder with a past date.
  // The cursors share the row, so the row itself stays.
  try {
    await db.command(
      `ALTER TABLE ${table(schema)} UPDATE holder = '', expires_at = toDateTime64(0, 3, 'UTC')
       WHERE k = {k:String} AND holder = {holder:String}`,
      { k: syncKey(syncId, env), holder },
      KEEPER_SETTINGS,
    )
  } catch (err) {
    // A race with a takeover: the lease is no longer ours to release.
    if (!isKeeperConflict(err)) throw err
  }
}

export async function getLease(db: ChClient, schema: string, syncId: string, env: string): Promise<Lease | null> {
  const row = await readSync(db, schema, syncId, env)
  if (!row || row.holder === '' || row.expires_at <= row.now) return null
  return { holder: row.holder, expiresAt: isoOf(row.expires_at) }
}

export async function getState<T>(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  stream: string,
): Promise<T | null> {
  const stored = parseStates(await readSync(db, schema, syncId, env))[stream]
  return stored === undefined ? null : (JSON.parse(stored) as T)
}

/**
 * Replace `states` with `next` only if it still reads `previous`. `swapped`
 * when it did; `unchanged` when the `WHERE` matched nothing; `conflict`
 * when Keeper refused the write because the row's version moved between the
 * mutation's read and its write — which any write to the row does, a lease
 * renewal included, whether or not it touched `states`.
 */
async function swapStates(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  previous: string,
  next: string,
  guard = '',
  params: Record<string, unknown> = {},
): Promise<'swapped' | 'unchanged' | 'conflict'> {
  try {
    await db.command(
      `ALTER TABLE ${table(schema)} UPDATE states = {next:String}
       WHERE k = {k:String} AND states = {previous:String}${guard}`,
      { ...params, k: syncKey(syncId, env), previous, next },
      KEEPER_SETTINGS,
    )
  } catch (err) {
    if (!isKeeperConflict(err)) throw err
    return 'conflict'
  }
  const after = await readSync(db, schema, syncId, env)
  return after !== null && after.states === next ? 'swapped' : 'unchanged'
}

export async function setState(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  stream: string,
  state: unknown,
): Promise<void> {
  const value = canonicalJson(state)
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    const row = await readSync(db, schema, syncId, env)
    if (!row) {
      if (await insertSync(db, schema, syncId, env, JSON.stringify({ [stream]: value }))) return
      continue
    }
    const states = parseStates(row)
    if (states[stream] === value) return
    const next = JSON.stringify({ ...states, [stream]: value })
    if ((await swapStates(db, schema, syncId, env, row.states, next)) === 'swapped') return
    await backoff()
  }
  throw new Error(`could not save the ${stream} state for ${env}: the row kept changing under ${ATTEMPTS} attempts`)
}

/**
 * One read, one conditional write: the holder, the unexpired lease and the
 * cursor as it was expected to be are all judged on the row as read, and
 * the write names that exact row, so it lands only if nothing changed.
 *
 * A write Keeper refused over the row's version, or a `WHERE` that matched
 * nothing, is not yet a refusal: the holder's own renewal moves the version
 * without touching a cursor, and a backfill checkpoint for another stream
 * changes `states` without touching this one. So the row is read again and
 * judged again, and only a lease that is no longer held or a cursor that
 * is no longer where it was expected refuses the checkpoint. The engine
 * treats a refusal as "someone else moved the cursor" and replays the
 * poll, which must not happen over a race nobody lost.
 */
export async function setStateIfHeld(
  db: ChClient,
  schema: string,
  syncId: string,
  env: string,
  stream: string,
  state: unknown,
  holder: string,
  expected: unknown,
): Promise<boolean> {
  const value = canonicalJson(state)
  const wanted = expected == null ? undefined : canonicalJson(expected)
  for (let attempt = 0; attempt < ATTEMPTS; attempt++) {
    if (attempt > 0) await backoff()
    const row = await readSync(db, schema, syncId, env)
    if (!isHeld(row, holder)) return false
    const states = parseStates(row)
    const stored = states[stream]
    // A write of ours on an earlier attempt may have landed while its
    // verdict was lost to a race; the row then already says what we meant.
    if (stored !== wanted) return attempt > 0 && stored === value
    const next = JSON.stringify({ ...states, [stream]: value })
    // Expiry is judged again by the server, in the write itself: the read
    // above may have waited, and a lease that ran out meanwhile must refuse.
    const outcome = await swapStates(
      db,
      schema,
      syncId,
      env,
      row!.states,
      next,
      ` AND holder = {holder:String} AND expires_at > now64(3, 'UTC')`,
      { holder },
    )
    if (outcome === 'swapped') return true
  }
  throw new Error(`could not checkpoint ${stream} for ${env}: the row kept changing under ${ATTEMPTS} attempts`)
}
