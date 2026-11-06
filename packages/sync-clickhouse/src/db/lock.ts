/**
 * The per-stream write lock.
 *
 * Every mirror write is read-decide-write: a batch reads the current state
 * of its rows, judges each in Node — the staleness gate, the shallow merge,
 * which tombstone to carry — and inserts the next versions. Postgres and
 * MySQL serialise that with row locks inside a transaction. ClickHouse has
 * neither, and two writers can meet on one key: the tail and a backfill run
 * side by side, and a poll can finish its writes after its lease has passed
 * to another process. Each would judge against the state before the other's
 * insert landed: a merge could drop the fields the other wrote, a tombstone
 * could lose a version tie to a live row it never saw, an append-only event
 * could be inserted twice.
 *
 * So each batch holds a lock for its table and environment while it reads
 * and writes. The lock is a KeeperMap row: a strict-mode `INSERT` takes it
 * (refused when the key is there), a `DELETE` of the holder's own row
 * releases it, and a row a crashed process left behind is taken over, once
 * it has expired on the server's clock, with the conditional `UPDATE` the
 * lease uses — so a live holder is never displaced. Every acquisition has
 * its own token, so two batches in one process contend like two processes
 * do. A writer that cannot get the lock within `waitMs` fails with
 * `LOCK_TIMEOUT`, and a batch that stalled past the lock's TTL, lost it,
 * and asks `held()` before writing fails with `LOCK_LOST`; the error
 * classifier reports both as transient, so the engine retries the poll
 * rather than quarantining the event, and the retry reads afresh.
 */
import { randomUUID } from 'node:crypto'
import { KEEPER_SETTINGS, type ChClient } from './client.js'
import { isKeeperConflict } from './errors.js'
import { quoteIdent } from '../schema/ddl.js'

/** How long a lock outlives the process that took it. A batch holds it for well under a second. */
export const LOCK_TTL_MS = 30_000
/** How long a writer waits for a lock before reporting a transient failure. */
export const LOCK_WAIT_MS = 60_000

const SEP = '\u0001'
/** Strict mode named outright: a client built without it would overwrite the row instead of being refused. */
const SETTINGS = { ...KEEPER_SETTINGS, keeper_map_strict_mode: 1 } as const

export interface LockOptions {
  schema: string
  /** Prefix of every token this writer takes; identifies the process in `_locks`. */
  holder: string
  ttlMs?: number
  waitMs?: number
}

const table = (schema: string) => `${quoteIdent(schema)}.\`_locks\``

/** The lock for one table in one environment. */
export const lockKey = (tableName: string, env: string) => `${tableName}${SEP}${env}`

const backoff = () => new Promise((r) => setTimeout(r, 10 + Math.random() * 40))

/** Take the lock, waiting for whoever holds it. Resolves to the token that releases it. */
export async function acquireStreamLock(db: ChClient, opts: LockOptions, key: string): Promise<string> {
  const token = `${opts.holder}:${randomUUID()}`
  const ttl = Math.max(1, Math.floor(opts.ttlMs ?? LOCK_TTL_MS))
  const waitMs = opts.waitMs ?? LOCK_WAIT_MS
  const deadline = Date.now() + waitMs
  const params = { k: key, holder: token, ttl }
  for (let attempt = 0; ; attempt++) {
    if (attempt > 0) {
      if (Date.now() >= deadline) {
        throw Object.assign(
          new Error(`could not take the write lock for ${key.replace(SEP, ' in ')} within ${waitMs} ms`),
          { code: 'LOCK_TIMEOUT' },
        )
      }
      await backoff()
    }
    try {
      await db.command(
        `INSERT INTO ${table(opts.schema)} (k, holder, expires_at)
         VALUES ({k:String}, {holder:String}, now64(3, 'UTC') + toIntervalMillisecond({ttl:UInt64}))`,
        params,
        SETTINGS,
      )
      return token
    } catch (err) {
      if (!isKeeperConflict(err)) throw err
    }
    // Taken. Ours only if whoever holds it is gone: the takeover names an
    // expired row, and a row that is not expired is left exactly as it was.
    try {
      await db.command(
        `ALTER TABLE ${table(opts.schema)} UPDATE
           holder = {holder:String},
           expires_at = now64(3, 'UTC') + toIntervalMillisecond({ttl:UInt64})
         WHERE k = {k:String} AND expires_at <= now64(3, 'UTC')`,
        params,
        SETTINGS,
      )
    } catch (err) {
      if (!isKeeperConflict(err)) throw err
      continue
    }
    const rows = await db.query<{ holder: string }>(`SELECT holder FROM ${table(opts.schema)} WHERE k = {k:String}`, {
      k: key,
    })
    if (rows[0]?.holder === token) return token
  }
}

/** Give the lock back. A lock that was taken over meanwhile is not ours to release and is left alone. */
export async function releaseStreamLock(db: ChClient, opts: LockOptions, key: string, token: string): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt++) {
    try {
      await db.command(
        `ALTER TABLE ${table(opts.schema)} DELETE WHERE k = {k:String} AND holder = {holder:String}`,
        { k: key, holder: token },
        SETTINGS,
      )
      return
    } catch (err) {
      if (!isKeeperConflict(err)) throw err
    }
    const rows = await db.query<{ holder: string }>(`SELECT holder FROM ${table(opts.schema)} WHERE k = {k:String}`, {
      k: key,
    })
    if (rows[0]?.holder !== token) return
    await backoff()
  }
  // Still ours after every attempt: it expires on its own, and says so in the log the caller keeps.
}

/** Throws `LOCK_LOST` unless the lock is still the caller's. */
export type HeldCheck = () => Promise<void>

/**
 * Run `fn` holding the lock for `key`, releasing it however `fn` ends. `fn`
 * is handed a check to call right before it writes: a batch that stalled
 * past the TTL has lost the lock to a writer whose rows it never read.
 */
export async function withStreamLock<T>(
  db: ChClient,
  opts: LockOptions,
  key: string,
  fn: (held: HeldCheck) => Promise<T>,
): Promise<T> {
  const token = await acquireStreamLock(db, opts, key)
  const held: HeldCheck = async () => {
    const rows = await db.query<{ holder: string }>(`SELECT holder FROM ${table(opts.schema)} WHERE k = {k:String}`, {
      k: key,
    })
    if (rows[0]?.holder !== token) {
      throw Object.assign(
        new Error(`the write lock for ${key.replace(SEP, ' in ')} was taken over: this batch stalled past its TTL`),
        { code: 'LOCK_LOST' },
      )
    }
  }
  try {
    return await fn(held)
  } finally {
    await releaseStreamLock(db, opts, key, token)
  }
}
