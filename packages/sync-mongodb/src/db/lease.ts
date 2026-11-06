/**
 * The single-writer lease and the cursors, as one document.
 *
 * In Postgres the lease is one row and each cursor another, and the tail's
 * checkpoint checks the two in a transaction. MongoDB only promises atomicity
 * for one document unless the deployment is a replica set, and a standalone
 * `mongod` — a developer's laptop, an embedded test server — is not. So the
 * lease and every cursor for an environment share a document in `_sync`,
 * keyed by `(sync_id, env)`:
 *
 *     { sync_id, env,
 *       lease: { holder, acquired_at, expires_at },
 *       state: { __tail__: { json, updated_at }, users: { json, updated_at }, ... } }
 *
 * That makes `setStateIfHeld` one conditional `updateOne` whose filter names
 * the holder, the unexpired lease and the expected cursor all at once: either
 * every condition holds and the write lands, or nothing happens. There is no
 * window between the check and the write, on any deployment.
 *
 * Taking the lease is one `findOneAndUpdate` with an update pipeline whose
 * condition only lets the lease change hands when it is ours, free or
 * expired. Expiry is judged by `$$NOW`, the server's clock, never the
 * client's, so two hosts with skewed clocks still agree on who holds it.
 *
 * Inside that pipeline a string that starts with `$` is a field path, so the
 * sync id, the environment and the holder — configured strings, any of which
 * could start with one — are wrapped in `$literal` wherever the pipeline
 * stores or compares them.
 *
 * Cursor state is stored as canonical JSON — keys sorted, recursively —
 * because MongoDB compares embedded documents field by field *in order*, and
 * the engine compares cursors by value: `{a, b}` and `{b, a}` are the same
 * cursor, as they are to Postgres's `jsonb`.
 */
import type { Lease, LeaseResult } from '@workos/sync-engine'
import { canonicalJson } from '@workos/sync-engine'
import type { Collection, Document } from 'mongodb'

export interface LeaseFields {
  holder: string
  acquired_at: Date
  expires_at: Date
}

export interface StateField {
  /** Canonical JSON; see `canonicalJson`. */
  json: string
  updated_at: Date
}

export interface SyncDoc extends Document {
  sync_id: string
  env: string
  lease?: LeaseFields | null
  state?: Record<string, StateField>
}

/** The field a stream's cursor lives under. Stream names are catalog-controlled, but a path separator in one would silently address the wrong thing. */
function stateField(stream: string): string {
  if (stream === '' || stream.includes('.') || stream.startsWith('$')) {
    throw new Error(`invalid state key ${JSON.stringify(stream)}: must not be empty, contain "." or start with "$"`)
  }
  return `state.${stream}`
}

const DUPLICATE_KEY = 11000

export async function acquireLease(
  coll: Collection<SyncDoc>,
  syncId: string,
  env: string,
  holder: string,
  ttlMs: number,
): Promise<LeaseResult> {
  const ttl = Math.max(0, Math.floor(ttlMs))
  // Free when there is no lease, when it is ours already, or when the holder
  // let it expire. Anything else leaves the lease exactly as it was.
  const free = {
    $or: [
      { $in: [{ $type: '$lease' }, ['missing', 'null']] },
      { $eq: ['$lease.holder', { $literal: holder }] },
      { $lte: ['$lease.expires_at', '$$NOW'] },
    ],
  }
  const update: Document[] = [
    {
      $set: {
        sync_id: { $literal: syncId },
        env: { $literal: env },
        lease: {
          $cond: [
            free,
            {
              holder: { $literal: holder },
              // A renewal keeps the original takeover time; a takeover starts a new one.
              acquired_at: {
                $cond: [{ $eq: ['$lease.holder', { $literal: holder }] }, '$lease.acquired_at', '$$NOW'],
              },
              expires_at: { $add: ['$$NOW', ttl] },
            },
            '$lease',
          ],
        },
      },
    },
  ]
  const attempt = () =>
    coll.findOneAndUpdate({ sync_id: syncId, env }, update, { upsert: true, returnDocument: 'after' })
  let doc: SyncDoc | null
  try {
    doc = await attempt()
  } catch (err) {
    // Two processes asking at once while no document exists: both try to
    // insert, the unique index lets one through, and the other sees the
    // document on its second try and is judged against it.
    if ((err as { code?: unknown }).code !== DUPLICATE_KEY) throw err
    doc = await attempt()
  }
  const lease = doc?.lease
  if (!lease) return { acquired: false, holder: null, expiresAt: null }
  return { acquired: lease.holder === holder, holder: lease.holder, expiresAt: lease.expires_at.toISOString() }
}

export async function releaseLease(
  coll: Collection<SyncDoc>,
  syncId: string,
  env: string,
  holder: string,
): Promise<void> {
  // Removing the lease rather than expiring it keeps `status` honest: a
  // released lease reads as free, not as a stale field with a past date.
  await coll.updateOne({ sync_id: syncId, env, 'lease.holder': holder }, { $unset: { lease: '' } })
}

export async function getLease(coll: Collection<SyncDoc>, syncId: string, env: string): Promise<Lease | null> {
  const doc = await coll.findOne(
    { sync_id: syncId, env, lease: { $ne: null }, $expr: { $gt: ['$lease.expires_at', '$$NOW'] } },
    { projection: { lease: 1 } },
  )
  const lease = doc?.lease
  return lease ? { holder: lease.holder, expiresAt: lease.expires_at.toISOString() } : null
}

export async function getState<T>(
  coll: Collection<SyncDoc>,
  syncId: string,
  env: string,
  stream: string,
): Promise<T | null> {
  const field = stateField(stream)
  const doc = await coll.findOne({ sync_id: syncId, env }, { projection: { [field]: 1 } })
  const stored = doc?.state?.[stream]
  return stored ? (JSON.parse(stored.json) as T) : null
}

export async function setState(
  coll: Collection<SyncDoc>,
  syncId: string,
  env: string,
  stream: string,
  state: unknown,
): Promise<void> {
  await coll.updateOne(
    { sync_id: syncId, env },
    { $set: { [stateField(stream)]: { json: canonicalJson(state), updated_at: new Date() } } },
    { upsert: true },
  )
}

/**
 * One document, one filter, one write: the holder, the lease's expiry by
 * the server's clock, and the cursor as it was expected to be are all in
 * the filter, so the write either matches and lands or matches nothing.
 */
export async function setStateIfHeld(
  coll: Collection<SyncDoc>,
  syncId: string,
  env: string,
  stream: string,
  state: unknown,
  holder: string,
  expected: unknown,
): Promise<boolean> {
  const field = stateField(stream)
  const r = await coll.updateOne(
    {
      sync_id: syncId,
      env,
      'lease.holder': holder,
      $expr: { $gt: ['$lease.expires_at', '$$NOW'] },
      [`${field}.json`]: expected == null ? { $exists: false } : canonicalJson(expected),
    },
    { $set: { [field]: { json: canonicalJson(state), updated_at: new Date() } } },
  )
  return r.matchedCount === 1
}
