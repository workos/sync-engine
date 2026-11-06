/**
 * The reconcile set difference, computed in MongoDB.
 *
 * The engine's `diffStream` once held every live local id in a `Set` and every
 * remote payload in a `Map`, so the memory a reconcile needed grew with the
 * size of the stream rather than the size of the drift: a directory with a
 * million users turned a weekly health check into an out-of-memory. Fivetran
 * and Airbyte stage the remote side into a table and diff in the database,
 * and so does this.
 *
 * The live keys are copied into one staging collection by the server
 * (`$out`) before the first page is requested, the remote pages are written
 * into another as they arrive, then three aggregations produce the answer: a
 * `$lookup` for `matched`, an anti-join from the local side for `localOnly`,
 * an anti-join from the remote side for `remoteOnly`. Node holds one page of
 * remote rows at a time, plus the drift.
 */
import { encodeKey, type IdDiff, type TableDef, chunk, keyValues } from '@workos/sync-engine'
import { randomBytes } from 'node:crypto'
import { MongoBulkWriteError, type Collection, type Db, type Document } from 'mongodb'
import { fieldPath, keyPath, type Names } from '../schema/names.js'
import type { MirrorDoc, WriteOptions } from './upsert.js'

export interface MongoDiff extends IdDiff {
  /** Remote rows with no value at the key path. Counted rather than thrown. */
  skipped: number
}

interface StagedKey extends Document {
  /** The whole key as one string; see `SEPARATOR`. */
  k: string
}

interface StagedRemote extends StagedKey {
  raw: Record<string, unknown>
}

/**
 * Composite keys are joined with a control character that no id or slug
 * contains, so that `$lookup` compares one string against one string. The
 * encoding the engine sees is still `encodeKey`'s: the join is undone before
 * a key leaves this module.
 */
const SEPARATOR = '\u0001'

const DUPLICATE_KEY = 11000
const NAMESPACE_NOT_FOUND = 26
const DEFAULT_BATCH_SIZE = 500

/** The key of a live row as one string, built by the server. */
function keyExpression(stream: TableDef): Document {
  const parts = stream.key.map((k) => ({ $toString: `$${fieldPath(keyPath(stream, k))}` }))
  if (parts.length === 1) return parts[0]!
  const concat: (Document | string)[] = []
  for (const [i, part] of parts.entries()) {
    if (i > 0) concat.push(SEPARATOR)
    concat.push(part)
  }
  return { $concat: concat }
}

const decode = (stream: TableDef, k: string) => encodeKey(stream.key.length === 1 ? [k] : k.split(SEPARATOR))

export async function diffIdsInMongo(
  db: Db,
  names: Names,
  opts: WriteOptions,
  stream: TableDef,
  env: string,
  remote: AsyncIterable<Record<string, unknown>[]>,
): Promise<MongoDiff> {
  const token = randomBytes(6).toString('hex')
  const localName = names.staging('local', token)
  const remoteName = names.staging('remote', token)
  const local = db.collection<StagedKey>(localName)
  const staged = db.collection<StagedRemote>(remoteName)
  const mirror = db.collection<MirrorDoc>(names.mirror(stream))

  try {
    // Append-only collections have no tombstone field; every row is live.
    const live: Document = stream.mode === 'append' ? { _env: env } : { _env: env, _deleted_at: null }

    // The live keys are frozen before the first page is requested, not read
    // once the pages are in. Listing a large stream takes minutes under the
    // API's rate limits and the tail keeps writing throughout: a row it
    // inserts after the listing has passed that object's position would be
    // live in the mirror and absent from the pages, and `--repair` would
    // tombstone an object that exists upstream. Deletion is terminal, so no
    // later event would undo that. Captured here, such a row is simply not a
    // candidate, which is how the in-memory diff behaved when it read
    // `liveIds` first.
    await mirror
      .aggregate(
        [
          { $match: live },
          { $project: { _id: 0, k: keyExpression(stream) } },
          // A row with no complete key cannot be reported, so it cannot be a candidate.
          { $match: { k: { $ne: null } } },
          { $out: localName },
        ],
        { allowDiskUse: true },
      )
      .toArray()
    await local.createIndex({ k: 1 }, { unique: true })
    await staged.createIndex({ k: 1 }, { unique: true })

    let skipped = 0
    for await (const page of remote) {
      const keyed: StagedRemote[] = []
      for (const data of page) {
        const id = keyValues(stream, data)
        if (!id) {
          skipped++
          continue
        }
        keyed.push({ k: id.join(SEPARATOR), raw: data })
      }
      for (const batch of chunk(keyed, opts.batchSize ?? DEFAULT_BATCH_SIZE)) {
        try {
          // An id the API returns twice (a page boundary moving under a
          // concurrent write) keeps its first copy. Nothing is lost that the
          // next reconcile would not find again.
          await staged.insertMany(batch, { ordered: false })
        } catch (err) {
          if (!(err instanceof MongoBulkWriteError)) throw err
          const errors = Array.isArray(err.writeErrors) ? err.writeErrors : [err.writeErrors]
          if (!errors.every((e) => e.code === DUPLICATE_KEY)) throw err
        }
      }
    }

    const lookup = (from: string) => ({ $lookup: { from, localField: 'k', foreignField: 'k', as: 'm' } })
    const matched = await local
      .aggregate<{ n: number }>([lookup(remoteName), { $match: { 'm.0': { $exists: true } } }, { $count: 'n' }])
      .toArray()
    const localOnly = await local
      .aggregate<StagedKey>(
        [lookup(remoteName), { $match: { m: { $size: 0 } } }, { $sort: { k: 1 } }, { $project: { _id: 0, k: 1 } }],
        { allowDiskUse: true },
      )
      .toArray()
    const remoteOnly = await staged
      .aggregate<StagedRemote>(
        [lookup(localName), { $match: { m: { $size: 0 } } }, { $sort: { k: 1 } }, { $project: { _id: 0, raw: 1 } }],
        { allowDiskUse: true },
      )
      .toArray()

    return {
      matched: matched[0]?.n ?? 0,
      localOnly: localOnly.map((r) => decode(stream, r.k)),
      remoteOnly: remoteOnly.map((r) => r.raw),
      skipped,
    }
  } finally {
    await dropQuietly(local)
    await dropQuietly(staged)
  }
}

async function dropQuietly<T extends Document>(coll: Collection<T>): Promise<void> {
  try {
    await coll.drop()
  } catch (err) {
    if ((err as { code?: unknown }).code !== NAMESPACE_NOT_FOUND) throw err
  }
}
