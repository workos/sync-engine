/**
 * Live views.
 *
 * Every query against a mirror collection has to remember
 * `{ _deleted_at: null }`, and forgetting it is the most common query
 * mistake: a seat count that includes users who left, a `$lookup` that
 * resurrects a revoked membership. Fivetran and dbt users solve this with a
 * view over each soft-deleted table, and MongoDB has views too. Ours are
 * generated from the catalog, in the same pass as the collections, so they
 * can never drift from them.
 *
 * Only the view layer is new. The engine keeps writing, redacting,
 * reconciling and reporting freshness against the base collections, and
 * the base collections remain the place to tell "deleted" from "never seen".
 */
import type { Logger, TableDef } from '@workos/sync-engine'
import type { Db, Document } from 'mongodb'
import type { Names } from './names.js'

/** `{ _deleted_at: null }` matches a missing field as well as an explicit null. */
export const LIVE_PIPELINE: Document[] = [{ $match: { _deleted_at: null } }]

export interface ApplyViewOptions {
  /**
   * Redefine a `_live` view over the right collection whose pipeline is not
   * the generated one. Off, `init()` fails and says which view it would have
   * redefined. A view over some other collection, or a plain collection under
   * the name, is never touched either way.
   */
  repairViews?: boolean
}

const NAMESPACE_EXISTS = 48

export type ViewOutcome = 'created' | 'kept' | 'rebuilt'

/**
 * Make sure `<stream>_live` exists over the stream's mirror collection with
 * the generated pipeline. Idempotent; a concurrent `init` racing to create
 * the same view is tolerated.
 */
export async function ensureLiveView(
  db: Db,
  names: Names,
  stream: TableDef,
  logger?: Logger,
  options: ApplyViewOptions = {},
): Promise<ViewOutcome> {
  const source = names.mirror(stream)
  const view = names.liveView(stream)
  const existing = (await db.listCollections({ name: view }, { nameOnly: false }).toArray())[0]
  if (!existing) {
    try {
      await db.createCollection(view, { viewOn: source, pipeline: LIVE_PIPELINE })
      return 'created'
    } catch (err) {
      if ((err as { code?: unknown }).code !== NAMESPACE_EXISTS) throw err
      // Another init got there first; judge what it made.
      return ensureLiveView(db, names, stream, logger, options)
    }
  }
  if (existing.type !== 'view' || existing.options?.['viewOn'] !== source) {
    throw new Error(
      `${db.databaseName}.${view} exists and is not the generated view over ${source}: ` +
        `rename or drop it, or choose another prefix`,
    )
  }
  if (JSON.stringify(existing.options?.['pipeline']) === JSON.stringify(LIVE_PIPELINE)) return 'kept'
  if (!options.repairViews) {
    throw new Error(
      `${db.databaseName}.${view} is a view over ${source} with a pipeline that is not the generated one; ` +
        `run init --repair to redefine it, or drop it`,
    )
  }
  await db.command({ collMod: view, viewOn: source, pipeline: LIVE_PIPELINE })
  logger?.warn('live view redefined to the generated pipeline', { view, source })
  return 'rebuilt'
}
