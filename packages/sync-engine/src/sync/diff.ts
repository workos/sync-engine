/**
 * The reconcile set difference, computed in Node.
 *
 * This is the algorithm `diffStream` used before destinations learned to stage
 * the remote side and diff in the database: every live id in a `Set`, every
 * remote payload in a `Map`. Memory grows with the stream, not with the drift,
 * so a destination should only delegate here when it has nothing to stage
 * into — an HTTP sink, a file — and accept the cost. `@workos/sync-postgres`
 * does not use it.
 */
import type { Destination, IdDiff } from '../destination.js'
import type { StreamDef } from '../types.js'

/**
 * The id a remote payload is diffed on: the first key column, read at its
 * catalog path, the way the destination's own key extraction reads it.
 * `undefined` when the payload has no value there.
 */
export function remoteKey(stream: StreamDef, data: Record<string, unknown>): string | undefined {
  const keyName = stream.key[0]!
  const path = stream.columns.find((c) => c.name === keyName)?.path ?? keyName
  let cur: unknown = data
  for (const p of Array.isArray(path) ? path : [path]) {
    if (cur == null || typeof cur !== 'object') return undefined
    cur = (cur as Record<string, unknown>)[p]
  }
  if (cur == null || cur === '') return undefined
  return String(cur)
}

export async function diffIdsInMemory(
  destination: Destination,
  stream: StreamDef,
  env: string,
  remote: AsyncIterable<Record<string, unknown>[]>,
): Promise<IdDiff> {
  const local = new Set(await destination.liveIds(stream, env))
  const byId = new Map<string, Record<string, unknown>>()
  for await (const page of remote) {
    for (const data of page) {
      const id = remoteKey(stream, data)
      // Unkeyed payloads are skipped, as the SQL path skips them.
      if (id !== undefined && !byId.has(id)) byId.set(id, data)
    }
  }
  const localOnly = [...local].filter((id) => !byId.has(id))
  const remoteOnly: Record<string, unknown>[] = []
  for (const [id, data] of byId) if (!local.has(id)) remoteOnly.push(data)
  return { matched: local.size - localOnly.length, localOnly, remoteOnly }
}
