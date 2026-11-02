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
import type { TableDef } from '../types.js'

/** Keep scalar ids unchanged; encode composite keys without delimiter collisions. */
export function encodeKey(values: string[]): string {
  return values.length === 1 ? values[0]! : JSON.stringify(values)
}

/** The complete key at its catalog paths; undefined when any component is missing. */
export function remoteKey(stream: TableDef, data: Record<string, unknown>): string | undefined {
  const values: string[] = []
  for (const name of stream.key) {
    const path = stream.columns.find((c) => c.name === name)?.path ?? name
    let cur: unknown = data
    for (const p of Array.isArray(path) ? path : [path]) {
      if (cur == null || typeof cur !== 'object') return undefined
      cur = (cur as Record<string, unknown>)[p]
    }
    if (cur == null || cur === '') return undefined
    values.push(String(cur))
  }
  return encodeKey(values)
}

/** Reconstruct a deletion payload at the same catalog paths used to read its key. */
export function keyPayload(stream: TableDef, id: string): Record<string, unknown> {
  const values: unknown = stream.key.length === 1 ? [id] : JSON.parse(id)
  if (
    !Array.isArray(values) ||
    values.length !== stream.key.length ||
    values.some((v) => typeof v !== 'string' || v === '')
  ) {
    throw new Error(`invalid reconciliation key for ${stream.name}: ${id}`)
  }
  const data: Record<string, unknown> = Object.create(null)
  for (const [i, name] of stream.key.entries()) {
    const path = stream.columns.find((c) => c.name === name)?.path ?? name
    const parts = Array.isArray(path) ? path : [path]
    let target = data
    for (const part of parts.slice(0, -1)) {
      target[part] ??= Object.create(null)
      target = target[part] as Record<string, unknown>
    }
    target[parts[parts.length - 1]!] = values[i]
  }
  return data
}

export async function diffIdsInMemory(
  destination: Destination,
  stream: TableDef,
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
