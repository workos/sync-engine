/**
 * Collection naming.
 *
 * The mirror lives in the application's own database, under a prefix, rather
 * than in a database of its own the way the Postgres mirror lives in a
 * schema: `$lookup` cannot cross databases, and joining the mirror with the
 * application's collections is the reason it exists. The prefix is what keeps
 * `workos_users` from colliding with an application's `users`.
 *
 * Bookkeeping collections carry a second underscore (`workos__sync`), which
 * marks them as the engine's own the way `_sync_state` does in Postgres.
 */
import type { ColumnDef, TableDef } from '@workos/sync-engine'

export const DEFAULT_PREFIX = 'workos_'
export const LIVE_VIEW_SUFFIX = '_live'

/**
 * Letters, digits and underscores, not starting with a digit. No `$`, which
 * MongoDB reserves; no `.`, which would make a collection name look like a
 * namespace; nothing that could become `system.*`.
 */
const PREFIX = /^[A-Za-z_][A-Za-z0-9_]*$/

export function validatePrefix(prefix: string): string {
  if (!PREFIX.test(prefix)) {
    throw new Error(
      `invalid collection prefix "${prefix}": expected letters, digits and underscores, not starting with a digit`,
    )
  }
  return prefix
}

/** Every collection the destination touches, derived from one prefix. */
export class Names {
  readonly prefix: string

  constructor(prefix: string = DEFAULT_PREFIX) {
    this.prefix = validatePrefix(prefix)
  }

  /** The mirror collection for a stream: `workos_users`. */
  mirror(stream: TableDef | string): string {
    return `${this.prefix}${typeof stream === 'string' ? stream : stream.name}`
  }

  /** The view over a mirror collection without its tombstones: `workos_users_live`. */
  liveView(stream: TableDef | string): string {
    return `${this.mirror(stream)}${LIVE_VIEW_SUFFIX}`
  }

  /** The lease and the cursors, one document per `(sync_id, env)`. */
  get sync(): string {
    return `${this.prefix}_sync`
  }

  /** The quarantine. */
  get rejected(): string {
    return `${this.prefix}_rejected`
  }

  /** Applied catalog fingerprints. */
  get migrations(): string {
    return `${this.prefix}_migrations`
  }

  /** A reconcile staging collection, unique to one run. */
  staging(side: 'local' | 'remote', token: string): string {
    return `${this.prefix}_reconcile_${side}_${token}`
  }
}

/**
 * The dotted path to a payload field inside `_raw`: `_raw.role.slug`. A
 * segment with a `.` in it or a leading `$` cannot be addressed this way, so
 * it is refused here rather than silently indexing the wrong thing.
 */
export function fieldPath(path: ColumnDef['path']): string {
  const parts = Array.isArray(path) ? path : [path]
  for (const p of parts) {
    if (p === '' || p.includes('.') || p.startsWith('$')) {
      throw new Error(
        `cannot address payload path ${JSON.stringify(parts)}: a segment is empty, contains "." or starts with "$"`,
      )
    }
  }
  return `_raw.${parts.join('.')}`
}

/** The catalog path of a key column, or the column name when it has no column. */
export function keyPath(stream: TableDef, key: string): ColumnDef['path'] {
  return stream.columns.find((c) => c.name === key)?.path ?? key
}
