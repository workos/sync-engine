/**
 * Event routing: WorkOS event name -> what to write.
 *
 * Design rule taken from a comparable sync engine's most expensive bug: an
 * unrecognised event type must never throw. Its handler ended a 107-case
 * switch with `default: throw`, which returned HTTP 400 upstream and turned an
 * over-broad endpoint subscription into a retry storm. Here, an unknown event
 * is archived to `events` and reported — the stream keeps moving.
 */
import { STREAMS, streamForId } from './catalog.js'
import type { WorkOSEvent } from './types.js'

export type RouteAction =
  | { kind: 'upsert'; stream: string; data: Record<string, unknown> }
  | { kind: 'delete'; stream: string; data: Record<string, unknown> }
  /** Membership pairs: the row IS the relationship, and removal is a real delete. */
  | { kind: 'link'; stream: string; data: Record<string, unknown> }
  | { kind: 'unlink'; stream: string; data: Record<string, unknown> }
  | { kind: 'append'; stream: string; data: Record<string, unknown> }
  | { kind: 'archive_only'; reason: 'no_mirror' | 'unknown' }

const upsertIndex = new Map<string, string>()
const deleteIndex = new Map<string, string>()
for (const s of STREAMS) {
  for (const e of s.events?.upsert ?? []) upsertIndex.set(e, s.name)
  for (const e of s.events?.delete ?? []) deleteIndex.set(e, s.name)
}

/**
 * Events that carry a reference or a transition rather than an object. These
 * cannot be upserted into a mirror table and are handled individually.
 */
const isAuthAttempt = (name: string) => name.startsWith('authentication.')

export function routeEvent(event: WorkOSEvent): RouteAction {
  const name = event.event
  const data = (event.data ?? {}) as Record<string, unknown>

  // Sign-in attempts have no object and no REST endpoint anywhere in the API.
  // Capturing them here is the only way this history ever exists.
  if (isAuthAttempt(name)) {
    return {
      kind: 'append',
      stream: 'auth_attempts',
      data: {
        ...data,
        event_id: event.id,
        event: name,
        created_at: event.created_at,
        // authentication.*_succeeded / _failed -> status
        status: name.endsWith('_failed') ? 'failed' : name.endsWith('_succeeded') ? 'succeeded' : null,
      },
    }
  }

  // Directory group membership: payload is {directory_id, user, group}.
  if (name === 'dsync.group.user_added' || name === 'dsync.group.user_removed') {
    const user = (data['user'] ?? {}) as Record<string, unknown>
    const group = (data['group'] ?? {}) as Record<string, unknown>
    const row = {
      directory_id: data['directory_id'] ?? user['directory_id'] ?? group['directory_id'],
      directory_group_id: group['id'],
      directory_user_id: user['id'],
    }
    return {
      kind: name.endsWith('added') ? 'link' : 'unlink',
      stream: 'directory_group_memberships',
      data: row,
    }
  }

  // AuthKit group membership: payload is IDs only.
  if (name === 'group.member_added' || name === 'group.member_removed') {
    return {
      kind: name.endsWith('added') ? 'link' : 'unlink',
      stream: 'group_memberships',
      data: {
        group_id: data['group_id'],
        organization_membership_id: data['organization_membership_id'],
      },
    }
  }

  const delStream = deleteIndex.get(name)
  if (delStream) return { kind: 'delete', stream: delStream, data: unwrapFor(delStream, data) }

  const upStream = upsertIndex.get(name)
  if (upStream) return { kind: 'upsert', stream: upStream, data: unwrapFor(upStream, data) }

  // Known event families we deliberately do not mirror (vault.*, radar.*,
  // agent.*, connection.saml_certificate_*, pipes.*). They stay queryable in
  // the `events` archive.
  if (/^(vault|radar|agent|pipes)\./.test(name) || name.startsWith('connection.saml_certificate')) {
    return { kind: 'archive_only', reason: 'no_mirror' }
  }

  return { kind: 'archive_only', reason: 'unknown' }
}

function unwrapFor(stream: string, data: Record<string, unknown>): Record<string, unknown> {
  const def = STREAMS.find((s) => s.name === stream)
  return def?.unwrap ? def.unwrap(data) : data
}

/**
 * The staleness timestamp for an event write, synthesized in the ingest layer.
 *
 * Never let the database stamp this with `now()`: a redelivered event would
 * always look newer than the row it is replacing, which defeats the gate
 * entirely. Prefer the object's own updated_at, fall back to the event time.
 */
export function sourceUpdatedAt(event: WorkOSEvent, data: Record<string, unknown>): string {
  const objUpdated = data['updated_at']
  if (typeof objUpdated === 'string' && objUpdated.length > 0) return objUpdated
  return event.created_at
}

/** Best-effort object id for an event, used for logging and hydration. */
export function objectIdOf(data: Record<string, unknown>): string | undefined {
  const id = data['id']
  return typeof id === 'string' ? id : undefined
}

export { streamForId }
