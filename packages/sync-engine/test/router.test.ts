import { describe, it, expect } from 'vitest'
import { routeEvent, sourceUpdatedAt } from '../src/router.js'
import { streamForId, STREAMS } from '../src/catalog.js'
import type { WorkOSEvent } from '../src/types.js'

const evt = (event: string, data: Record<string, unknown> = {}, over: Partial<WorkOSEvent> = {}): WorkOSEvent => ({
  id: 'event_01',
  event,
  data,
  created_at: '2026-01-01T00:00:00.000Z',
  ...over,
})

describe('event routing', () => {
  it('routes object lifecycle events to their stream', () => {
    expect(routeEvent(evt('user.created', { id: 'user_1' }))).toMatchObject({ kind: 'upsert', stream: 'users' })
    expect(routeEvent(evt('user.updated', { id: 'user_1' }))).toMatchObject({ kind: 'upsert', stream: 'users' })
    expect(routeEvent(evt('user.deleted', { id: 'user_1' }))).toMatchObject({ kind: 'delete', stream: 'users' })
    expect(routeEvent(evt('organization.created', { id: 'org_1' }))).toMatchObject({
      kind: 'upsert',
      stream: 'organizations',
    })
    expect(routeEvent(evt('dsync.user.deleted', { id: 'directory_user_1' }))).toMatchObject({
      kind: 'delete',
      stream: 'directory_users',
    })
  })

  it('archives an unknown event instead of throwing', () => {
    // A comparable engine ended its switch with `default: throw`, which returned
    // HTTP 400 upstream and turned every new event type into a retry storm.
    const action = routeEvent(evt('quantum_widget.entangled', { id: 'qw_1' }))
    expect(action).toEqual({ kind: 'archive_only', reason: 'unknown' })
  })

  it('archives known families that have no mirror table', () => {
    for (const name of ['vault.data.read', 'radar.challenge_created', 'agent.registration.created', 'pipes.connected_account.connected']) {
      expect(routeEvent(evt(name, {}))).toMatchObject({ kind: 'archive_only', reason: 'no_mirror' })
    }
  })

  it('captures authentication events, which exist nowhere else in the API', () => {
    const action = routeEvent(
      evt('authentication.password_failed', {
        user_id: 'user_1',
        email: 'ada@example.com',
        error: { code: 'invalid_credentials' },
      }),
    )
    expect(action).toMatchObject({ kind: 'append', stream: 'auth_attempts' })
    if (action.kind !== 'append') throw new Error('unreachable')
    expect(action.data).toMatchObject({
      event_id: 'event_01',
      event: 'authentication.password_failed',
      status: 'failed',
      user_id: 'user_1',
    })
  })

  it('derives success and failure from the event name', () => {
    const ok = routeEvent(evt('authentication.sso_succeeded', { user_id: 'u' }))
    expect(ok.kind === 'append' && ok.data['status']).toBe('succeeded')
  })

  it('turns directory group membership events into a link and an unlink', () => {
    const added = routeEvent(
      evt('dsync.group.user_added', {
        directory_id: 'directory_1',
        user: { id: 'directory_user_1' },
        group: { id: 'directory_group_1' },
      }),
    )
    expect(added).toMatchObject({ kind: 'link', stream: 'directory_group_memberships' })
    if (added.kind !== 'link') throw new Error('unreachable')
    expect(added.data).toEqual({
      directory_id: 'directory_1',
      directory_group_id: 'directory_group_1',
      directory_user_id: 'directory_user_1',
    })

    const removed = routeEvent(
      evt('dsync.group.user_removed', {
        directory_id: 'directory_1',
        user: { id: 'directory_user_1' },
        group: { id: 'directory_group_1' },
      }),
    )
    expect(removed.kind).toBe('unlink')
  })

  it('handles group membership events that carry only ids', () => {
    const added = routeEvent(
      evt('group.member_added', { group_id: 'group_1', organization_membership_id: 'om_1' }),
    )
    expect(added).toMatchObject({ kind: 'link', stream: 'group_memberships' })
  })

  it('unwraps the verification_failed envelope', () => {
    // This event is {reason, organization_domain: {...}} rather than the object.
    const action = routeEvent(
      evt('organization_domain.verification_failed', {
        reason: 'dns_record_not_found',
        organization_domain: { id: 'org_domain_1', domain: 'acme.com', state: 'failed' },
      }),
    )
    expect(action).toMatchObject({ kind: 'upsert', stream: 'organization_domains' })
    if (action.kind !== 'upsert') throw new Error('unreachable')
    expect(action.data['id']).toBe('org_domain_1')
  })

  it('routes role events that carry a slug but no id', () => {
    const action = routeEvent(evt('role.updated', { object: 'role', slug: 'admin', permissions: ['a'] }))
    expect(action).toMatchObject({ kind: 'upsert', stream: 'roles' })
  })
})

describe('staleness timestamp', () => {
  it("prefers the object's own updated_at", () => {
    const e = evt('user.updated', { id: 'user_1', updated_at: '2026-05-05T00:00:00.000Z' })
    expect(sourceUpdatedAt(e, e.data)).toBe('2026-05-05T00:00:00.000Z')
  })

  it('falls back to the event timestamp', () => {
    const e = evt('user.updated', { id: 'user_1' })
    expect(sourceUpdatedAt(e, e.data)).toBe('2026-01-01T00:00:00.000Z')
  })
})

describe('id prefix dispatch', () => {
  it('matches the longest prefix', () => {
    // conn_ is a proper prefix of conn_app_.
    expect(streamForId('conn_01ABC')?.name).toBe('connections')
    expect(streamForId('directory_user_01ABC')?.name).toBe('directory_users')
    expect(streamForId('directory_group_01ABC')?.name).toBe('directory_groups')
    expect(streamForId('directory_01ABC')?.name).toBe('directories')
    expect(streamForId('org_domain_01ABC')?.name).toBe('organization_domains')
    expect(streamForId('org_01ABC')?.name).toBe('organizations')
  })

  it('returns nothing for an unrecognised prefix', () => {
    expect(streamForId('unknown_01ABC')).toBeUndefined()
  })
})

describe('catalog integrity', () => {
  it('gives every stream a key that exists as a column', () => {
    for (const s of STREAMS) {
      for (const k of s.key) {
        expect(s.columns.find((c) => c.name === k), `${s.name}.${k}`).toBeDefined()
      }
    }
  })

  it('maps each event type to exactly one stream', () => {
    const seen = new Map<string, string>()
    for (const s of STREAMS) {
      for (const e of [...(s.events?.upsert ?? []), ...(s.events?.delete ?? [])]) {
        expect(seen.has(e), `${e} claimed by ${seen.get(e)} and ${s.name}`).toBe(false)
        seen.set(e, s.name)
      }
    }
  })

  it('never marks a stream as mirror-mode without a way to populate it', () => {
    for (const s of STREAMS) {
      if (s.mode !== 'mirror') continue
      const hasEvents = (s.events?.upsert?.length ?? 0) > 0
      const hasList = Boolean(s.list)
      const embedded = s.name === 'organization_domains'
      expect(hasEvents || hasList || embedded, s.name).toBe(true)
    }
  })
})
