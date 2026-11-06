import { describe, it, expect } from 'vitest'
import {
  streamByName,
  STREAMS,
  defaultStreams,
  selectStreams,
  userReferenceColumns,
  userReferencePaths,
} from '../src/catalog.js'

describe('selectStreams', () => {
  it('selects the defaults, with events, when given nothing', () => {
    const names = selectStreams().map((s) => s.name)
    expect(names).toEqual(
      STREAMS.filter((s) => defaultStreams().includes(s.name) || s.name === 'events').map((s) => s.name),
    )
    expect(names).toContain('events')
  })

  it('always includes events, whatever is named', () => {
    expect(selectStreams(['users']).map((s) => s.name)).toEqual(['users', 'events'])
  })

  it('appends extra columns to the stream they name', () => {
    const [users] = selectStreams(['users'], { users: [{ name: 'plan', type: 'text', path: ['metadata', 'plan'] }] })
    expect(users!.columns.at(-1)).toMatchObject({ name: 'plan' })
    // The catalog itself is untouched.
    expect(STREAMS.find((s) => s.name === 'users')!.columns.some((c) => c.name === 'plan')).toBe(false)
  })

  it('refuses names the catalog does not know and columns for streams not selected', () => {
    expect(() => selectStreams(['customers'])).toThrow(/unknown stream: customers/)
    expect(() => selectStreams(['users'], { organizations: [{ name: 'x', type: 'text', path: 'x' }] })).toThrow(
      /extraColumns names a stream that is not selected: organizations/,
    )
  })
})

describe('user references', () => {
  it('names the columns of a stream that hold a user id, and the users key itself', () => {
    expect(userReferenceColumns(streamByName('users')!).map((c) => c.name)).toEqual(['id'])
    expect(userReferenceColumns(streamByName('organization_memberships')!).map((c) => c.name)).toEqual(['user_id'])
    expect(userReferenceColumns(streamByName('invitations')!).map((c) => c.name)).toEqual([
      'inviter_user_id',
      'accepted_user_id',
    ])
    expect(userReferenceColumns(streamByName('organizations')!)).toEqual([])
  })

  it('collects the payload paths that name a user across the given streams, extra and nested ones included', () => {
    expect(
      userReferencePaths(STREAMS)
        .map((p) => p.join('.'))
        .sort(),
    ).toEqual(['accepted_user_id', 'inviter_user_id', 'user_id'])
    const withExtra = selectStreams(['organizations'], {
      organizations: [
        { name: 'owner', type: 'text', path: ['metadata', 'owner_user_id'], references: 'users' },
        // Named `user_id` without declaring anything: counted, as it always was.
        { name: 'user_id', type: 'text', path: 'user_id' },
      ],
    })
    const organizations = withExtra.find((s) => s.name === 'organizations')!
    expect(userReferenceColumns(organizations).map((c) => c.name)).toEqual(['owner', 'user_id'])
    expect(
      userReferencePaths(withExtra)
        .map((p) => p.join('.'))
        .sort(),
    ).toEqual(['metadata.owner_user_id', 'user_id'])
  })
})
