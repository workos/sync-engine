import { describe, it, expect } from 'vitest'
import { STREAMS, defaultStreams, selectStreams } from '../src/catalog.js'

describe('selectStreams', () => {
  it('selects the defaults, with events, when given nothing', () => {
    const names = selectStreams().map((s) => s.name)
    expect(names).toEqual(STREAMS.filter((s) => defaultStreams().includes(s.name) || s.name === 'events').map((s) => s.name))
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
