/**
 * The strict ISO-8601 parser the staleness gate depends on. It has to agree
 * with `parse_iso8601` in the Postgres destination about what is a date, so
 * the same event is quarantined by both rather than stored by one.
 */
import { describe, it, expect } from 'vitest'
import { parseIso8601, parseInstant, parseInstantOrNull, InvalidTimestampError } from '../src/db/timestamps.js'

const at = (iso: string) => {
  const r = parseIso8601(iso)
  return 'date' in r ? r.date.toISOString() : r.failure
}

describe('parseIso8601', () => {
  it('reads the shapes WorkOS emits', () => {
    expect(at('2026-01-02T03:04:05.678Z')).toBe('2026-01-02T03:04:05.678Z')
    expect(at('2026-01-02T03:04:05Z')).toBe('2026-01-02T03:04:05.000Z')
    expect(at('2026-01-02 03:04:05.1z')).toBe('2026-01-02T03:04:05.100Z')
    // Beyond millisecond precision is dropped, not rounded, like a BSON date.
    expect(at('2026-01-02T03:04:05.6789999Z')).toBe('2026-01-02T03:04:05.678Z')
  })

  it('applies an offset, and reads no designator as UTC', () => {
    expect(at('2026-01-02T03:04:05+02:00')).toBe('2026-01-02T01:04:05.000Z')
    expect(at('2026-01-02T03:04:05-0530')).toBe('2026-01-02T08:34:05.000Z')
    expect(at('2026-01-02T03:04:05')).toBe('2026-01-02T03:04:05.000Z')
  })

  it('refuses what is not an instant', () => {
    expect(at('2026-01-02')).toBe('shape')
    expect(at('yesterday')).toBe('shape')
    expect(at('')).toBe('shape')
    expect(at('1700000000')).toBe('shape')
  })

  it('refuses a well-shaped value whose fields are out of range, which V8 would roll over', () => {
    expect(new Date('2026-02-30T12:00:00Z').getTime()).not.toBeNaN()
    expect(at('2026-02-30T12:00:00Z')).toBe('range')
    expect(at('2026-13-01T00:00:00Z')).toBe('range')
    expect(at('2026-04-31T00:00:00Z')).toBe('range')
    expect(at('2026-01-01T24:00:00Z')).toBe('range')
    expect(at('2026-01-01T00:60:00Z')).toBe('range')
    expect(at('0000-01-01T00:00:00Z')).toBe('range')
    // Leap years are real, and so is the leap second.
    expect(at('2024-02-29T00:00:00Z')).toBe('2024-02-29T00:00:00.000Z')
    expect(at('2100-02-29T00:00:00Z')).toBe('range')
    expect(at('2026-06-30T23:59:60Z')).toBe('2026-07-01T00:00:00.000Z')
  })

  it('keeps years below 100 as written', () => {
    expect(at('0042-01-01T00:00:00Z')).toBe('0042-01-01T00:00:00.000Z')
  })
})

describe('parseInstant', () => {
  it('throws a deterministic, coded error the engine can quarantine on', () => {
    expect(() => parseInstant('2026-02-30T12:00:00Z')).toThrow(InvalidTimestampError)
    expect(() => parseInstant('2026-02-30T12:00:00Z')).toThrow(/out of range/)
    expect(() => parseInstant('soon')).toThrow(/not an ISO-8601 instant/)
    try {
      parseInstant('soon')
    } catch (err) {
      expect((err as InvalidTimestampError).code).toBe('INVALID_TIMESTAMP')
    }
    expect(parseInstant('2026-01-02T03:04:05.678Z').toISOString()).toBe('2026-01-02T03:04:05.678Z')
  })

  it('has a lenient twin for fields that must never fail a write', () => {
    expect(parseInstantOrNull('2026-02-30T12:00:00Z')).toBeNull()
    expect(parseInstantOrNull(undefined)).toBeNull()
    expect(parseInstantOrNull(42)).toBeNull()
    expect(parseInstantOrNull('2026-01-02T03:04:05Z')?.toISOString()).toBe('2026-01-02T03:04:05.000Z')
  })
})
