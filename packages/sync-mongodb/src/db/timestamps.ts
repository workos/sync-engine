/**
 * ISO-8601 parsing, strict.
 *
 * `Row.updatedAt` and an event's `created_at` arrive as strings and are
 * stored as BSON dates, so the staleness gate compares instants rather than
 * text. `new Date(iso)` is not strict enough for that: V8 rolls `2026-02-30`
 * over into March instead of rejecting it, so a timestamp the Postgres
 * destination refuses — and quarantines the event for — would be silently
 * mis-stored here. This parser mirrors `parse_iso8601` in
 * `@workos/sync-postgres`: the same shape, the same range checks, and a
 * verdict rather than a guess for anything else.
 */

const ISO = /^(\d{4})-(\d{2})-(\d{2})[Tt ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?\s*(?:([Zz])|([+-])(\d{2}):?(\d{2}))?$/

// Day counts of a non-leap year, with February corrected for the year in hand.
const daysInMonth = (year: number, month: number) =>
  new Date(Date.UTC(2001, month, 0)).getUTCDate() + (month === 2 && isLeap(year) ? 1 : 0)
const isLeap = (year: number) => (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0

export type ParseFailure = 'shape' | 'range'

/** The instant `iso` names, or why it names none. */
export function parseIso8601(iso: string): { date: Date } | { failure: ParseFailure } {
  const m = ISO.exec(iso)
  if (!m) return { failure: 'shape' }
  const [, y, mo, d, h, mi, s, frac, zulu, sign, oh, om] = m
  const year = Number(y)
  const month = Number(mo)
  const day = Number(d)
  const hour = Number(h)
  const minute = Number(mi)
  const second = Number(s)
  if (year < 1 || month < 1 || month > 12) return { failure: 'range' }
  if (day < 1 || day > daysInMonth(year, month)) return { failure: 'range' }
  if (hour > 23 || minute > 59 || second > 60) return { failure: 'range' }
  // An offset is hours and minutes too; `+00:99` names no zone.
  if (sign && (Number(oh) > 23 || Number(om) > 59)) return { failure: 'range' }
  // Millisecond precision is what BSON dates carry; further digits are dropped.
  const ms = frac ? Number(frac.slice(0, 3).padEnd(3, '0')) : 0
  // Built in two steps so that years below 100 are not read as 19xx.
  const date = new Date(Date.UTC(2000, month - 1, day, hour, minute, second, ms))
  date.setUTCFullYear(year)
  if (!zulu && sign) {
    const offset = (Number(oh) * 60 + Number(om)) * 60_000
    date.setTime(date.getTime() - (sign === '-' ? -offset : offset))
  }
  return { date }
}

/** Deterministic: the same input fails the same way every time, which is what lets the engine quarantine on it. */
export class InvalidTimestampError extends Error {
  readonly code = 'INVALID_TIMESTAMP'

  constructor(iso: string, failure: ParseFailure) {
    super(
      `invalid timestamp ${JSON.stringify(iso)}: ${
        failure === 'range' ? 'date/time field value out of range' : 'not an ISO-8601 instant'
      }`,
    )
    this.name = 'InvalidTimestampError'
  }
}

/** `iso` as a date, or an error the engine can quarantine on. */
export function parseInstant(iso: string): Date {
  const r = parseIso8601(iso)
  if ('date' in r) return r.date
  throw new InvalidTimestampError(iso, r.failure)
}

/** `iso` as a date, or null: for fields that must never fail a write. */
export function parseInstantOrNull(iso: unknown): Date | null {
  if (typeof iso !== 'string') return null
  const r = parseIso8601(iso)
  return 'date' in r ? r.date : null
}
