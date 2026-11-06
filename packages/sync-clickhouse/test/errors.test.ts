import { describe, it, expect } from 'vitest'
import { isTransientClickHouseError, isKeeperConflict } from '../src/db/errors.js'

const err = (fields: Record<string, unknown>, message = 'x') => Object.assign(new Error(message), fields)

describe('isTransientClickHouseError', () => {
  it('calls timeouts, lost connections, resource pressure and Keeper races transient', () => {
    for (const code of ['159', '209', '210', '241', '252', '319', '394', '999', '242']) {
      expect(isTransientClickHouseError(err({ code, type: 'SOMETHING' }))).toBe(true)
    }
  })

  it('calls verdicts on the statement deterministic: bad values, missing objects, syntax, access', () => {
    const cases: [string, string][] = [
      ['6', 'CANNOT_PARSE_TEXT'],
      ['27', 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'],
      ['36', 'BAD_ARGUMENTS'],
      ['47', 'UNKNOWN_IDENTIFIER'],
      ['60', 'UNKNOWN_TABLE'],
      ['62', 'SYNTAX_ERROR'],
      ['497', 'ACCESS_DENIED'],
    ]
    for (const [code, type] of cases) expect(isTransientClickHouseError(err({ code, type }))).toBe(false)
  })

  it('recognises socket errors and the client-side failures that carry no code', () => {
    for (const code of ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE']) {
      expect(isTransientClickHouseError(err({ code }))).toBe(true)
    }
    expect(isTransientClickHouseError(new Error('socket hang up'))).toBe(true)
    expect(isTransientClickHouseError(new Error('Timeout error.'))).toBe(true)
  })

  it('presumes anything it cannot classify is deterministic', () => {
    expect(isTransientClickHouseError(new Error('plain'))).toBe(false)
    expect(isTransientClickHouseError(err({ code: 'INVALID_TIMESTAMP' }))).toBe(false)
    expect(isTransientClickHouseError(null)).toBe(false)
    expect(isTransientClickHouseError('ECONNRESET')).toBe(false)
  })
})

describe('isKeeperConflict', () => {
  it('names the two strict-mode refusals and nothing else', () => {
    expect(isKeeperConflict(err({ code: '999' }, 'Transaction failed (Node exists): Op #0'))).toBe(true)
    expect(isKeeperConflict(err({ code: '999' }, 'Transaction failed (Bad version): Op #0'))).toBe(true)
    expect(isKeeperConflict(err({ code: '999' }, 'Session expired'))).toBe(false)
    expect(isKeeperConflict(err({ code: '6' }, 'Node exists'))).toBe(false)
  })
})
