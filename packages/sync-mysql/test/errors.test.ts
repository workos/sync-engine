import { describe, it, expect } from 'vitest'
import { isTransientMysqlError } from '../src/db/errors.js'

const err = (fields: Record<string, unknown>) => Object.assign(new Error('x'), fields)

describe('isTransientMysqlError', () => {
  it('calls deadlocks, lock waits, lost connections and exhausted resources transient', () => {
    for (const errno of [1213, 1205, 2013, 2006, 1040, 1021, 1317, 1290, 3024, 4031]) {
      expect(isTransientMysqlError(err({ errno, code: 'ER_SOMETHING', sqlState: 'HY000' }))).toBe(true)
    }
  })

  it('calls verdicts on the statement deterministic: duplicates, bad values, missing objects, syntax', () => {
    const cases: [number, string, string][] = [
      [1062, 'ER_DUP_ENTRY', '23000'],
      [1292, 'ER_TRUNCATED_WRONG_VALUE', '22007'],
      [1048, 'ER_BAD_NULL_ERROR', '23000'],
      [1406, 'ER_DATA_TOO_LONG', '22001'],
      [1411, 'ER_WRONG_VALUE_FOR_TYPE', 'HY000'],
      [3140, 'ER_INVALID_JSON_TEXT', '22032'],
      [1146, 'ER_NO_SUCH_TABLE', '42S02'],
      [1054, 'ER_BAD_FIELD_ERROR', '42S22'],
      [1064, 'ER_PARSE_ERROR', '42000'],
      [1045, 'ER_ACCESS_DENIED_ERROR', '28000'],
    ]
    for (const [errno, code, sqlState] of cases) {
      expect(isTransientMysqlError(err({ errno, code, sqlState }))).toBe(false)
    }
  })

  it('recognises the driver and socket errors mysql2 surfaces without an errno', () => {
    for (const code of ['PROTOCOL_CONNECTION_LOST', 'ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE']) {
      expect(isTransientMysqlError(err({ code }))).toBe(true)
    }
  })

  it('trusts the SQLSTATE class for connection and rollback errors it has no errno for', () => {
    expect(isTransientMysqlError(err({ errno: 9999, sqlState: '08S01' }))).toBe(true)
    expect(isTransientMysqlError(err({ errno: 9999, sqlState: '40001' }))).toBe(true)
  })

  it('presumes anything it cannot classify is deterministic', () => {
    expect(isTransientMysqlError(new Error('plain'))).toBe(false)
    expect(isTransientMysqlError(err({ code: 'ER_INVALID_TIMESTAMP' }))).toBe(false)
    expect(isTransientMysqlError(null)).toBe(false)
    expect(isTransientMysqlError('ECONNRESET')).toBe(false)
  })
})
