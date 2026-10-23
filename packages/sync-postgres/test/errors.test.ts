/**
 * The transient-error classifier that decides whether a failed write can ever
 * lead to a quarantine. A wrong answer in either direction is costly: a
 * connection drop called deterministic quarantines good events, and a bad
 * cast called transient stalls the tail forever.
 */
import { describe, it, expect } from 'vitest'
import { isTransientPgError } from '../src/db/errors.js'

const pgError = (code: string) => Object.assign(new Error(`SQLSTATE ${code}`), { code })
const socketError = (code: string) => Object.assign(new Error(code), { code, syscall: 'read' })

describe('isTransientPgError', () => {
  it('calls server-state classes transient: connection, rollback, resources, intervention, system, internal', () => {
    const codes = [
      '08000', // connection_exception
      '08006', // connection_failure
      '08003', // connection_does_not_exist
      '40001', // serialization_failure
      '40P01', // deadlock_detected
      '53100', // disk_full
      '53200', // out_of_memory
      '53300', // too_many_connections
      '57014', // query_canceled (statement_timeout)
      '57P01', // admin_shutdown
      '58030', // io_error
      'XX000', // internal_error
    ]
    for (const code of codes) expect(isTransientPgError(pgError(code)), code).toBe(true)
  })

  it('calls verdicts on the statement deterministic: data, integrity, syntax and access', () => {
    const codes = [
      '22P02', // invalid_text_representation — 'maybe'::boolean
      '22007', // invalid_datetime_format
      '22008', // datetime_field_overflow — the ::timestamptz cast
      '23505', // unique_violation
      '23502', // not_null_violation
      '42703', // undefined_column
      '42P01', // undefined_table
      '42883', // undefined_function
    ]
    for (const code of codes) expect(isTransientPgError(pgError(code)), code).toBe(false)
  })

  it('calls a lock timeout transient even though its class is not', () => {
    // 55P03 lock_not_available: a write that ran past lock_timeout behind
    // another transaction. Waiting fixes it; quarantining would not.
    expect(isTransientPgError(Object.assign(new Error('lock timeout'), { code: '55P03' }))).toBe(true)
    // The rest of class 55 (object_not_in_prerequisite_state) stays deterministic.
    expect(isTransientPgError(Object.assign(new Error('not in prerequisite state'), { code: '55000' }))).toBe(false)
  })

  it('recognises the socket errors node-postgres surfaces unwrapped', () => {
    for (const code of ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE']) {
      expect(isTransientPgError(socketError(code)), code).toBe(true)
    }
    expect(isTransientPgError(socketError('ENOENT'))).toBe(false)
  })

  it('presumes anything it cannot classify is deterministic', () => {
    // The two-sightings rule is what guards this gap; the classifier itself
    // must not guess in the direction that skips data.
    expect(isTransientPgError(new Error('Connection terminated unexpectedly'))).toBe(false)
    expect(isTransientPgError(Object.assign(new Error('x'), { code: 42 }))).toBe(false)
    expect(isTransientPgError('ECONNRESET')).toBe(false)
    expect(isTransientPgError(null)).toBe(false)
    expect(isTransientPgError(undefined)).toBe(false)
  })
})
