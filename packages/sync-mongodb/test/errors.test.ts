/**
 * The transient-error classifier that decides whether a failed write can ever
 * lead to a quarantine. A wrong answer in either direction is costly: a
 * connection drop called deterministic quarantines good events, and a bad
 * document called transient stalls the tail forever.
 */
import { describe, it, expect } from 'vitest'
import { MongoNetworkError, MongoNetworkTimeoutError, MongoServerError, MongoServerSelectionError } from 'mongodb'
import { isTransientMongoError } from '../src/db/errors.js'

const serverError = (code: number, errorLabels: string[] = []) =>
  new MongoServerError({ errmsg: `code ${code}`, code, errorLabels })
const socketError = (code: string) => Object.assign(new Error(code), { code, syscall: 'read' })

describe('isTransientMongoError', () => {
  it('calls a dead socket or an unreachable cluster transient', () => {
    expect(isTransientMongoError(new MongoNetworkError('connection closed'))).toBe(true)
    expect(isTransientMongoError(new MongoNetworkTimeoutError('timed out'))).toBe(true)
    expect(isTransientMongoError(Object.create(MongoServerSelectionError.prototype) as Error)).toBe(true)
  })

  it('calls server-state codes transient: elections, shutdowns, lock contention, write conflicts, time limits', () => {
    const codes = [
      6, // HostUnreachable
      7, // HostNotFound
      24, // LockTimeout
      46, // LockBusy
      50, // MaxTimeMSExpired
      89, // NetworkTimeout
      91, // ShutdownInProgress
      112, // WriteConflict
      189, // PrimarySteppedDown
      262, // ExceededTimeLimit
      10107, // NotWritablePrimary
      11600, // InterruptedAtShutdown
      11602, // InterruptedDueToReplStateChange
      13435, // NotPrimaryNoSecondaryOk
      13436, // NotPrimaryOrSecondary
    ]
    for (const code of codes) expect(isTransientMongoError(serverError(code)), String(code)).toBe(true)
  })

  it('trusts the labels the server attaches to a retryable failure, whatever the code', () => {
    expect(isTransientMongoError(serverError(2, ['RetryableWriteError']))).toBe(true)
    expect(isTransientMongoError(serverError(2, ['TransientTransactionError']))).toBe(true)
    expect(isTransientMongoError(serverError(2, ['SomethingElse']))).toBe(false)
  })

  it('calls verdicts on the document deterministic: duplicates, bad values, validation, size', () => {
    const codes = [
      11000, // DuplicateKey
      2, // BadValue
      121, // DocumentValidationFailure
      10334, // BSONObjectTooLarge
      17280, // KeyTooLong
      26, // NamespaceNotFound
      48, // NamespaceExists
    ]
    for (const code of codes) expect(isTransientMongoError(serverError(code)), String(code)).toBe(false)
  })

  it('recognises the socket errors node surfaces unwrapped, and as a cause', () => {
    for (const code of ['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE']) {
      expect(isTransientMongoError(socketError(code)), code).toBe(true)
      expect(isTransientMongoError(new Error('wrapped', { cause: socketError(code) })), code).toBe(true)
    }
    expect(isTransientMongoError(socketError('ENOENT'))).toBe(false)
  })

  it('presumes anything it cannot classify is deterministic', () => {
    // The two-sightings rule is what guards this gap; the classifier itself
    // must not guess in the direction that skips data.
    expect(isTransientMongoError(new Error('connection closed unexpectedly'))).toBe(false)
    expect(isTransientMongoError(Object.assign(new Error('x'), { code: 'INVALID_TIMESTAMP' }))).toBe(false)
    expect(isTransientMongoError('ECONNRESET')).toBe(false)
    expect(isTransientMongoError(null)).toBe(false)
    expect(isTransientMongoError(undefined)).toBe(false)
  })
})
