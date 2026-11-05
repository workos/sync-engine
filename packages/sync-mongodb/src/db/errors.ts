/**
 * Classifying MongoDB errors for the quarantine.
 *
 * The engine only ever sets an event aside for an error that will not go away
 * on retry. Getting this wrong in either direction is expensive: treat a
 * dropped connection as deterministic and a flaky network quarantines good
 * events; treat a bad payload as transient and the tail stalls forever, which
 * is the failure the quarantine exists to prevent.
 *
 * The driver says which kind an error was, three ways: the class
 * (`MongoNetworkError` for a socket that died, `MongoServerSelectionError`
 * for a cluster with nothing to talk to), the error labels the server
 * attaches to a retryable failure, and the numeric server code.
 */
import { MongoError, MongoNetworkError, MongoServerSelectionError } from 'mongodb'

/**
 * Server codes that describe the server's state, not the document's:
 *
 *     6  HostUnreachable                  7  HostNotFound
 *    24  LockTimeout                     46  LockBusy
 *    50  MaxTimeMSExpired                64  WriteConcernTimeout
 *    89  NetworkTimeout                  91  ShutdownInProgress
 *   112  WriteConflict                  134  ReadConcernMajorityNotAvailableYet
 *   189  PrimarySteppedDown             262  ExceededTimeLimit
 * 10107  NotWritablePrimary           11600  InterruptedAtShutdown
 * 11602  InterruptedDueToReplStateChange
 * 13435  NotPrimaryNoSecondaryOk      13436  NotPrimaryOrSecondary
 *
 * Not in the list, on purpose: 11000 (duplicate key), 2 (BadValue), 121
 * (document failed validation), 10334 (BSON object too large), 17280 (key too
 * large to index). Those are verdicts on the document and will be returned
 * again, identically, however many times it is retried.
 */
const TRANSIENT_SERVER_CODES = new Set([
  6, 7, 24, 46, 50, 64, 89, 91, 112, 134, 189, 262, 10107, 11600, 11602, 13435, 13436,
])

/** Labels the server attaches when it is telling the client to try again. */
const TRANSIENT_LABELS = ['RetryableWriteError', 'TransientTransactionError']

/** Node socket errors, as they surface on an error's `code` or `cause`. */
const TRANSIENT_SOCKET_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'])

/**
 * Whether `err` is a MongoDB or socket error that a retry could fix. Anything
 * without a recognisable class, label or code is reported as deterministic,
 * which is the conservative answer: the engine's two-sightings rule covers
 * the gap.
 */
export function isTransientMongoError(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false
  if (err instanceof MongoNetworkError || err instanceof MongoServerSelectionError) return true
  if (err instanceof MongoError && TRANSIENT_LABELS.some((label) => err.hasErrorLabel(label))) return true
  const code = (err as { code?: unknown }).code
  if (typeof code === 'number') return TRANSIENT_SERVER_CODES.has(code)
  if (typeof code === 'string') return TRANSIENT_SOCKET_CODES.has(code)
  // A driver error wrapping the socket error that caused it.
  const cause = (err as { cause?: unknown }).cause
  if (typeof cause === 'object' && cause !== null) {
    const causeCode = (cause as { code?: unknown }).code
    return typeof causeCode === 'string' && TRANSIENT_SOCKET_CODES.has(causeCode)
  }
  return false
}
