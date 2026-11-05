/**
 * Classifying ClickHouse errors for the quarantine.
 *
 * The engine only ever sets an event aside for an error that will not go
 * away on retry. The server reports every error with a numeric code and a
 * symbolic type (`999 KEEPER_EXCEPTION`), which the client surfaces as
 * `code` (a string) and `type`. The list below names the codes that
 * describe the server's state rather than the row's: timeouts, lost
 * connections, resource pressure, a replica mid-failover, a Keeper
 * transaction that lost a race. Not in it, on purpose: a value the server
 * cannot parse, a missing table or column, a syntax error, a Keeper node
 * that already exists. Those are verdicts on the statement and come back
 * identically however many times it is retried.
 */

/** Server error codes about timing, connections or resources. */
const TRANSIENT_CODES = new Set<string>([
  '159', // TIMEOUT_EXCEEDED
  '160', // TOO_SLOW
  '202', // TOO_MANY_SIMULTANEOUS_QUERIES
  '203', // NO_FREE_CONNECTION
  '209', // SOCKET_TIMEOUT
  '210', // NETWORK_ERROR
  '236', // ABORTED
  '241', // MEMORY_LIMIT_EXCEEDED
  '242', // TABLE_IS_READ_ONLY
  '243', // NOT_ENOUGH_SPACE
  '252', // TOO_MANY_PARTS
  '290', // LIMIT_EXCEEDED
  '319', // UNKNOWN_STATUS_OF_INSERT
  '394', // QUERY_WAS_CANCELLED
  '999', // KEEPER_EXCEPTION: a lost race or a lost session, both retried
  '1002', // UNKNOWN_EXCEPTION: the server could not say, which is not a verdict on the row
])

/** Node socket errors the client surfaces as-is, with the errno name in `code`. */
const TRANSIENT_SOCKET_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EAI_AGAIN',
])

/** Message fragments of client-side failures that carry no code. */
const TRANSIENT_MESSAGES = ['socket hang up', 'Timeout error', 'ECONNRESET', 'fetch failed', 'network error']

/**
 * Whether `err` is a ClickHouse or socket error that a retry could fix.
 * Anything without a recognisable code is reported as deterministic, which
 * is the conservative answer: the engine's two-sightings rule covers the gap.
 */
export function isTransientClickHouseError(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false
  const { code, message } = err as { code?: unknown; message?: unknown }
  // Raised here, not by the server: a write lock (`lock.ts`) that stayed
  // taken for the whole wait. Whoever holds it finishes or expires.
  if (code === 'LOCK_TIMEOUT') return true
  if (typeof code === 'string' && (TRANSIENT_CODES.has(code) || TRANSIENT_SOCKET_CODES.has(code))) return true
  if (typeof message === 'string' && TRANSIENT_MESSAGES.some((m) => message.includes(m))) return true
  return false
}

/** Whether `err` is the server's refusal of a KeeperMap write that lost a race or found its key taken. */
export function isKeeperConflict(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false
  const { code, message } = err as { code?: unknown; message?: unknown }
  return (
    code === '999' &&
    typeof message === 'string' &&
    (message.includes('Node exists') || message.includes('Bad version'))
  )
}
