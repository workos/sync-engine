/**
 * Classifying MySQL errors for the quarantine.
 *
 * The engine only ever sets an event aside for an error that will not go
 * away on retry. Getting this wrong in either direction is expensive: treat
 * a connection drop as deterministic and a flaky network quarantines good
 * events; treat a bad value as transient and the tail stalls forever, which
 * is the failure the quarantine exists to prevent.
 *
 * MySQL errors carry a numeric `errno` and a symbolic `code`
 * (`ER_LOCK_DEADLOCK`), plus a SQLSTATE that is less telling than
 * Postgres's: most server errors share the generic `HY000`, so the class
 * alone cannot be trusted. The list below names the errors that describe the
 * server's state rather than the row's. Not in it, on purpose: duplicate
 * keys, bad values, too-long data, a missing table or column, a JSON parse
 * failure. Those are verdicts on the statement and come back identically
 * however many times it is retried.
 */

/** Server errors about timing, connections or resources, by `errno`. */
const TRANSIENT_ERRNO = new Set<number>([
  1021, // ER_DISK_FULL
  1040, // ER_CON_COUNT_ERROR
  1041, // ER_OUT_OF_RESOURCES
  1053, // ER_SERVER_SHUTDOWN
  1114, // ER_RECORD_FILE_FULL
  1152, // ER_ABORTING_CONNECTION
  1158, // ER_NET_READ_ERROR
  1159, // ER_NET_READ_INTERRUPTED
  1160, // ER_NET_ERROR_ON_WRITE
  1161, // ER_NET_WRITE_INTERRUPTED
  1184, // ER_NEW_ABORTING_CONNECTION
  1203, // ER_TOO_MANY_USER_CONNECTIONS
  1205, // ER_LOCK_WAIT_TIMEOUT
  1213, // ER_LOCK_DEADLOCK
  1290, // ER_OPTION_PREVENTS_STATEMENT: read_only during a failover
  1317, // ER_QUERY_INTERRUPTED
  1637, // ER_TOO_MANY_CONCURRENT_TRXS
  1689, // ER_LOCK_ABORTED
  1874, // ER_INNODB_READ_ONLY
  1927, // ER_CONNECTION_KILLED
  2006, // CR_SERVER_GONE_ERROR
  2013, // CR_SERVER_LOST
  3024, // ER_QUERY_TIMEOUT: max_execution_time
  3032, // ER_SERVER_OFFLINE_MODE
  3572, // ER_LOCK_NOWAIT
  4031, // ER_CLIENT_INTERACTION_TIMEOUT
])

/** Driver-level codes mysql2 raises when the connection itself fails. */
const TRANSIENT_CODES = new Set<string>([
  'PROTOCOL_CONNECTION_LOST',
  'PROTOCOL_SEQUENCE_TIMEOUT',
  'PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR',
  'PROTOCOL_ENQUEUE_AFTER_QUIT',
  'PROTOCOL_ENQUEUE_AFTER_DESTROY',
  'PROTOCOL_PACKETS_OUT_OF_ORDER',
  'ECONNRESET',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EAI_AGAIN',
])

/** SQLSTATE classes that are about the connection or the transaction, whatever the errno. */
const TRANSIENT_SQLSTATE_CLASSES = new Set(['08', '40'])

/**
 * Whether `err` is a MySQL or socket error that a retry could fix. Anything
 * without a recognisable code is reported as deterministic, which is the
 * conservative answer: the engine's two-sightings rule covers the gap.
 */
export function isTransientMysqlError(err: unknown): boolean {
  if (typeof err !== 'object' || err === null) return false
  const { code, errno, sqlState } = err as { code?: unknown; errno?: unknown; sqlState?: unknown }
  if (typeof code === 'string' && TRANSIENT_CODES.has(code)) return true
  if (typeof errno === 'number' && TRANSIENT_ERRNO.has(errno)) return true
  if (typeof sqlState === 'string' && sqlState.length === 5 && TRANSIENT_SQLSTATE_CLASSES.has(sqlState.slice(0, 2))) {
    return true
  }
  return false
}
