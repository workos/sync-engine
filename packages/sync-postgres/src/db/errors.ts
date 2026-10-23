/**
 * Classifying Postgres errors for the quarantine.
 *
 * The engine only ever sets an event aside for an error that will not go away
 * on retry. The database tells us which kind it was: every error carries a
 * five-character SQLSTATE whose first two characters are its class, and the
 * classes split cleanly into "the server or the connection had a bad moment"
 * and "this input is wrong". Getting this backwards in either direction is
 * expensive: treat a connection drop as deterministic and a flaky network
 * quarantines good events; treat a bad cast as transient and the tail stalls
 * forever, which is the failure this whole feature exists to prevent.
 */

/**
 * SQLSTATE classes that describe the server's state, not the row's:
 *
 *   08  connection exception          the socket died mid-statement
 *   40  transaction rollback          serialization failure, deadlock
 *   53  insufficient resources        out of memory, disk full, too many connections
 *   57  operator intervention         admin shutdown, statement timeout, cancel
 *   58  system error                  I/O error on the server
 *   XX  internal error                the server itself is broken
 *
 * Not in the list, on purpose: 22 (data exception — a bad cast), 23 (integrity
 * constraint), and 42 (syntax or access rule — a missing column, a bad
 * generated expression). Those are verdicts on the statement and will be
 * returned again, identically, however many times it is retried.
 */
const TRANSIENT_SQLSTATE_CLASSES = new Set(['08', '40', '53', '57', '58', 'XX'])

/** Node socket errors `pg` surfaces as-is, with the errno name in `code`. */
const TRANSIENT_SOCKET_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ETIMEDOUT', 'EPIPE'])

/**
 * Whether `err` is a Postgres or socket error that a retry could fix. Anything
 * without a recognisable `code` is reported as deterministic, which is the
 * conservative answer: the engine's two-sightings rule covers the gap.
 */
export function isTransientPgError(err: unknown): boolean {
  const code = codeOf(err)
  if (!code) return false
  if (TRANSIENT_SOCKET_CODES.has(code)) return true
  return code.length === 5 && TRANSIENT_SQLSTATE_CLASSES.has(code.slice(0, 2))
}

function codeOf(err: unknown): string | undefined {
  if (typeof err !== 'object' || err === null) return undefined
  const code = (err as { code?: unknown }).code
  return typeof code === 'string' ? code : undefined
}
