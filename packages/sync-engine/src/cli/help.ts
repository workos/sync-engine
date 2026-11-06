/**
 * The help text, the same for every destination's command apart from what
 * the spec says differs: the technology's name, what the connection string
 * is, which variable carries the CA certificate, what `init` does, and the
 * flags and variables that say where the mirror lives.
 */

/** One row of a help section: the name, and the description's lines, the first beside the name. */
export interface HelpRow {
  name: string
  text: string[]
}

export interface HelpSpec {
  /** The command's name as installed: `workos-sync-postgres`. */
  command: string
  /** The technology, for the banner: `Postgres`. */
  technology: string
  /** What a mirrored record is called in this database: `row` or `document`. */
  rowNoun: string
  /** What `DATABASE_URL` is: `Postgres connection string`. */
  connection: string
  /** The variable holding the CA certificate: `WORKOS_SYNC_PG_SSL_CA`. */
  tlsCaVar: string
  /** The `init` command: its usage and the lines describing it. */
  init: HelpRow
  /** The flags that say where the mirror lives: `--schema`, or `--database` and `--prefix`. */
  targetFlags: HelpRow[]
  /** The variables that say where the mirror lives. */
  targetEnv: HelpRow[]
}

const COMMAND_COLUMN = 23
const FLAG_COLUMN = 23
const ENV_COLUMN = 29

/** `name` beside its first line, the rest aligned under it. */
function row(name: string, text: string[], column: number): string {
  const [first = '', ...rest] = text
  const indent = ' '.repeat(2 + column)
  const head = name.length + 2 < 2 + column ? `  ${name.padEnd(column)}${first}` : `  ${name}\n${indent}${first}`
  return [head, ...rest.map((line) => `${indent}${line}`)].join('\n')
}

export function renderHelp(spec: HelpSpec): string {
  const command = (name: string, text: string[]) => row(name, text, COMMAND_COLUMN)
  const flag = (name: string, text: string[]) => row(name, text, FLAG_COLUMN)
  const env = (name: string, text: string[]) => row(name, text, ENV_COLUMN)
  return `${spec.command} — mirror WorkOS data into ${spec.technology}

Usage
  ${spec.command} <command> [flags]

Commands
${command(spec.init.name, spec.init.text)}
${command('backfill [--resume] [--max-pages <n>]', [
  'Load current state from the REST list endpoints.',
  '--resume continues each stream from its checkpoint and',
  'skips streams already complete; --max-pages stops after',
  'n pages across all streams, for hosts with a time limit',
  '(run again with --resume until every stream is complete)',
])}
${command('listen [--health-port <n>]', [
  'Tail the Events API and apply changes continuously',
  '(waits while another listener holds the lease);',
  'with a port, serve GET /healthz and /metrics on it',
])}
${command('tick', ['One poll of the Events API, then exit (for a cron job);', 'exits 3 if a listener holds the lease'])}
${command('reconcile [--repair] [--resume] [--max-pages <n>] [--exclude-streams <a,b>]', [
  'Compare the mirror against the API; --repair fixes drift.',
  '--max-pages stops starting streams once n pages have been',
  'fetched (a stream that starts runs to its last page, since',
  'a diff needs the whole listing, unless it alone needs more',
  'than n: it is then abandoned and reported oversized until',
  'a run with a larger budget or none); run again with',
  '--resume until every stream reports complete.',
  '--exclude-streams leaves streams out, for one that has to',
  'run somewhere without a time limit',
])}
${command('refresh [--resume] [--max-pages <n>]', [
  `Re-list top-level streams, replacing each ${spec.rowNoun};`,
  '--max-pages and --resume slice it like backfill, with',
  'which it shares checkpoints',
])}
${command('status', ['Cursor position, poll age, and per-stream freshness'])}
${command('redact --user <id>', ['Delete one subject from the mirror and event archive'])}
${command('rejected', ['Show events quarantined by the tail; exits 2 if there are any'])}
${command('         --replay', ['Re-apply them all (or one with --id <event_id>) once the cause is fixed'])}
${command('streams', ['Show the streams this build can mirror'])}

Flags
${flag('--database-url <url>', [`${spec.connection} (or DATABASE_URL)`])}
${spec.targetFlags.map((r) => flag(r.name, r.text)).join('\n')}
${flag('--streams <a,b,c>', ['Restrict to specific streams (events is always included)'])}

Environment
${env('DATABASE_URL', [spec.connection])}
${env(spec.tlsCaVar, [
  "CA certificate the database's certificate must chain to, as PEM",
  'or base64 of it; TLS is then required and verified against it',
  '(default: the system trust store)',
])}
${env('WORKOS_API_KEY', ['API key for the environment to mirror'])}
${env('WORKOS_ENVIRONMENT_NAME', [`Label written to _env on every ${spec.rowNoun} (default: production)`])}
${env('WORKOS_API_URL', ['API base URL (default: https://api.workos.com)'])}
${spec.targetEnv.map((r) => env(r.name, r.text)).join('\n')}
${env('WORKOS_SYNC_ID', ['Sync identifier when several syncs share a database (default: default)'])}
${env('WORKOS_SYNC_HYDRATE', ['off | on_create | always, or users=always,organizations=off', '(default: off)'])}
${env('WORKOS_SYNC_POLL_MS', ['Idle wait between polls (default: 5000)'])}
${env('WORKOS_SYNC_LAG_MS', ['Hold back events younger than this (default: 5000)'])}
${env('WORKOS_SYNC_MAX_EVENTS_PER_TICK', ['Events to apply per poll (default: one page)'])}
${env('WORKOS_SYNC_LEASE_TTL_MS', ['How long the tail lease outlives a crashed listener, at least 1 (default: 60000)'])}
${env('WORKOS_SYNC_HEALTH_PORT', ['listen only: serve /healthz and /metrics on this port (default: off)'])}
${env('WORKOS_SYNC_HEALTH_HOST', ['Interface the health server binds (default: 0.0.0.0)'])}
${env('WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS', [
  "/healthz returns 503 once any environment's last successful",
  'poll is older than this (default: 60000)',
])}
${env('WORKOS_SYNC_OTEL', [
  '1 starts the OpenTelemetry Node SDK for the command, configured by',
  'the OTEL_* variables, and flushes it on exit (default: off)',
])}
${env('LOG_LEVEL', ['debug | info | warn | error | silent'])}
`
}
