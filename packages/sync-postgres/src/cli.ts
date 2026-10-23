#!/usr/bin/env node
/**
 * workos-sync-postgres — command line interface.
 *
 *   workos-sync-postgres init [--repair]      create the schema; --repair rebuilds drifted columns and generated views
 *   workos-sync-postgres backfill [--resume]  page the list endpoints
 *   workos-sync-postgres listen               tail the Events API
 *   workos-sync-postgres tick                 one poll, then exit (for cron)
 *   workos-sync-postgres reconcile [--repair] detect (and optionally fix) drift
 *   workos-sync-postgres refresh              re-list to refresh partial event fields
 *   workos-sync-postgres status               cursor age and per-stream freshness
 *   workos-sync-postgres redact --user <id>   erase one subject
 *   workos-sync-postgres streams              list what this build can mirror
 */
import {
  SyncEngine,
  LeaseHeldError,
  STREAMS,
  DEFAULT_HYDRATE,
  defaultStreams,
  createLogger,
  startHealthServer,
  type EnvironmentConfig,
  type HydrateMode,
  type Logger,
  type LogLevel,
  type SyncEngineConfig,
} from '@workos/sync-engine'
import { PostgresDestination } from './destination.js'
import { healthOptionsFromEnv } from './health.js'
import type { SqlClient } from './db/client.js'

interface Args {
  command: string
  flags: Record<string, string | boolean>
}

function parseArgs(argv: string[]): Args {
  const [command = 'help', ...rest] = argv
  const flags: Record<string, string | boolean> = {}
  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i]!
    if (!arg.startsWith('--')) continue
    const key = arg.slice(2)
    const next = rest[i + 1]
    if (next && !next.startsWith('--')) {
      flags[key] = next
      i++
    } else {
      flags[key] = true
    }
  }
  return { command, flags }
}

/**
 * One key, one environment, one process. `WORKOS_ENVIRONMENT_NAME` is the label
 * written to every row as `_env`. It is yours to choose because no API call
 * reports which environment a key belongs to, and it must stay stable across
 * key rotation. To mirror several environments into one database, run one
 * process per key with a different label each; cursors are kept per label.
 */
function environmentsFromEnv(): EnvironmentConfig[] {
  const apiKey = process.env['WORKOS_API_KEY']
  if (!apiKey) throw new Error('set WORKOS_API_KEY')
  return [
    {
      name: process.env['WORKOS_ENVIRONMENT_NAME'] ?? 'production',
      apiKey,
      apiUrl: process.env['WORKOS_API_URL'],
    },
  ]
}

const HYDRATE_MODES: HydrateMode[] = ['off', 'on_create', 'always']

/**
 * `WORKOS_SYNC_HYDRATE` is either one mode for every stream (`off`,
 * `on_create`, `always`) or a per-stream list (`users=always,organizations=off`).
 * Unset means the engine default: users and organizations on create.
 */
export function parseHydrate(value: string | undefined): SyncEngineConfig['hydrate'] {
  if (!value || !value.trim()) return undefined
  const v = value.trim()
  if ((HYDRATE_MODES as string[]).includes(v)) return v as HydrateMode
  const out: Record<string, HydrateMode> = { ...DEFAULT_HYDRATE }
  for (const pair of v.split(',')) {
    const idx = pair.indexOf('=')
    const stream = pair.slice(0, idx).trim()
    const mode = pair.slice(idx + 1).trim()
    if (idx < 1 || !(HYDRATE_MODES as string[]).includes(mode)) {
      throw new Error(
        `invalid WORKOS_SYNC_HYDRATE entry "${pair}": expected off | on_create | always, or stream=mode,...`,
      )
    }
    out[stream] = mode as HydrateMode
  }
  return out
}

/**
 * `--repair` on `init` means rebuilding drifted columns from `_raw` and
 * rebuilding generated `_live` views with their grants; on `reconcile` it
 * means repairing rows. Sharing the flag name is deliberate — it is "fix what
 * you found" in both — but each command reads only its own meaning.
 */
export function repairRequested(command: string, flags: Record<string, string | boolean>): boolean {
  return command === 'init' && Boolean(flags['repair'])
}

/**
 * Built apart from the rest of the config because `init` reads the drift
 * report back off it after the engine has run; the engine's own `init()`
 * returns nothing. `db` is a test seam: a pre-built client (PGlite) in place
 * of a connection string, so the CLI can be exercised end to end.
 */
function destinationFromEnv(
  flags: Record<string, string | boolean>,
  repair: boolean,
  db?: SqlClient,
): PostgresDestination {
  const databaseUrl = (flags['database-url'] as string) ?? process.env['DATABASE_URL']
  if (!db && !databaseUrl) throw new Error('set DATABASE_URL (or pass --database-url)')
  return new PostgresDestination({
    databaseUrl,
    db,
    schema: (flags['schema'] as string) ?? process.env['WORKOS_SYNC_SCHEMA'] ?? 'workos',
    syncId: process.env['WORKOS_SYNC_ID'] ?? 'default',
    repairColumns: repair,
    repairViews: repair,
  })
}

/**
 * `WORKOS_SYNC_LEASE_TTL_MS` is a number of milliseconds, at least 1. Unset
 * or empty means the engine default. Anything else is refused here rather
 * than further down: an empty value would otherwise become zero, and a lease
 * that expires on arrival excludes nobody.
 */
export function parseLeaseTtl(value: string | undefined): number | undefined {
  if (!value || !value.trim()) return undefined
  const n = Number(value)
  if (!Number.isFinite(n) || Math.floor(n) < 1) {
    throw new Error(`invalid WORKOS_SYNC_LEASE_TTL_MS "${value}": expected a number of milliseconds of at least 1`)
  }
  return n
}

function configFromEnv(
  flags: Record<string, string | boolean>,
  destination: PostgresDestination,
): SyncEngineConfig & { logger: Logger } {
  const streams = typeof flags['streams'] === 'string' ? flags['streams'].split(',') : undefined
  return {
    destination,
    environments: environmentsFromEnv(),
    streams,
    hydrate: parseHydrate(process.env['WORKOS_SYNC_HYDRATE']),
    pollIntervalMs: Number(process.env['WORKOS_SYNC_POLL_MS'] ?? 5000),
    lagWindowMs: Number(process.env['WORKOS_SYNC_LAG_MS'] ?? 5000),
    maxEventsPerTick: process.env['WORKOS_SYNC_MAX_EVENTS_PER_TICK']
      ? Number(process.env['WORKOS_SYNC_MAX_EVENTS_PER_TICK'])
      : undefined,
    leaseTtlMs: parseLeaseTtl(process.env['WORKOS_SYNC_LEASE_TTL_MS']),
    logger: createLogger((process.env['LOG_LEVEL'] as LogLevel) ?? 'info'),
  }
}

const HELP = `workos-sync-postgres — mirror WorkOS data into Postgres

Usage
  workos-sync-postgres <command> [flags]

Commands
  init [--repair]      Create the schema and bookkeeping tables. Exits 2 if a generated
                       column has drifted from the catalog, and fails if a generated _live
                       view must be rebuilt; --repair rebuilds columns from _raw and views
                       with their grants carried over
  backfill [--resume]  Load current state from the REST list endpoints
  listen [--health-port <n>]
                       Tail the Events API and apply changes continuously
                       (waits while another listener holds the lease);
                       with a port, serve GET /healthz and /metrics on it
  tick                 One poll of the Events API, then exit (for a cron job);
                       exits 3 if a listener holds the lease
  reconcile [--repair] Compare the mirror against the API; --repair fixes drift
  refresh              Re-list top-level streams (refreshes fields events omit)
  status               Cursor position, poll age, and per-stream freshness
  redact --user <id>   Delete one subject from the mirror and event archive
  streams              Show the streams this build can mirror

Flags
  --database-url <url>   Postgres connection string (or DATABASE_URL)
  --schema <name>        Target schema (or WORKOS_SYNC_SCHEMA; default: workos)
  --streams <a,b,c>      Restrict to specific streams (events is always included)

Environment
  DATABASE_URL                 Postgres connection string
  WORKOS_API_KEY               API key for the environment to mirror
  WORKOS_ENVIRONMENT_NAME      Label written to _env on every row (default: production)
  WORKOS_API_URL               API base URL (default: https://api.workos.com)
  WORKOS_SYNC_SCHEMA           Target schema (default: workos)
  WORKOS_SYNC_ID               Sync identifier when several syncs share a database (default: default)
  WORKOS_SYNC_HYDRATE          off | on_create | always, or users=always,organizations=off
                               (default: users and organizations on_create)
  WORKOS_SYNC_POLL_MS          Idle wait between polls (default: 5000)
  WORKOS_SYNC_LAG_MS           Hold back events younger than this (default: 5000)
  WORKOS_SYNC_MAX_EVENTS_PER_TICK  Events to apply per poll (default: one page)
  WORKOS_SYNC_LEASE_TTL_MS     How long the tail lease outlives a crashed listener, at least 1 (default: 60000)
  WORKOS_SYNC_HEALTH_PORT      listen only: serve /healthz and /metrics on this port (default: off)
  WORKOS_SYNC_HEALTH_HOST      Interface the health server binds (default: 0.0.0.0)
  WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS
                               /healthz returns 503 once any environment's last successful
                               poll is older than this (default: 60000)
  LOG_LEVEL                    debug | info | warn | error | silent
`

export interface MainDeps {
  /** A pre-built client in place of `DATABASE_URL`, so tests can run the CLI against PGlite. */
  db?: SqlClient
}

export async function main(argv = process.argv.slice(2), deps: MainDeps = {}): Promise<number> {
  const { command, flags } = parseArgs(argv)

  if (command === 'help' || flags['help']) {
    process.stdout.write(HELP)
    return 0
  }

  if (command === 'streams') {
    const defaults = new Set(defaultStreams())
    const rows = STREAMS.map((s) => ({
      stream: s.name,
      group: s.group,
      mode: s.mode,
      default: defaults.has(s.name) ? 'yes' : 'no (opt-in)',
      backfill: s.list ? 'yes' : s.name === 'organization_domains' ? 'from organizations' : 'no',
      events: (s.events?.upsert?.length ?? 0) + (s.events?.delete?.length ?? 0),
    }))
    process.stdout.write(JSON.stringify(rows, null, 2) + '\n')
    return 0
  }

  const destination = destinationFromEnv(flags, repairRequested(command, flags), deps.db)
  const config = configFromEnv(flags, destination)
  const engine = new SyncEngine(config)
  try {
    switch (command) {
      case 'init': {
        await engine.init()
        const drift = destination.lastMigration?.drift ?? []
        const repaired = destination.lastMigration?.repaired ?? []
        if (drift.length > 0 || repaired.length > 0) {
          process.stdout.write(JSON.stringify({ repaired, drift }, null, 2) + '\n')
        }
        // Orphans are advisory. Anything else means a projection is wrong and
        // the fingerprint was withheld, so exit 2 the way `reconcile` does.
        const unresolved = drift.some((d) => d.kind !== 'orphan')
        if (!unresolved) process.stdout.write('schema ready\n')
        return unresolved ? 2 : 0
      }
      case 'backfill': {
        const results = await engine.backfill({ resume: Boolean(flags['resume']) })
        process.stdout.write(JSON.stringify(results, null, 2) + '\n')
        return 0
      }
      case 'listen': {
        // Up before the engine connects, so a probe during a slow schema init
        // sees "starting" rather than a refused connection.
        const healthOptions = healthOptionsFromEnv(flags)
        const health = healthOptions ? await startHealthServer(engine, healthOptions) : null
        if (healthOptions && health) {
          config.logger.info('health server listening', {
            host: healthOptions.host,
            port: health.port,
            maxPollAgeMs: healthOptions.maxPollAgeMs,
          })
        }
        const shutdown = () => {
          engine.stop()
        }
        process.on('SIGINT', shutdown)
        process.on('SIGTERM', shutdown)
        try {
          await engine.listen()
        } finally {
          await health?.close()
        }
        return 0
      }
      case 'tick': {
        try {
          const r = await engine.tick()
          process.stdout.write(JSON.stringify(r, null, 2) + '\n')
          return 0
        } catch (err) {
          // Distinct from a failure (1) and from drift (2): the sync is fine,
          // something else is tailing it. Usually a `listen` left running.
          if (err instanceof LeaseHeldError) {
            process.stderr.write(`${err.message}; nothing to do\n`)
            return 3
          }
          throw err
        }
      }
      case 'reconcile': {
        const r = await engine.reconcile({ repair: Boolean(flags['repair']) })
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        // A non-zero exit makes this usable as a scheduled check.
        const drift = Object.values(r.diffs)
          .flat()
          .some((d) => d.localOnly.length > 0 || d.remoteOnly.length > 0)
        return drift && !flags['repair'] ? 2 : 0
      }
      case 'refresh': {
        const r = await engine.refresh()
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        return 0
      }
      case 'status': {
        const r = await engine.status()
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        return 0
      }
      case 'redact': {
        const user = flags['user']
        if (typeof user !== 'string') throw new Error('redact requires --user <id>')
        const r = await engine.redact(user)
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        return 0
      }
      default:
        process.stderr.write(`unknown command: ${command}\n\n${HELP}`)
        return 1
    }
  } finally {
    await engine.close()
  }
}

// Run when invoked directly, not when imported by a test.
const invokedDirectly =
  process.argv[1] && (process.argv[1].endsWith('cli.js') || process.argv[1].endsWith('cli.ts'))
if (invokedDirectly) {
  main().then(
    (code) => process.exit(code),
    (err) => {
      process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`)
      process.exit(1)
    },
  )
}
