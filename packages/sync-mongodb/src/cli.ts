#!/usr/bin/env node
/**
 * workos-sync-mongodb — command line interface.
 *
 *   workos-sync-mongodb init [--repair]       create the collections, indexes and views; --repair
 *                                             redefines generated views whose pipeline changed
 *   workos-sync-mongodb backfill [--resume] [--max-pages <n>]
 *                                             page the list endpoints, optionally n pages at a time
 *   workos-sync-mongodb listen                tail the Events API
 *   workos-sync-mongodb tick                  one poll, then exit (for cron)
 *   workos-sync-mongodb reconcile [--repair]  detect (and optionally fix) drift
 *   workos-sync-mongodb refresh               re-list to refresh partial event fields
 *   workos-sync-mongodb status                cursor age and per-stream freshness
 *   workos-sync-mongodb redact --user <id>    erase one subject
 *   workos-sync-mongodb rejected [--replay]   quarantined events; replay them once fixed
 *   workos-sync-mongodb streams               list what this build can mirror
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
import { X509Certificate } from 'node:crypto'
import type { MongoClient } from 'mongodb'
import { MongoDestination } from './destination.js'
import { healthOptionsFromEnv } from './health.js'
import { startOtel } from './otel.js'
import { DEFAULT_PREFIX } from './schema/names.js'

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
 * written to every document as `_env`. It is yours to choose because no API
 * call reports which environment a key belongs to, and it must stay stable
 * across key rotation. To mirror several environments into one database, run
 * one process per key with a different label each; cursors are kept per label.
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
 * Unset means the engine default: off.
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
 * `--repair` on `init` means redefining generated `_live` views; on
 * `reconcile` it means repairing rows. Sharing the flag name is deliberate —
 * it is "fix what you found" in both — but each command reads only its own
 * meaning.
 */
export function repairRequested(command: string, flags: Record<string, string | boolean>): boolean {
  return command === 'init' && Boolean(flags['repair'])
}

const PEM_CERTIFICATE = /-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----/g

/** True when `text` holds at least one PEM certificate and every one of them parses. */
function isCertificateBundle(text: string): boolean {
  const blocks = text.match(PEM_CERTIFICATE)
  if (!blocks) return false
  try {
    for (const block of blocks) new X509Certificate(block)
    return true
  } catch {
    return false
  }
}

/**
 * `WORKOS_SYNC_MONGODB_TLS_CA` is the CA certificate the database's
 * certificate must chain to: PEM, or the PEM encoded as base64 for a secret
 * store that cannot hold a multi-line value. A PEM whose newlines arrived as
 * the two characters `\n` is straightened out, since that is how some hosts
 * hand back a pasted certificate. Unset or empty means the driver's own
 * defaults. Every certificate in the value is parsed, so a truncated or
 * damaged one is refused here rather than handed to the driver, where it
 * would surface as a handshake failure against the server and read as the
 * server's fault rather than the configuration's.
 */
export function parseSslCa(value: string | undefined): string | undefined {
  if (!value || !value.trim()) return undefined
  const v = value.trim().replace(/\\n/g, '\n')
  if (isCertificateBundle(v)) return v
  const decoded = Buffer.from(v, 'base64').toString('utf8')
  if (isCertificateBundle(decoded)) return decoded
  throw new Error(
    'invalid WORKOS_SYNC_MONGODB_TLS_CA: expected one or more PEM certificates, or them encoded as base64',
  )
}

function databaseFromFlags(flags: Record<string, string | boolean>): string | undefined {
  const v = (flags['database'] as string | undefined) ?? process.env['WORKOS_SYNC_DATABASE']
  return v && v.trim() ? v.trim() : undefined
}

function prefixFromFlags(flags: Record<string, string | boolean>): string {
  return (flags['prefix'] as string) ?? process.env['WORKOS_SYNC_PREFIX'] ?? DEFAULT_PREFIX
}

function streamsFromFlags(flags: Record<string, string | boolean>): string[] | undefined {
  return typeof flags['streams'] === 'string' ? flags['streams'].split(',') : undefined
}

/**
 * Built apart from the rest of the config because `init` reads the report
 * back off it after the engine has run; the engine's own `init()` returns
 * nothing. `client` is a test seam: a pre-built client in place of a
 * connection string, so the CLI can be exercised end to end.
 */
function destinationFromEnv(
  flags: Record<string, string | boolean>,
  repair: boolean,
  client?: MongoClient,
): MongoDestination {
  const url = (flags['database-url'] as string) ?? process.env['DATABASE_URL']
  if (!client && !url) throw new Error('set DATABASE_URL (or pass --database-url)')
  return new MongoDestination({
    url,
    sslCa: parseSslCa(process.env['WORKOS_SYNC_MONGODB_TLS_CA']),
    client,
    database: databaseFromFlags(flags),
    prefix: prefixFromFlags(flags),
    syncId: process.env['WORKOS_SYNC_ID'] ?? 'default',
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

/**
 * `--max-pages <n>` is a positive integer. Absent means unbounded. A bare
 * `--max-pages` with no value, zero, or a fraction is refused here, before the
 * engine connects: a budget of nothing would stop before the first page and
 * look like a backfill that found nothing.
 */
export function parseMaxPages(value: string | boolean | undefined): number | undefined {
  if (value === undefined || value === false) return undefined
  const n = typeof value === 'string' ? Number(value.trim()) : NaN
  if (typeof value !== 'string' || !value.trim() || !Number.isInteger(n) || n < 1) {
    throw new Error(`invalid --max-pages "${String(value)}": expected a positive integer`)
  }
  return n
}

function configFromEnv(
  flags: Record<string, string | boolean>,
  destination: MongoDestination,
): SyncEngineConfig & { logger: Logger } {
  const streams = streamsFromFlags(flags)
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

const HELP = `workos-sync-mongodb — mirror WorkOS data into MongoDB

Usage
  workos-sync-mongodb <command> [flags]

Commands
  init [--repair]      Create the collections, indexes, _live views and bookkeeping.
                       Fails if a generated _live view has a pipeline that is not the
                       generated one; --repair redefines it
  backfill [--resume] [--max-pages <n>]
                       Load current state from the REST list endpoints.
                       --resume continues each stream from its checkpoint and
                       skips streams already complete; --max-pages stops after
                       n pages across all streams, for hosts with a time limit
                       (run again with --resume until every stream is complete)
  listen [--health-port <n>]
                       Tail the Events API and apply changes continuously
                       (waits while another listener holds the lease);
                       with a port, serve GET /healthz and /metrics on it
  tick                 One poll of the Events API, then exit (for a cron job);
                       exits 3 if a listener holds the lease
  reconcile [--repair] [--resume] [--max-pages <n>]
                       Compare the mirror against the API; --repair fixes drift.
                       --max-pages stops starting streams once n pages have been
                       fetched (a stream that starts runs to its last page, since
                       a diff needs the whole listing); run again with --resume
                       until every stream reports complete
  refresh [--resume] [--max-pages <n>]
                       Re-list top-level streams, replacing each document;
                       --max-pages and --resume slice it like backfill, with
                       which it shares checkpoints
  status               Cursor position, poll age, and per-stream freshness
  redact --user <id>   Delete one subject from the mirror and event archive
  rejected             Show events quarantined by the tail; exits 2 if there are any
           --replay    Re-apply them all (or one with --id <event_id>) once the cause is fixed
  streams              Show the streams this build can mirror

Flags
  --database-url <url>   MongoDB connection string (or DATABASE_URL)
  --database <name>      Database the mirror lives in (or WORKOS_SYNC_DATABASE;
                         default: the one the connection string names, else workos)
  --prefix <prefix>      Collection prefix (or WORKOS_SYNC_PREFIX; default: workos_)
  --streams <a,b,c>      Restrict to specific streams (events is always included)

Environment
  DATABASE_URL                 MongoDB connection string (mongodb:// or mongodb+srv://)
  WORKOS_SYNC_MONGODB_TLS_CA   CA certificate the database's certificate must chain to, as PEM
                               or base64 of it; TLS is then required and verified against it
                               (default: the system trust store)
  WORKOS_API_KEY               API key for the environment to mirror
  WORKOS_ENVIRONMENT_NAME      Label written to _env on every document (default: production)
  WORKOS_API_URL               API base URL (default: https://api.workos.com)
  WORKOS_SYNC_DATABASE         Database the mirror lives in (default: from the connection string)
  WORKOS_SYNC_PREFIX           Collection prefix (default: workos_)
  WORKOS_SYNC_ID               Sync identifier when several syncs share a database (default: default)
  WORKOS_SYNC_HYDRATE          off | on_create | always, or users=always,organizations=off
                               (default: off)
  WORKOS_SYNC_POLL_MS          Idle wait between polls (default: 5000)
  WORKOS_SYNC_LAG_MS           Hold back events younger than this (default: 5000)
  WORKOS_SYNC_MAX_EVENTS_PER_TICK  Events to apply per poll (default: one page)
  WORKOS_SYNC_LEASE_TTL_MS     How long the tail lease outlives a crashed listener, at least 1 (default: 60000)
  WORKOS_SYNC_HEALTH_PORT      listen only: serve /healthz and /metrics on this port (default: off)
  WORKOS_SYNC_HEALTH_HOST      Interface the health server binds (default: 0.0.0.0)
  WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS
                               /healthz returns 503 once any environment's last successful
                               poll is older than this (default: 60000)
  WORKOS_SYNC_OTEL             1 starts the OpenTelemetry Node SDK for the command, configured by
                               the OTEL_* variables, and flushes it on exit (default: off)
  LOG_LEVEL                    debug | info | warn | error | silent
`

export interface MainDeps {
  /** A pre-built client in place of `DATABASE_URL`, so tests can run the CLI against a test server. */
  client?: MongoClient
  /** A whole engine in place of one built from the environment, for tests of commands that need faults injected. */
  engine?: SyncEngine
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

  // The SDK, when asked for, goes up before the engine is built, so that the
  // instruments the engine makes at construction are real ones, and comes
  // down after the command returns, which is what exports a one-shot
  // command's metrics at all.
  const otel = await startOtel()
  try {
    return await runCommand(command, flags, deps)
  } finally {
    await otel?.shutdown()
  }
}

async function runCommand(command: string, flags: Record<string, string | boolean>, deps: MainDeps): Promise<number> {
  // With an injected engine there is no destination of ours to read the
  // report from, and nothing to build from the environment.
  const destination = deps.engine ? undefined : destinationFromEnv(flags, repairRequested(command, flags), deps.client)
  const config = destination ? configFromEnv(flags, destination) : undefined
  const engine = deps.engine ?? new SyncEngine(config!)
  const logger = config?.logger ?? createLogger((process.env['LOG_LEVEL'] as LogLevel) ?? 'info')
  try {
    switch (command) {
      case 'init': {
        await engine.init()
        const orphans = destination?.lastMigration?.orphans ?? []
        const views = destination?.lastMigration?.views ?? []
        // Orphans are advisory: an index the catalog no longer calls for is
        // still serving whatever query it was made for.
        if (orphans.length > 0 || views.length > 0) {
          process.stdout.write(JSON.stringify({ views, orphans }, null, 2) + '\n')
        }
        process.stdout.write('schema ready\n')
        return 0
      }
      case 'backfill': {
        const results = await engine.backfill({
          resume: Boolean(flags['resume']),
          maxPages: parseMaxPages(flags['max-pages']),
        })
        process.stdout.write(JSON.stringify(results, null, 2) + '\n')
        // A budget that stops short is the outcome asked for, not an error:
        // the JSON says which streams are complete, and the next run resumes.
        return 0
      }
      case 'listen': {
        // Up before the engine connects, so a probe during a slow schema init
        // sees "starting" rather than a refused connection.
        const healthOptions = healthOptionsFromEnv(flags)
        const health = healthOptions ? await startHealthServer(engine, healthOptions) : null
        if (healthOptions && health) {
          logger.info('health server listening', {
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
        const r = await engine.reconcile({
          repair: Boolean(flags['repair']),
          resume: Boolean(flags['resume']),
          maxPages: parseMaxPages(flags['max-pages']),
        })
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        // A non-zero exit makes this usable as a scheduled check.
        const drift = Object.values(r.diffs)
          .flat()
          .some((d) => d.localOnly.length > 0 || d.remoteOnly.length > 0)
        return drift && !flags['repair'] ? 2 : 0
      }
      case 'refresh': {
        const r = await engine.refresh({
          resume: Boolean(flags['resume']),
          maxPages: parseMaxPages(flags['max-pages']),
        })
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
      case 'rejected': {
        const id = flags['id']
        if (id !== undefined && typeof id !== 'string') throw new Error('--id requires an event id')
        const only = typeof id === 'string' ? { ids: [id] } : {}
        if (flags['replay']) {
          const r = await engine.replayRejected(only)
          process.stdout.write(JSON.stringify(r, null, 2) + '\n')
          // Anything still rejected after a replay is still a problem.
          return r.stillRejected.length > 0 ? 2 : 0
        }
        const r = await engine.rejected(only)
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        // Like `reconcile`: a non-zero exit makes this a scheduled check.
        const any = Object.values(r).some((rows) => rows.length > 0)
        return any ? 2 : 0
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
const invokedDirectly = process.argv[1] && (process.argv[1].endsWith('cli.js') || process.argv[1].endsWith('cli.ts'))
if (invokedDirectly) {
  main().then(
    (code) => process.exit(code),
    (err) => {
      process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`)
      process.exit(1)
    },
  )
}
