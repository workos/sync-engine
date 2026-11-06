/**
 * The command line every destination ships, run over a spec of what differs:
 * how to build the destination, what `init` reports, and the words the help
 * text needs. The commands, their flags, their exit codes and the way the
 * environment is read are the same for every database, so they live here.
 *
 * Exit codes: 0 done; 1 failed; 2 found something (drift, quarantined
 * events, a projection that needs attention); 3 another process holds the
 * tail lease.
 */
import { SyncEngine, LeaseHeldError } from '../engine.js'
import type { Destination } from '../destination.js'
import { createLogger } from '../logger.js'
import type { LogLevel } from '../logger.js'
import { STREAMS, defaultStreams } from '../catalog.js'
import { startHealthServer } from '../health.js'
import {
  engineConfigFromEnv,
  parseArgs,
  parseExcludeStreams,
  parseMaxPages,
  parseSslCa,
  repairRequested,
  streamsFromFlags,
  type CliFlags,
} from './flags.js'
import { healthOptionsFromEnv } from './health-options.js'
import { renderHelp, type HelpSpec } from './help.js'
import { startOtel } from './otel.js'

/** What the spec gets to build its destination with: everything read the same way for every database. */
export interface DestinationOptions<TClient> {
  /** `--database-url`, or `DATABASE_URL`. One of this and `db` is present. */
  databaseUrl: string | undefined
  /** The CA certificate from the spec's variable, already checked. */
  sslCa: string | undefined
  /** A pre-built client in place of the connection string: the test seam. */
  db: TClient | undefined
  /** `WORKOS_SYNC_ID`. */
  syncId: string
  /** `init --repair`. */
  repair: boolean
}

/** What `init` prints once the destination has run, and whether it should exit 2. */
export interface InitReport {
  output?: unknown
  failed: boolean
}

export interface CliSpec<TClient, TDestination extends Destination> extends HelpSpec {
  /** The `service.name` reported to OpenTelemetry unless `OTEL_SERVICE_NAME` says otherwise. */
  serviceName: string
  /** `init --print-sql`: the DDL `init` would run, when the destination has DDL to print. */
  printSchemaSql?: (streamNames: string[] | undefined, flags: CliFlags) => string
  /** Build the destination from the flags and what was read from the environment. */
  destination(flags: CliFlags, options: DestinationOptions<TClient>): TDestination
  /** What `init` reports after the destination ran: drifted columns, replaced views. Nothing by default. */
  initReport?(destination: TDestination): InitReport
}

export interface MainDeps<TClient = unknown> {
  /** A pre-built client in place of `DATABASE_URL`, so tests can run the command against a test database. */
  db?: TClient
  /** A whole engine in place of one built from the environment, for tests of commands that need faults injected. */
  engine?: SyncEngine
}

export async function runCli<TClient, TDestination extends Destination>(
  spec: CliSpec<TClient, TDestination>,
  argv: string[] = process.argv.slice(2),
  deps: MainDeps<TClient> = {},
): Promise<number> {
  const { command, flags } = parseArgs(argv)
  const help = renderHelp(spec)

  if (command === 'help' || flags['help']) {
    process.stdout.write(help)
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

  // The DDL alone, before anything that would need a database or a key.
  if (command === 'init' && flags['print-sql'] && spec.printSchemaSql) {
    process.stdout.write(spec.printSchemaSql(streamsFromFlags(flags), flags))
    return 0
  }

  // The SDK, when asked for, goes up before the engine is built, so that the
  // instruments the engine makes at construction are real ones, and comes
  // down after the command returns, which is what exports a one-shot
  // command's metrics at all.
  const otel = await startOtel(spec.serviceName)
  try {
    return await runCommand(spec, command, flags, deps, help)
  } finally {
    await otel?.shutdown()
  }
}

function destinationFromEnv<TClient, TDestination extends Destination>(
  spec: CliSpec<TClient, TDestination>,
  command: string,
  flags: CliFlags,
  db: TClient | undefined,
): TDestination {
  const databaseUrl = (flags['database-url'] as string | undefined) ?? process.env['DATABASE_URL']
  if (!db && !databaseUrl) throw new Error('set DATABASE_URL (or pass --database-url)')
  return spec.destination(flags, {
    databaseUrl,
    sslCa: parseSslCa(process.env[spec.tlsCaVar], spec.tlsCaVar),
    db,
    syncId: process.env['WORKOS_SYNC_ID'] ?? 'default',
    repair: repairRequested(command, flags),
  })
}

async function runCommand<TClient, TDestination extends Destination>(
  spec: CliSpec<TClient, TDestination>,
  command: string,
  flags: CliFlags,
  deps: MainDeps<TClient>,
  help: string,
): Promise<number> {
  // With an injected engine there is no destination of ours to read a
  // report from, and nothing to build from the environment.
  const destination = deps.engine ? undefined : destinationFromEnv(spec, command, flags, deps.db)
  const config = destination ? engineConfigFromEnv(flags, destination) : undefined
  const engine = deps.engine ?? new SyncEngine(config!)
  const logger = config?.logger ?? createLogger((process.env['LOG_LEVEL'] as LogLevel) ?? 'info')
  const out = (value: unknown) => process.stdout.write(JSON.stringify(value, null, 2) + '\n')
  try {
    switch (command) {
      case 'init': {
        await engine.init()
        const report = destination && spec.initReport ? spec.initReport(destination) : { failed: false }
        if (report.output !== undefined) out(report.output)
        if (!report.failed) process.stdout.write('schema ready\n')
        // Something needs attention and the fingerprint was withheld: exit 2
        // the way `reconcile` does.
        return report.failed ? 2 : 0
      }
      case 'backfill': {
        out(await engine.backfill({ resume: Boolean(flags['resume']), maxPages: parseMaxPages(flags['max-pages']) }))
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
          out(await engine.tick())
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
          excludeStreams: parseExcludeStreams(flags['exclude-streams']),
        })
        out(r)
        // A non-zero exit makes this usable as a scheduled check.
        const drift = Object.values(r.diffs)
          .flat()
          .some((d) => d.localOnly.length > 0 || d.remoteOnly.length > 0)
        return drift && !flags['repair'] ? 2 : 0
      }
      case 'refresh': {
        out(await engine.refresh({ resume: Boolean(flags['resume']), maxPages: parseMaxPages(flags['max-pages']) }))
        return 0
      }
      case 'status': {
        out(await engine.status())
        return 0
      }
      case 'redact': {
        const user = flags['user']
        if (typeof user !== 'string') throw new Error('redact requires --user <id>')
        out(await engine.redact(user))
        return 0
      }
      case 'rejected': {
        const id = flags['id']
        if (id !== undefined && typeof id !== 'string') throw new Error('--id requires an event id')
        const only = typeof id === 'string' ? { ids: [id] } : {}
        if (flags['replay']) {
          const r = await engine.replayRejected(only)
          out(r)
          // Anything still rejected after a replay is still a problem.
          return r.stillRejected.length > 0 ? 2 : 0
        }
        const r = await engine.rejected(only)
        out(r)
        // Like `reconcile`: a non-zero exit makes this a scheduled check.
        return Object.values(r).some((rows) => rows.length > 0) ? 2 : 0
      }
      default:
        process.stderr.write(`unknown command: ${command}\n\n${help}`)
        return 1
    }
  } finally {
    await engine.close()
  }
}

/** The exit of a command run as the process entrypoint: the code, or the error's message and 1. */
export function exitWith(run: Promise<number>): void {
  run.then(
    (code) => process.exit(code),
    (err) => {
      process.stderr.write(`${err instanceof Error ? err.message : String(err)}\n`)
      process.exit(1)
    },
  )
}
