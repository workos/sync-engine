#!/usr/bin/env node
/**
 * workos-pg-sync — command line interface.
 *
 *   workos-pg-sync init                 create the schema
 *   workos-pg-sync backfill [--resume]  page the list endpoints
 *   workos-pg-sync listen               tail the Events API
 *   workos-pg-sync reconcile [--repair] detect (and optionally fix) drift
 *   workos-pg-sync refresh              re-list to refresh partial event fields
 *   workos-pg-sync status               cursor age and per-stream freshness
 *   workos-pg-sync redact --user <id>   erase one subject
 *   workos-pg-sync streams              list what this build can mirror
 */
import { SyncEngine } from './engine.js'
import { STREAMS } from './catalog.js'
import { createLogger, type LogLevel } from './logger.js'
import type { EnvironmentConfig, SyncEngineConfig } from './types.js'

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
 * Environments come from `WORKOS_API_KEY` for the simple case, or
 * `WORKOS_ENVIRONMENTS` as `name=key,name=key` when mirroring several into one
 * database. A workspace can hold far more than the two you might assume, and
 * environment display names repeat across projects, so the label here is yours
 * to choose and must stay stable — it is written to every row.
 */
function environmentsFromEnv(): EnvironmentConfig[] {
  const multi = process.env['WORKOS_ENVIRONMENTS']
  if (multi) {
    return multi.split(',').map((pair) => {
      const idx = pair.indexOf('=')
      if (idx < 1) throw new Error(`invalid WORKOS_ENVIRONMENTS entry: ${pair}`)
      return {
        name: pair.slice(0, idx).trim(),
        apiKey: pair.slice(idx + 1).trim(),
        apiUrl: process.env['WORKOS_API_URL'],
      }
    })
  }
  const apiKey = process.env['WORKOS_API_KEY']
  if (!apiKey) throw new Error('set WORKOS_API_KEY (or WORKOS_ENVIRONMENTS)')
  return [
    {
      name: process.env['WORKOS_ENVIRONMENT_NAME'] ?? 'production',
      apiKey,
      apiUrl: process.env['WORKOS_API_URL'],
    },
  ]
}

function configFromEnv(flags: Record<string, string | boolean>): SyncEngineConfig {
  const databaseUrl = (flags['database-url'] as string) ?? process.env['DATABASE_URL']
  if (!databaseUrl) throw new Error('set DATABASE_URL (or pass --database-url)')
  const streams = typeof flags['streams'] === 'string' ? flags['streams'].split(',') : undefined
  return {
    databaseUrl,
    schema: (flags['schema'] as string) ?? process.env['WORKOS_SYNC_SCHEMA'] ?? 'workos',
    environments: environmentsFromEnv(),
    streams,
    hydrate: (process.env['WORKOS_SYNC_HYDRATE'] as never) ?? { users: 'on_create', organizations: 'on_create' },
    pollIntervalMs: Number(process.env['WORKOS_SYNC_POLL_MS'] ?? 5000),
    lagWindowMs: Number(process.env['WORKOS_SYNC_LAG_MS'] ?? 5000),
    syncId: process.env['WORKOS_SYNC_ID'] ?? 'default',
    logger: createLogger((process.env['LOG_LEVEL'] as LogLevel) ?? 'info'),
  }
}

const HELP = `workos-pg-sync — mirror WorkOS data into Postgres

Usage
  workos-pg-sync <command> [flags]

Commands
  init                 Create the schema and bookkeeping tables
  backfill [--resume]  Load current state from the REST list endpoints
  listen               Tail the Events API and apply changes continuously
  reconcile [--repair] Compare the mirror against the API; --repair fixes drift
  refresh              Re-list every stream (refreshes fields events omit)
  status               Cursor position, age, and per-stream freshness
  redact --user <id>   Delete one subject from the mirror and event archive
  streams              Show the streams this build can mirror

Flags
  --database-url <url>   Postgres connection string (or DATABASE_URL)
  --schema <name>        Target schema (default: workos)
  --streams <a,b,c>      Restrict to specific streams

Environment
  DATABASE_URL           Postgres connection string
  WORKOS_API_KEY         API key for a single environment
  WORKOS_ENVIRONMENTS    name=key,name=key to mirror several environments
  WORKOS_SYNC_SCHEMA     Target schema (default: workos)
  LOG_LEVEL              debug | info | warn | error | silent
`

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const { command, flags } = parseArgs(argv)

  if (command === 'help' || flags['help']) {
    process.stdout.write(HELP)
    return 0
  }

  if (command === 'streams') {
    const rows = STREAMS.map((s) => ({
      stream: s.name,
      tier: s.tier,
      mode: s.mode,
      backfill: s.list ? 'yes' : 'no',
      events: (s.events?.upsert?.length ?? 0) + (s.events?.delete?.length ?? 0),
    }))
    process.stdout.write(JSON.stringify(rows, null, 2) + '\n')
    return 0
  }

  const engine = new SyncEngine(configFromEnv(flags))
  try {
    switch (command) {
      case 'init': {
        await engine.init()
        process.stdout.write('schema ready\n')
        return 0
      }
      case 'backfill': {
        const results = await engine.backfill({ resume: Boolean(flags['resume']) })
        process.stdout.write(JSON.stringify(results, null, 2) + '\n')
        return 0
      }
      case 'listen': {
        const shutdown = () => {
          engine.stop()
        }
        process.on('SIGINT', shutdown)
        process.on('SIGTERM', shutdown)
        await engine.listen()
        return 0
      }
      case 'tick': {
        const r = await engine.tick()
        process.stdout.write(JSON.stringify(r, null, 2) + '\n')
        return 0
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
