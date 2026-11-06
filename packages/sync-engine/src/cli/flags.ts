/**
 * The command line's flags and environment, read the same way by every
 * destination's command. Each value is checked here, before anything
 * connects, with a message that names the variable or flag at fault.
 */
import { X509Certificate } from 'node:crypto'
import { realpathSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { createLogger } from '../logger.js'
import type { LogLevel } from '../logger.js'
import type { Destination } from '../destination.js'
import { DEFAULT_HYDRATE } from '../types.js'
import type { EnvironmentConfig, HydrateMode, Logger, SyncEngineConfig } from '../types.js'

export type CliFlags = Record<string, string | boolean>

export interface ParsedArgs {
  command: string
  flags: CliFlags
}

/** `<command> [--flag value] [--switch]`: a flag followed by a value takes it, one followed by a flag or nothing is `true`. */
export function parseArgs(argv: string[]): ParsedArgs {
  const [command = 'help', ...rest] = argv
  const flags: CliFlags = {}
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

/** `--streams a,b,c`, or undefined for the catalog's defaults. */
export function streamsFromFlags(flags: CliFlags): string[] | undefined {
  return typeof flags['streams'] === 'string' ? flags['streams'].split(',') : undefined
}

/**
 * One key, one environment, one process. `WORKOS_ENVIRONMENT_NAME` is the label
 * written to every row as `_env`. It is yours to choose because no API call
 * reports which environment a key belongs to, and it must stay stable across
 * key rotation. To mirror several environments into one database, run one
 * process per key with a different label each; cursors are kept per label.
 */
export function environmentsFromEnv(env: NodeJS.ProcessEnv = process.env): EnvironmentConfig[] {
  const apiKey = env['WORKOS_API_KEY']
  if (!apiKey) throw new Error('set WORKOS_API_KEY')
  return [
    {
      name: env['WORKOS_ENVIRONMENT_NAME'] ?? 'production',
      apiKey,
      apiUrl: env['WORKOS_API_URL'],
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
 * `--repair` on `init` means rebuilding what drifted — columns from `_raw`,
 * generated `_live` views — and on `reconcile` it means repairing rows.
 * Sharing the flag name is deliberate: it is "fix what you found" in both.
 * Each command reads only its own meaning.
 */
export function repairRequested(command: string, flags: CliFlags): boolean {
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
 * The CA certificate the database's certificate must chain to, from the
 * variable `name` (`WORKOS_SYNC_PG_SSL_CA`, say): PEM, or the PEM encoded as
 * base64 for a secret store that cannot hold a multi-line value. A PEM whose
 * newlines arrived as the two characters `\n` is straightened out, since
 * that is how some hosts hand back a pasted certificate. Unset or empty
 * means the driver's own defaults. Every certificate in the value is parsed,
 * so a truncated or damaged one is refused here rather than handed to the
 * driver, where it would surface as a handshake failure against the server
 * and read as the server's fault rather than the configuration's.
 */
export function parseSslCa(value: string | undefined, name = 'the CA certificate'): string | undefined {
  if (!value || !value.trim()) return undefined
  const v = value.trim().replace(/\\n/g, '\n')
  if (isCertificateBundle(v)) return v
  const decoded = Buffer.from(v, 'base64').toString('utf8')
  if (isCertificateBundle(decoded)) return decoded
  throw new Error(`invalid ${name}: expected one or more PEM certificates, or them encoded as base64`)
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

/**
 * `--exclude-streams <a,b>` names streams `reconcile` leaves out, for one
 * whose listing has to run somewhere without a time limit. A bare flag is
 * refused here: excluding nothing by accident is the mistake the flag exists
 * to prevent. The names themselves are checked by the engine.
 */
export function parseExcludeStreams(value: string | boolean | undefined): string[] | undefined {
  if (value === undefined || value === false) return undefined
  const names =
    typeof value === 'string'
      ? value
          .split(',')
          .map((s) => s.trim())
          .filter(Boolean)
      : []
  if (names.length === 0) {
    throw new Error(`invalid --exclude-streams "${String(value)}": expected a comma-separated list of stream names`)
  }
  return names
}

/** The engine's configuration from the flags and the environment, around a destination already built. */
export function engineConfigFromEnv(
  flags: CliFlags,
  destination: Destination,
  env: NodeJS.ProcessEnv = process.env,
): SyncEngineConfig & { logger: Logger } {
  return {
    destination,
    environments: environmentsFromEnv(env),
    streams: streamsFromFlags(flags),
    hydrate: parseHydrate(env['WORKOS_SYNC_HYDRATE']),
    pollIntervalMs: Number(env['WORKOS_SYNC_POLL_MS'] ?? 5000),
    lagWindowMs: Number(env['WORKOS_SYNC_LAG_MS'] ?? 5000),
    maxEventsPerTick: env['WORKOS_SYNC_MAX_EVENTS_PER_TICK']
      ? Number(env['WORKOS_SYNC_MAX_EVENTS_PER_TICK'])
      : undefined,
    leaseTtlMs: parseLeaseTtl(env['WORKOS_SYNC_LEASE_TTL_MS']),
    logger: createLogger((env['LOG_LEVEL'] as LogLevel) ?? 'info'),
  }
}

/**
 * Whether `moduleUrl` is the script Node was started with, as opposed to an
 * import by a test. npm installs a `bin` as a symlink named after the
 * command, so `process.argv[1]` ends in `workos-sync-postgres` rather than
 * `cli.js`; both sides are resolved to real paths so the installed command
 * counts.
 */
export function invokedDirectly(argv1: string | undefined, moduleUrl: string): boolean {
  if (!argv1) return false
  try {
    return realpathSync(argv1) === realpathSync(fileURLToPath(moduleUrl))
  } catch {
    return false
  }
}
