/**
 * Health server settings for the `listen` command.
 *
 * Off unless a port is given, because the CLI also runs as one-shot commands
 * and a cron `tick`, where a listening socket would be a surprise. The Docker
 * image sets the port for the same reason it sets `NODE_ENV`: a container is
 * the deployment where something external wants to ask whether the process is
 * alive. Everything the server needs is read here, in one place, so `cli.ts`
 * only has to ask for a config and hand it to `startHealthServer`.
 */
import { DEFAULT_MAX_POLL_AGE_MS, type HealthServerOptions } from '@workos/sync-engine'

type Flags = Record<string, string | boolean>

/**
 * `--health-port` or `WORKOS_SYNC_HEALTH_PORT` turns the server on;
 * `WORKOS_SYNC_HEALTH_HOST` and `WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS` tune it.
 * Returns null when it should stay off.
 */
export function healthOptionsFromEnv(flags: Flags, env: NodeJS.ProcessEnv = process.env): HealthServerOptions | null {
  const raw = typeof flags['health-port'] === 'string' ? flags['health-port'] : env['WORKOS_SYNC_HEALTH_PORT']
  if (raw === undefined || raw.trim() === '') return null
  const port = Number(raw)
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error(`invalid health port "${raw}": expected an integer between 0 and 65535`)
  }
  const maxRaw = env['WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS']
  const maxPollAgeMs = maxRaw === undefined || maxRaw.trim() === '' ? DEFAULT_MAX_POLL_AGE_MS : Number(maxRaw)
  if (!Number.isFinite(maxPollAgeMs) || maxPollAgeMs <= 0) {
    throw new Error(
      `invalid WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS "${maxRaw}": expected a positive number of milliseconds`,
    )
  }
  return {
    port,
    host: env['WORKOS_SYNC_HEALTH_HOST']?.trim() || '0.0.0.0',
    maxPollAgeMs,
  }
}
