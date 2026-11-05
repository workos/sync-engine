/**
 * OpenTelemetry for the command line.
 *
 * The engine depends on the OpenTelemetry API only, and the API does nothing
 * until a process installs an SDK. The CLI is a process of its own, so it is
 * where one can live: `WORKOS_SYNC_OTEL=1` starts the Node SDK, configured by
 * the standard `OTEL_*` variables (exporter, endpoint, headers, service
 * name), before the engine is built, and shuts it down once the command
 * returns. The shutdown is the part that matters for `tick` and the sliced
 * commands: they exit seconds after they start, long before the SDK's
 * periodic export would have fired, and shutting down flushes.
 *
 * The SDK is an optional dependency of this package, so a host that embeds
 * the library and runs its own SDK can leave it out; the container image has
 * it. A missing module is reported as such rather than as a stack trace.
 */
export interface OtelHandle {
  /** Flush and stop the SDK. Call it once the command has returned. */
  shutdown(): Promise<void>
}

/** The part of `@opentelemetry/sdk-node` this module uses, so nothing here types against the package. */
export interface SdkModule {
  NodeSDK: new (configuration?: { serviceName?: string }) => { start(): void; shutdown(): Promise<void> }
}

/** The `service.name` reported, unless `OTEL_SERVICE_NAME` says otherwise. */
export const DEFAULT_SERVICE_NAME = 'workos-sync-mysql'

const ON = new Set(['1', 'true', 'yes', 'on'])
const OFF = new Set(['0', 'false', 'no', 'off'])

/** `WORKOS_SYNC_OTEL` is on, off, or unset, which is off. Anything else is refused up front. */
export function otelEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  const raw = env['WORKOS_SYNC_OTEL']
  if (raw === undefined || raw.trim() === '') return false
  const v = raw.trim().toLowerCase()
  if (ON.has(v)) return true
  if (OFF.has(v)) return false
  throw new Error(`invalid WORKOS_SYNC_OTEL "${raw}": expected 1 or 0`)
}

/**
 * Start the SDK when `WORKOS_SYNC_OTEL` asks for it; null when it does not.
 * The SDK reads its own configuration from the process environment; `env`
 * only decides whether to start it and what to call the service. `load` is
 * a test seam standing in for the import.
 */
export async function startOtel(
  env: NodeJS.ProcessEnv = process.env,
  load: () => Promise<SdkModule> = () => import('@opentelemetry/sdk-node'),
): Promise<OtelHandle | null> {
  if (!otelEnabled(env)) return null
  let sdkNode: SdkModule
  try {
    sdkNode = await load()
  } catch (err) {
    if (isModuleNotFound(err)) {
      throw new Error(
        'WORKOS_SYNC_OTEL is set but @opentelemetry/sdk-node is not installed: ' +
          'add it to the host (npm install @opentelemetry/sdk-node), or unset WORKOS_SYNC_OTEL',
      )
    }
    throw err
  }
  const named = Boolean(env['OTEL_SERVICE_NAME']?.trim())
  const sdk = new sdkNode.NodeSDK(named ? {} : { serviceName: DEFAULT_SERVICE_NAME })
  sdk.start()
  return { shutdown: () => sdk.shutdown() }
}

function isModuleNotFound(err: unknown): boolean {
  const code = (err as { code?: unknown } | null)?.code
  return code === 'ERR_MODULE_NOT_FOUND' || code === 'MODULE_NOT_FOUND'
}
