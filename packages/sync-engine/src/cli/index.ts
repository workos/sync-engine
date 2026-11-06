export {
  parseArgs,
  streamsFromFlags,
  environmentsFromEnv,
  parseHydrate,
  repairRequested,
  parseSslCa,
  parseLeaseTtl,
  parseMaxPages,
  parseExcludeStreams,
  engineConfigFromEnv,
  invokedDirectly,
} from './flags.js'
export type { CliFlags, ParsedArgs } from './flags.js'
export { healthOptionsFromEnv } from './health-options.js'
export { otelEnabled, startOtel } from './otel.js'
export type { OtelHandle, SdkModule } from './otel.js'
export { renderHelp } from './help.js'
export type { HelpRow, HelpSpec } from './help.js'
export { runCli, exitWith } from './run.js'
export type { CliSpec, DestinationOptions, InitReport, MainDeps } from './run.js'
