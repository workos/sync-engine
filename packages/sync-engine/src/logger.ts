import type { Logger } from './types.js'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error' | 'silent'

const ORDER: Record<Exclude<LogLevel, 'silent'>, number> = { debug: 10, info: 20, warn: 30, error: 40 }

/**
 * Structured JSON logs. Per-batch counts — including `skipped` — are the
 * engine's health signal: a spike in skipped writes means events are arriving
 * out of order or a backfill is colliding with the tail.
 */
export function createLogger(level: LogLevel = 'info', sink: (line: string) => void = console.log): Logger {
  const min = level === 'silent' ? Infinity : ORDER[level]
  const emit = (lvl: Exclude<LogLevel, 'silent'>, msg: string, fields?: Record<string, unknown>) => {
    if (ORDER[lvl] < min) return
    sink(JSON.stringify({ ts: new Date().toISOString(), level: lvl, msg, ...fields }))
  }
  return {
    debug: (m, f) => emit('debug', m, f),
    info: (m, f) => emit('info', m, f),
    warn: (m, f) => emit('warn', m, f),
    error: (m, f) => emit('error', m, f),
  }
}

export const silentLogger: Logger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
}
