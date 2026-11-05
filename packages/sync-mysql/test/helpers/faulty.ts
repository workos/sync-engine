/**
 * Test doubles for the poison-event quarantine.
 *
 * `FaultyDestination` is the real MySQL destination with one write fault
 * injected: it stands in for a write a payload cannot survive, or a router
 * bug on one shape, without having to construct the offending data for
 * every scenario. The error it throws is configurable so the same double
 * can imitate a deterministic failure, a dropped connection, or a socket
 * error.
 */
import type { Logger, Row, TableDef, WriteMode, WriteStats } from '@workos/sync-engine'
import { MysqlDestination } from '../../src/destination.js'

export class FaultyDestination extends MysqlDestination {
  /** Object id whose mirror write fails. `null` means no fault. */
  poison: string | null = null
  /** Fail every write, as an outage would. */
  failAll = false
  /**
   * Fail only multi-row writes: the batch attempt fails, every event succeeds
   * alone. That is what an interaction between rows, or a transient error the
   * classifier does not know, looks like to the quarantine.
   */
  failBatches = false
  /** What the fault throws; defaults to the shape of a MySQL bad-value error. */
  fault: () => Error = () =>
    Object.assign(new Error("Incorrect datetime value: 'maybe' for column '_updated_at'"), {
      code: 'ER_TRUNCATED_WRONG_VALUE',
      errno: 1292,
      sqlState: '22007',
    })
  /** How many times the fault fired, so a test can prove isolation happened. */
  faults = 0

  override async upsert(stream: TableDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats> {
    const hit =
      this.failAll ||
      (this.failBatches && rows.length > 1) ||
      (this.poison !== null && rows.some((r) => r.data['id'] === this.poison))
    if (hit) {
      this.faults++
      throw this.fault()
    }
    return super.upsert(stream, env, rows, mode)
  }
}

export interface LogLine {
  level: 'debug' | 'info' | 'warn' | 'error'
  msg: string
  fields: Record<string, unknown>
}

/** A logger that keeps every line, so a test can assert on what was said. */
export function captureLogger(): { logger: Logger; lines: LogLine[] } {
  const lines: LogLine[] = []
  const at =
    (level: LogLine['level']) =>
    (msg: string, fields: Record<string, unknown> = {}) => {
      lines.push({ level, msg, fields })
    }
  return {
    lines,
    logger: { debug: at('debug'), info: at('info'), warn: at('warn'), error: at('error') },
  }
}
