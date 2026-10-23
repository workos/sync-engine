/**
 * Credential redaction.
 *
 * Some REST objects carry a live credential alongside their state: an
 * invitation's acceptance `token` and the URL that embeds it, a webhook
 * endpoint's signing `secret`. Mirrored verbatim, a read-only grant on the
 * mirror becomes the ability to join an organization as the invitee, or to
 * forge a signed webhook delivery. Event payloads omit these fields; the REST
 * objects written by backfill, refresh, repair and hydration do not.
 *
 * A stream declares the paths (`StreamDef.redact`) and every row is stripped
 * of them before it crosses the `Destination` boundary. The engine wraps
 * whichever destination it is given, so a destination never has to know.
 *
 * Not to be confused with `Destination.redact`, which erases one subject.
 */
import type { Destination, FreshnessRow, Lease, LeaseResult, Row, WriteMode } from './destination.js'
import type { Logger, StreamDef, WriteStats } from './types.js'

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === 'object' && v !== null && !Array.isArray(v)

/**
 * `obj` without the value at `parts`, copying only the objects along the path.
 * Returns `obj` itself when the path is absent, so a payload that never had
 * the field (an event payload) is passed through untouched.
 */
function without(obj: Record<string, unknown>, parts: readonly string[]): Record<string, unknown> {
  const [head, ...rest] = parts
  if (head === undefined || !Object.hasOwn(obj, head)) return obj
  if (rest.length === 0) {
    const copy = { ...obj }
    delete copy[head]
    return copy
  }
  const child = obj[head]
  if (!isRecord(child)) return obj
  const next = without(child, rest)
  return next === child ? obj : { ...obj, [head]: next }
}

/**
 * The payload with the stream's `redact` paths removed. The input is never
 * mutated, and is returned as-is when there is nothing to strip.
 */
export function redactPayload(
  stream: StreamDef,
  data: Record<string, unknown>,
): Record<string, unknown> {
  let out = data
  for (const path of stream.redact ?? []) {
    out = without(out, typeof path === 'string' ? [path] : path)
  }
  return out
}

export function redactRows(stream: StreamDef, rows: Row[]): Row[] {
  if (!stream.redact?.length) return rows
  return rows.map((row) => {
    const data = redactPayload(stream, row.data)
    return data === row.data ? row : { ...row, data }
  })
}

/**
 * A destination that strips each stream's declared credential paths from every
 * row before handing it to the wrapped one. Reads pass straight through.
 */
export class RedactingDestination implements Destination {
  constructor(private readonly inner: Destination) {}

  init(streams: StreamDef[], logger?: Logger): Promise<void> {
    return this.inner.init(streams, logger)
  }

  close(): Promise<void> {
    return this.inner.close()
  }

  upsert(stream: StreamDef, env: string, rows: Row[], mode: WriteMode): Promise<WriteStats> {
    return this.inner.upsert(stream, env, redactRows(stream, rows), mode)
  }

  delete(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return this.inner.delete(stream, env, redactRows(stream, rows))
  }

  relink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return this.inner.relink(stream, env, redactRows(stream, rows))
  }

  liveIds(stream: StreamDef, env: string): Promise<string[]> {
    return this.inner.liveIds(stream, env)
  }

  rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]> {
    return this.inner.rawRows(stream, env)
  }

  freshness(stream: StreamDef, env: string, lagWindowHours: number): Promise<FreshnessRow> {
    return this.inner.freshness(stream, env, lagWindowHours)
  }

  redact(streams: StreamDef[], env: string, userId: string): Promise<{ table: string; rows: number }[]> {
    return this.inner.redact(streams, env, userId)
  }

  getState<T>(env: string, stream: string): Promise<T | null> {
    return this.inner.getState<T>(env, stream)
  }

  setState(env: string, stream: string, state: unknown): Promise<void> {
    return this.inner.setState(env, stream, state)
  }

  acquireLease(env: string, holder: string, ttlMs: number): Promise<LeaseResult> {
    return this.inner.acquireLease(env, holder, ttlMs)
  }

  releaseLease(env: string, holder: string): Promise<void> {
    return this.inner.releaseLease(env, holder)
  }

  getLease(env: string): Promise<Lease | null> {
    return this.inner.getLease(env)
  }
}

/** `destination` behind credential redaction. Wrapping twice is a no-op. */
export function redacting(destination: Destination): Destination {
  return destination instanceof RedactingDestination
    ? destination
    : new RedactingDestination(destination)
}
