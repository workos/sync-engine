/**
 * Credential redaction.
 *
 * Some WorkOS objects carry a live credential alongside their state: an
 * invitation's acceptance `token` and the URL that embeds it, a webhook
 * endpoint's signing `secret`, an organization domain's DNS challenge
 * (`verification_token`, with its record name `verification_prefix`), which
 * also rides along inside the organization's `domains` array. Mirrored
 * verbatim, a read-only grant on the mirror becomes the ability to join an
 * organization as the invitee, to forge a signed webhook delivery, or to read
 * the challenge a pending domain is waiting on.
 *
 * A stream declares the paths (`StreamDef.redact`) and every row is stripped
 * of them before it crosses the `Destination` boundary. The engine wraps
 * whichever destination it is given, so a destination never has to know.
 *
 * Events are not exempt. WorkOS omits some credentials from event payloads —
 * an invitation's token, a password reset's token, a magic auth code — but
 * not others: every `organization_domain.*` event carries the verification
 * token, and `organization.*` events carry it inside `domains`. So the
 * `events` archive and the quarantine store are stripped too, each event by
 * the paths of the stream that subscribes to its type (`redactEvent`).
 *
 * Not to be confused with `Destination.redact`, which erases one subject.
 */
import { STREAMS } from './catalog.js'
import type {
  Destination,
  FreshnessRow,
  IdDiff,
  Lease,
  LeaseResult,
  RejectedEvent,
  Row,
  WriteMode,
} from './destination.js'
import type { Logger, StreamDef, WorkOSEvent, WriteStats } from './types.js'

const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)

/**
 * `obj` without the value at `parts`, copying only the objects along the path.
 * Returns `obj` itself when the path is absent, so a payload that never had
 * the field (an event payload) is passed through untouched. An array met
 * along the path is traversed: the rest of the path applies to each element.
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
  const next = withoutIn(child, rest)
  return next === child ? obj : { ...obj, [head]: next }
}

/** `value` without `parts`: an object is descended, an array is mapped, anything else is left as it is. */
function withoutIn(value: unknown, parts: readonly string[]): unknown {
  if (isRecord(value)) return without(value, parts)
  if (!Array.isArray(value)) return value
  let copy: unknown[] | undefined
  value.forEach((element, i) => {
    const next = withoutIn(element, parts)
    if (next === element) return
    copy ??= [...value]
    copy[i] = next
  })
  return copy ?? value
}

/**
 * The payload with the stream's `redact` paths removed. The input is never
 * mutated, and is returned as-is when there is nothing to strip.
 */
export function redactPayload(stream: StreamDef, data: Record<string, unknown>): Record<string, unknown> {
  let out = data
  for (const path of stream.redact ?? []) {
    out = without(out, typeof path === 'string' ? [path] : path)
  }
  return out
}

const subscribes = (stream: StreamDef, type: string): boolean =>
  (stream.events?.upsert?.includes(type) ?? false) || (stream.events?.delete?.includes(type) ?? false)

/**
 * The stream's paths stripped from an event payload, at the place the stream
 * reads the object from. Most events are the object; a few wrap it (see
 * `StreamDef.unwrap`), and then the wrapped object is stripped and its
 * siblings kept, so the archived event still says why it happened.
 */
function redactWherever(stream: StreamDef, data: Record<string, unknown>): Record<string, unknown> {
  const inner = stream.unwrap ? stream.unwrap(data) : data
  if (inner === data) return redactPayload(stream, data)
  for (const [key, value] of Object.entries(data)) {
    if (value !== inner) continue
    const stripped = redactPayload(stream, inner)
    return stripped === inner ? data : { ...data, [key]: stripped }
  }
  // `unwrap` built an object instead of pointing into the payload, so there is
  // nowhere to write back. The top level is all that can be stripped.
  return redactPayload(stream, data)
}

/**
 * `event` with the credential paths of every stream that subscribes to its
 * type stripped from the payload. An event for a stream with nothing to
 * redact, or one no stream subscribes to, comes back as the same object.
 * Applied to the `events` archive and the quarantine store, which hold whole
 * events; `streams` is the whole catalog by default, because an event is
 * archived whether or not its stream is selected.
 */
export function redactEvent(event: WorkOSEvent, streams: readonly StreamDef[] = STREAMS): WorkOSEvent {
  if (!isRecord(event.data)) return event
  let data = event.data
  for (const stream of streams) {
    if (stream.redact?.length && subscribes(stream, event.event)) data = redactWherever(stream, data)
  }
  return data === event.data ? event : { ...event, data }
}

/** The archive stream. Its rows are whole events, stripped per event type rather than per stream. */
const ARCHIVE = 'events'

export function redactRows(stream: StreamDef, rows: Row[]): Row[] {
  if (stream.name === ARCHIVE) return redactArchiveRows(rows)
  if (!stream.redact?.length) return rows
  return rows.map((row) => {
    const data = redactPayload(stream, row.data)
    return data === row.data ? row : { ...row, data }
  })
}

/** Archive rows with each event stripped; the same array when no event needed it, which is most batches. */
function redactArchiveRows(rows: Row[]): Row[] {
  let copy: Row[] | undefined
  rows.forEach((row, i) => {
    const before = row.data as unknown as WorkOSEvent
    const after = redactEvent(before)
    if (after === before) return
    copy ??= [...rows]
    copy[i] = { ...row, data: after as unknown as Record<string, unknown> }
  })
  return copy ?? rows
}

/**
 * A destination that strips each stream's declared credential paths from every
 * row before handing it to the wrapped one. Reads pass straight through.
 *
 * Its methods take `StreamDef`, not the `TableDef` the `Destination` contract
 * is typed on, because the paths to strip are declared on the stream and the
 * engine — the only caller — always has one. A bare `TableDef` would simply
 * have nothing to strip. Method parameters are bivariant, so this still
 * satisfies `Destination`.
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

  unlink(stream: StreamDef, env: string, rows: Row[]): Promise<WriteStats> {
    return this.inner.unlink(stream, env, redactRows(stream, rows))
  }

  liveIds(stream: StreamDef, env: string): Promise<string[]> {
    return this.inner.liveIds(stream, env)
  }

  rawRows(stream: StreamDef, env: string): Promise<Record<string, unknown>[]> {
    return this.inner.rawRows(stream, env)
  }

  /**
   * Remote pages are stripped before they are staged, so a credential never
   * reaches the database even transiently, and the `remoteOnly` payloads that
   * come back are already clean when repair upserts them.
   */
  diffIds(stream: StreamDef, env: string, remote: AsyncIterable<Record<string, unknown>[]>): Promise<IdDiff> {
    if (!stream.redact?.length) return this.inner.diffIds(stream, env, remote)
    async function* stripped() {
      for await (const page of remote) yield page.map((data) => redactPayload(stream, data))
    }
    return this.inner.diffIds(stream, env, stripped())
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

  setStateIfHeld(env: string, stream: string, state: unknown, holder: string, expected: unknown): Promise<boolean> {
    return this.inner.setStateIfHeld(env, stream, state, holder, expected)
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

  /**
   * The quarantine holds whole events, so they are stripped the way the
   * archive is: by the paths of the stream each event belongs to. What
   * `rejected` reads back and `replayRejected` re-applies is then clean.
   */
  reject(env: string, rows: { event: WorkOSEvent; error: string }[]): Promise<void> {
    return this.inner.reject(
      env,
      rows.map((row) => {
        const event = redactEvent(row.event)
        return event === row.event ? row : { ...row, event }
      }),
    )
  }

  rejected(env: string, opts?: { ids?: string[] }): Promise<RejectedEvent[]> {
    return this.inner.rejected(env, opts)
  }

  rejectedCount(env: string): Promise<number> {
    return this.inner.rejectedCount(env)
  }

  clearRejected(env: string, ids: string[]): Promise<number> {
    return this.inner.clearRejected(env, ids)
  }

  isTransientError(err: unknown): boolean {
    return this.inner.isTransientError?.(err) ?? false
  }
}

/** `destination` behind credential redaction. Wrapping twice is a no-op. */
export function redacting(destination: Destination): Destination {
  return destination instanceof RedactingDestination ? destination : new RedactingDestination(destination)
}
