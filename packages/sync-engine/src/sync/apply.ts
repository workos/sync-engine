/**
 * Applying a batch of events to the mirror.
 *
 * Events are grouped by target stream and written in batches, but the ordering
 * within a stream is preserved so the staleness gate sees them in the order
 * they were produced. Every event is archived first, so even an event with no
 * mirror table — or one this version of the engine has never heard of — remains
 * queryable in `workos.events`.
 */
import type { Destination } from '../destination.js'
import { streamByName } from '../catalog.js'
import { routeEvent, sourceUpdatedAt, objectIdOf } from '../router.js'
import type { WorkOSClient } from '../workos/client.js'
import type { HydrateMode, Logger, WorkOSEvent, WriteStats } from '../types.js'
import { addStats, emptyStats } from '../types.js'

export interface ApplyOptions {
  env: string
  logger?: Logger
  hydrate?: HydrateMode | Record<string, HydrateMode>
  api?: WorkOSClient
}

export interface ApplyResult {
  stats: WriteStats
  byStream: Record<string, WriteStats>
  archived: number
  unknownEvents: string[]
  hydrated: number
}

function hydrateModeFor(opts: ApplyOptions, stream: string): HydrateMode {
  const h = opts.hydrate
  if (!h) return 'off'
  if (typeof h === 'string') return h
  return h[stream] ?? 'off'
}

export async function applyEvents(
  destination: Destination,
  events: WorkOSEvent[],
  opts: ApplyOptions,
): Promise<ApplyResult> {
  const result: ApplyResult = {
    stats: emptyStats(),
    byStream: {},
    archived: 0,
    unknownEvents: [],
    hydrated: 0,
  }
  if (events.length === 0) return result

  const bump = (stream: string, s: WriteStats) => {
    result.byStream[stream] = addStats(result.byStream[stream] ?? emptyStats(), s)
    result.stats = addStats(result.stats, s)
  }

  // 1. Archive every event verbatim. This is the local audit trail, the replay
  //    source, and the reason an unrecognised event is never a data loss event.
  const eventsStream = streamByName('events')!
  const archived = await destination.upsert(
    eventsStream,
    opts.env,
    events.map((e) => ({ data: e as unknown as Record<string, unknown>, updatedAt: e.created_at })),
    'replace',
  )
  result.archived = archived.created

  // 2. Route each event, preserving order within a stream.
  type Op = { kind: string; data: Record<string, unknown>; updatedAt: string }
  const ops = new Map<string, Op[]>()
  const push = (stream: string, op: Op) => {
    const list = ops.get(stream) ?? []
    list.push(op)
    ops.set(stream, list)
  }

  for (const event of events) {
    const action = routeEvent(event)
    if (action.kind === 'archive_only') {
      if (action.reason === 'unknown' && !result.unknownEvents.includes(event.event)) {
        result.unknownEvents.push(event.event)
      }
      continue
    }
    let data = action.data
    const updatedAt = sourceUpdatedAt(event, data)

    // Optional hydration: re-fetch the object so fields the event payload
    // omits (external_id, metadata, ...) are correct immediately rather than
    // at the next reconcile.
    if (opts.api && (action.kind === 'upsert' || action.kind === 'delete')) {
      const def = streamByName(action.stream)
      const mode = hydrateModeFor(opts, action.stream)
      const isCreate = event.event.endsWith('.created')
      const want = mode === 'always' || (mode === 'on_create' && isCreate)
      const id = objectIdOf(data)
      if (want && def?.get && id && action.kind === 'upsert') {
        const fresh = await opts.api.get<Record<string, unknown>>(def.get(id))
        if (fresh) {
          data = fresh
          result.hydrated++
        } else {
          // A 404 during hydration is itself a delete signal: the object is
          // gone upstream and we may never have received the event.
          push(action.stream, { kind: 'delete', data, updatedAt })
          continue
        }
      }
    }

    push(action.stream, { kind: action.kind, data, updatedAt })
  }

  // 3. Write, one stream at a time. A failure in one stream must not stop the
  //    others or advance their state.
  for (const [streamName, streamOps] of ops) {
    const def = streamByName(streamName)
    if (!def) continue

    // Group consecutive ops of the same kind so ordering is preserved while
    // still batching. An upsert followed by a delete must not be reordered.
    let i = 0
    while (i < streamOps.length) {
      const kind = streamOps[i]!.kind
      const run: Op[] = []
      while (i < streamOps.length && streamOps[i]!.kind === kind) {
        run.push(streamOps[i]!)
        i++
      }
      const rows = run.map((o) => ({ data: o.data, updatedAt: o.updatedAt }))
      switch (kind) {
        case 'upsert':
          // Merge: event payloads are subsets of the REST object.
          bump(streamName, await destination.upsert(def, opts.env, rows, 'merge'))
          break
        case 'append':
          bump(streamName, await destination.upsert(def, opts.env, rows, 'replace'))
          break
        case 'delete':
          bump(streamName, await destination.delete(def, opts.env, rows))
          break
        case 'link':
          bump(streamName, await destination.relink(def, opts.env, rows))
          break
        case 'unlink':
          bump(streamName, await destination.delete(def, opts.env, rows))
          break
      }
    }
  }

  if (result.unknownEvents.length > 0) {
    // Not an error: an unrecognised event is archived and reported, never
    // thrown. Throwing here is what turns a new upstream event type into a
    // retry storm.
    opts.logger?.info('archived events with no mirror table', {
      events: result.unknownEvents,
    })
  }

  return result
}
