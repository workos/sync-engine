/**
 * What every destination does to a batch of rows before it writes them,
 * whatever the database: find each row's key in its payload, collapse or
 * order the rows that share one, cut the batch into pieces, and read the
 * timestamps the staleness gate compares.
 */
import type { Row } from './destination.js'
import type { TableDef } from './types.js'

/** The value at `path` in `data`: a top-level key, or the steps into nested objects. */
export function readPath(data: Record<string, unknown>, path: string | string[]): unknown {
  const parts = Array.isArray(path) ? path : [path]
  let cur: unknown = data
  for (const p of parts) {
    if (cur == null || typeof cur !== 'object') return undefined
    cur = (cur as Record<string, unknown>)[p]
  }
  return cur
}

/**
 * Key values for a row, in `stream.key` order. Undefined if any part is
 * missing, empty, or not a scalar: an object where an id should be is a
 * malformed row, not a key, and must never reach a query as one.
 */
export function keyValues(stream: TableDef, data: Record<string, unknown>): string[] | undefined {
  const out: string[] = []
  for (const k of stream.key) {
    const col = stream.columns.find((c) => c.name === k)
    const v = readPath(data, col ? col.path : k)
    if (v == null || v === '' || typeof v === 'object') return undefined
    out.push(String(v))
  }
  return out
}

/**
 * The instant `iso` names, in microseconds since the epoch, or NaN.
 * `Date.parse` reads three fractional digits; a database may keep six, so
 * the rest of the fraction is read here and two rows a microsecond apart
 * stay ordered the way the staleness gate will order them.
 */
function micros(iso: string): number {
  const frac = /\.(\d+)/.exec(iso)?.[1] ?? ''
  const ms = Date.parse(frac.length > 3 ? iso.replace(`.${frac}`, `.${frac.slice(0, 3)}`) : iso)
  if (Number.isNaN(ms)) return ms
  return ms * 1000 + Number(frac.slice(3, 6).padEnd(3, '0'))
}

/**
 * Whether one timestamp is strictly earlier than another, by the instant
 * each names: `01:00:00+01:00` sorts after `00:30:00Z` as text and is half an
 * hour earlier as a time. A value that is not a date is ordered as text; the
 * database refuses it on the write either way.
 */
function isBefore(a: string, atA: number, b: string, atB: number): boolean {
  if (Number.isNaN(atA) || Number.isNaN(atB)) return a < b
  return atA < atB
}

/**
 * Collapse rows that share a primary key, keeping the newest. It serves the
 * delete, relink and unlink paths, whose rows are whole; upserts use
 * `wavesByKey`.
 */
export function dedupeByKey(stream: TableDef, rows: Row[]): { rows: Row[]; dropped: number } {
  const byKey = new Map<string, { row: Row; at: number }>()
  let dropped = 0
  for (const row of rows) {
    const key = keyValues(stream, row.data)
    if (!key) {
      dropped++
      continue
    }
    const k = key.join('\0')
    const at = micros(row.updatedAt)
    const existing = byKey.get(k)
    if (!existing || !isBefore(row.updatedAt, at, existing.row.updatedAt, existing.at)) byKey.set(k, { row, at })
  }
  return { rows: [...byKey.values()].map((e) => e.row), dropped }
}

/**
 * Split a batch into waves in which no key appears twice, keeping order: the
 * n-th row for a key goes in wave n. Written one after another, the waves
 * are exactly the rows written one at a time — each row meets the staleness
 * gate against what the row before it left stored, and a merge overlays it
 * on that — while a batch with no repeated key, which is nearly every batch,
 * is still a single write.
 *
 * Keeping the newest row, as `dedupeByKey` does, would be wrong for a merge
 * write: a hydrated create (the whole REST object) followed in the same poll
 * by an update event whose payload is narrower would lose every field the
 * event omits until the next refresh, which is what hydration exists to
 * prevent. Folding the payloads here instead would be wrong the other way:
 * a row older than the stored one is refused on its own, but folded under a
 * newer row's timestamp its fields would overwrite the stored values. Only
 * the database knows the stored timestamp, so the database applies the gate.
 */
export function wavesByKey(stream: TableDef, rows: Row[]): { waves: Row[][]; dropped: number } {
  const waves: Row[][] = []
  const seen = new Map<string, number>()
  let dropped = 0
  for (const row of rows) {
    const key = keyValues(stream, row.data)
    if (!key) {
      dropped++
      continue
    }
    const k = key.join('\0')
    const n = seen.get(k) ?? 0
    seen.set(k, n + 1)
    const wave = waves[n] ?? (waves[n] = [])
    wave.push(row)
  }
  return { waves, dropped }
}

/** `items` in pieces of `size`. A size that would never advance the loop is refused rather than looped on. */
export function chunk<T>(items: T[], size: number): T[][] {
  if (!Number.isInteger(size) || size < 1) {
    throw new Error(`batchSize must be a positive integer, got ${String(size)}`)
  }
  const out: T[][] = []
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size))
  return out
}

/**
 * `Row.updatedAt` as a `Date`, or an error carrying a code the destination's
 * error classifier reports as deterministic, so the engine quarantines the
 * event rather than retrying it forever.
 */
export function parseUpdatedAt(value: string): Date {
  const d = new Date(value)
  if (typeof value !== 'string' || Number.isNaN(d.getTime())) {
    throw Object.assign(new Error(`invalid updatedAt timestamp: ${JSON.stringify(value)}`), {
      code: 'INVALID_TIMESTAMP',
    })
  }
  return d
}

/** Structural equality with key order ignored, which is what `is distinct from` means for jsonb. */
export function jsonEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (typeof a !== typeof b || a === null || b === null || typeof a !== 'object') return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a)) {
    const bb = b as unknown[]
    return a.length === bb.length && a.every((v, i) => jsonEqual(v, bb[i]))
  }
  const ao = a as Record<string, unknown>
  const bo = b as Record<string, unknown>
  const keys = Object.keys(ao)
  if (keys.length !== Object.keys(bo).length) return false
  return keys.every((k) => Object.hasOwn(bo, k) && jsonEqual(ao[k], bo[k]))
}

/** `value` as JSON with every object's keys sorted, so equal values serialise equally. */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortKeys(value))
}

function sortKeys(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sortKeys)
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {}
    for (const key of Object.keys(value as Record<string, unknown>).sort()) {
      const v = (value as Record<string, unknown>)[key]
      if (v !== undefined) out[key] = sortKeys(v)
    }
    return out
  }
  return value
}
