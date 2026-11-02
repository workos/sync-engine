/**
 * Optional webhook receiver.
 *
 * Webhooks are a *trigger*, not a source of truth, in this design. A verified
 * delivery wakes the poller so the tail runs immediately instead of at the next
 * interval; the state itself still flows through the ordered Events API. That
 * keeps near-real-time latency without inheriting the failure modes of webhook
 * delivery — no ordering guarantee, at-least-once delivery, retries that stop
 * after three days, and no way to replay what you missed.
 */
import { createHmac, timingSafeEqual } from 'node:crypto'

export class WebhookVerificationError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'WebhookVerificationError'
  }
}

export interface VerifyOptions {
  /** Raw request body. Must be the exact bytes received — not a re-serialized object. */
  payload: string | Buffer
  /** The `WorkOS-Signature` header: `t=<ms>,v1=<hex>`. */
  signature: string
  secret: string
  /** Maximum age of a delivery, in ms. Matches the WorkOS SDK default. */
  toleranceMs?: number
  now?: () => number
}

export function parseSignatureHeader(header: string): { timestamp: number; hash: string } {
  const parts = header.split(',').reduce<Record<string, string>>((acc, part) => {
    const idx = part.indexOf('=')
    if (idx > 0) acc[part.slice(0, idx).trim()] = part.slice(idx + 1).trim()
    return acc
  }, {})
  const t = Number(parts['t'])
  const v1 = parts['v1']
  if (!Number.isFinite(t) || !v1) {
    throw new WebhookVerificationError('malformed WorkOS-Signature header')
  }
  return { timestamp: t, hash: v1 }
}

/**
 * Verify a webhook signature.
 *
 * Verification happens on the raw bytes and before any JSON parsing, so a
 * malformed body from an unauthenticated caller never reaches the parser.
 */
export function verifyWebhook(opts: VerifyOptions): boolean {
  const { timestamp, hash } = parseSignatureHeader(opts.signature)
  const now = opts.now ? opts.now() : Date.now()
  const tolerance = opts.toleranceMs ?? 180_000

  if (Math.abs(now - timestamp) > tolerance) {
    throw new WebhookVerificationError(`timestamp outside tolerance (${Math.abs(now - timestamp)}ms > ${tolerance}ms)`)
  }

  const body = typeof opts.payload === 'string' ? opts.payload : opts.payload.toString('utf8')
  const expected = createHmac('sha256', opts.secret).update(`${timestamp}.${body}`).digest('hex')

  const a = Buffer.from(expected, 'utf8')
  const b = Buffer.from(hash, 'utf8')
  if (a.length !== b.length) {
    throw new WebhookVerificationError('signature mismatch')
  }
  if (!timingSafeEqual(a, b)) {
    throw new WebhookVerificationError('signature mismatch')
  }
  return true
}
