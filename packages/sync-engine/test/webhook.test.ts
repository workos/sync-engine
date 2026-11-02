import { describe, it, expect } from 'vitest'
import { createHmac } from 'node:crypto'
import { verifyWebhook, parseSignatureHeader, WebhookVerificationError } from '../src/webhook.js'

const SECRET = 'whsec_test_secret'

function sign(body: string, timestamp: number, secret = SECRET): string {
  const hash = createHmac('sha256', secret).update(`${timestamp}.${body}`).digest('hex')
  return `t=${timestamp},v1=${hash}`
}

describe('webhook verification', () => {
  const body = JSON.stringify({ id: 'event_1', event: 'user.created' })

  it('accepts a correctly signed delivery', () => {
    const now = Date.now()
    expect(verifyWebhook({ payload: body, signature: sign(body, now), secret: SECRET, now: () => now })).toBe(true)
  })

  it('rejects a tampered body', () => {
    const now = Date.now()
    const sig = sign(body, now)
    expect(() =>
      verifyWebhook({
        payload: body.replace('user.created', 'user.deleted'),
        signature: sig,
        secret: SECRET,
        now: () => now,
      }),
    ).toThrow(WebhookVerificationError)
  })

  it('rejects the wrong secret', () => {
    const now = Date.now()
    expect(() =>
      verifyWebhook({ payload: body, signature: sign(body, now, 'whsec_other'), secret: SECRET, now: () => now }),
    ).toThrow(WebhookVerificationError)
  })

  it('rejects a replayed delivery outside the tolerance', () => {
    const now = Date.now()
    const old = now - 10 * 60_000
    expect(() => verifyWebhook({ payload: body, signature: sign(body, old), secret: SECRET, now: () => now })).toThrow(
      /tolerance/,
    )
  })

  it('accepts a delivery inside the tolerance', () => {
    const now = Date.now()
    const recent = now - 60_000
    expect(verifyWebhook({ payload: body, signature: sign(body, recent), secret: SECRET, now: () => now })).toBe(true)
  })

  it('verifies raw bytes, so a re-serialized body fails', () => {
    // Re-serializing JSON changes whitespace and key order; signature
    // verification must run on the exact bytes received.
    const now = Date.now()
    const sig = sign(body, now)
    const reserialized = JSON.stringify(JSON.parse(body), null, 2)
    expect(() => verifyWebhook({ payload: reserialized, signature: sig, secret: SECRET, now: () => now })).toThrow()
  })

  it('accepts a Buffer payload', () => {
    const now = Date.now()
    expect(
      verifyWebhook({ payload: Buffer.from(body, 'utf8'), signature: sign(body, now), secret: SECRET, now: () => now }),
    ).toBe(true)
  })

  it('rejects a malformed header rather than treating it as unsigned', () => {
    for (const bad of ['', 'garbage', 't=abc,v1=xx', 'v1=onlyhash', 't=123']) {
      expect(() => parseSignatureHeader(bad)).toThrow(WebhookVerificationError)
    }
  })

  it('rejects a signature of the wrong length without a timing leak', () => {
    const now = Date.now()
    expect(() =>
      verifyWebhook({ payload: body, signature: `t=${now},v1=short`, secret: SECRET, now: () => now }),
    ).toThrow(/signature mismatch/)
  })
})
