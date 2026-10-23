import { describe, it, expect } from 'vitest'
import { healthOptionsFromEnv } from '../src/health.js'

describe('WORKOS_SYNC_HEALTH_*', () => {
  it('stays off unless a port is given', () => {
    expect(healthOptionsFromEnv({}, {})).toBeNull()
    expect(healthOptionsFromEnv({}, { WORKOS_SYNC_HEALTH_PORT: '' })).toBeNull()
  })

  it('reads the port from the environment with the documented defaults', () => {
    expect(healthOptionsFromEnv({}, { WORKOS_SYNC_HEALTH_PORT: '8080' })).toEqual({
      port: 8080,
      host: '0.0.0.0',
      maxPollAgeMs: 60_000,
    })
  })

  it('lets the flag win over the environment, and honours host and window overrides', () => {
    expect(
      healthOptionsFromEnv(
        { 'health-port': '9000' },
        {
          WORKOS_SYNC_HEALTH_PORT: '8080',
          WORKOS_SYNC_HEALTH_HOST: '127.0.0.1',
          WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS: '300000',
        },
      ),
    ).toEqual({ port: 9000, host: '127.0.0.1', maxPollAgeMs: 300_000 })
  })

  it('rejects values that would otherwise fail later or silently', () => {
    expect(() => healthOptionsFromEnv({}, { WORKOS_SYNC_HEALTH_PORT: 'eighty' })).toThrow(/invalid health port/)
    expect(() => healthOptionsFromEnv({}, { WORKOS_SYNC_HEALTH_PORT: '70000' })).toThrow(/invalid health port/)
    expect(() =>
      healthOptionsFromEnv({}, { WORKOS_SYNC_HEALTH_PORT: '8080', WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS: '0' }),
    ).toThrow(/WORKOS_SYNC_HEALTH_MAX_POLL_AGE_MS/)
  })
})
