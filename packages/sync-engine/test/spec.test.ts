/**
 * The catalog's REST paths are hand-written from the WorkOS API reference, so
 * nothing in the engine would notice a renamed endpoint until a backfill 404s.
 * This checks every `list` and `get` path against `@workos/openapi-spec`,
 * which is generated from the API source and is what the SDKs are built from.
 *
 * Event names are not checked here: `STREAMS` is typed against the spec's
 * `EventSchema` union in `src/catalog.ts`, so a retired event fails
 * `pnpm typecheck` instead.
 *
 * Path parameter names differ between the catalog and the spec
 * (`{organization_id}` vs `{organizationId}`). They never reach the wire, so
 * both sides are normalized to `{}` before comparing.
 */
import { describe, it, expect } from 'vitest'
import { serviceEndpoints } from '@workos/openapi-spec/services'
import { STREAMS } from '../src/catalog.js'

const normalize = (path: string) => path.replace(/\{[^}]+\}/g, '{}')

const specGetPaths = new Set(
  Object.values(serviceEndpoints)
    .flat()
    .filter((e) => e.method === 'GET')
    .map((e) => normalize(e.path)),
)

describe('catalog REST paths against the published OpenAPI spec', () => {
  it('reads GET operations out of the spec package', () => {
    expect(specGetPaths.has('/organizations')).toBe(true)
  })

  it('every list path is a GET operation in the spec', () => {
    const missing: string[] = []
    for (const s of STREAMS) {
      if (s.list && !specGetPaths.has(normalize(s.list.path))) {
        missing.push(`${s.name}: ${s.list.path}`)
      }
    }
    expect(missing).toEqual([])
  })

  it('every get-by-id path is a GET operation in the spec', () => {
    const missing: string[] = []
    for (const s of STREAMS) {
      if (!s.get) continue
      const path = s.get('{id}')
      if (!specGetPaths.has(normalize(path))) missing.push(`${s.name}: ${path}`)
    }
    expect(missing).toEqual([])
  })
})
